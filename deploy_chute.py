import re
import sys
import uuid
import aiohttp
import asyncio
import orjson as json
import traceback
import semver
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from loguru import logger
from typing import Dict, Any, Optional
from sqlalchemy import select, func, case, text, or_, update
from sqlalchemy.orm import selectinload
from prometheus_api_client import PrometheusConnect
from api.config import settings, validator_by_hotkey, Validator
from api.redis_pubsub import RedisListener
from api.auth import sign_request
from api.database import get_session, engine, Base
from api.chute.schemas import Chute
from api.server.schemas import Server
from api.gpu.schemas import GPU
from api.deployment.schemas import Deployment
from api.exceptions import DeploymentFailure
import api.k8s as k8s

# 62cc0462-8983-5ef1-8859-92ccf726e235
# Qwen/Qwen2.5-72B-Instruct
# fac926de-91f9-4687-86a0-1b476e64534e
# chutes-miner-gpu-0

_VLD_KEY = settings.validators[0].hotkey
_CHUTE_ID = "16f41e4f-f2ca-5580-a0a6-46727ae4c212" #"e3653034-9f58-5cf6-84f8-d5555e55fbd6"
_SERVER_ID = "fac926de-91f9-4687-86a0-1b476e64534e"


class Gepetto:

    def __init__(self):
        pass

    # region deploy

    async def deploy_chute(self, chute_id: str, server_id: str, validator: str):

        logger.info(f"Attempting to deploy {chute_id=} on {server_id=}")

        if (chute := await self.load_chute(chute_id, validator)) is None:
            logger.error(f"No chute found: {chute_id}")
            return

        if (server := await self.load_server(server_id, validator)) is None:
            logger.error(f"No server found: {server_id}")
            return

        deployment = None
        try:
            launch_token = await self.get_launch_token(chute)
            logger.debug(f"Launch token: {launch_token}")

            deploy_id = str(uuid.uuid4())
            service = await k8s.create_service_for_deployment(chute, deploy_id)
            deployment, k8s_dep = await k8s.deploy_chute(
                chute.chute_id,
                server.server_id,
                deploy_id,
                service,
                token=launch_token["token"] if launch_token else None,
                config_id=launch_token["config_id"] if launch_token else None,
            )
            logger.success(
                f"Successfully deployed {chute.name=} ({chute.chute_id}) on {server.name=} ({server.server_id}):\n{deployment}"
            )
            if not launch_token:
                await self.announce_deployment(deployment)

        except DeploymentFailure as exc:
            logger.error(
                f"Error attempting to deploy {chute.name=} ({chute.chute_id}) on {server.name=} ({server.server_id}): {exc}\n{traceback.format_exc()}"
            )

            if deployment:
                logger.info(f"Undeploying deployment due to failure {deployment.deployment_id=}")
                await self.undeploy(deployment.deployment_id)
                logger.info("Undeployed deployment")

    # endregion

    # region funcs

    @staticmethod
    async def load_chute(chute_id: str, validator: str):
        """
        Helper to load a chute from the local database.
        """
        async with get_session() as session:
            return (
                await session.execute(
                    select(Chute).where(Chute.chute_id == chute_id).where(Chute.validator == validator)
                )
            ).unique().scalar_one_or_none()

    @staticmethod
    async def load_server(server_id: str, validator: str):
        """
        Helper to load a server from the local database.
        """
        async with get_session() as session:
            return (
                await session.execute(
                    select(Server).where(Server.server_id == server_id).where(Server.validator == validator)
                )
            ).unique().scalar_one_or_none()

    async def get_launch_token(self, chute: Chute, job_id: str = None):
        """
        Fetch a launch config JWT, if the chutes version supports/requires it.
        """
        if not chute.chutes_version:
            return None
        core_version = re.match(r"^([0-9]+\.[0-9]+\.[0-9]+).*", chute.chutes_version).group(1)
        if semver.compare(core_version or "0.0.0", "0.3.0") < 0:
            return None
        if (validator := validator_by_hotkey(chute.validator)) is None:
            raise DeploymentFailure(f"Validator not found: {chute.validator}")
        try:
            async with aiohttp.ClientSession(raise_for_status=False) as session:
                headers, _ = sign_request(purpose="launch")
                params = {"chute_id": chute.chute_id}
                if job_id:
                    params["job_id"] = job_id
                logger.warning(f"SENDING LAUNCH TOKEN REQUEST WITH {headers=}")
                async with session.get(
                    f"{validator.api}/instances/launch_config", headers=headers, params=params
                ) as resp:
                    if resp.status == 423:
                        logger.warning(
                            f"Unable to scale up {chute.chute_id} at this time, "
                            f"at capacity or blocked for another reason: {await resp.text()}"
                        )
                    elif resp.status != 200:
                        logger.error(f"Failed to fetch launch token: {resp.status=} -> {await resp.text()}")
                    resp.raise_for_status()
                    return await resp.json()
        except Exception as exc:
            logger.warning(f"Unable to fetch launch config token: {exc}")
            raise DeploymentFailure(f"Failed to fetch JWT for launch: {exc}")

    async def announce_deployment(self, deployment: Deployment):
        """
        Tell a validator that we're creating a deployment.
        """
        logger.info(f"Announcing deployment {deployment.deployment_id=} to {deployment.validator=}")

        if (vali := validator_by_hotkey(deployment.validator)) is None:
            logger.warning(f"No validator for deployment: {deployment.deployment_id}")
            return
        body = {"node_ids": [gpu.gpu_id for gpu in deployment.gpus], "host": deployment.host, "port": deployment.port}
        try:
            async with aiohttp.ClientSession(raise_for_status=False) as session:
                headers, payload_string = sign_request(payload=body)
                async with session.post(
                    f"{vali.api}/instances/{deployment.chute_id}/", headers=headers, data=payload_string
                ) as resp:
                    if resp.status >= 300:
                        logger.error(f"Error announcing deployment to validator:\n{await resp.text()}")
                    resp.raise_for_status()
                    instance = await resp.json()

                    # Track the instance ID.
                    async with get_session() as session:
                        await session.execute(
                            update(Deployment)
                            .where(Deployment.deployment_id == deployment.deployment_id)
                            .values({"instance_id": instance["instance_id"]})
                        )
                        await session.commit()
                    logger.success(f"Successfully advertised instance: {instance['instance_id']}")
        except Exception as exc:
            logger.warning(f"Error announcing deployment: {exc}\n{traceback.format_exc()}")
            raise DeploymentFailure(f"Failed to announce deployment {deployment.deployment_id}: {exc=}")

    async def undeploy(self, deployment_id: str, instance_id: str = None):
        """
        Delete a deployment.
        """
        logger.info(f"Removing all traces of deployment: {deployment_id}")

        # Clean up the database.
        chute_id = None
        validator_hotkey = None
        async with get_session() as session:
            deployment = (
                (await session.execute(select(Deployment).where(Deployment.deployment_id == deployment_id)))
                .unique()
                .scalar_one_or_none()
            )
            if deployment:
                if not instance_id:
                    instance_id = deployment.instance_id
                chute_id = deployment.chute_id
                validator_hotkey = deployment.validator
                await session.delete(deployment)
                await session.commit()

        # Clean up the validator's instance record.
        if instance_id:
            if (vali := validator_by_hotkey(validator_hotkey)) is not None:
                await self.purge_validator_instance(vali, chute_id, instance_id)

        # Purge in k8s if still there.
        await k8s.undeploy(deployment_id)
        logger.success(f"Removed {deployment_id=}")

    @staticmethod
    async def purge_validator_instance(vali: Validator, chute_id: str, instance_id: str):
        try:
            async with aiohttp.ClientSession() as session:
                headers, _ = sign_request(purpose="instances")
                async with session.delete(f"{vali.api}/instances/{chute_id}/{instance_id}", headers=headers) as resp:
                    logger.debug(await resp.text())
                    if resp.status not in (200, 404):
                        raise Exception(f"status_code={resp.status}, response text: {await resp.text()}")
                    elif resp.status == 200:
                        logger.info(f"Deleted instance from validator {vali.hotkey}")
                    else:
                        logger.info(f"{instance_id=} already purged from {vali.hotkey}")
        except Exception as exc:
            logger.warning(f"Error purging {instance_id=} from {vali.hotkey=}: {exc}")

    # endregion


async def main():

    logger.info("Start to manual deploy...")

    gepetto = Gepetto()
    await gepetto.deploy_chute(_CHUTE_ID, _SERVER_ID, _VLD_KEY)


if __name__ == "__main__":
    asyncio.run(main())
