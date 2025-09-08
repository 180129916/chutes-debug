"""
Gepetto - coordinate all the things.

V250830：
* 手动命令：python gepetto.py [command] --args
  + chutes | scales
  + deploy | undeploy
  + recycle
* 定时执行 activator、reconciler、autoscaler
* 自动部署，限定模型白名单：_CANDIDATE_CHUTES
  仅允许存在于白名单的匹配模型才能部署
* 增加失败回避策略，避免重复部署同一个排名最高但已失败的模型：
  + 记录模型实例创建和删除时间，前后时间间隔太短的，则认为部署失败
  + 在白名单排名列表中，优先选择未被标记为“失败”的最高分模型
  + 若所有候选模型均失败，则选择其中最早被删除的模型
"""

# 候选 Chutes
_CANDIDATE_CHUTES = [
    # A100_sxm
    "00331b13-3614-5b0a-ab48-56116167aeef",  # moonshotai/Kimi-Dev-72B
    "0d7184a2-32a3-53e0-9607-058c37edaab5",  # Qwen/Qwen3-32B
    "16f41e4f-f2ca-5580-a0a6-46727ae4c212",  # unsloth/gemma-3-27b-it
    "2560a461-980d-5521-b66b-788ff11ebe8b",  # Qwen/Qwen3-30B-A3B-Instruct-2507
    "41d0bdad-9127-5d30-8d9f-e743577f4293",  # Qwen/Qwen2.5-VL-32B-Instruct
    "62cc0462-8983-5ef1-8859-92ccf726e235",  # Qwen/Qwen2.5-72B-Instruct
    "986b874c-bf41-549f-b28f-4322f86fa4ba",  # unsloth/Mistral-Small-24B-Instruct-2501
    "e282615a-094f-5c87-8918-a0846722f1db",  # deepseek-ai/DeepSeek-R1-Distill-Llama-70B
    "e3653034-9f58-5cf6-84f8-d5555e55fbd6",  # chutesai/Mistral-Small-3.1-24B-Instruct-2503
    # 5090
    "83594561-0940-5839-ac92-1d94dd280567",  # unsloth/gemma-3-12b-it
    "5a127880-f03c-55a7-8fbe-540f337d2362",  # OpenBuddy/OpenBuddy-Qwen3-Coder-30B-A3B-Base
    "299acab3-b162-556e-8c8f-ef7d21821918",  # deepseek-ai/DeepSeek-R1-Distill-Qwen-7B
]

import argparse
import os
import re
import sys
import threading
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
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
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


def log_filter(rec):
    if rec["name"] == "api.redis_pubsub":
        return rec["level"].no >= 30
    elif rec["name"] == "__main__":
        return rec["level"].no >= 10
    else:
        return rec["level"].no >= 20


logger.remove()
logger.add(sys.stdout, filter=log_filter)


_VLD_API = settings.validators[0].api  # "https://api.chutes.ai"
_VLD_KEY = settings.validators[0].hotkey


_SECS_ACTIVATE = 900
_SECS_AUTOSCALE = 600
_SECS_RECONCILE = 1800


class Gepetto:

    # region inin & run

    def __init__(self):
        """
        Constructor.
        """
        self.pubsub = RedisListener()
        self.remote_chutes = {validator.hotkey: {} for validator in settings.validators}
        self.remote_images = {validator.hotkey: {} for validator in settings.validators}
        self.remote_instances = {validator.hotkey: {} for validator in settings.validators}
        self.remote_nodes = {validator.hotkey: {} for validator in settings.validators}
        self.remote_metrics = {validator.hotkey: {} for validator in settings.validators}
        self._chute_traces = {}
        self._scale_lock = asyncio.Lock()
        self.setup_handlers()

    def setup_handlers(self):
        """
        Configure the various event listeners/handlers.
        """
        # self.pubsub.on_event("bounty_change")(self.bounty_changed)
        self.pubsub.on_event("chute_created")(self.chute_created)
        self.pubsub.on_event("chute_deleted")(self.chute_deleted)
        self.pubsub.on_event("chute_updated")(self.chute_updated)
        self.pubsub.on_event("gpu_deleted")(self.gpu_deleted)
        self.pubsub.on_event("gpu_verified")(self.gpu_verified)
        self.pubsub.on_event("job_created")(self.job_created)
        self.pubsub.on_event("job_deleted")(self.job_deleted)
        self.pubsub.on_event("image_created")(self.image_created)
        self.pubsub.on_event("image_deleted")(self.image_deleted)
        self.pubsub.on_event("image_updated")(self.image_updated)
        self.pubsub.on_event("instance_activated")(self.instance_activated)
        self.pubsub.on_event("instance_created")(self.instance_created)
        self.pubsub.on_event("instance_deleted")(self.instance_deleted)
        self.pubsub.on_event("instance_verified")(self.instance_verified)
        self.pubsub.on_event("rolling_update")(self.rolling_update)
        self.pubsub.on_event("server_deleted")(self.server_deleted)

    async def run(self):
        """
        Main loop.
        """

        logger.info(f"Gepetto main loop running... (PID: {os.getppid()} / {os.getpid()})  Thread: {threading.get_ident()}")

        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        await self.reconcile()

        # asyncio.create_task(self.activator())
        asyncio.create_task(self.autoscaler())
        asyncio.create_task(self.reconciler())

        await self.pubsub.start()

    # endregion

    # region event handlers

    async def bounty_changed(self, event_data):
        """
        Bounty has changed for a chute.
        """
        logger.info(f"bounty_change event: {event_data}")

    async def image_created(self, event_data: Dict[str, Any]):
        """
        An image was created, we could be extra eager and pull the image onto each GPU node so it's hot faster.
        """
        logger.info(f"image_created event: {event_data}")

    async def image_deleted(self, event_data: Dict[str, Any]):
        """
        An image was deleted (should clean up maybe?)
        """
        logger.info(f"image_deleted event: {event_data}")

    async def image_updated(self, event_data: Dict[str, Any]):
        """
        Image was updated, i.e. the chutes version of an image was upgraded.
        """
        logger.info(f"image_updated event: {event_data}")

    async def job_created(self, event_data: Dict[str, Any]):
        """
        Job available for processing.

        MINERS: This is another crtically important method to optimize. You don't want to
                blindly accept jobs and preempt your existing deployments most likely, but
                there are benefits to accepting them (you get a bounty, compute multiplier
                is semi-dynamic and may provide more compute units than a chute, etc).
        """
        logger.info(f"job_created event: {event_data}")

        chute_id = event_data["chute_id"]
        job_id = event_data["job_id"]
        gpu_count = event_data["gpu_count"]
        validator_hotkey = event_data["validator"]
        disk_gb = event_data["disk_gb"]

        if settings.miner_ss58 in event_data.get("excluded", []):
            logger.warning("Miner hotkey excluded from job!")
            return
        if (validator := validator_by_hotkey(validator_hotkey)) is None:
            logger.warning(f"Validator not found: {validator_hotkey}")
            return

        # Do we already have a node that can accept the job, without pre-emption?
        chute = await self.get_chute(chute_id, validator_hotkey)
        if not chute:
            logger.warning(f"Failed to load chute: {chute_id}")
            return
        server = await self.optimal_scale_up_server(chute, disk_gb=disk_gb)
        if server:
            await self.run_job(chute, job_id, server, validator, disk_gb)
            return

        # XXX This is where you as a miner definitely want to customize the strategy!
        supported_gpus = set(chute.supported_gpus)
        if supported_gpus - set(["h200", "b200"]):
            # Generally speaking, non-h200/b200 GPUs typically have lower compute multipliers than
            # the job would provide because they regularly do not have even one request in flight
            # on average, although that is not always the case, so this should be updated to be smarter.
            logger.info(f"Attempting a pre-empting deploy of {job_id=} {chute_id=} with {supported_gpus=} and {gpu_count=}")
            await self.preempting_deploy(chute, job_id=job_id, disk_gb=disk_gb)

    async def job_deleted(self, event_data: Dict[str, Any]):
        """
        Job has been deleted.
        """
        logger.info(f"job_deleted event: {event_data}")

        async with get_session() as session:
            deployment = (
                (await session.execute(select(Deployment).where(Deployment.job_id == event_data["job_id"]))).unique().scalar_one_or_none()
            )
        if deployment:
            logger.info(f"Received job_deleted event, undeploying {deployment.deployment_id=}!")
            await self.undeploy(deployment.deployment_id)

    async def gpu_verified(self, event_data):
        """
        Validator has finished verifying a GPU, so it is ready for use.
        """
        logger.info(f"gpu_verified event: {event_data}")

        async with get_session() as session:
            await session.execute(update(GPU).where(GPU.server_id == event_data.get("gpu_id")).values({"verified": True}))
            await session.commit()

    async def gpu_deleted(self, event_data):
        """
        GPU no longer exists in validator inventory for some reason.

        MINERS: This shouldn't really happen, unless the validator purges it's database
                or some such other rare event.  You may want to configure alerts or something
                in this code block just in case.
        """
        logger.info(f"gpu_deleted event: {event_data}")
        gpu_id = event_data["gpu_id"]
        async with get_session() as session:
            gpu = (await session.execute(select(GPU).where(GPU.gpu_id == gpu_id))).unique().scalar_one_or_none()
            if gpu:
                if gpu.deployment:
                    await self.undeploy(gpu.deployment_id)
                validator_hotkey = gpu.validator
                if (validator := validator_by_hotkey(validator_hotkey)) is not None:
                    await self.remove_gpu_from_validator(validator, gpu_id)
                await session.delete(gpu)
                await session.commit()

    async def server_deleted(self, event_data: Dict[str, Any]):
        """
        An entire kubernetes node was removed from your inventory.

        MINERS: This will happen when you remove a node intentionally, but otherwise
                should not really happen.  Also want to monitor this situation I think.
        """
        logger.info(f"server_deleted event: {event_data}")
        server_id = event_data["server_id"]
        async with get_session() as session:
            server = (await session.execute(select(Server).where(Server.server_id == server_id))).unique().scalar_one_or_none()
            if server:
                await asyncio.gather(*[self.gpu_deleted({"gpu_id": gpu.gpu_id}) for gpu in server.gpus])
                await session.refresh(server)
                await session.delete(server)
                await session.commit()

    async def rolling_update(self, event_data: Dict[str, Any]):
        """
        A rolling update event, meaning we need to re-create a single instance.
        """
        chute_id = event_data["chute_id"]
        version = event_data["new_version"]
        validator_hotkey = event_data["validator"]
        instance_id = event_data["instance_id"]
        image = event_data.get("image", None)
        reason = event_data.get("reason", "chute updated")
        logger.info(f"rolling_update event: {event_data}")

    # endregion

    # region chute events

    async def chute_created(self, event_data: Dict[str, Any], desired_count: int = 1):
        """
        A brand new chute was added to validator inventory.

        MINERS: This is a critical optimization path. A chute being created
                does not necessarily mean inference will be requested. The
                base mining code here *will* deploy the chute however, given
                sufficient resources are available.
        """
        logger.info(f"chute_created event: {event_data}")

        chute_id = event_data["chute_id"]
        version = event_data["version"]
        validator_hotkey = event_data["validator"]

        if (validator := validator_by_hotkey(validator_hotkey)) is None:
            logger.warning(f"Validator not found: {validator_hotkey}")
            return

        # Already in inventory?
        if (chute := await self.load_chute(chute_id, validator_hotkey)) is not None:
            logger.info(f"Chute {chute_id=} is already tracked in inventory?")
            return

        # Load the chute details, preferably from the local cache.
        chute_dict = None
        try:
            async with aiohttp.ClientSession(raise_for_status=True) as session:
                headers, _ = sign_request(purpose="miner")
                async with session.get(f"{validator.api}/miner/chutes/{chute_id}/{version}", headers=headers) as resp:
                    chute_dict = await resp.json()
        except Exception:
            logger.error(f"Error loading remote chute data: {chute_id=} {version=}")
            return

        # Track in inventory.
        async with get_session() as session:
            chute = Chute(
                chute_id=chute_id,
                validator=validator.hotkey,
                name=chute_dict["name"],
                image=chute_dict["image"],
                code=chute_dict["code"],
                filename=chute_dict["filename"],
                ref_str=chute_dict["ref_str"],
                version=chute_dict["version"],
                supported_gpus=chute_dict["supported_gpus"],
                gpu_count=chute_dict["node_selector"]["gpu_count"],
                chutes_version=chute_dict["chutes_version"],
                ban_reason=None,
            )
            session.add(chute)
            await session.commit()
            await session.refresh(chute)

        await k8s.create_code_config_map(chute)

        # Don't deploy if this is a job-only chute, i.e. it has no "cords" to serve
        # so there's nothing to deploy.
        if event_data.get("job_only"):
            return

        # This should never be anything other than 0, but just in case...
        # current_count = await self.count_non_job_deployments(chute.chute_id, chute.version, chute.validator)
        # if not current_count:
        #     await self.scale_chute(chute, desired_count=desired_count, preempt=False)

    async def chute_deleted(self, event_data: Dict[str, Any]):
        """
        A chute (or specific version of a chute) was removed from validator inventory.
        """
        logger.info(f"chute_deleted event: {event_data}")

        chute_id = event_data["chute_id"]
        version = event_data["version"]
        validator = event_data["validator"]

        async with get_session() as session:
            chute = (
                await session.execute(
                    select(Chute)
                    .where(Chute.chute_id == chute_id)
                    .where(Chute.version == version)
                    .where(Chute.validator == validator)
                    .options(selectinload(Chute.deployments))
                )
            ).scalar_one_or_none()
            if chute:
                if chute.deployments:
                    await asyncio.gather(*[self.undeploy(deployment.deployment_id) for deployment in chute.deployments])
                await session.delete(chute)
                await session.commit()
        await k8s.delete_code(chute_id, version)

    async def chute_updated(self, event_data: Dict[str, Any]):
        """
        Chute has been updated.
        """
        logger.info(f"chute_updated event: {event_data}")

        chute_id = event_data["chute_id"]
        version = event_data["version"]
        validator_hotkey = event_data["validator"]

        if (validator := validator_by_hotkey(validator_hotkey)) is None:
            logger.warning(f"Validator not found: {validator_hotkey}")
            return

        # Reload the definition directly from the validator.
        chute_dict = None
        try:
            async with aiohttp.ClientSession(raise_for_status=True) as session:
                headers, _ = sign_request(purpose="miner")
                async with session.get(f"{validator.api}/miner/chutes/{chute_id}/{version}", headers=headers) as resp:
                    chute_dict = await resp.json()
        except Exception:
            logger.error(f"Error loading remote chute data: {chute_id=} {version=}")
            return

        # Upsert the chute in the local DB.
        async with get_session() as db:
            chute = (await db.execute(select(Chute).where(Chute.chute_id == chute_id))).unique().scalar_one_or_none()
            if chute:
                for key in ("image", "code", "filename", "ref_str", "version", "supported_gpus", "chutes_version"):
                    setattr(chute, key, chute_dict.get(key))
                chute.gpu_count = chute_dict["node_selector"]["gpu_count"]
                chute.ban_reason = None
            else:
                chute = Chute(
                    chute_id=chute_id,
                    validator=validator.hotkey,
                    name=chute_dict["name"],
                    image=chute_dict["image"],
                    code=chute_dict["code"],
                    filename=chute_dict["filename"],
                    ref_str=chute_dict["ref_str"],
                    version=chute_dict["version"],
                    supported_gpus=chute_dict["supported_gpus"],
                    gpu_count=chute_dict["node_selector"]["gpu_count"],
                    chutes_version=chute_dict["chutes_version"],
                    ban_reason=None,
                )
                db.add(chute)
            await db.commit()
            await db.refresh(chute)
            await k8s.create_code_config_map(chute)

    # endregion

    # region instance events

    async def instance_created(self, event_data):
        """
        Instance has been created - only relevant when using new launch config system.
        """
        if event_data["miner_hotkey"] != settings.miner_ss58:
            return

        logger.info(f"instance_created event: {event_data}")

        config_id = event_data["config_id"]
        instance_id = event_data["instance_id"]

        async with get_session() as session:
            deployment = (await session.execute(select(Deployment).where(Deployment.config_id == config_id))).unique().scalar_one_or_none()
            if not deployment:
                logger.error("Could not find deployment")
                return
            deployment.instance_id = instance_id
            await session.commit()
            # await session.refresh(deployment)

        # await self.activate(deployment)

    async def instance_activated(self, event_data: dict[str, Any]):
        """
        An instance has been marked as active (new chutes lib flow).
        """
        logger.info(f"instance_activated event: {event_data}")

        config_id = event_data["config_id"]
        async with get_session() as session:
            await session.execute(
                text("UPDATE deployments SET active = true, stub = false WHERE config_id = :config_id"),
                {"config_id": config_id},
            )

    async def instance_verified(self, event_data):
        """
        Validator has finished verifying an instance/deployment, so it should start receiving requests.
        """
        logger.info(f"instance_verified event: {event_data}")

        if event_data["miner_hotkey"] != settings.miner_ss58:
            return

        async with get_session() as session:
            await session.execute(
                update(Deployment).where(Deployment.instance_id == event_data.get("instance_id")).values({"verified_at": func.now()})
            )
        # Nothing to do here really, it should just start receiving traffic.

    async def instance_deleted(self, event_data: Dict[str, Any]):
        """
        An instance was removed validator side, likely meaning there were too
        many consecutive failures in inference.
        """
        logger.info(f"instance_deleted event: {event_data}")

        instance_id = event_data["instance_id"]
        async with get_session() as session:
            deployment = (
                (await session.execute(select(Deployment).where(Deployment.instance_id == instance_id))).unique().scalar_one_or_none()
            )
        if deployment:
            await self.undeploy(deployment.deployment_id)

        cid = event_data["chute_id"]
        self._trace_deleted(cid)

        logger.info(f"Finished processing instance_deleted event for {instance_id=}")

    # endregion

    # region reconcile

    async def reconciler(self):
        """
        Reconcile on a regular basis.
        """
        logger.info("Reconciler running...")

        while True:
            await asyncio.sleep(_SECS_RECONCILE)
            try:
                await self.reconcile()
            except Exception as exc:
                logger.error(f"Unexpected error in reconciliation loop: {exc}\n{traceback.format_exc()}")

    async def reconcile(self):
        """
        Put our local system back in harmony with the validators.
        """
        logger.info("Start to reconcile ...")
        try:
            await self.remote_refresh_all()
        except Exception as exc:
            logger.error(f"Failed to refresh remote resources: {exc}")
            return

        # Get the chutes currently undergoing a rolling update.
        updating = {}
        for validator in settings.validators:
            updating[validator.hotkey] = {}
            async with aiohttp.ClientSession(raise_for_status=True) as session:
                async with session.get(f"{validator.api}/chutes/rolling_updates") as resp:
                    for item in await resp.json():
                        updating[validator.hotkey][item["chute_id"]] = item

        # Compare local items to validators' inventory.
        tasks = []
        chutes_to_remove = set()
        all_chutes = set()
        all_deployments = set()
        all_instances = set()
        all_configs = set()

        # Get both new job-based and old deployment-based chutes
        k8s_chutes = await k8s.get_deployed_chutes()
        k8s_legacy_deployments = await k8s.get_deployed_chutes_legacy()

        # Combine both into a single set of deployment IDs
        k8s_chute_ids = {c["deployment_id"] for c in k8s_chutes}
        k8s_legacy_ids = {d["deployment_id"] for d in k8s_legacy_deployments}
        all_k8s_ids = k8s_chute_ids | k8s_legacy_ids

        # Log legacy deployments for visibility
        if k8s_legacy_ids:
            logger.info(f"Found {len(k8s_legacy_ids)} legacy deployment-based chutes: {k8s_legacy_ids}")

        # Get all pods with config_id labels for orphan detection
        k8s_config_ids = set()
        try:
            pods = await k8s.get_pods_by_label("chutes/config-id")
            k8s_config_ids = {pod["metadata"]["labels"]["chutes/config-id"] for pod in pods}
        except Exception as exc:
            logger.error(f"Failed to get pods by config-id label: {exc}")

        # Build map of config_id -> instance from remote inventory.
        remote_by_config_id = {}
        for validator, instances in self.remote_instances.items():
            for instance_id, data in instances.items():
                config_id = data.get("config_id")
                if config_id:
                    remote_by_config_id[config_id] = {**data, "instance_id": instance_id, "validator": validator}

        # Update chutes image field based on remote_images
        chute_map = {}
        async with get_session() as session:
            image_updates = {}
            for validator, images in self.remote_images.items():
                for image_id, image_data in images.items():
                    image_str = f"{image_data['username']}/{image_data['name']}:{image_data['tag']}"
                    if image_data.get("patch_version") and image_data["patch_version"] != "initial":
                        image_str += f"-{image_data['patch_version']}"
                    image_updates[image_id] = {"image": image_str, "chutes_version": image_data["chutes_version"]}
            for validator, chutes in self.remote_chutes.items():
                for chute_id, chute_data in chutes.items():
                    image_id = chute_data.get("image_id")
                    if image_id:
                        if image_id not in chute_map:
                            chute_map[image_id] = []
                        chute_map[image_id].append(chute_id)
            if image_updates and chute_map:
                async for row in (await session.stream(select(Chute))).unique():
                    chute = row[0]
                    for image_id, chute_ids in chute_map.items():
                        if chute.chute_id in chute_ids and image_id in image_updates:
                            update_data = image_updates[image_id]
                            if chute.image != update_data["image"]:
                                logger.info(f"Updating chute {chute.chute_id} image from '{chute.image}' to '{update_data['image']}'")
                                chute.image = update_data["image"]
                            if update_data["chutes_version"] != chute.chutes_version:
                                logger.info(
                                    f"Updating chute {chute.chute_id} chutes_version from '{chute.chutes_version}' to '{update_data['chutes_version']}'"
                                )
                                chute.chutes_version = update_data["chutes_version"]
                            break
                await session.commit()

        async with get_session() as session:
            # Clean up based on deployments/instances.
            async for row in (await session.stream(select(Deployment))).unique():
                deployment = row[0]

                # Make sure the instances created with launch configs have the instance ID tracked.
                if deployment.config_id and not deployment.instance_id:
                    remote_match = remote_by_config_id.get(deployment.config_id)
                    if remote_match and remote_match.get("validator") == deployment.validator:
                        deployment.instance_id = remote_match["instance_id"]
                        logger.info(
                            f"Updated deployment {deployment.deployment_id} with instance_id={deployment.instance_id} "
                            f"based on matching config_id={deployment.config_id}"
                        )

                # Reconcile the verified/active state for instances.
                if deployment.instance_id:
                    remote_instance = (self.remote_instances.get(deployment.validator) or {}).get(deployment.instance_id)
                    if remote_instance:
                        if remote_instance.get("last_verified_at") and not deployment.verified_at:
                            deployment.verified_at = func.now()
                            logger.info(f"Marking deployment {deployment.deployment_id} as verified based on remote status")
                        remote_active = remote_instance.get("active", True)
                        if deployment.active != remote_active:
                            deployment.active = remote_active
                            deployment.stub = False
                            logger.info(f"Updating deployment {deployment.deployment_id} active status to {deployment.active}")

                # Early check for orphaned deployments with config_id
                if deployment.config_id and deployment.config_id not in k8s_config_ids:
                    logger.warning(
                        f"Deployment {deployment.deployment_id} has config_id={deployment.config_id} but no matching pod in k8s, cleaning up"
                    )
                    if deployment.instance_id:
                        if (vali := validator_by_hotkey(deployment.validator)) is not None:
                            await self.purge_validator_instance(vali, deployment.chute_id, deployment.instance_id)
                    await session.delete(deployment)
                    continue

                # Check if instance exists on validator
                if deployment.instance_id and deployment.instance_id not in (self.remote_instances.get(deployment.validator) or {}):
                    logger.warning(
                        f"Deployment: {deployment.deployment_id} (instance_id={deployment.instance_id}) on validator {deployment.validator} not found"
                    )
                    tasks.append(asyncio.create_task(self.instance_deleted({"instance_id": deployment.instance_id})))
                    # Skip the rest of processing for this deployment since instance is gone
                    continue

                remote = (self.remote_chutes.get(deployment.validator) or {}).get(deployment.chute_id)

                # Track deployments by their launch configs.
                if deployment.config_id:
                    all_configs.add(deployment.config_id)

                # Special handling for deployments with job_id
                if hasattr(deployment, "job_id") and deployment.job_id:
                    # Always track the instance_id for job deployments to prevent premature purging
                    if deployment.instance_id:
                        all_instances.add(deployment.instance_id)

                    if remote:
                        logger.info(
                            f"Keeping deployment with job_id={deployment.job_id} for chute {deployment.chute_id} (remote version={remote.get('version')}, local version={deployment.version})"
                        )
                        all_deployments.add(deployment.deployment_id)
                        continue
                    else:
                        # Chute associated with the job doesn't exist anymore.
                        logger.warning(
                            f"Job deployment {deployment.deployment_id} with job_id={deployment.job_id} not found in remote inventory"
                        )
                        identifier = f"{deployment.validator}:{deployment.chute_id}:{deployment.version}"
                        if identifier not in chutes_to_remove:
                            chutes_to_remove.add(identifier)
                            tasks.append(
                                asyncio.create_task(
                                    self.chute_deleted(
                                        {
                                            "chute_id": deployment.chute_id,
                                            "version": deployment.version,
                                            "validator": deployment.validator,
                                        }
                                    )
                                )
                            )
                        continue

                # Normal deployment handling (no job_id)
                if not remote or remote["version"] != deployment.version:
                    update = updating.get(deployment.validator, {}).get(deployment.chute_id)
                    if update:
                        logger.warning(f"Skipping reconciliation for chute with rolling {update=}")
                        all_deployments.add(deployment.deployment_id)
                        if deployment.instance_id:
                            all_instances.add(deployment.instance_id)
                        continue

                    logger.warning(
                        f"Chute: {deployment.chute_id} version={deployment.version} on validator {deployment.validator} not found"
                    )
                    identifier = f"{deployment.validator}:{deployment.chute_id}:{deployment.version}"
                    if identifier not in chutes_to_remove:
                        chutes_to_remove.add(identifier)
                        tasks.append(
                            asyncio.create_task(
                                self.chute_deleted(
                                    {
                                        "chute_id": deployment.chute_id,
                                        "version": deployment.version,
                                        "validator": deployment.validator,
                                    }
                                )
                            )
                        )
                    # Don't continue here - we still need to check k8s state and cleanup

                # Check if deployment exists in k8s (either as job or legacy deployment)
                deployment_in_k8s = deployment.deployment_id in all_k8s_ids

                # Special handling for legacy deployments
                if deployment.deployment_id in k8s_legacy_ids:
                    logger.info(f"Found legacy deployment {deployment.deployment_id}, preserving it")
                    all_deployments.add(deployment.deployment_id)
                    if deployment.instance_id:
                        all_instances.add(deployment.instance_id)
                    continue

                # Delete deployments that never made it past stub stage or disappeared from k8s
                if not deployment.stub and not deployment_in_k8s:
                    logger.warning(f"Deployment has disappeared from kubernetes: {deployment.deployment_id}")
                    if deployment.instance_id:
                        if (vali := validator_by_hotkey(deployment.validator)) is not None:
                            await self.purge_validator_instance(vali, deployment.chute_id, deployment.instance_id)
                    await session.delete(deployment)
                    continue

                # Clean up old stubs
                deployment_age = datetime.now(timezone.utc) - deployment.created_at
                if (deployment.stub or not deployment.instance_id) and deployment_age >= timedelta(minutes=60):
                    logger.warning(f"Deployment is still a stub after 60 minutes, deleting! {deployment.deployment_id}")
                    await session.delete(deployment)
                    continue

                # Check for terminated jobs or jobs that never started
                if not deployment.active or deployment.verified_at is None and deployment_age >= timedelta(minutes=5):
                    try:
                        kd = await k8s.get_deployment(deployment.deployment_id)
                    except Exception as exc:
                        if "Not Found" in str(exc) or "(404)" in str(exc):
                            await self.undeploy(deployment.deployment_id)
                        continue

                    destroyed = False
                    job_status = kd.get("status", {})

                    # Check job completion status
                    if job_status.get("succeeded", 0) > 0:
                        logger.info(f"Job completed successfully: {deployment.deployment_id}")
                        await self.undeploy(deployment.deployment_id)
                        destroyed = True
                    elif job_status.get("failed", 0) > 0:
                        logger.warning(f"Job failed: {deployment.deployment_id}")
                        await self.undeploy(deployment.deployment_id)
                        destroyed = True

                    # Check for terminated pods (for Jobs that don't update status properly)
                    if not destroyed:
                        for pod in kd.get("pods", []):
                            pod_state = pod.get("state") or {}
                            if pod_state.get("terminated"):
                                terminated = pod_state["terminated"]
                                exit_code = terminated.get("exit_code", 0)
                                if exit_code == 0:
                                    logger.info(f"Job pod completed successfully: {deployment.deployment_id}")
                                else:
                                    logger.warning(f"Job pod terminated with error: {deployment.deployment_id}, exit_code={exit_code}")
                                await self.undeploy(deployment.deployment_id)
                                destroyed = True
                                break

                    if destroyed:
                        continue

                # Track valid deployments
                all_deployments.add(deployment.deployment_id)
                if deployment.instance_id:
                    all_instances.add(deployment.instance_id)

            await session.commit()

            # Purge validator instances not deployed locally
            for validator, instances in self.remote_instances.items():
                if (vali := validator_by_hotkey(validator)) is None:
                    continue
                for instance_id, data in instances.items():
                    config_id = data.get("config_id", None)
                    if instance_id not in all_instances and (not config_id or config_id not in all_configs):
                        chute_id = data["chute_id"]
                        logger.warning(f"Found validator {chute_id=} {instance_id=} {config_id=} not deployed locally!")
                        await self.purge_validator_instance(vali, chute_id, instance_id)

            # Purge k8s deployments that aren't tracked anymore
            # BUT exclude legacy deployments from deletion
            for deployment_id in all_k8s_ids - all_deployments:
                if deployment_id in k8s_legacy_ids:
                    logger.info(f"Preserving legacy kubernetes deployment: {deployment_id}")
                    continue
                logger.warning(f"Removing kubernetes deployment that is no longer tracked: {deployment_id}")
                tasks.append(asyncio.create_task(self.undeploy(deployment_id)))

            # GPUs that no longer exist in validator inventory.
            all_gpus = []
            for nodes in self.remote_nodes.values():
                all_gpus.extend(nodes)

            local_gpu_ids = set()
            async for row in (await session.stream(select(GPU))).unique():
                gpu = row[0]
                local_gpu_ids.add(gpu.gpu_id)
                if gpu.gpu_id not in all_gpus:
                    logger.warning(f"GPU {gpu.gpu_id} is no longer in validator {gpu.validator} inventory")
                    # XXX we need this reconciliation somehow, but API downtime is really problematic here...
                    # tasks.append(
                    #     asyncio.create_task(
                    #         self.gpu_deleted({"gpu_id": gpu.gpu_id, "validator": gpu.validator})
                    #     )
                    # )

            # XXX this also seems problematic currently :thinking:
            ## GPUs in validator inventory that don't exist locally.
            # for validator_hotkey, nodes in self.remote_nodes.items():
            #    for gpu_id in nodes:
            #        if gpu_id not in local_gpu_ids:
            #            logger.warning(
            #                f"Found GPU in inventory of {validator_hotkey} that is not local: {gpu_id}"
            #            )
            #            if (validator := validator_by_hotkey(validator_hotkey)) is not None:
            #                await self.remove_gpu_from_validator(validator, gpu_id)

            # Process chutes
            async for row in (await session.stream(select(Chute))).unique():
                chute = row[0]
                identifier = f"{chute.validator}:{chute.chute_id}:{chute.version}"
                all_chutes.add(identifier)

                if identifier in chutes_to_remove:
                    continue

                remote = (self.remote_chutes.get(chute.validator) or {}).get(chute.chute_id)
                if not remote or remote["version"] != chute.version:
                    update = updating.get(chute.validator, {}).get(chute.chute_id)
                    if update:
                        logger.warning(f"Skipping reconciliation for chute with rolling {update=}")
                        continue

                    logger.warning(
                        f"Chute: {chute.chute_id} version={chute.version} on validator {chute.validator} not found: remote={remote['version'] if remote else 'None'}"
                    )
                    tasks.append(
                        asyncio.create_task(
                            self.chute_deleted({"chute_id": chute.chute_id, "version": chute.version, "validator": chute.validator})
                        )
                    )
                    chutes_to_remove.add(identifier)

            # Find new chutes
            for validator, chutes in self.remote_chutes.items():
                for chute_id, config in chutes.items():
                    identifier = f"{validator}:{chute_id}:{config['version']}"
                    if identifier not in all_chutes:
                        update = updating.get(validator, {}).get(chute_id)
                        if update:
                            logger.warning(f"Skipping chute reconciliation for chute with rolling {update=}")
                            continue
                        logger.info(f"Found a new/untracked chute: {chute_id}")
                        tasks.append(
                            asyncio.create_task(
                                self.chute_created({"chute_id": chute_id, "version": config["version"], "validator": validator})
                            )
                        )

            # Check Kubernetes nodes
            nodes = await k8s.get_kubernetes_nodes()
            node_ids = {node["server_id"] for node in nodes}
            all_server_ids = set()

            servers = (await session.execute(select(Server))).unique().scalars().all()
            for server in servers:
                if server.server_id not in node_ids:
                    logger.warning(f"Server {server.server_id} no longer in kubernetes node list!")
                    tasks.append(asyncio.create_task(self.server_deleted({"server_id": server.server_id})))
                all_server_ids.add(server.server_id)

            # XXX We won't do the opposite (remove k8s nodes that aren't tracked) because they could be in provisioning status.
            for node_id in node_ids - all_server_ids:
                logger.warning(f"Server/node {node_id} not tracked in inventory, ignoring...")

            await asyncio.gather(*tasks)

    # endregion

    # region activate

    async def activator(self):
        """
        Loop to mark all deployments ready in the validator when they are ready in k8s.
        """
        logger.info("Activator running...")

        while True:
            try:
                await asyncio.sleep(_SECS_ACTIVATE)

                query = select(Deployment).where(
                    or_(Deployment.active.is_(False), Deployment.verified_at.is_(None)),
                    Deployment.stub.is_(False),
                    Deployment.instance_id.is_not(None),
                    Deployment.job_id.is_(None),  # Skip job deployments in activator
                )

                async with get_session() as session:
                    deployments = (await session.execute(query)).unique().scalars().all()

                if not deployments:
                    # logger.info("No deployments: not (active or verified) and not stub")
                    continue

                # For each deployment, check if it's ready to go in kubernetes.
                for deployment in deployments:

                    core_version = re.match(r"^([0-9]+\.[0-9]+\.[0-9]+).*", deployment.chute.chutes_version or "0.0.0").group(1)
                    if semver.compare(core_version or "0.0.0", "0.3.0") >= 0:
                        # The new chutes library activates the chute as part of startup flow via JWT.
                        # logger.warning(f"Deployment chute.version={deployment.chute.chutes_version}")
                        continue

                    k8s_deployment = await k8s.get_deployment(deployment.deployment_id)
                    if not k8s_deployment:
                        logger.warning("NO K8s!")

                    if k8s_deployment.get("ready"):
                        await self.activate(deployment)
                    else:
                        logger.warning(f"Deployment {deployment.deployment_id} not ready")

            except Exception as exc:
                logger.error(f"Error performing announcement loop: {exc}")

            # logger.info("Waiting for next activation")

    async def activate(self, deployment: Deployment):
        """
        Tell a validator that a deployment is active/ready.
        """
        logger.info(f"Start to activate {deployment.deployment_id=} ...")

        if (vali := validator_by_hotkey(deployment.validator)) is None:
            logger.warning(f"No validator for deployment: {deployment.deployment_id}")
            return
        body = {"active": True}
        async with aiohttp.ClientSession(raise_for_status=False) as session:
            headers, payload_string = sign_request(payload=body, purpose="instance")
            async with session.patch(
                f"{vali.api}/instances/{deployment.chute_id}/{deployment.instance_id}",
                headers=headers,
                data=payload_string,
            ) as resp:
                if resp.status >= 300:
                    logger.error(f"Error activating deployment:\n{await resp.text()}")
                resp.raise_for_status()
                data = await resp.json()
                async with get_session() as session:
                    deployment = (
                        (await session.execute(select(Deployment).where(Deployment.deployment_id == deployment.deployment_id)))
                        .unique()
                        .scalar_one_or_none()
                    )
                    if deployment:
                        deployment.active = True
                        if data.get("verified"):
                            deployment.verified_at = func.now()
                        await session.commit()
                logger.success(f"Successfully activated {deployment.instance_id=}")

    # endregion

    # region funcs

    async def remote_refresh_all(self):
        """
        Refresh chutes from the validators.
        """
        for validator in settings.validators:
            for clazz, id_field in (
                ("chutes", "chute_id"),
                ("images", "image_id"),
                ("nodes", "uuid"),
                ("instances", "instance_id"),
                ("metrics", "chute_id"),
            ):
                logger.debug(f"Refreshing {clazz} from {validator.hotkey}...")
                await self._remote_refresh_objects(
                    getattr(self, f"remote_{clazz}"), validator.hotkey, f"{validator.api}/miner/{clazz}/", id_field
                )

    @staticmethod
    async def _remote_refresh_objects(pointer: Dict[str, Any], hotkey: str, url: str, id_key: str):
        """
        Refresh images/chutes from validator(s).
        """
        async with aiohttp.ClientSession(raise_for_status=True, read_bufsize=10 * 1024 * 1024) as session:
            headers, _ = sign_request(purpose="miner")
            updated_items = {}
            explicit_null = False
            params = {}
            if "instances" in url:
                params["explicit_null"] = "True"
            async with session.get(url, headers=headers, params=params) as resp:
                async for content_enc in resp.content:
                    content = content_enc.decode()
                    if content.startswith("data: {"):
                        data = json.loads(content[6:])
                        updated_items[data[id_key]] = data
                    elif content.startswith("data: NO_ITEMS"):
                        explicit_null = True
            if updated_items or explicit_null:
                pointer[hotkey] = updated_items

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
                (await session.execute(select(Deployment).where(Deployment.deployment_id == deployment_id))).unique().scalar_one_or_none()
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
    async def load_chute(chute_id: str, validator: str):
        """
        Helper to load a chute from the local database.
        """
        async with get_session() as session:
            return (
                await session.execute(select(Chute).where(Chute.chute_id == chute_id).where(Chute.validator == validator))
            ).scalar_one_or_none()

    @staticmethod
    async def load_server(server_id: str, validator: str):
        """
        Helper to load a server from the local database.
        """
        async with get_session() as session:
            return (
                await session.execute(select(Server).where(Server.chute_id == server_id).where(Server.validator == validator))
            ).scalar_one_or_none()

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
                async with session.get(f"{validator.api}/instances/launch_config", headers=headers, params=params) as resp:
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
                async with session.post(f"{vali.api}/instances/{deployment.chute_id}/", headers=headers, data=payload_string) as resp:
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

    # endregion

    # region autoscaling

    async def autoscaler(self):
        logger.info("Autoscaler runing...")

        if not _CANDIDATE_CHUTES:
            logger.warning("_FIX_CHUTES is empty, skipping autoscaling.")
            return

        eval = Evaluator()
        depl = Deployer()

        while True:
            try:
                await asyncio.sleep(_SECS_AUTOSCALE)

                self._trace_refresh()

                cands = await eval.get_chute_servers()
                quals = [it for it in cands if it["chute_id"] in _CANDIDATE_CHUTES]
                tracs = [{**it, **self._chute_traces.get(it["chute_id"], {"failed": False})} for it in quals]
                choosed = None
                unfailed = [it for it in tracs if not it.get("failed")]
                if len(unfailed) > 0:
                    unfailed.sort(key=lambda it: it["value_ratio"], reverse=True)
                    choosed = unfailed[0]
                elif len(tracs) > 0:
                    tracs.sort(key=lambda it: (it.get("deleted_at"), it["value_ratio"] * -1), reverse=False)
                    choosed = tracs[0]

                if not choosed and len(cands) > 0:
                    choosed = cands[0]

                if choosed:
                    chute_id = choosed["chute_id"]
                    server_id = choosed["server_id"]

                    self._trace_created(chute_id)
                    await depl.deploy(chute_id, server_id)

            except Exception as ex:
                logger.error(f"Auto scaling failed: {ex}")

                if chute_id:
                    self._trace_deleted(chute_id)

    def _trace_created(self, chute_id: str):
        trc = self._chute_traces.get(chute_id, {})
        trc["created_at"] = datetime.now()
        self._chute_traces[chute_id] = trc

    def _trace_deleted(self, chute_id: str):
        trc = self._chute_traces.get(chute_id, {})
        trc["deleted_at"] = datetime.now()
        if trc.get("created_at"):
            trc["failed"] = (trc["deleted_at"] - trc["created_at"]) <= timedelta(minutes=16)
        else:
            trc["created_at"] = None
            trc["failed"] = False
        self._chute_traces[chute_id] = trc

    def _trace_refresh(self):
        now = datetime.now()
        for trc in self._chute_traces.values():
            fld = trc.get("failed")
            dlt = trc.get("deleted_at")
            if fld and dlt and now - dlt > timedelta(hours=1):
                    trc["failed"] = False


    # endregion


class Evaluator:

    _API_URL = settings.validators[0].api  # "https://api.chutes.ai"
    _VLD_KEY = settings.validators[0].hotkey

    # region priv

    @staticmethod
    async def _load_remote_objects(url: str, id_key: str):
        """
        从验证器刷新指定类型的资源（如chutes、images、instances等）到本地缓存。
        注：通过SSE（Server-Sent Events）接收资源更新，更新本地缓存字典。
        """
        async with aiohttp.ClientSession(raise_for_status=True, read_bufsize=10 * 1024 * 1024) as session:
            headers, _ = sign_request(purpose="miner")  # 生成请求签名
            params = {}
            items = {}

            async with session.get(url, headers=headers, params=params) as resp:
                # 读取SSE响应内容
                async for content_enc in resp.content:
                    content = content_enc.decode()
                    if content.startswith("data: {"):
                        # 解析资源数据（SSE格式为"data: {json}"）
                        data = json.loads(content[6:])
                        items[data[id_key]] = data
                    elif content.startswith("data: NO_ITEMS"):
                        # 明确返回无资源
                        # explicit_null = True
                        continue
                    else:
                        if content.strip() != "":
                            logger.warning(f"Unexpected content: {content}")

            return items

    async def _load_remote_chutes(self):
        chutes = await self._load_remote_objects(f"{self._API_URL}/miner/chutes/", "chute_id")
        return chutes

    async def _load_remote_metrics(self):
        metrics = await self._load_remote_objects(f"{self._API_URL}/miner/metrics/", "chute_id")
        return metrics

    async def _load_utilization(self):
        items = {}
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            # 从验证器API获取chute利用率
            async with session.get(f"{self._API_URL}/chutes/utilization") as resp:
                data = await resp.json()
                for it in data:
                    items[it["chute_id"]] = it

                return items

    async def _load_local_chute(self, chute_id: str, version: str, validator: str):
        async with get_session() as session:
            return (
                await session.execute(
                    select(Chute).where(Chute.chute_id == chute_id).where(Chute.version == version).where(Chute.validator == validator)
                )
            ).scalar_one_or_none()

    async def _calc_chute_gains(self, validator: str, rmt_chutes: dict, rmt_metrics: dict, rmt_utilizs: dict) -> list[dict]:

        chute_gains = []
        for chute_id, chute_info in rmt_chutes.items():
            try:
                if not chute_info.get("cords"):
                    continue

                metric = rmt_metrics.get(chute_id)
                if not metric:
                    continue

                utiliz = rmt_utilizs.get(chute_id)
                if not utiliz:
                    continue
                if not utiliz.get("scalable") or utiliz.get("update_in_progress"):
                    continue

                chute_name = chute_info.get("name")
                chute_version = chute_info.get("version")

                loc_chute = await self._load_local_chute(chute_id, chute_version, validator)
                if not loc_chute:
                    continue

                if loc_chute.ban_reason is not None:
                    continue

                loc_count = 0
                ins_count = metric.get("instance_count", 0)
                rate_limit = metric.get("rate_limit_count", 0)

                if loc_count >= ins_count and not rate_limit:
                    continue

                if ins_count >= 5 and rate_limit > 0:
                    rate_limit /= 5

                cmp_time = metric.get("total_compute_time", 0)
                cmp_mltp = metric.get("compute_multiplier", 1)
                cmp_cons = cmp_time * cmp_mltp
                tot_invc = metric.get("total_invocations", 1)
                per_invc = cmp_cons / (tot_invc or 1.0)
                sum_cons = cmp_cons + per_invc * rate_limit
                ptt_gain = sum_cons / (ins_count + 1)

                chute_gains.append(
                    {
                        "chute_id": chute_id,
                        "chute_name": chute_name,
                        "chute_version": chute_version,
                        "chute_image": loc_chute.image,
                        "chute_validator": validator,
                        "potential_gain": ptt_gain,
                        "instance_count": ins_count,
                        "gpu_supported": list(loc_chute.supported_gpus),
                        "gpu_count": loc_chute.gpu_count,
                        "chute_metrics": metric,
                        "chute_utilization": utiliz,
                    }
                )

            except Exception as ex:
                logger.error(f"Error handling chute {chute_id}: {ex}")
                continue

        chute_gains = sorted(chute_gains, key=lambda x: x["potential_gain"], reverse=True)

        return chute_gains

    async def _find_suitable_server(self, gpu_list, gpu_count, disk_gb: int = 100) -> Optional[Server]:

        supported_gpus = list(gpu_list)
        # 若支持h200且有其他GPU，优先排除h200
        if "h200" in supported_gpus and set(supported_gpus) - set(["h200"]):
            supported_gpus = list(set(supported_gpus) - set(["h200"]))

        # 子查询：统计每个服务器的总GPU数（支持的GPU且已验证）
        total_gpus_per_server = (
            select(Server.server_id, func.count(GPU.gpu_id).label("total_gpus"))
            .select_from(Server)
            .join(GPU, Server.server_id == GPU.server_id)
            .where(GPU.model_short_ref.in_(supported_gpus), GPU.verified.is_(True))
            .group_by(Server.server_id)
            .subquery()
        )
        # 子查询：统计每个服务器的已用GPU数（已验证且已分配部署）
        used_gpus_per_server = (
            select(Server.server_id, func.count(GPU.gpu_id).label("used_gpus"))
            .select_from(Server)
            .join(GPU, Server.server_id == GPU.server_id)
            .where(GPU.verified.is_(True), GPU.deployment_id.isnot(None))
            .group_by(Server.server_id)
            .subquery()
        )
        # 主查询：筛选可用服务器（空闲GPU满足需求、未锁定）
        query = (
            select(
                Server,
                total_gpus_per_server.c.total_gpus,
                func.coalesce(used_gpus_per_server.c.used_gpus, 0).label("used_gpus"),
                (total_gpus_per_server.c.total_gpus - func.coalesce(used_gpus_per_server.c.used_gpus, 0)).label(
                    "free_gpus"
                ),  # 空闲GPU数 = 总GPU - 已用GPU
            )
            .select_from(Server)
            .join(total_gpus_per_server, Server.server_id == total_gpus_per_server.c.server_id)
            .outerjoin(used_gpus_per_server, Server.server_id == used_gpus_per_server.c.server_id)
            .join(GPU, Server.server_id == GPU.server_id)
            .where(
                GPU.model_short_ref.in_(supported_gpus),
                GPU.verified.is_(True),
                # 空闲GPU需满足chute的GPU数量需求
                (total_gpus_per_server.c.total_gpus - func.coalesce(used_gpus_per_server.c.used_gpus, 0) >= gpu_count),
                Server.locked.is_(False),  # 服务器未锁定
            )
            # 排序策略：优先选择每小时成本低、空闲GPU少的服务器（装箱策略）
            .order_by(Server.hourly_cost.asc(), text("free_gpus ASC"))
        )

        async with get_session() as session:
            servers = (await session.execute(query)).unique().scalars().all()
            # 检查服务器磁盘空间是否足够
            for server in servers:
                if await k8s.check_node_has_disk_available(server.name, disk_gb):
                    return server
        return None

    async def _list_chutes_servers(self, chute_gains: list) -> list[dict]:

        cht_svrs = []
        for chute in chute_gains:
            try:
                gpu_list = chute["gpu_supported"]
                gpu_count = chute["gpu_count"]

                suit_svr = await self._find_suitable_server(gpu_list, gpu_count)
                if not suit_svr:
                    continue

                value_ratio = chute.get("potential_gain", 0) / (suit_svr.hourly_cost * gpu_count)

                cht_svrs.append(
                    {
                        **chute,
                        "value_ratio": value_ratio,
                        "hourly_cost": suit_svr.hourly_cost,
                        "server_id": suit_svr.server_id,
                        "server_name": suit_svr.name,
                        "server_gpus": suit_svr.gpu_count,
                    }
                )

            except Exception as ex:
                print(f"Error handling chute: {ex}\n{chute}")
                continue

        cht_svrs = sorted(cht_svrs, key=lambda x: x["value_ratio"], reverse=True)
        return cht_svrs

    # endregion

    # region util

    @staticmethod
    def _save_json_file(path: str, data: Any):
        path = os.path.abspath(path)
        jdat = json.dumps(data)
        with open(path, "wb") as f:
            f.write(jdat)

    @staticmethod
    def _print_chute_gains(gains: list):

        print(f"{'chute_id':<37} {'chute_name':<35} {'gain':>10} {'inst':>5}  {'gpu_supported':<10} | {'gpu_count'}")
        print("-" * 160)

        for it in gains:
            print(
                f"{it['chute_id']:<37} "
                f"{it['chute_name'][:35]:<35} "
                f"{it['potential_gain']:>10.2f} "
                f"{it['instance_count']:>5}  "
                f"{','.join(it['gpu_supported'])}  | "
                f"{it['gpu_count']}"
            )

    @staticmethod
    def _print_chute_servers(items: list):

        print(f"{'chute_id':<37} {'chute_name':<35}  {'server_id':<37} {'server_name':<20} {'value':>10} {'cost':>6}  {'gpu'}")
        print("-" * 160)

        for it in items:
            print(
                f"{it['chute_id']:<37} "
                f"{it['chute_name'][:35]:<35}  "
                f"{it['server_id']:<37} "
                f"{it['server_name'][:20]:<20} "
                f"{it['value_ratio']:>10.2f}  "
                f"{it['hourly_cost']:>5.2f}   "
                f"{it['gpu_count']}"
            )

    # endregion

    # region publ

    async def list_chute_gains(self):

        rmt_chutes = await self._load_remote_chutes()
        logger.debug(f"Loaded remote chutes: {len(rmt_chutes)}")

        rmt_metrics = await self._load_remote_metrics()
        logger.debug(f"Loaded remote metrics: {len(rmt_metrics)}")

        rmt_utils = await self._load_utilization()
        logger.debug(f"Loaded remote utilization: {len(rmt_utils)}")

        cht_gains = await self._calc_chute_gains(self._VLD_KEY, rmt_chutes, rmt_metrics, rmt_utils)
        logger.debug(f"Calculated chute gains: {len(cht_gains)}")

        if len(cht_gains) == 0:
            return

        self._save_json_file("scalable_chutes.json", cht_gains)
        self._print_chute_gains(cht_gains)

    async def list_chute_servers(self):

        rmt_chutes = await self._load_remote_chutes()
        logger.debug(f"Loaded remote chutes: {len(rmt_chutes)}")

        rmt_metrics = await self._load_remote_metrics()
        logger.debug(f"Loaded remote metrics: {len(rmt_metrics)}")

        rmt_utils = await self._load_utilization()
        logger.debug(f"Loaded remote utilization: {len(rmt_utils)}")

        cht_gains = await self._calc_chute_gains(self._VLD_KEY, rmt_chutes, rmt_metrics, rmt_utils)
        logger.debug(f"Calculated chute gains: {len(cht_gains)}")

        if len(cht_gains) == 0:
            return

        self._save_json_file("chute_gains.json", cht_gains)
        self._print_chute_gains(cht_gains)

        cht_svrs = await self._list_chutes_servers(cht_gains)
        logger.debug(f"Matched scalable servers: {len(cht_svrs)}")

        if len(cht_svrs) == 0:
            return

        self._save_json_file("chute_servers.json", cht_svrs)
        self._print_chute_servers(cht_svrs)

    async def get_chute_servers(self) -> list[dict]:
        rmt_chutes = await self._load_remote_chutes()
        rmt_metrics = await self._load_remote_metrics()
        rmt_utils = await self._load_utilization()
        cht_gains = await self._calc_chute_gains(self._VLD_KEY, rmt_chutes, rmt_metrics, rmt_utils)
        if len(cht_gains) == 0:
            return []
        cht_svrs = await self._list_chutes_servers(cht_gains)
        return cht_svrs

    # endregion


class Deployer:

    _VLD_KEY = settings.validators[0].hotkey

    # region priv

    @staticmethod
    async def _load_chute(chute_id: str, vald_key: str):
        """
        Helper to load a chute from the local database.
        """
        async with get_session() as session:
            return (
                (await session.execute(select(Chute).where(Chute.chute_id == chute_id).where(Chute.validator == vald_key)))
                .unique()
                .scalar_one_or_none()
            )

    @staticmethod
    async def _load_server(server_id: str, vald_key: str):
        """
        Helper to load a server from the local database.
        """
        async with get_session() as session:
            return (
                (await session.execute(select(Server).where(Server.server_id == server_id).where(Server.validator == vald_key)))
                .unique()
                .scalar_one_or_none()
            )

    @staticmethod
    async def _get_launch_token(chute: Chute, job_id: str = None):
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
                async with session.get(f"{validator.api}/instances/launch_config", headers=headers, params=params) as resp:
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

    @staticmethod
    async def _announce_deployment(deployment: Deployment):
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
                async with session.post(f"{vali.api}/instances/{deployment.chute_id}/", headers=headers, data=payload_string) as resp:
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

    @staticmethod
    async def _purge_validator_instance(vali: Validator, chute_id: str, instance_id: str):
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

    # region publ

    async def deploy(self, chute_id: str, server_id: str, vald_key: str | None = None):

        logger.info(f"Attempting to deploy {chute_id=} on {server_id=}")

        if not chute_id or not server_id:
            logger.warning("Invalid chute_id or server_id")
            return

        if not vald_key:
            vald_key = self._VLD_KEY

        if (chute := await self._load_chute(chute_id, vald_key)) is None:
            logger.error(f"No chute found: {chute_id}")
            return

        if (server := await self._load_server(server_id, vald_key)) is None:
            logger.error(f"No server found: {server_id}")
            return

        deployment = None
        try:
            launch_token = await self._get_launch_token(chute)
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
            logger.success(f"Successfully deployed {chute.name=} ({chute.chute_id}) on {server.name=} ({server.server_id}):\n{deployment}")
            if not launch_token:
                await self._announce_deployment(deployment)

        except DeploymentFailure as exc:
            logger.error(
                f"Error attempting to deploy {chute.name=} ({chute.chute_id}) on {server.name=} ({server.server_id}): {exc}\n{traceback.format_exc()}"
            )

            if deployment:
                logger.info(f"Undeploying deployment due to failure {deployment.deployment_id=}")
                await self.undeploy(deployment.deployment_id)
                logger.info("Undeployed deployment")

    async def undeploy(self, deployment_id: str, instance_id: str = None):
        """
        Delete a deployment.
        """
        logger.info(f"Removing all traces of deployment: {deployment_id}")

        if not deployment_id:
            logger.warning("No deployment_id")
            return

        # Clean up the database.
        chute_id = None
        validator_hotkey = None
        async with get_session() as session:
            deployment = (
                (await session.execute(select(Deployment).where(Deployment.deployment_id == deployment_id))).unique().scalar_one_or_none()
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
                await self._purge_validator_instance(vali, chute_id, instance_id)

        # Purge in k8s if still there.
        await k8s.undeploy(deployment_id)
        logger.success(f"Removed {deployment_id=}")

    async def update(self, chute_id: str, vald_key: str | None = None):

        logger.info(f"Update chute ({chute_id}) to k8s")

        if not vald_key:
            vald_key = self._VLD_KEY

        try:
            if (chute := await self._load_chute(chute_id, vald_key)) is None:
                logger.error(f"No chute found: {chute_id}")
                return

            await k8s.create_code_config_map(chute)

        except Exception as e:
            logger.error(f"Error udpate chute: {e}")

    # endregion


class Recycler:

    # region inin

    def __init__(self):
        """
        Constructor.
        """
        self.remote_chutes = {validator.hotkey: {} for validator in settings.validators}
        self.remote_images = {validator.hotkey: {} for validator in settings.validators}
        self.remote_instances = {validator.hotkey: {} for validator in settings.validators}
        self.remote_nodes = {validator.hotkey: {} for validator in settings.validators}
        self.remote_metrics = {validator.hotkey: {} for validator in settings.validators}

    # endregion

    # region publ

    async def recycle(self):
        """
        Put our local system back in harmony with the validators.
        """
        logger.info("Start to recycle ...")
        try:
            await self.remote_refresh_all()
        except Exception as exc:
            logger.error(f"Failed to refresh remote resources: {exc}")
            return

        # Get the chutes currently undergoing a rolling update.
        updating = {}
        for validator in settings.validators:
            updating[validator.hotkey] = {}
            async with aiohttp.ClientSession(raise_for_status=True) as session:
                async with session.get(f"{validator.api}/chutes/rolling_updates") as resp:
                    for item in await resp.json():
                        updating[validator.hotkey][item["chute_id"]] = item

        # Compare local items to validators' inventory.
        tasks = []
        chutes_to_remove = set()
        all_chutes = set()
        all_deployments = set()
        all_instances = set()
        all_configs = set()

        # Get both new job-based and old deployment-based chutes
        k8s_chutes = await k8s.get_deployed_chutes()
        k8s_legacy_deployments = await k8s.get_deployed_chutes_legacy()

        # Combine both into a single set of deployment IDs
        k8s_chute_ids = {c["deployment_id"] for c in k8s_chutes}
        k8s_legacy_ids = {d["deployment_id"] for d in k8s_legacy_deployments}
        all_k8s_ids = k8s_chute_ids | k8s_legacy_ids

        # Log legacy deployments for visibility
        if k8s_legacy_ids:
            logger.info(f"Found {len(k8s_legacy_ids)} legacy deployment-based chutes: {k8s_legacy_ids}")

        # Get all pods with config_id labels for orphan detection
        k8s_config_ids = set()
        try:
            pods = await k8s.get_pods_by_label("chutes/config-id")
            k8s_config_ids = {pod["metadata"]["labels"]["chutes/config-id"] for pod in pods}
        except Exception as exc:
            logger.error(f"Failed to get pods by config-id label: {exc}")

        # Build map of config_id -> instance from remote inventory.
        remote_by_config_id = {}
        for validator, instances in self.remote_instances.items():
            for instance_id, data in instances.items():
                config_id = data.get("config_id")
                if config_id:
                    remote_by_config_id[config_id] = {**data, "instance_id": instance_id, "validator": validator}

        # Update chutes image field based on remote_images
        chute_map = {}
        async with get_session() as session:
            image_updates = {}
            for validator, images in self.remote_images.items():
                for image_id, image_data in images.items():
                    image_str = f"{image_data['username']}/{image_data['name']}:{image_data['tag']}"
                    if image_data.get("patch_version") and image_data["patch_version"] != "initial":
                        image_str += f"-{image_data['patch_version']}"
                    image_updates[image_id] = {"image": image_str, "chutes_version": image_data["chutes_version"]}
            for validator, chutes in self.remote_chutes.items():
                for chute_id, chute_data in chutes.items():
                    image_id = chute_data.get("image_id")
                    if image_id:
                        if image_id not in chute_map:
                            chute_map[image_id] = []
                        chute_map[image_id].append(chute_id)
            if image_updates and chute_map:
                async for row in (await session.stream(select(Chute))).unique():
                    chute = row[0]
                    for image_id, chute_ids in chute_map.items():
                        if chute.chute_id in chute_ids and image_id in image_updates:
                            update_data = image_updates[image_id]
                            if chute.image != update_data["image"]:
                                logger.info(f"Updating chute {chute.chute_id} image from '{chute.image}' to '{update_data['image']}'")
                                chute.image = update_data["image"]
                            if update_data["chutes_version"] != chute.chutes_version:
                                logger.info(
                                    f"Updating chute {chute.chute_id} chutes_version from '{chute.chutes_version}' to '{update_data['chutes_version']}'"
                                )
                                chute.chutes_version = update_data["chutes_version"]
                            break
                await session.commit()

        async with get_session() as session:
            # Clean up based on deployments/instances.
            async for row in (await session.stream(select(Deployment))).unique():
                deployment = row[0]

                # Make sure the instances created with launch configs have the instance ID tracked.
                if deployment.config_id and not deployment.instance_id:
                    remote_match = remote_by_config_id.get(deployment.config_id)
                    if remote_match and remote_match.get("validator") == deployment.validator:
                        deployment.instance_id = remote_match["instance_id"]
                        logger.info(
                            f"Updated deployment {deployment.deployment_id} with instance_id={deployment.instance_id} "
                            f"based on matching config_id={deployment.config_id}"
                        )

                # Reconcile the verified/active state for instances.
                if deployment.instance_id:
                    remote_instance = (self.remote_instances.get(deployment.validator) or {}).get(deployment.instance_id)
                    if remote_instance:
                        if remote_instance.get("last_verified_at") and not deployment.verified_at:
                            deployment.verified_at = func.now()
                            logger.info(f"Marking deployment {deployment.deployment_id} as verified based on remote status")
                        remote_active = remote_instance.get("active", True)
                        if deployment.active != remote_active:
                            deployment.active = remote_active
                            deployment.stub = False
                            logger.info(f"Updating deployment {deployment.deployment_id} active status to {deployment.active}")

                # Early check for orphaned deployments with config_id
                if deployment.config_id and deployment.config_id not in k8s_config_ids:
                    logger.warning(
                        f"Deployment {deployment.deployment_id} has config_id={deployment.config_id} but no matching pod in k8s, cleaning up"
                    )
                    if deployment.instance_id:
                        if (vali := validator_by_hotkey(deployment.validator)) is not None:
                            await self.purge_validator_instance(vali, deployment.chute_id, deployment.instance_id)
                    await session.delete(deployment)
                    continue

                # Check if instance exists on validator
                if deployment.instance_id and deployment.instance_id not in (self.remote_instances.get(deployment.validator) or {}):
                    logger.warning(
                        f"Deployment: {deployment.deployment_id} (instance_id={deployment.instance_id}) on validator {deployment.validator} not found"
                    )
                    tasks.append(asyncio.create_task(self.instance_deleted({"instance_id": deployment.instance_id})))
                    # Skip the rest of processing for this deployment since instance is gone
                    continue

                remote = (self.remote_chutes.get(deployment.validator) or {}).get(deployment.chute_id)

                # Track deployments by their launch configs.
                if deployment.config_id:
                    all_configs.add(deployment.config_id)

                # Special handling for deployments with job_id
                if hasattr(deployment, "job_id") and deployment.job_id:
                    # Always track the instance_id for job deployments to prevent premature purging
                    if deployment.instance_id:
                        all_instances.add(deployment.instance_id)

                    if remote:
                        logger.info(
                            f"Keeping deployment with job_id={deployment.job_id} for chute {deployment.chute_id} (remote version={remote.get('version')}, local version={deployment.version})"
                        )
                        all_deployments.add(deployment.deployment_id)
                        continue
                    else:
                        # Chute associated with the job doesn't exist anymore.
                        logger.warning(
                            f"Job deployment {deployment.deployment_id} with job_id={deployment.job_id} not found in remote inventory"
                        )
                        identifier = f"{deployment.validator}:{deployment.chute_id}:{deployment.version}"
                        if identifier not in chutes_to_remove:
                            chutes_to_remove.add(identifier)
                            tasks.append(
                                asyncio.create_task(
                                    self.chute_deleted(
                                        {
                                            "chute_id": deployment.chute_id,
                                            "version": deployment.version,
                                            "validator": deployment.validator,
                                        }
                                    )
                                )
                            )
                        continue

                # Normal deployment handling (no job_id)
                if not remote or remote["version"] != deployment.version:
                    update = updating.get(deployment.validator, {}).get(deployment.chute_id)
                    if update:
                        logger.warning(f"Skipping reconciliation for chute with rolling {update=}")
                        all_deployments.add(deployment.deployment_id)
                        if deployment.instance_id:
                            all_instances.add(deployment.instance_id)
                        continue

                    logger.warning(
                        f"Chute: {deployment.chute_id} version={deployment.version} on validator {deployment.validator} not found"
                    )
                    identifier = f"{deployment.validator}:{deployment.chute_id}:{deployment.version}"
                    if identifier not in chutes_to_remove:
                        chutes_to_remove.add(identifier)
                        tasks.append(
                            asyncio.create_task(
                                self.chute_deleted(
                                    {
                                        "chute_id": deployment.chute_id,
                                        "version": deployment.version,
                                        "validator": deployment.validator,
                                    }
                                )
                            )
                        )
                    # Don't continue here - we still need to check k8s state and cleanup

                # Check if deployment exists in k8s (either as job or legacy deployment)
                deployment_in_k8s = deployment.deployment_id in all_k8s_ids

                # Special handling for legacy deployments
                if deployment.deployment_id in k8s_legacy_ids:
                    logger.info(f"Found legacy deployment {deployment.deployment_id}, preserving it")
                    all_deployments.add(deployment.deployment_id)
                    if deployment.instance_id:
                        all_instances.add(deployment.instance_id)
                    continue

                # Delete deployments that never made it past stub stage or disappeared from k8s
                if not deployment.stub and not deployment_in_k8s:
                    logger.warning(f"Deployment has disappeared from kubernetes: {deployment.deployment_id}")
                    if deployment.instance_id:
                        if (vali := validator_by_hotkey(deployment.validator)) is not None:
                            await self.purge_validator_instance(vali, deployment.chute_id, deployment.instance_id)
                    await session.delete(deployment)
                    continue

                # Clean up old stubs
                deployment_age = datetime.now(timezone.utc) - deployment.created_at
                if (deployment.stub or not deployment.instance_id) and deployment_age >= timedelta(minutes=60):
                    logger.warning(f"Deployment is still a stub after 60 minutes, deleting! {deployment.deployment_id}")
                    await session.delete(deployment)
                    continue

                # Check for terminated jobs or jobs that never started
                if not deployment.active or deployment.verified_at is None and deployment_age >= timedelta(minutes=5):
                    try:
                        kd = await k8s.get_deployment(deployment.deployment_id)
                    except Exception as exc:
                        if "Not Found" in str(exc) or "(404)" in str(exc):
                            await self.undeploy(deployment.deployment_id)
                        continue

                    destroyed = False
                    job_status = kd.get("status", {})

                    # Check job completion status
                    if job_status.get("succeeded", 0) > 0:
                        logger.info(f"Job completed successfully: {deployment.deployment_id}")
                        await self.undeploy(deployment.deployment_id)
                        destroyed = True
                    elif job_status.get("failed", 0) > 0:
                        logger.warning(f"Job failed: {deployment.deployment_id}")
                        await self.undeploy(deployment.deployment_id)
                        destroyed = True

                    # Check for terminated pods (for Jobs that don't update status properly)
                    if not destroyed:
                        for pod in kd.get("pods", []):
                            pod_state = pod.get("state") or {}
                            if pod_state.get("terminated"):
                                terminated = pod_state["terminated"]
                                exit_code = terminated.get("exit_code", 0)
                                if exit_code == 0:
                                    logger.info(f"Job pod completed successfully: {deployment.deployment_id}")
                                else:
                                    logger.warning(f"Job pod terminated with error: {deployment.deployment_id}, exit_code={exit_code}")
                                await self.undeploy(deployment.deployment_id)
                                destroyed = True
                                break

                    if destroyed:
                        continue

                # Track valid deployments
                all_deployments.add(deployment.deployment_id)
                if deployment.instance_id:
                    all_instances.add(deployment.instance_id)

            await session.commit()

            # Purge validator instances not deployed locally
            for validator, instances in self.remote_instances.items():
                if (vali := validator_by_hotkey(validator)) is None:
                    continue
                for instance_id, data in instances.items():
                    config_id = data.get("config_id", None)
                    if instance_id not in all_instances and (not config_id or config_id not in all_configs):
                        chute_id = data["chute_id"]
                        logger.warning(f"Found validator {chute_id=} {instance_id=} {config_id=} not deployed locally!")
                        await self.purge_validator_instance(vali, chute_id, instance_id)

            # Purge k8s deployments that aren't tracked anymore
            # BUT exclude legacy deployments from deletion
            for deployment_id in all_k8s_ids - all_deployments:
                if deployment_id in k8s_legacy_ids:
                    logger.info(f"Preserving legacy kubernetes deployment: {deployment_id}")
                    continue
                logger.warning(f"Removing kubernetes deployment that is no longer tracked: {deployment_id}")
                tasks.append(asyncio.create_task(self.undeploy(deployment_id)))

            # GPUs that no longer exist in validator inventory.
            all_gpus = []
            for nodes in self.remote_nodes.values():
                all_gpus.extend(nodes)

            local_gpu_ids = set()
            async for row in (await session.stream(select(GPU))).unique():
                gpu = row[0]
                local_gpu_ids.add(gpu.gpu_id)
                if gpu.gpu_id not in all_gpus:
                    logger.warning(f"GPU {gpu.gpu_id} is no longer in validator {gpu.validator} inventory")
                    # XXX we need this reconciliation somehow, but API downtime is really problematic here...
                    # tasks.append(
                    #     asyncio.create_task(
                    #         self.gpu_deleted({"gpu_id": gpu.gpu_id, "validator": gpu.validator})
                    #     )
                    # )

            # XXX this also seems problematic currently :thinking:
            ## GPUs in validator inventory that don't exist locally.
            # for validator_hotkey, nodes in self.remote_nodes.items():
            #    for gpu_id in nodes:
            #        if gpu_id not in local_gpu_ids:
            #            logger.warning(
            #                f"Found GPU in inventory of {validator_hotkey} that is not local: {gpu_id}"
            #            )
            #            if (validator := validator_by_hotkey(validator_hotkey)) is not None:
            #                await self.remove_gpu_from_validator(validator, gpu_id)

            # Process chutes
            async for row in (await session.stream(select(Chute))).unique():
                chute = row[0]
                identifier = f"{chute.validator}:{chute.chute_id}:{chute.version}"
                all_chutes.add(identifier)

                if identifier in chutes_to_remove:
                    continue

                remote = (self.remote_chutes.get(chute.validator) or {}).get(chute.chute_id)
                if not remote or remote["version"] != chute.version:
                    update = updating.get(chute.validator, {}).get(chute.chute_id)
                    if update:
                        logger.warning(f"Skipping reconciliation for chute with rolling {update=}")
                        continue

                    logger.warning(
                        f"Chute: {chute.chute_id} version={chute.version} on validator {chute.validator} not found: remote={remote['version'] if remote else 'None'}"
                    )
                    tasks.append(
                        asyncio.create_task(
                            self.chute_deleted({"chute_id": chute.chute_id, "version": chute.version, "validator": chute.validator})
                        )
                    )
                    chutes_to_remove.add(identifier)

            # Find new chutes
            for validator, chutes in self.remote_chutes.items():
                for chute_id, config in chutes.items():
                    identifier = f"{validator}:{chute_id}:{config['version']}"
                    if identifier not in all_chutes:
                        update = updating.get(validator, {}).get(chute_id)
                        if update:
                            logger.warning(f"Skipping chute reconciliation for chute with rolling {update=}")
                            continue
                        logger.info(f"Found a new/untracked chute: {chute_id}")
                        tasks.append(
                            asyncio.create_task(
                                self.chute_created({"chute_id": chute_id, "version": config["version"], "validator": validator})
                            )
                        )

            # Check Kubernetes nodes
            nodes = await k8s.get_kubernetes_nodes()
            node_ids = {node["server_id"] for node in nodes}
            all_server_ids = set()

            servers = (await session.execute(select(Server))).unique().scalars().all()
            for server in servers:
                if server.server_id not in node_ids:
                    logger.warning(f"Server {server.server_id} no longer in kubernetes node list!")
                    tasks.append(asyncio.create_task(self.server_deleted({"server_id": server.server_id})))
                all_server_ids.add(server.server_id)

            # XXX We won't do the opposite (remove k8s nodes that aren't tracked) because they could be in provisioning status.
            for node_id in node_ids - all_server_ids:
                logger.warning(f"Server/node {node_id} not tracked in inventory, ignoring...")

            await asyncio.gather(*tasks)

    # endregion

    # region priv

    async def chute_created(self, event_data: Dict[str, Any], desired_count: int = 1):
        """
        A brand new chute was added to validator inventory.

        MINERS: This is a critical optimization path. A chute being created
                does not necessarily mean inference will be requested. The
                base mining code here *will* deploy the chute however, given
                sufficient resources are available.
        """

        chute_id = event_data["chute_id"]
        version = event_data["version"]
        validator_hotkey = event_data["validator"]
        logger.info(f"chute_created: {chute_id=}")

        if (validator := validator_by_hotkey(validator_hotkey)) is None:
            logger.warning(f"Validator not found: {validator_hotkey}")
            return

        # Already in inventory?
        if (chute := await self.load_chute(chute_id, validator_hotkey)) is not None:
            logger.info(f"Chute {chute_id=} is already tracked in inventory?")
            return

        # Load the chute details, preferably from the local cache.
        chute_dict = None
        try:
            async with aiohttp.ClientSession(raise_for_status=True) as session:
                headers, _ = sign_request(purpose="miner")
                async with session.get(f"{validator.api}/miner/chutes/{chute_id}/{version}", headers=headers) as resp:
                    chute_dict = await resp.json()
        except Exception:
            logger.error(f"Error loading remote chute data: {chute_id=} {version=}")
            return

        # Track in inventory.
        async with get_session() as session:
            chute = Chute(
                chute_id=chute_id,
                validator=validator.hotkey,
                name=chute_dict["name"],
                image=chute_dict["image"],
                code=chute_dict["code"],
                filename=chute_dict["filename"],
                ref_str=chute_dict["ref_str"],
                version=chute_dict["version"],
                supported_gpus=chute_dict["supported_gpus"],
                gpu_count=chute_dict["node_selector"]["gpu_count"],
                chutes_version=chute_dict["chutes_version"],
                ban_reason=None,
            )
            session.add(chute)
            await session.commit()
            await session.refresh(chute)

        await k8s.create_code_config_map(chute)

        # Don't deploy if this is a job-only chute, i.e. it has no "cords" to serve
        # so there's nothing to deploy.
        if event_data.get("job_only"):
            return

    async def chute_deleted(self, event_data: Dict[str, Any]):
        """
        A chute (or specific version of a chute) was removed from validator inventory.
        """
        chute_id = event_data["chute_id"]
        version = event_data["version"]
        validator = event_data["validator"]
        logger.info(f"chute_deleted: {chute_id=} {version=}")

        async with get_session() as session:
            chute = (
                await session.execute(
                    select(Chute)
                    .where(Chute.chute_id == chute_id)
                    .where(Chute.version == version)
                    .where(Chute.validator == validator)
                    .options(selectinload(Chute.deployments))
                )
            ).scalar_one_or_none()
            if chute:
                if chute.deployments:
                    await asyncio.gather(*[self.undeploy(deployment.deployment_id) for deployment in chute.deployments])
                await session.delete(chute)
                await session.commit()
        await k8s.delete_code(chute_id, version)

    async def instance_deleted(self, event_data: Dict[str, Any]):
        """
        An instance was removed validator side, likely meaning there were too
        many consecutive failures in inference.
        """
        instance_id = event_data["instance_id"]
        logger.info(f"instance_deleted: {instance_id=}")
        async with get_session() as session:
            deployment = (
                (await session.execute(select(Deployment).where(Deployment.instance_id == instance_id))).unique().scalar_one_or_none()
            )
        if deployment:
            await self.undeploy(deployment.deployment_id)

    async def server_deleted(self, event_data: Dict[str, Any]):
        """
        An entire kubernetes node was removed from your inventory.

        MINERS: This will happen when you remove a node intentionally, but otherwise
                should not really happen.  Also want to monitor this situation I think.
        """
        server_id = event_data["server_id"]
        logger.info(f"server_deleted event: {event_data}")

    async def remote_refresh_all(self):
        """
        Refresh chutes from the validators.
        """
        for validator in settings.validators:
            for clazz, id_field in (
                ("chutes", "chute_id"),
                ("images", "image_id"),
                ("nodes", "uuid"),
                ("instances", "instance_id"),
                ("metrics", "chute_id"),
            ):
                logger.debug(f"Refreshing {clazz} from {validator.hotkey}...")
                await self._remote_refresh_objects(
                    getattr(self, f"remote_{clazz}"), validator.hotkey, f"{validator.api}/miner/{clazz}/", id_field
                )

    @staticmethod
    async def _remote_refresh_objects(pointer: Dict[str, Any], hotkey: str, url: str, id_key: str):
        """
        Refresh images/chutes from validator(s).
        """
        async with aiohttp.ClientSession(raise_for_status=True, read_bufsize=10 * 1024 * 1024) as session:
            headers, _ = sign_request(purpose="miner")
            updated_items = {}
            explicit_null = False
            params = {}
            if "instances" in url:
                params["explicit_null"] = "True"
            async with session.get(url, headers=headers, params=params) as resp:
                async for content_enc in resp.content:
                    content = content_enc.decode()
                    if content.startswith("data: {"):
                        data = json.loads(content[6:])
                        updated_items[data[id_key]] = data
                    elif content.startswith("data: NO_ITEMS"):
                        explicit_null = True
            if updated_items or explicit_null:
                pointer[hotkey] = updated_items

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
                (await session.execute(select(Deployment).where(Deployment.deployment_id == deployment_id))).unique().scalar_one_or_none()
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
    async def load_chute(chute_id: str, validator: str):
        """
        Helper to load a chute from the local database.
        """
        async with get_session() as session:
            return (
                await session.execute(select(Chute).where(Chute.chute_id == chute_id).where(Chute.validator == validator))
            ).scalar_one_or_none()

    # endregion


class ChuteManager:

    def __init__(self, validator_key: str, remote_chutes: dict[str, dict]):
        self._validator_key = validator_key
        self._remote_chutes = remote_chutes

    # region public

    async def empty_chutes(self):

        async with get_session() as session:
            # FK constraints by Deployments
            cnt = 0
            async for row in (await session.stream(select(Chute))).unique():
                chute = row[0]
                cid = chute.chute_id
                ver = chute.version
                await k8s.delete_code(cid, ver)
                await session.delete(chute)
                cnt += 1
            await session.commit()

        logger.info(f"Emptied {cnt} chutes")

    async def refresh_chutes(self, renew: bool = False):

        if renew:
            await self.empty_chutes()

        rmt_chutes = self._remote_chutes
        if not rmt_chutes:
            rmt_chutes = await RemoteApi.get_remote_chutes(self._validator_key)
        loc_chutes = await self.get_local_chutes(self._validator_key)

        cnt_rmv = 0
        for chute in loc_chutes:
            rmt_cht = rmt_chutes.get(chute.chute_id)
            if not rmt_cht or rmt_cht["version"] != chute.version:
                await self.delete_chute(chute)
                cnt_rmv += 1

        cnt_crt = 0
        for chute in rmt_chutes.values():
            loc_cht = loc_chutes.get(chute["chute_id"])
            if not loc_cht:
                await self.create_chute(chute)
                cnt_crt += 1

        logger.info(f"Refreshed chutes, created {cnt_crt}, removed {cnt_rmv}")

    async def create_chute(self, chute_dict: dict):

        async with get_session() as session:
            chute_row = Chute(
                validator=self._validator_key,
                chute_id=chute_dict["chute_id"],
                chutes_version=chute_dict["chutes_version"],
                name=chute_dict["name"],
                image=chute_dict["image"],
                filename=chute_dict["filename"],
                code=chute_dict["code"],
                ref_str=chute_dict["ref_str"],
                version=chute_dict["version"],
                supported_gpus=chute_dict["supported_gpus"],
                gpu_count=chute_dict["node_selector"]["gpu_count"],
                ban_reason=None,
            )
            session.add(chute_row)
            await session.commit()
            await session.refresh(chute_row)
        await k8s.create_code_config_map(chute_row)
        logger.info(f"Created chute id: {chute_row.chute_id} name: {chute_row.name}")

    async def create_chute_by_id(self, chute_id: str, version: str):

        chute_dict = await RemoteApi.get_remote_chute(self._validator_key, chute_id, version)
        await self.create_chute(chute_dict)

    # endregion

    # region static

    @staticmethod
    async def get_local_chute_by_id(val_key: str, chute_id: str) -> Chute | None:

        async with get_session() as session:
            res = (
                await session.query(Chute).where(Chute.validator == val_key).where(Chute.chute_id == chute_id).unique().scalar_one_or_none()
            )
            return res

    @staticmethod
    async def get_local_chute_with_deployments(val_key: str, chute_id: str, version: str) -> Chute | None:

        async with get_session() as session:
            res = (
                await session.execute(
                    select(Chute)
                    .where(Chute.chute_id == chute_id)
                    .where(Chute.version == version)
                    .where(Chute.validator == val_key)
                    .options(selectinload(Chute.deployments))
                )
            ).scalar_one_or_none()
            return res

    @staticmethod
    async def get_local_chutes(val_key: str) -> list[Chute]:

        async with get_session() as session:
            res = await session.query(Chute).where(Chute.validator == val_key).all()
            return res

    # endregion


# region utils


def _api_addr(validator_key: str):
    vld = validator_by_hotkey(validator_key)
    if not vld:
        raise ValueError(f"Validator {validator_key} not found")
    return vld.api


async def _resp_err(resp: aiohttp.ClientResponse):
    txt = await resp.text(encoding="utf-8", errors="replace")
    msg = f"{resp.method} {resp.url} failed {resp.reason} [{resp.status}]\n"
    msg += txt or "<no content>"
    logger.error(msg)
    resp.raise_for_status()


# endregion


class RemoteApi:

    @staticmethod
    async def load_remote_objects(val_key: str, obj_name: str, key_name: str) -> dict[str, dict]:
        """
        从验证器获取指定类型的资源（如 chutes、images、instances、nodes、metrics 等）
        注：通过SSE（Server-Sent Events）接收资源数据。
        """
        async with aiohttp.ClientSession(read_bufsize=10 * 1024 * 1024) as session:
            url = f"{_api_addr(val_key)}/miners/{obj_name}"
            hdr, _ = sign_request(purpose="miner")  # 生成请求签名
            par = {}
            if "instances" in url:
                par["explicit_null"] = "True"

            items = {}
            async with session.get(url, headers=hdr, params=par) as resp:

                if resp.ok:
                    # 读取SSE响应内容
                    async for content_enc in resp.content:
                        content = content_enc.decode()
                        if content.startswith("data: {"):
                            # 解析资源数据（SSE格式为"data: {json}"）
                            data = json.loads(content[6:])
                            items[data[key_name]] = data
                        elif content.startswith("data: NO_ITEMS"):
                            # 明确返回无资源
                            # explicit_null = True
                            continue
                        else:
                            if content.strip() != "":
                                logger.warning(f"Unexpected content: {content}")
                else:
                    await _resp_err(resp)

            return items

    @staticmethod
    async def get_remote_chute(val_key: str, chute_id: str, version: str) -> dict:
        async with aiohttp.ClientSession() as session:
            url = f"{_api_addr(val_key)}/miner/chutes/{chute_id}/{version}"
            hdr, _ = sign_request(purpose="miner")
            async with session.get(url, headers=hdr) as resp:
                if resp.ok:
                    data = await resp.json()
                    return data
                else:
                    await _resp_err(resp)

    @staticmethod
    async def get_remote_chutes(val_key: str) -> dict[str, dict]:
        obj = "chutes"
        key = "chute_id"
        items = await RemoteApi.load_remote_objects(val_key, obj, key)
        return items

    @staticmethod
    async def get_remote_metrics(val_key: str) -> dict[str, dict]:
        obj = "metrics"
        key = "chute_id"
        items = await RemoteApi.load_remote_objects(val_key, obj, key)
        return items

    @staticmethod
    async def get_lauch_token(val_key: str, chute_id: str, job_id: str = None) -> dict:
        async with aiohttp.ClientSession() as session:
            url = f"{_api_addr(val_key)}/instances/launch_config"
            hdr, _ = sign_request(purpose="launch")
            par = {"chute_id": chute_id}
            if job_id:
                par["job_id"] = job_id

            async with session.get(url, headers=hdr, params=par) as resp:
                if resp.ok:
                    data = await resp.json()
                    return data
                else:
                    await _resp_err(resp)

    @staticmethod
    async def post_deployment(deployment: Deployment) -> dict:
        async with aiohttp.ClientSession() as session:
            val = deployment.validator
            url = f"{_api_addr(val)}/instances/{deployment.chute_id}/"
            par = {
                "node_ids": [gpu.gpu_id for gpu in deployment.gpus],
                "host": deployment.host,
                "port": deployment.port,
            }
            hdr, pld = sign_request(payload=par, purpose="instance")
            async with session.post(url, headers=hdr, data=pld) as resp:
                if resp.ok:
                    obj = await resp.json()
                    return obj
                else:
                    await _resp_err(resp)

    @staticmethod
    async def purge_instance(val_key: str, chute_id: str, instance_id: str) -> bool:
        async with aiohttp.ClientSession() as session:
            url = f"{_api_addr(val_key)}/instances/{chute_id}/{instance_id}"
            hdr, _ = sign_request(purpose="instance")
            async with session.delete(url, headers=hdr) as resp:
                if resp.ok or resp.status == 404:
                    return True
                else:
                    await _resp_err(resp)


def main():
    gepetto = Gepetto()
    asyncio.run(gepetto.run())


def list_chute_gains():
    eval = Evaluator()
    asyncio.run(eval.list_chute_gains())


def list_chute_servers():
    eval = Evaluator()
    asyncio.run(eval.list_chute_servers())


def deploy_chute(chute_id: str, server_id: str):
    depl = Deployer()
    asyncio.run(depl.deploy(chute_id, server_id))


def undeploy_chute(deployment_id: str):
    depl = Deployer()
    asyncio.run(depl.undeploy(deployment_id))


def refresh_chute(force: bool = False):
    cht_mgr = ChuteManager(_VLD_KEY, {})
    asyncio.run(cht_mgr.refresh_chutes(force))


def recycle_deploy():
    recy = Recycler()
    asyncio.run(recy.recycle())


def parse_cmds_args():

    parser = argparse.ArgumentParser(description="Chutes Miner Commands")

    subprs = parser.add_subparsers(dest="cmd", help="Available commands")

    ch = subprs.add_parser("chutes", help="List remote available chutes")

    sc = subprs.add_parser("scales", help="List scalable chutes & servers")
    # sc.add_argument("-a", "--all", action="store_true", help="Show all servers")

    re = subprs.add_parser("recycle", help="Delete invalid deployments")
    # re.add_argument("-f", "--force", action="store_true", help="Force clean")

    de = subprs.add_parser("deploy", help="Deploy chute on server")
    de.add_argument("-c", "--chute_id", type=str, help="CHUT_ID: 8xxxxxxx-4xxx-4xxx-4xxx-12xxxxxxxxxx")
    de.add_argument("-s", "--server_id", type=str, help="SERVER_ID: 8xxxxxxx-4xxx-4xxx-4xxx-12xxxxxxxxxx")
    # de.add_argument("-a", "--auto", action="store_true", help="Auto deploy")

    un = subprs.add_parser("undeploy", help="Undeploy chute from server")
    un.add_argument("-i", "--id", type=str, help="Deployment_ID: 8xxxxxxx-4xxx-4xxx-4xxx-12xxxxxxxxxx")
    # un.add_argument("-a", "--all", action="store_true", help="Undeploy all instances")

    cr = subprs.add_parser("refresh-chute", help="Refresh chutes from remote")
    cr.add_argument("-f", "--force", action="store_true", help="Force refresh")

    args = parser.parse_args()
    args.print_help = parser.print_help

    return args


if __name__ == "__main__":

    args = parse_cmds_args()

    if args.cmd == "chutes":
        list_chute_gains()

    elif args.cmd == "scales":
        list_chute_servers()

    elif args.cmd == "deploy":
        deploy_chute(args.chute_id, args.server_id)

    elif args.cmd == "undeploy":
        undeploy_chute(args.id)

    elif args.cmd == "refresh-chute":
        refresh_chute(args.force)

    elif args.cmd == "recycle":
        recycle_deploy()

    else:
        main()
