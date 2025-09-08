
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

class Gepetto:

    # region inin & run

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

    # region reconcile

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
                                logger.info(
                                    f"Updating chute {chute.chute_id} image from '{chute.image}' to '{update_data['image']}'"
                                )
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
                    remote_instance = (self.remote_instances.get(deployment.validator) or {}).get(
                        deployment.instance_id
                    )
                    if remote_instance:
                        if remote_instance.get("last_verified_at") and not deployment.verified_at:
                            deployment.verified_at = func.now()
                            logger.info(
                                f"Marking deployment {deployment.deployment_id} as verified based on remote status"
                            )
                        remote_active = remote_instance.get("active", True)
                        if deployment.active != remote_active:
                            deployment.active = remote_active
                            deployment.stub = False
                            logger.info(
                                f"Updating deployment {deployment.deployment_id} active status to {deployment.active}"
                            )

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
                if deployment.instance_id and deployment.instance_id not in (
                    self.remote_instances.get(deployment.validator) or {}
                ):
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
                if (deployment.stub or not deployment.instance_id) and deployment_age >= timedelta(minutes=30):
                    logger.warning(f"Deployment is still a stub after 30 minutes, deleting! {deployment.deployment_id}")
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
                                    logger.warning(
                                        f"Job pod terminated with error: {deployment.deployment_id}, exit_code={exit_code}"
                                    )
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
                            self.chute_deleted(
                                {"chute_id": chute.chute_id, "version": chute.version, "validator": chute.validator}
                            )
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
                                self.chute_created(
                                    {"chute_id": chute_id, "version": config["version"], "validator": validator}
                                )
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

    # region funcs

    async def chute_deleted(self, event_data: Dict[str, Any]):
        """
        A chute (or specific version of a chute) was removed from validator inventory.
        """
        chute_id = event_data["chute_id"]
        version = event_data["version"]
        validator = event_data["validator"]
        logger.info(f"Received chute_deleted event: {chute_id=} {version=}")
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

        logger.success(f"Handled chute_deleted event: {chute_id=} {version=}")

    async def instance_deleted(self, event_data: Dict[str, Any]):
        """
        An instance was removed validator side, likely meaning there were too
        many consecutive failures in inference.
        """
        instance_id = event_data["instance_id"]
        logger.info(f"Received instance_deleted event for {instance_id=}")
        async with get_session() as session:
            deployment = (
                (await session.execute(select(Deployment).where(Deployment.instance_id == instance_id)))
                .unique()
                .scalar_one_or_none()
            )
        if deployment:
            await self.undeploy(deployment.deployment_id)
        logger.info(f"Finished processing instance_deleted event for {instance_id=}")

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

    # endregion


async def main():
    gepetto = Gepetto()
    await gepetto.reconcile()


if __name__ == "__main__":
    asyncio.run(main())
