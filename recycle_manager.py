import argparse
import os
import sys
import threading
import uuid
import aiohttp
import asyncio
import orjson as json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from loguru import logger
from typing import Any, Optional
from sqlalchemy import select, func, case, text, or_, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from api.config import settings, validator_by_hotkey, Validator
from api.redis_pubsub import RedisListener
from api.auth import sign_request
from api.config import k8s_core_client
from api.database import get_session, engine, Base
from api.chute.schemas import Chute
from api.server.schemas import Server
from api.gpu.schemas import GPU
from api.deployment.schemas import Deployment
from api.exceptions import DeploymentFailure

# from remote_api import RemoteApi


class RecycleManager:

    _SECS_RECYCLE = 300

    def __init__(self, remote_chutes: dict[str, dict]):
        self._remote_chutes = remote_chutes

    # region public

    async def run(self):
        pass

    async def recycle(self) -> None:
        pass

    # endregion

    # region static

    @staticmethod
    async def clean_nonexisted_services() -> None:
        try:
            async with get_session() as db:
                dids = (await db.execute(select(Deployment.deployment_id))).unique().scalars().all()

            ns = settings.namespace
            ks = k8s_core_client()
            svcs = ks.list_namespaced_service(
                namespace=ns,
                label_selector="chutes/deployment-id",
            )

            for svc in svcs.items:
                name = svc.metadata.name
                depl = svc.metadata.labels.get("chutes/deployment-id")
                if name and depl and depl not in dids:
                    ks.delete_namespaced_service(
                        namespace=ns,
                        name=name,
                    )
                    logger.info(f"Deleted service {name}")

        except Exception as ex:
            logger.error(f"Failed to clean chute services:\n{ex}")

    @staticmethod
    async def clean_nonexisted_configmaps() -> None:
        try:
            async with get_session() as db:
                chutes = (await db.execute(select(Chute))).unique().scalars().all()

            codes = ["chute-code-" + str(uuid.uuid5(uuid.NAMESPACE_OID, f"{ch.chute_id}::{ch.version}")) for ch in chutes]

            ns = settings.namespace
            ks = k8s_core_client()
            cms = ks.list_namespaced_config_map(
                namespace=ns,
                label_selector="chutes/chute-id",
            )
            for cm in cms.items:
                name = cm.metadata.name
                # cid = cm.metadata.labels.get("chutes/chute-id")
                # ver = cm.metadata.labels.get("chutes/version")
                if name and name not in codes:
                    ks.delete_namespaced_config_map(
                        namespace=ns,
                        name=name,
                    )
                    logger.info(f"Deleted configmap {name}")

        except Exception as ex:
            logger.error(f"Failed to clean configmaps:\n{ex}")

    # endregion


def test_cases():

    _VLD_KEY = settings.validators[0].hotkey

    async def _test_clean_nonexisted_services():
        await RecycleManager.clean_nonexisted_services()

    async def _test_clean_nonexisted_configmaps():
        await RecycleManager.clean_nonexisted_configmaps()

    # -----------------------------------------
    return [
        _test_clean_nonexisted_services,
        _test_clean_nonexisted_configmaps,
    ]


async def run_tests(stop_on_error: bool = True):

    logger.info(f"Testing {__file__} ...")

    tests = test_cases()

    tot = len(tests)
    idx = 0
    psd = 0
    for test in tests:

        idx += 1
        name = test.__name__
        try:
            logger.info(f"Testing {idx}/{tot}: {name}")
            await test()
            psd += 1
        except Exception as ex:
            logger.error(f"{name} failed:\n{ex}")
            if stop_on_error:
                break

    logger.info(f"Total {tot} tests: {idx} tested, {psd} passed.")


if __name__ == "__main__":
    asyncio.run(run_tests(False))

# copy to gepetto pod：
# scp debugging/recycle_manager.py cpu0:~/new/debug && ssh cpu0 "microk8s kubectl cp ~/new/debug/recycle_manager.py chutes/gepetto-78644499f9-lvqb9:/tmp/"
# exec sh gepetto pod：
# export PYTHONPATH="${PYTHONPATH}:/tmp"    # 即："/app:/tmp"
# poetry run python /tmp/recycle_manager.py
