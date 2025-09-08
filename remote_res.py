import os
import re
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

_API_URL = "https://api.chutes.ai"


async def _load_remote_objects(url: str, id_key: str):
    """
    从验证器刷新指定类型的资源（如chutes、images、instances等）到本地缓存。
    注：通过SSE（Server-Sent Events）接收资源更新，更新本地缓存字典。
    """
    async with aiohttp.ClientSession(raise_for_status=True) as session:
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


async def _load_remote_chutes():
    chutes = await _load_remote_objects(f"{_API_URL}/miner/chutes/", "chute_id")
    return chutes

async def _load_remote_metrics():
    metrics = await _load_remote_objects(f"{_API_URL}/miner/metrics/", "chute_id")
    return metrics

async def _load_utilization():
    items = {}
    async with aiohttp.ClientSession(raise_for_status=True) as session:
        # 从验证器API获取chute利用率
        async with session.get(f"{_API_URL}/chutes/utilization") as resp:
            data = await resp.json()
            for it in data:
                items[it["chute_id"]] = it
            
            return items

def _save_json_file(path: str, data: Any):
    path = os.path.abspath(path)
    jdat = json.dumps(data)
    with open(path, "wb") as f:
        f.write(jdat)

async def main():

    chutes = await _load_remote_chutes()
    _save_json_file("remote_chutes.json", chutes)
    print(f"Loaded remote chutes: {len(chutes)}")

    metrics = await _load_remote_metrics()
    _save_json_file("remote_metrics.json", metrics)
    print(f"Loaded remote metrics: {len(metrics)}")

    utils = await _load_utilization()
    _save_json_file("remote_utils.json", utils)
    print(f"Loaded remote utilization: {len(utils)}")

if __name__ == "__main__":
    asyncio.run(main())
