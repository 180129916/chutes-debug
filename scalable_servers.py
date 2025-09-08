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

_API_URL = settings.validators[0].api  # "https://api.chutes.ai"
_VLD_KEY = settings.validators[0].hotkey


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


async def _load_local_chute(chute_id: str, version: str, validator: str):
    async with get_session() as session:
        return (
            await session.execute(
                select(Chute)
                .where(Chute.chute_id == chute_id)
                .where(Chute.version == version)
                .where(Chute.validator == validator)
            )
        ).scalar_one_or_none()


async def _calc_chute_gains(validator: str, rmt_chutes: dict, rmt_metrics: dict, rmt_utilizs: dict) -> list[dict]:

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

            loc_chute = await _load_local_chute(chute_id, chute_version, validator)
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
            print(f"Error handling chute {chute_id}: {ex}")
            continue

    chute_gains = sorted(chute_gains, key=lambda x: x["potential_gain"], reverse=True)

    return chute_gains


async def _find_suitable_server(gpu_list, gpu_count, disk_gb: int = 100) -> Optional[Server]:

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


async def _list_chutes_servers(chute_gains: list) -> list[dict]:

    cht_svrs = []
    for chute in chute_gains:
        try:
            gpu_list = chute["gpu_supported"]
            gpu_count = chute["gpu_count"]

            suit_svr = await _find_suitable_server(gpu_list, gpu_count)
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


def _save_json_file(path: str, data: Any):
    path = os.path.abspath(path)
    jdat = json.dumps(data)
    with open(path, "wb") as f:
        f.write(jdat)


def _print_chute_gains(gains: list):

    print(f"{'chute_id':<37} {'chute_name':<32} {'gain':<10} {'ins_cnt':<8} {'gpu_supported':<10} | {'gpu_count'}")
    print("-" * 160)

    for it in gains:
        print(
            f"{it['chute_id']:<37} "
            f"{it['chute_name'][:30]:<30} "
            f"{it['potential_gain']:>10.2f} "
            f"{it['instance_count']:>6}  "
            f"{','.join(it['gpu_supported'])}  | "
            f"{it['gpu_count']}"
        )


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


def _print_chute_servers(items: list):

    print(
        f"{'chute_id':<37} {'chute_name':<35}  {'server_id':<37} {'server_name':<20} {'value':>10} {'cost':>6}  {'gpu'}"
    )
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


async def main():

    print("Loading remote resources...")

    rmt_chutes = await _load_remote_chutes()
    print(f"Loaded remote chutes: {len(rmt_chutes)}")

    rmt_metrics = await _load_remote_metrics()
    print(f"Loaded remote metrics: {len(rmt_metrics)}")

    rmt_utils = await _load_utilization()
    print(f"Loaded remote utilization: {len(rmt_utils)}")

    print("Calculating chute gains...")

    cht_gains = await _calc_chute_gains(_VLD_KEY, rmt_chutes, rmt_metrics, rmt_utils)
    print(f"Calculated chute gains: {len(cht_gains)}")

    if len(cht_gains) == 0:
        return

    _print_chute_gains(cht_gains)

    print("Matching scalable servers...")

    cht_svrs = await _list_chutes_servers(cht_gains)
    print(f"Matched scalable servers: {len(cht_svrs)}")

    if len(cht_svrs) == 0:
        return

    _save_json_file("scalable_servers.json", cht_svrs)
    _print_chute_servers(cht_svrs)


if __name__ == "__main__":
    asyncio.run(main())
