"""HTTP API —— 消费端点（项目 token 鉴权）+ admin 热配置 / 项目管理。"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import BaseModel, Field, ValidationError

from proxy_hub import pool, projects, runtime_config
from proxy_hub.config import settings

logger = logging.getLogger(__name__)
router = APIRouter()


class SignalBody(BaseModel):
    """消费方信号的可选 body：把查询 ID/查询词带过来做日志关联。

    query_id = 精确锚点（uuid，term 检索直达该查询的刷新/活跃）；
    query = 人可读锚点（查询词，模糊检索——不必先翻消费方日志复制 uuid）。
    """

    query_id: str | None = Field(default=None, max_length=64)
    query: str | None = Field(default=None, max_length=256)


class RefreshBody(SignalBody):
    """refresh 专用信号。wait_s：维护锁被占时服务端最多等待秒数——持锁方
    正在两阶段刷新（≈fetch+probe 3-5s），等它落地胜过自己再抽一批；等锁
    成功且池已可下发时直接复用持锁方成果，防等待方扎堆各抽一批烧配额。"""

    wait_s: float = Field(default=0.0, ge=0.0, le=30.0)


class BlockSignalBody(SignalBody):
    """风控信号：消费方引擎层识别到"连得上但被目标站风控（验证码/秒拒）"。

    target = 引擎名片段（如 "baidu"/"quark"），服务端按 forwarder 近期建联
    映射（子串匹配，近 30s）关联回当时用的上游 IP，即时剔除出池。"""

    target: str = Field(min_length=1, max_length=64)


def _log_signal(endpoint: str, project: str, body: SignalBody | None) -> None:
    """信号落日志：query_id 顶层字段（term 可达）+ query 进 metrics（模糊可搜）。"""
    if not (body and body.query_id):
        return
    text = (body.query or "").strip()
    metrics: dict[str, Any] = {"project": project, "signal": endpoint.lstrip("/")}
    if text:
        metrics["query"] = text[:200]
    logger.info(
        "api: %s project=%s query_id=%s%s",
        endpoint, project, body.query_id,
        f" query='{text[:60]}'" if text else "",
        extra={"query_id": body.query_id, "metrics": metrics},
    )


# ---------------------------------------------------------------------------
# 消费端点（项目 token：Authorization: Bearer <项目token>）
# ---------------------------------------------------------------------------

async def _require_project(authorization: str = Header(default="")) -> str:
    token = authorization.removeprefix("Bearer ").strip()
    if not token:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                            detail="project token required")
    project = await projects.resolve_token(token)
    if project is None:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                            detail="invalid project token")
    return project


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/proxy")
async def get_proxy(project: str = Depends(_require_project),
                    target: str = "") -> dict[str, Any]:
    """取一个当前可用代理 URL（pull 模式）。池空返回 503。

    可选 target 参数声明目标站（如 target=baidu.com），优先发没服务过该
    目标的 IP（防重轮换）；target 命中独享名单时自动路由独享池。
    """
    if not pool.pool_size(project):
        await pool.sync_project(project)  # 本地镜像空才回源（每 5s tick 也会同步）
    url = await pool.pick_proxy_routed(project, target)
    if url is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail="proxy pool is empty")
    return {"proxy": url}


@router.get("/proxies")
async def list_proxies(project: str = Depends(_require_project)) -> dict[str, Any]:
    await pool.sync_project(project)
    return {"proxies": pool.get_proxy_urls(project)}


@router.post("/activity", status_code=status.HTTP_204_NO_CONTENT)
async def activity(project: str = Depends(_require_project),
                   body: SignalBody | None = None) -> None:
    """消费方活跃信号（建议进程内节流 ≥10s/次）。驱动 demand-driven 补位与 QPS 升档。
    走隧道的消费方不需要调——建连即隐式活跃。

    可选 body {"query_id": "...", "query": "..."}：query_id 精确关联消费方日志，
    query（查询词）让人可直接在日志里搜到这次信号（跨链路排查用）。"""
    _log_signal("/activity", project, body)
    await pool.record_activity(project)


@router.post("/refresh")
async def force_refresh(project: str = Depends(_require_project),
                        body: RefreshBody | None = None) -> dict[str, Any]:
    """强制换批（两阶段替换）：新批次补满入池后再淘汰旧 IP，刷新期间池子不空。

    锁被占（别的请求正在刷）时不秒拒：按 body.wait_s 等待持锁方落地；补位撞
    供应商失败冷却且旧池不可下发时，等短冷却窗口过后再试一次。响应含
    executed（本次真正换批了吗）/waited（等过锁吗），消费方据此外化
    "等到新 IP 再用"的语义。可选 body {"query_id": ..., "query": ...,
    "wait_s": ...}：query_id 落日志字段，与该查询在消费方侧的日志精确关联。"""
    _log_signal("/refresh", project, body)
    result = await pool.refresh(project, wait_s=body.wait_s if body else 0.0)
    return {"ok": result.ok, "pool_size": pool.pool_size(project),
            "executed": result.executed, "waited": result.waited}


@router.post("/block-signal")
async def block_signal(project: str = Depends(_require_project),
                       body: BlockSignalBody | None = None) -> dict[str, Any]:
    """风控信号（消费方引擎层上报"被目标站验证码/秒拒"）：按 forwarder 近期
    建联映射关联出当时服务该目标站的上游 IP，**按目标站拉黑标记**——该 IP
    只是不再派给这个站，其他站照常用；同一 IP 被 ≥3 个目标站风控才真剔出池
    （每项目每分钟限剔 3 个）。响应 marked/killed 分别为本次标记/剔除数
    （全 0 = 信号滞后/未命中映射，属正常）。"""
    from proxy_hub import forwarder

    if body is None:
        raise HTTPException(status_code=422, detail="target is required")
    _log_signal("/block-signal", project, body)
    marked = killed = 0
    for url in forwarder.recent_upstreams(project, body.target):
        action = await pool.record_block(project, url, body.target)
        marked += action == "marked"
        killed += action == "killed"
    return {"ok": True, "marked": marked, "killed": killed,
            "pool_size": pool.pool_size(project)}


@router.get("/status")
async def get_status(project: str = Depends(_require_project)) -> dict[str, Any]:
    await pool.sync_project(project)
    return await pool.status_snapshot(project)


# ---------------------------------------------------------------------------
# Admin 端点（Bearer token）
# ---------------------------------------------------------------------------


def _require_admin(authorization: str = Header(default="")) -> None:
    token = authorization.removeprefix("Bearer ").strip()
    if not settings.admin_token or token != settings.admin_token:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="admin access required")


@router.get("/admin/config", dependencies=[Depends(_require_admin)])
async def get_config() -> dict[str, Any]:
    return await runtime_config.get_effective_view()


@router.put("/admin/config", dependencies=[Depends(_require_admin)])
async def put_config(patch: dict[str, Any]) -> dict[str, Any]:
    """部分更新热配置。非法值（含时段表格式错误）400 拒绝，不落盘。"""
    try:
        cfg = await runtime_config.set_runtime_config(patch)
    except ValidationError as e:
        # errors() 的 ctx 里可能带 ValueError 对象，不可 JSON 序列化，剔除后再返回
        detail = [
            {"loc": err.get("loc"), "msg": err.get("msg"), "type": err.get("type")}
            for err in e.errors(include_context=False)
        ]
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=detail) from e
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
    return {"ok": True, "effective": cfg.model_dump()}


@router.delete("/admin/config", dependencies=[Depends(_require_admin)])
async def delete_config() -> dict[str, bool]:
    """清除全部覆盖，回 env 默认（当前值入审计，可 rollback 找回）。"""
    await runtime_config.clear_runtime_config()
    return {"ok": True}


@router.post("/admin/config/rollback", dependencies=[Depends(_require_admin)])
async def rollback_config() -> dict[str, Any]:
    cfg = await runtime_config.rollback_runtime_config()
    return {"ok": True, "effective": cfg.model_dump()}


# ---------------------------------------------------------------------------
# Admin 项目管理
# ---------------------------------------------------------------------------


@router.post("/admin/projects", dependencies=[Depends(_require_admin)],
             status_code=status.HTTP_201_CREATED)
async def create_project(body: dict[str, Any]) -> dict[str, Any]:
    """注册项目。token 仅此一次完整返回，请当场保存。

    pool_type: "shared"（默认，短效共享池）| "exclusive"（独享池，需配置
    PROXY_HUB_EXCLUSIVE_* 三件套才实际补位）。"""
    name = str(body.get("name") or "")
    pool_size = int(body.get("pool_size") or 0)
    pool_type = str(body.get("pool_type") or "shared")
    try:
        return await projects.create_project(name, pool_size, pool_type)
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e


@router.get("/admin/projects", dependencies=[Depends(_require_admin)])
async def list_projects() -> dict[str, Any]:
    return {"projects": await projects.list_projects()}


@router.delete("/admin/projects/{name}", dependencies=[Depends(_require_admin)])
async def delete_project(name: str) -> dict[str, bool]:
    """删除项目：注销 token + 清池。不存在的项目 404。"""
    if not await projects.delete_project(name):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"project {name!r} not found")
    await pool.delete_pool(name)
    return {"ok": True}
