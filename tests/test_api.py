"""API 路由测试：项目 token 鉴权、admin 鉴权、热配置校验、项目管理、pull 端点。

httpx ASGITransport 不触发 lifespan（不连 Redis/供应商），
Redis 交互由 conftest 的 FakeRedis stub 接管。
"""
import time

import pytest
from httpx import ASGITransport, AsyncClient

from proxy_hub import pool, projects
from proxy_hub.config import settings
from proxy_hub.main import app


@pytest.fixture
def client():
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


@pytest.fixture
async def project_token():
    proj = await projects.create_project("testproj")
    return proj["token"]


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def test_healthz(client):
    resp = await client.get("/healthz")
    assert resp.status_code == 200 and resp.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# 消费端点：项目 token 鉴权
# ---------------------------------------------------------------------------

async def test_consumer_endpoints_require_token(client):
    assert (await client.get("/proxy")).status_code == 403
    assert (await client.get("/proxy", headers=_bearer("bogus"))).status_code == 403
    assert (await client.post("/activity")).status_code == 403
    assert (await client.get("/status")).status_code == 403


async def test_proxy_empty_pool_503(client, project_token):
    resp = await client.get("/proxy", headers=_bearer(project_token))
    assert resp.status_code == 503


async def test_pull_proxy_from_pool(client, fake_redis, project_token):
    pool._pools["testproj"] = [("http://1.2.3.4:8000", time.time() + 120)]
    resp = await client.get("/proxy", headers=_bearer(project_token))
    assert resp.status_code == 200
    assert resp.json()["proxy"] == "http://1.2.3.4:8000"


async def test_pull_proxy_with_target(client, fake_redis, project_token):
    pool._pools["testproj"] = [("http://1.2.3.4:8000", time.time() + 120)]
    resp = await client.get("/proxy?target=baidu.com", headers=_bearer(project_token))
    assert resp.status_code == 200
    assert "baidu.com" in pool._target_hist["testproj"]["http://1.2.3.4:8000"]


async def test_activity_and_status(client, fake_redis, project_token):
    assert (await client.post("/activity",
                              headers=_bearer(project_token))).status_code == 204
    assert await pool.has_recent_activity("testproj", 60) is True
    resp = await client.get("/status", headers=_bearer(project_token))
    assert resp.status_code == 200
    body = resp.json()
    assert body["project"] == "testproj"
    assert body["pool_size"] == 0
    assert body["tier"] in ("peak", "normal", "offpeak")


async def test_refresh_response_shape(client, fake_redis, project_token):
    """/refresh 响应：ok（池非空）+ executed（真正换批）+ waited（等过锁），
    wait_s 超限（>30）被 422 拒绝。"""
    resp = await client.post("/refresh", headers=_bearer(project_token),
                             json={"query_id": "q1", "wait_s": 0})
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is False and body["pool_size"] == 0
    assert body["executed"] is False and body["waited"] is False
    assert (await client.post("/refresh", headers=_bearer(project_token),
                              json={"wait_s": 31})).status_code == 422


async def test_block_signal_endpoint(client, fake_redis, project_token):
    """/block-signal：按近期建联映射对 IP 打目标站拉黑标记（不直接剔除）；
    无映射时全 0 不报错。"""
    from proxy_hub import forwarder
    from proxy_hub.redis_client import redis_key

    pool._block_kill_ts.clear()
    pool._target_blocked.clear()
    forwarder._recent_conns.clear()
    await fake_redis.zadd(redis_key(pool.K_POOL.format(project="testproj")),
                          {"http://1.2.3.4:8000": time.time() + 120,
                           "http://5.6.7.8:8000": time.time() + 120})
    await pool.sync_project("testproj")
    forwarder.note_conn("testproj", "www.baidu.com", "http://1.2.3.4:8000")

    resp = await client.post("/block-signal", headers=_bearer(project_token),
                             json={"target": "baidu", "query_id": "q1"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["marked"] == 1 and body["killed"] == 0
    # 只标记不剔除：IP 还在池里，但不再派给 baidu
    assert pool.get_proxy_urls("testproj") == ["http://1.2.3.4:8000",
                                               "http://5.6.7.8:8000"]
    assert "baidu" in pool._target_blocked["testproj"]["http://1.2.3.4:8000"]

    # 信号滞后（IP 已不在池）：全 0，不报错
    forwarder._recent_conns.clear()
    forwarder.note_conn("testproj", "www.baidu.com", "http://9.9.9.9:8000")
    resp = await client.post("/block-signal", headers=_bearer(project_token),
                             json={"target": "baidu"})
    assert resp.status_code == 200
    assert resp.json()["marked"] == 0 and resp.json()["killed"] == 0
    # 缺 target：422
    assert (await client.post("/block-signal", headers=_bearer(project_token),
                              json={})).status_code == 422


async def test_signal_body_query_id_and_text_logged(client, fake_redis,
                                                    project_token, caplog):
    """信号 body 的 query_id/query 落日志：query_id 顶层字段 term 可达，
    query（查询词）进 metrics 供模糊检索。"""
    import logging

    with caplog.at_level(logging.INFO, logger="proxy_hub.api.routes"):
        resp = await client.post(
            "/activity",
            headers={**_bearer(project_token), "Content-Type": "application/json"},
            json={"query_id": "q-abc", "query": "Python asyncio 最佳实践"},
        )
    assert resp.status_code == 204
    rec = next(r for r in caplog.records if "/activity" in r.getMessage())
    assert rec.query_id == "q-abc"
    assert rec.metrics["query"] == "Python asyncio 最佳实践"
    assert "Python asyncio" in rec.getMessage()


# ---------------------------------------------------------------------------
# Admin：热配置
# ---------------------------------------------------------------------------

async def test_admin_requires_token(client, monkeypatch):
    monkeypatch.setattr(settings, "admin_token", "secret")
    assert (await client.get("/admin/config")).status_code == 403
    assert (await client.put("/admin/config", json={})).status_code == 403
    ok = await client.get("/admin/config", headers=_bearer("secret"))
    assert ok.status_code == 200
    assert "effective" in ok.json() and "overrides" in ok.json()


async def test_admin_put_validation(client, monkeypatch):
    monkeypatch.setattr(settings, "admin_token", "secret")
    headers = _bearer("secret")
    bad = await client.put("/admin/config", json={"schedule": "垃圾"}, headers=headers)
    assert bad.status_code == 400
    good = await client.put(
        "/admin/config",
        json={"schedule": "peak=08:00-22:00,offpeak=22:00-08:00"},
        headers=headers,
    )
    assert good.status_code == 200
    assert good.json()["effective"]["schedule"] == "peak=08:00-22:00,offpeak=22:00-08:00"


# ---------------------------------------------------------------------------
# Admin：项目管理
# ---------------------------------------------------------------------------

async def test_admin_project_crud(client, fake_redis, monkeypatch):
    monkeypatch.setattr(settings, "admin_token", "secret")
    headers = _bearer("secret")

    created = await client.post("/admin/projects",
                                json={"name": "crawler", "pool_size": 8},
                                headers=headers)
    assert created.status_code == 201
    token = created.json()["token"]
    assert len(token) > 20

    # 查重
    dup = await client.post("/admin/projects", json={"name": "crawler"},
                            headers=headers)
    assert dup.status_code == 400

    # 非法名字
    bad = await client.post("/admin/projects", json={"name": "Bad Name"},
                            headers=headers)
    assert bad.status_code == 400

    # 非法 pool_type
    bad_type = await client.post("/admin/projects",
                                 json={"name": "x1", "pool_type": "bogus"},
                                 headers=headers)
    assert bad_type.status_code == 400

    # pool_type=exclusive 注册成功并在列表返回；缺省为 shared
    ex = await client.post("/admin/projects",
                           json={"name": "exp", "pool_type": "exclusive"},
                           headers=headers)
    assert ex.status_code == 201
    assert ex.json()["pool_type"] == "exclusive"
    assert created.json()["pool_type"] == "shared"

    listed = await client.get("/admin/projects", headers=headers)
    assert set(listed.json()["projects"]) == {"crawler", "exp"}
    assert listed.json()["projects"]["exp"]["pool_type"] == "exclusive"

    # 项目 token 立即可用于消费端点
    assert (await client.post("/activity", headers=_bearer(token))).status_code == 204

    # 删除（顺带清池）
    pool._pools["crawler"] = [("http://x:1", time.time() + 120)]
    assert (await client.delete("/admin/projects/crawler",
                                headers=headers)).status_code == 200
    assert (await client.delete("/admin/projects/crawler",
                                headers=headers)).status_code == 404
    assert pool.pool_size("crawler") == 0
    assert (await client.post("/activity", headers=_bearer(token))).status_code == 403


async def test_admin_projects_require_admin(client, monkeypatch):
    monkeypatch.setattr(settings, "admin_token", "secret")
    assert (await client.post("/admin/projects", json={"name": "x"})).status_code == 403
    assert (await client.get("/admin/projects")).status_code == 403
