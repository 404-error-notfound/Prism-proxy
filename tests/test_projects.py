"""项目注册表测试：创建/校验/查重/token 解析/删除。"""
import pytest

from proxy_hub import projects


async def test_create_and_resolve(fake_redis):
    proj = await projects.create_project("crawler", pool_size=8)
    assert proj["name"] == "crawler"
    assert proj["pool_size"] == 8
    assert len(proj["token"]) > 20
    assert await projects.resolve_token(proj["token"]) == "crawler"
    assert await projects.resolve_token("bogus") is None


async def test_default_pool_size_zero_follows_tier(fake_redis):
    proj = await projects.create_project("p1")
    assert proj["pool_size"] == 0


async def test_pool_type_default_and_explicit(fake_redis):
    """pool_type 缺省 shared；exclusive 显式注册；老项目 JSON 无字段按 shared。"""
    shared = await projects.create_project("p1")
    assert shared["pool_type"] == "shared"
    ex = await projects.create_project("p2", pool_type="exclusive")
    assert ex["pool_type"] == "exclusive"
    with pytest.raises(ValueError, match="pool_type"):
        await projects.create_project("p3", pool_type="bogus")
    # 老项目（JSON 无 pool_type 字段）按 shared 处理
    assert projects.pool_type_of({"token": "x"}) == "shared"
    assert projects.pool_type_of(None) == "shared"


async def test_duplicate_rejected(fake_redis):
    await projects.create_project("p1")
    with pytest.raises(ValueError, match="already exists"):
        await projects.create_project("p1")


@pytest.mark.parametrize("bad", ["", "Bad", "-x", "x" * 33, "has space", "中文"])
async def test_invalid_names_rejected(fake_redis, bad):
    with pytest.raises(ValueError, match="invalid project name"):
        await projects.create_project(bad)


async def test_list_and_delete(fake_redis):
    await projects.create_project("a")
    await projects.create_project("b")
    table = await projects.list_projects()
    assert set(table) == {"a", "b"}
    assert await projects.delete_project("a") is True
    assert await projects.delete_project("a") is False  # 已删
    assert set(await projects.list_projects()) == {"b"}


async def test_token_resolution_after_cache_bust(fake_redis):
    """创建后缓存即失效，resolve 立刻可见（隧道认证不能等 10s）。"""
    proj = await projects.create_project("fresh")
    assert await projects.resolve_token(proj["token"]) == "fresh"
