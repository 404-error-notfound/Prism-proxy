"""proxy-hub 配置 —— 全部经环境变量（PROXY_HUB_ 前缀）注入。

运行期可热更新的项（时段表/三档参数/自适应/判死轮数）不在这里逐条定义，
见 proxy_hub/schedule.py 的 RuntimeConfig：env 提供默认值，Redis config:runtime 覆盖。
"""
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict

# 出站探测统一使用的真实 Chrome UA：非浏览器 UA 易被目标站风控。
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="PROXY_HUB_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # 基础
    app_env: str = "dev"
    log_level: str = "INFO"
    host: str = "0.0.0.0"
    port: int = 8100

    # ES 日志落库（可选）：结构化 JSON 日志经内存队列异步批量写独立索引 proxyhub_logs
    log_es_enabled: bool = False
    log_es_url: str = "http://localhost:9200"
    log_es_index: str = "proxyhub_logs"
    log_es_min_level: str = "INFO"
    log_es_batch_size: int = 100
    log_es_flush_interval_s: float = 5.0
    # ES 集群鉴权（开了 security 的集群必填，否则 shipper 全 401）
    log_es_user: str = ""
    log_es_password: str = ""

    # Redis（主备协调、共享池、热配置、成本埋点都依赖它）
    redis_url: str = "redis://localhost:6379/0"
    redis_password: str = ""
    redis_key_prefix: str = "proxyhub:"
    redis_cluster_mode: bool = False

    # Admin API 鉴权（Bearer token）。为空 = admin 端点全部拒绝（安全默认）。
    admin_token: str = ""

    # 供应商（provider 插件，首发 qgnet）
    provider: str = "qgnet"
    # 提取 IP 的 API 地址（空 = 禁用抓取，服务退化为只读/只推已有池）
    provider_api_url: str = ""
    # 供应商账密（qg.net Authkey/Authpwd）：填了则代理 URL 用 http://key:pwd@server；
    # 留空则裸连 http://server（走 IP 白名单）。二选一
    provider_auth_key: str = ""
    provider_auth_pwd: str = ""

    # 青果独享代理（可选产品；EXCLUSIVE_API_URL + EXCLUSIVE_AUTH_KEY 配置才启用，
    # 缺省 = 独享功能关闭，项目 pool_type=exclusive 时补位跳过只记 warn）。
    # 独享 API 与短效共享是不同接口集（提取IP/查询在用IP/释放IP/更换IP，
    # 基址 https://exclusive.proxy.qg.net，文档 doc/1843）。
    exclusive_api_url: str = ""        # 独享 API 链接（控制台生成，自带 ?key=...）
    # 注意：这两个是**代理建连账密**（青果控制台"代理验证-账密"，独享代理不带
    # 账密建连返回 407），不是 API key——API 鉴权的 key= 已含在 API 链接里
    exclusive_auth_key: str = ""
    exclusive_auth_pwd: str = ""
    # 独享 IP 默认租期（秒）：仅当提取响应缺到期时间字段时用（正常以响应为准）
    exclusive_default_ttl_s: float = 86400.0
    # 独享池体检探针（轻量中立连通性探测，204 空响应无反爬）：独享 IP 7×24
    # 被反复打搜索引擎探测页等于主动烧自己，独享池改用它且禁用 slow-as-dead
    exclusive_health_url: str = "https://connect.rom.miui.com/generate_204"

    # 自建 HTTP 转发代理（隧道模式）：proxy-hub 自己监听此端口说代理协议
    # （CONNECT / 普通 HTTP），按项目账密（Proxy-Authorization: user=项目名, pwd=项目
    # token）认证，每连接按目标站防重轮换挑上游 IP。0 = 关闭转发（纯 Pull 模式）。
    forward_port: int = 8101
    # 转发代理并发连接上限（超出排队）
    forward_max_conns: int = 512

    # 健康探测目标（三态：能搜索/能出网/死）。marker 为逗号分隔的字节串，
    # 命中任一 marker 或响应体大于 health_min_body_bytes 即判"能搜索"。
    health_check_url: str = "https://cn.bing.com/search?q=新闻"
    health_check_markers: str = 'id="b_results",class="b_algo"'
    health_check_timeout_s: float = 4.0
    health_min_body_bytes: int = 20000
    health_user_agent: str = DEFAULT_USER_AGENT
    # 体检延迟门槛（秒）：探测全程超过此值按死处理，对齐转发层 connect 预算
    # （forwarder 2s）。0 = 不启用。防"体检放行慢 IP、隧道里必超时"
    health_max_latency_s: float = 2.0

    # 刷新失败冷却（秒）：期间不再重试供应商 API，防止供应商故障时狂抽
    fail_cooldown_s: float = 15.0
    # 分布式锁 TTL（秒）：持锁者宕机后其他副本在此时间内补位
    refresh_lock_ttl_s: int = 60
    # 供应商 API 请求超时（秒）
    provider_api_timeout_s: float = 10.0
    # IP 有效期兜底（秒）：仅当供应商响应的 deadline 缺失/解析失败时使用。
    # 正常路径每个 IP 的过期时刻以供应商返回的 deadline 为准，换套餐无需改这里
    ip_ttl_fallback_s: float = 60.0


settings = Settings()
