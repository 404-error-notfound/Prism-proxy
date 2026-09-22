# ----------------------------------------------------------------
# proxy-hub — 与 prism 保持一致的开发/部署镜像约定
# 基于 python:3.11-slim（Debian Bookworm，走 daocloud 镜像加速）
# 代码通过 volume 挂载，uvicorn --reload 热重载，改代码无需重新 build
# 注意：不要加 `# syntax=docker/dockerfile:1` 指令——BuildKit 会去 docker.io
# 拉前端镜像，国内镜像加速器常拉不动导致构建直接失败；内置前端已支持 --mount
# ----------------------------------------------------------------
FROM docker.m.daocloud.io/library/python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Asia/Shanghai

# ---- 切换阿里云 apt 镜像（加速国内构建）----
RUN sed -i 's|deb.debian.org|mirrors.aliyun.com|g' /etc/apt/sources.list.d/debian.sources 2>/dev/null \
    || sed -i 's|deb.debian.org|mirrors.aliyun.com|g' /etc/apt/sources.list 2>/dev/null \
    || true

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ---- Python 依赖（阿里云 pypi 镜像 + pip 缓存挂载）----
COPY pyproject.toml .
COPY proxy_hub ./proxy_hub
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install \
    -i https://mirrors.aliyun.com/pypi/simple/ \
    --trusted-host mirrors.aliyun.com \
    -e ".[dev]"

EXPOSE 8100 8101

# 热重载启动（volume 挂载代码后生效；生产可在 compose 里去掉 --reload）
CMD ["uvicorn", "proxy_hub.main:app", "--host", "0.0.0.0", "--port", "8100", "--reload", "--reload-dir", "proxy_hub"]
