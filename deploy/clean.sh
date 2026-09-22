#!/usr/bin/env bash
# 清空本项目全部容器（环境重置用）
# 用法：
#   clean.sh    停止并删除 proxy-hub project 下的全部 compose 容器，
#               以及历史 tag... 项目残留。镜像、数据卷（pip_cache）不动。
# 注意：执行后服务全部中断，需重新 deploy 才能恢复。
set -euo pipefail

# 项目根目录
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# 兼容 docker compose v2 / docker-compose v1
if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
else
  COMPOSE=(docker-compose)
fi

cd "$ROOT_DIR"
export COMPOSE_PROJECT_NAME=proxy-hub
export COMPOSE_HTTP_TIMEOUT=300

# 1) 先走 compose 正常 down：清理当前 project 的容器和网络
"${COMPOSE[@]}" down --remove-orphans || true

# 2) 再按 compose 服务标签兜底：历史 project（tag... 等）可能是在别的目录
#    （packet 解包目录）启动的，working_dir 标签不可靠；按服务名识别最稳
SERVICES=(proxy-hub)
for svc in "${SERVICES[@]}"; do
  for cid in $(docker ps -aq --filter "label=com.docker.compose.service=$svc"); do
    name="$(docker inspect -f '{{.Name}}' "$cid" 2>/dev/null || echo "$cid")"
    echo ">>> 删除容器: ${name#/}"
    docker rm -f "$cid" >/dev/null 2>&1 || true
  done
done

# 3) 验证：应无任何残留
leftover=""
for svc in "${SERVICES[@]}"; do
  leftover="$leftover $(docker ps -aq --filter "label=com.docker.compose.service=$svc")"
done
leftover="$(echo "$leftover" | tr -d '[:space:]')"
if [ -z "$leftover" ]; then
  echo ">>> 容器已全部清空（镜像与数据卷保留），重新部署: deploy.sh <环境> restart"
else
  echo ">>> [FAIL] 有容器未能删除，请手工检查" >&2
  exit 1
fi
