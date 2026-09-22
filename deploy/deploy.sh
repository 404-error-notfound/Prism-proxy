#!/usr/bin/env bash
# 部署脚本（按变更类型选择更新策略）
# 用法：
#   deploy.sh <环境> [restart|config|build|stop|down]
#
#   restart 只改了代码（默认）。执行 docker compose restart 重启容器。
#   config  改了 .env 或 docker-compose.yml。强制重建容器以重新读取配置。
#   build   改了 Dockerfile 或 pyproject.toml。强制 build 镜像并重建容器。
#   stop    停止容器（保留容器，可 up/start 恢复）。
#   down    停止并销毁容器（维护时使用）。
#
#   兼容旧习惯：code / start / up 会被视为 restart。
# 环境对应文件：test -> .env.test，prod -> .env.prod，其余环境取 .env.<环境>（需存在）
# 在任意目录下执行均可，效果与在项目根目录手工执行 docker compose 完全一致
set -euo pipefail

ENV="${1:-}"
ACTION="${2:-restart}"

# 项目根目录（脚本位于 deploy/ 下，向上取一级）
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# 兼容旧写法：code / start / up 都视为 restart
if [[ "$ACTION" == "code" || "$ACTION" == "start" || "$ACTION" == "up" ]]; then
  ACTION="restart"
fi

# 部署日志落盘到 deploy/logs 目录（尽早设置，捕获全部输出）
LOG_DIR="$ROOT_DIR/deploy/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/deploy-${ENV:-unknown}-${ACTION}-$(date +%Y%m%d-%H%M%S).log"
exec > >(tee -a "$LOG_FILE") 2>&1
echo ">>> 部署日志: $LOG_FILE"

if [ -z "$ENV" ]; then
  echo "用法: $0 <环境> [restart|config|build|stop|down]，如: $0 test restart" >&2
  exit 1
fi

case "$ACTION" in
  restart | config | build | stop | down) ;;
  *)
    echo "用法: $0 <环境> [restart|config|build|stop|down]，如: $0 test restart" >&2
    exit 1
    ;;
esac

# 环境变量文件：prod -> .env.prod，其余环境直接对应 .env.<环境>
ENV_FILE="$ROOT_DIR/.env.$ENV"
if [ "$ENV" = "prod" ]; then
  ENV_FILE="$ROOT_DIR/.env.prod"
fi
if [ ! -f "$ENV_FILE" ]; then
  echo "[ERROR] 环境文件不存在: $ENV_FILE" >&2
  exit 1
fi

# 兼容 docker compose v2 / docker-compose v1
if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
else
  COMPOSE=(docker-compose)
fi

# 前置检查：docker 命令存在且当前用户有权访问守护进程，
# 否则 compose 会以一屏 urllib3 traceback 报 Permission denied，难定位
command -v docker >/dev/null 2>&1 || { echo "[ERROR] 未找到 docker 命令" >&2; exit 1; }
docker info >/dev/null 2>&1 || {
  echo "[ERROR] 无法访问 Docker 守护进程：当前用户（$(whoami)）无权限或 daemon 未运行。" >&2
  echo "        若是权限问题，请执行: sudo usermod -aG docker $(whoami) 并重新建立会话" >&2
  exit 1
}

# 固定项目目录执行，并强制指定 compose project 名称为 proxy-hub，
# 避免从 tag 命名目录部署时容器名、网络名带上该前缀。
cd "$ROOT_DIR"
export COMPOSE_PROJECT_NAME=proxy-hub
export COMPOSE_HTTP_TIMEOUT=300

# compose 服务列表（与 docker-compose.yml 一致），用于状态检测
SERVICES=(proxy-hub)

# 检测全部服务运行状态
check_all_running() {
  local failed=0
  for svc in "${SERVICES[@]}"; do
    local status
    # restart: always 下崩溃循环的容器 Running 也是 true（两次重启的间隙），
    # 必须看 Restarting 标志，否则发布脚本会把崩溃中的服务误判为正常
    status=$("${COMPOSE[@]}" ps --quiet "$svc" | xargs docker inspect -f '{{.State.Status}}{{if .State.Restarting}}(restarting){{end}}' 2>/dev/null | head -n1)
    if [[ "$status" != "running" ]]; then
      echo ">>> [异常] 服务 $svc 状态异常: ${status:-missing}"
      failed=1
    fi
  done
  return $failed
}

# 输出最终服务状态
report_status() {
  echo -e "\n>>> 最终运行状态："
  local fail_cnt=0
  for svc in "${SERVICES[@]}"; do
    local cid name status restarts
    cid=$("${COMPOSE[@]}" ps --quiet "$svc")
    if [[ -z "$cid" ]]; then
      echo "    [FAIL] $svc 无容器实例"
      fail_cnt=$((fail_cnt+1))
      continue
    fi
    name=$(docker inspect -f '{{.Name}}' "$cid")
    name="${name#/}"
    status=$(docker inspect -f '{{.State.Status}}{{if .State.Restarting}}(restarting){{end}}' "$cid")
    restarts=$(docker inspect -f '{{.RestartCount}}' "$cid")
    if [[ "$status" == "running" ]]; then
      echo "    [OK]   $svc 容器:$name 重启次数:$restarts"
    else
      echo "    [FAIL] $svc 容器:$name 状态:$status 重启次数:$restarts"
      echo "    ---- $svc 最近 20 行日志 ----"
      docker logs --tail 20 "$cid" 2>&1 | sed 's/^/    /' >&2 || true
      fail_cnt=$((fail_cnt+1))
    fi
  done
  if [[ "$fail_cnt" -eq 0 ]]; then
    echo ">>> 全部服务正常运行"
    return 0
  else
    echo ">>> [严重] 存在 $fail_cnt 个服务异常，请查看日志排查（docker logs <容器名>）" >&2
    return 1
  fi
}

# 清理历史 project 残留容器（如发布平台旧 tag 项目）：
# 这些容器不在当前 project 下，compose 不会接管；不清理会一直占用端口等资源。
# 按 compose 服务标签识别（不看 working_dir——平台可能在 packet 目录里启动容器），
# 只删"别家的"，当前 project 的容器一律不动。
clean_legacy_projects() {
  local current svc cid name
  current="$("${COMPOSE[@]}" ps -q 2>/dev/null || true)"
  for svc in "${SERVICES[@]}"; do
    for cid in $(docker ps -aq --filter "label=com.docker.compose.service=$svc"); do
      if grep -qF "$cid" <<<"$current"; then
        continue
      fi
      name="$(docker inspect -f '{{.Name}}' "$cid" 2>/dev/null || echo "$cid")"
      echo ">>> 清理历史项目容器: ${name#/}"
      docker rm -f "$cid" >/dev/null 2>&1 || true
    done
  done
}

# 删除本项目全部容器（含历史 tag project 残留），stop/down 停机时使用。
# 与 clean.sh 的兜底逻辑一致：按服务标签识别，不看 working_dir。
remove_all_containers() {
  local svc cid name
  for svc in "${SERVICES[@]}"; do
    for cid in $(docker ps -aq --filter "label=com.docker.compose.service=$svc"); do
      name="$(docker inspect -f '{{.Name}}' "$cid" 2>/dev/null || echo "$cid")"
      echo ">>> 删除容器: ${name#/}"
      docker rm -f "$cid" >/dev/null 2>&1 || true
    done
  done
}

# 等待 proxy-hub 健康检查通过（服务 warm_up 完成）。
# 容器 running ≠ 可服务：启动后 /healthz 可能有几秒不响应。
check_health() {
  local i
  if ! command -v curl >/dev/null 2>&1; then
    echo ">>> [警告] 未找到 curl，跳过健康检查"
    return 0
  fi
  echo ">>> 等待 proxy-hub 健康检查通过（最长 5 分钟）..."
  for i in $(seq 1 30); do
    if curl -fsS http://localhost:8100/healthz >/dev/null 2>&1; then
      echo ">>> proxy-hub 健康检查通过"
      return 0
    fi
    sleep 10
  done
  echo ">>> [严重] proxy-hub 健康检查超时（300s），请执行 ${COMPOSE[*]} logs proxy-hub 排查" >&2
  return 1
}

# 根据变更类型构造 up 参数
compose_up() {
  local extra_args=()
  case "$ACTION" in
    config)
      extra_args=(--force-recreate)
      ;;
    build)
      extra_args=(--build --force-recreate)
      ;;
  esac
  "${COMPOSE[@]}" up -d "${extra_args[@]}"
}

case "$ACTION" in
  restart)
    # 同步环境变量（compose env_file 读取 .env）
    cp "$ENV_FILE" "$ROOT_DIR/.env"
    echo "环境变量文件已就绪（来源：$(basename "$ENV_FILE")）："
    echo "  $ROOT_DIR/.env"
    echo ">>> 部署模式: $ACTION"

    clean_legacy_projects
    echo ">>> 执行 ${COMPOSE[*]} restart"
    "${COMPOSE[@]}" restart

    if ! report_status; then
      exit 1
    fi
    check_health
    ;;
  config | build)
    # 同步环境变量（compose env_file 读取 .env）
    cp "$ENV_FILE" "$ROOT_DIR/.env"
    echo "环境变量文件已就绪（来源：$(basename "$ENV_FILE")）："
    echo "  $ROOT_DIR/.env"
    echo ">>> 部署模式: $ACTION"

    max_retry=5
    clean_legacy_projects
    for ((attempt=1; attempt<=max_retry; attempt++)); do
      echo ">>> 第 $attempt/$max_retry 轮执行 ${COMPOSE[*]} up -d ${ACTION}"
      if compose_up; then
        echo ">>> up 执行完成，校验服务状态"
      else
        echo ">>> up 执行出现告警，等待5秒后重试"
      fi
      if check_all_running; then
        break
      fi
      sleep 5
    done

    if ! report_status; then
      exit 1
    fi
    check_health
    ;;
  stop)
    echo ">>> 执行停止操作 ${COMPOSE[*]} stop（保留容器，可 up/start 恢复）"
    "${COMPOSE[@]}" stop || true
    echo ">>> 容器已停止"
    ;;
  down)
    echo ">>> 执行销毁操作 ${COMPOSE[*]} down（会中断业务，仅维护使用）"
    # down 清理当前 proxy-hub project；历史 tag project 容器按服务标签兜底删除
    "${COMPOSE[@]}" down --remove-orphans || true
    remove_all_containers
    echo ">>> 所有容器已销毁"
    ;;
esac

echo ">>> 完成操作: $ENV $ACTION"
