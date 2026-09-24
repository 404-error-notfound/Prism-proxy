#!/usr/bin/env bash
# 生产环境部署脚本（委托到 deploy.sh，避免逻辑重复）
#
# 用途：
#   生产部署的统一入口，等价于 deploy.sh prod <action>。
#   当前拓扑：proxy-hub 单服务（多副本主备靠同一 Redis + 分布式锁自动协调）。
#
# 用法：
#   deploy-prod.sh              等价于 deploy.sh prod restart
#   deploy-prod.sh config       等价于 deploy.sh prod config
#   deploy-prod.sh build        等价于 deploy.sh prod build
#   deploy-prod.sh stop         停止容器（保留容器）
#   deploy-prod.sh down         停止并销毁容器（维护时使用）
#
# 前置条件：
#   - 项目根目录存在 .env.prod（deploy.sh 会同步为 .env 供 compose env_file 读取）
#   - 其余前置条件同 deploy.sh（docker 可用、有守护进程权限）
set -euo pipefail

ACTION="${1:-restart}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# 用 bash 显式调用：脚本以 100644 入库（无执行位），发布平台 rsync 到服务器后
# 直接 exec 会 Permission denied，故不依赖文件执行权限
exec bash "$ROOT_DIR/deploy/deploy.sh" prod "$ACTION"
