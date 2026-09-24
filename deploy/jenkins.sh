#!/bin/bash
set -e

# CI 部署脚本（Jenkins 等构建系统通用）
#
# 用途：
#   CI 构建入口：按分支确定部署环境 → rsync 推送代码
#   → 远程执行 deploy/deploy.sh <环境> restart（按 APP_ENV 选择 .env.<环境>）。
#
# 参数（由 CI 构建参数/环境注入，非命令行传参）：
#   branchs      Git 分支，当前支持 origin/test
#   WORKSPACE    CI 自动注入的工作区路径
#   DEPLOY_HOST  目标服务器地址（必填，不在仓库中硬编码任何内网地址）
#   DEPLOY_USER  目标机登录用户（可选，默认 deploy）
#
# 前置条件：
#   - CI agent 上有 rsync、ssh
#   - CI agent 到目标机 ${DEPLOY_USER}@${DEPLOY_HOST} 已配免密 ssh（22 端口）
#   - 目标机 ${WEB_PATH} 下已有 .env.<环境>（rsync 显式排除 .env，不覆盖服务器配置）

PROJECT=proxy-hub
SOURCE_CODE=${WORKSPACE}
WEB_PATH=/opt/web/proxy-hub
WEB_USER=${DEPLOY_USER:-deploy}

# ── 1. 根据分支确定部署环境；目标机地址由 DEPLOY_HOST 注入 ──
echo ">>> 当前分支: ${branchs}"

if [ "${branchs}" == "origin/test" ]; then
    APP_ENV="test"
else
    echo "[ERROR] 未识别的分支: ${branchs}，终止部署"
    exit 1
fi

IP="${DEPLOY_HOST:?必须通过 DEPLOY_HOST 环境变量指定目标服务器地址}"

echo ">>> 目标服务器: ${IP}  环境: ${APP_ENV}"

# ── 2. 推送代码（含点文件）─────────────────────────
echo ">>> 推送代码至 ${WEB_USER}@${IP}:${WEB_PATH}"
rsync -avzP \
    --include='.*' \
    --exclude='.git/' \
    --exclude='.idea/' \
    --exclude='.env' \
    -e "ssh -p22" \
    "${SOURCE_CODE}/" \
    "${WEB_USER}@${IP}:${WEB_PATH}/"


# ── 3. 远程执行部署脚本（deploy.sh 按 APP_ENV 选择 .env.<环境>）──
echo ">>> 远程执行 deploy/deploy.sh ${APP_ENV} restart"
ssh "${WEB_USER}@${IP}" WEB_PATH="$WEB_PATH" APP_ENV="$APP_ENV" bash <<'EOF'
chmod +x "$WEB_PATH/deploy/deploy.sh"
"$WEB_PATH/deploy/deploy.sh" "$APP_ENV" restart
EOF
echo ">>> 部署完成 [${PROJECT}] [${APP_ENV}] [${IP}]"
