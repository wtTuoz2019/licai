#!/usr/bin/env bash
# 服务器一键部署 / 启动操作台
# 用法:
#   ./start.sh              安装依赖并后台启动
#   ./start.sh stop         停止
#   ./start.sh restart      重启
#   ./start.sh status       查看状态
#   ./start.sh systemd      写成开机自启（需要 sudo）
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8765}"
PID_FILE="$ROOT/logs/web.pid"
LOG_FILE="$ROOT/logs/web.log"
SERVICE_NAME="${SERVICE_NAME:-licai}"

log() { printf '%s\n' "$*"; }
die() { printf '错误: %s\n' "$*" >&2; exit 1; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1
}

python_bin() {
  if need_cmd python3; then
    command -v python3
  elif need_cmd python; then
    command -v python
  else
    die "找不到 python3，先安装: sudo apt-get install -y python3 python3-venv python3-pip"
  fi
}

ensure_dirs() {
  mkdir -p "$ROOT/logs" "$ROOT/data"
}

listening_pid() {
  if need_cmd lsof; then
    lsof -nP -iTCP:"$PORT" -sTCP:LISTEN 2>/dev/null | awk 'NR==2 { print $2 }'
    return
  fi
  if need_cmd ss; then
    ss -lntp 2>/dev/null | awk -v p=":$PORT" '$4 ~ p { if (match($0, /pid=[0-9]+/)) { print substr($0, RSTART+4, RLENGTH-4); exit } }'
  fi
}

is_running() {
  local pid=""
  if [[ -f "$PID_FILE" ]]; then
    pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "${pid}" ]] && kill -0 "$pid" 2>/dev/null; then
      return 0
    fi
  fi
  pid="$(listening_pid || true)"
  [[ -n "${pid}" ]]
}

stop_web() {
  local pid=""
  if [[ -f "$PID_FILE" ]]; then
    pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  fi
  if [[ -z "${pid}" ]]; then
    pid="$(listening_pid || true)"
  fi
  if [[ -n "${pid}" ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.3
    done
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
    log "已停止 pid=$pid"
  else
    log "操作台没有在跑"
  fi
  rm -f "$PID_FILE"
}

install_system() {
  local py
  py="$(python_bin)"
  if "$py" -c "import venv, ensurepip" >/dev/null 2>&1; then
    return
  fi
  if need_cmd apt-get; then
    log "安装 Python 环境..."
    sudo apt-get update -y
    sudo apt-get install -y python3 python3-venv python3-pip
  else
    die "当前系统没有 python3-venv，请先自行安装 Python 3"
  fi
}

setup_venv() {
  local py
  py="$(python_bin)"
  if [[ ! -x "$ROOT/.venv/bin/python" ]]; then
    log "创建虚拟环境..."
    "$py" -m venv "$ROOT/.venv"
  fi
  log "安装依赖..."
  "$ROOT/.venv/bin/python" -m pip install -U pip >/dev/null
  "$ROOT/.venv/bin/python" -m pip install -r "$ROOT/requirements.txt"
}

env_has_password() {
  [[ -f "$ROOT/.env" ]] || return 1
  grep -Eq '^LICAI_DASHBOARD_PASSWORD=.+' "$ROOT/.env"
}

write_env_key() {
  local key="$1"
  local val="$2"
  SET_KEY="$key" SET_VAL="$val" "$(python_bin)" - <<'PY'
from pathlib import Path
import os
path = Path(".env")
key = os.environ["SET_KEY"]
val = os.environ["SET_VAL"]
lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
out, found = [], False
prefix = key + "="
for line in lines:
    if line.startswith(prefix):
        out.append(prefix + val)
        found = True
    else:
        out.append(line)
if not found:
    if out and out[-1] != "":
        out.append("")
    out.append(prefix + val)
text = "\n".join(out).rstrip() + "\n"
path.write_text(text, encoding="utf-8")
PY
}

ensure_env() {
  if [[ ! -f "$ROOT/.env" ]]; then
    if [[ -f "$ROOT/.env.example" ]]; then
      cp "$ROOT/.env.example" "$ROOT/.env"
    else
      : > "$ROOT/.env"
    fi
    chmod 600 "$ROOT/.env"
  fi
  if [[ -n "${LICAI_DASHBOARD_PASSWORD:-}" ]]; then
    write_env_key LICAI_DASHBOARD_PASSWORD "$LICAI_DASHBOARD_PASSWORD"
    return
  fi
  if env_has_password; then
    return
  fi
  if [[ ! -t 0 ]]; then
    die "还没有访问密码。先执行: export LICAI_DASHBOARD_PASSWORD='你的密码' && ./start.sh"
  fi
  local p1 p2
  log "第一次部署需要设置操作台访问密码（打开网页时输入）。"
  read -r -s -p "访问密码: " p1
  echo
  [[ -n "$p1" ]] || die "密码不能为空"
  read -r -s -p "再输入一次: " p2
  echo
  [[ "$p1" == "$p2" ]] || die "两次密码不一致"
  write_env_key LICAI_DASHBOARD_PASSWORD "$p1"
  chmod 600 "$ROOT/.env"
}

start_web() {
  if is_running; then
    log "已在运行，先重启..."
    stop_web
  fi
  nohup "$ROOT/.venv/bin/python" -m licai web --host "$HOST" --port "$PORT" >>"$LOG_FILE" 2>&1 &
  local pid=$!
  echo "$pid" > "$PID_FILE"
  sleep 0.8
  if ! kill -0 "$pid" 2>/dev/null; then
    rm -f "$PID_FILE"
    die "启动失败，看日志: $LOG_FILE"
  fi
  log "操作台已启动  pid=$pid"
  log "本机:     http://127.0.0.1:$PORT"
  log "服务器:   http://服务器IP:$PORT"
  log "日志:     $LOG_FILE"
  log "先输入 .env 里的访问密码，再在页面绑定账号。"
}

install_systemd() {
  local unit="/etc/systemd/system/${SERVICE_NAME}.service"
  local user
  user="$(id -un)"
  [[ "$(id -u)" -eq 0 ]] || need_cmd sudo || die "写入 systemd 需要 sudo"
  local sudo_cmd=()
  if [[ "$(id -u)" -ne 0 ]]; then
    sudo_cmd=(sudo)
  fi
  stop_web || true
  "${sudo_cmd[@]}" tee "$unit" >/dev/null <<EOF
[Unit]
Description=licai dashboard
After=network.target

[Service]
Type=simple
User=$user
WorkingDirectory=$ROOT
EnvironmentFile=-$ROOT/.env
ExecStart=$ROOT/.venv/bin/python -m licai web --host $HOST --port $PORT
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF
  "${sudo_cmd[@]}" systemctl daemon-reload
  "${sudo_cmd[@]}" systemctl enable --now "$SERVICE_NAME"
  log "已安装开机自启: systemctl status $SERVICE_NAME"
}

status_web() {
  local pid=""
  if is_running; then
    pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    [[ -n "$pid" ]] || pid="$(listening_pid || true)"
    log "运行中  pid=${pid:-?}  http://$HOST:$PORT"
  else
    log "未运行"
    return 1
  fi
}

cmd="${1:-start}"
case "$cmd" in
  start|"")
    ensure_dirs
    install_system
    setup_venv
    ensure_env
    start_web
    ;;
  stop)
    stop_web
    ;;
  restart)
    ensure_dirs
    install_system
    setup_venv
    ensure_env
    start_web
    ;;
  status)
    status_web
    ;;
  systemd|install)
    ensure_dirs
    install_system
    setup_venv
    ensure_env
    install_systemd
    ;;
  *)
    die "未知命令: $cmd
用法: ./start.sh [start|stop|restart|status|systemd]"
    ;;
esac
