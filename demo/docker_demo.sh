#!/usr/bin/env bash
# Docker Compose 演示辅助（在「有 docker compose 的主机」上、仓库根目录运行）：
#
#   bash demo/docker_demo.sh up          # docker compose up -d --build 并等待健康
#   bash demo/docker_demo.sh idle        # 核对空闲/查询时 state.json 不写、看门狗不醒
#   bash demo/docker_demo.sh fallback 15 # 发 15s 试切 -> 强杀 API -> 测实际回退时点
#   bash demo/docker_demo.sh wdrestart   # 单独重启看门狗进程，验证按原期限回退
#   bash demo/docker_demo.sh verify      # 容器内跑 10 组关键场景自检（dryrun 路由）
#
# Swagger 手工操作：浏览器打开 http://localhost:8080/docs
set -euo pipefail
cd "$(dirname "$0")/.."

DC="docker compose"
BASE="${BASE_URL:-http://localhost:8080}"
TARGET_IP="${TARGET_IP:-172.30.0.100}"

log() { printf '\n\033[1;36m>> %s\033[0m\n' "$*"; }

wait_health() {
  for _ in $(seq 1 50); do
    if curl -sf "$BASE/api/health" >/dev/null 2>&1; then return 0; fi
    sleep 0.4
  done
  echo "gateway API 在 $BASE 不可用" >&2
  return 1
}

cmd_up() {
  $DC up -d --build
  wait_health
  echo "Swagger: $BASE/docs"
}

# 取容器内看门狗 PID（pid 与 TSW_IO_DEBUG 日志中的 pid 一致）
wd_pid() {
  $DC exec -T gateway sh -c "pgrep -f 'app.watchdog' | head -1" | tr -d '\r\n'
}

stat_state() {
  $DC exec -T gateway stat -c '%i:%Y:%s' /state/state.json | tr -d '\r'
}

cmd_idle() {
  wait_health
  local pid before after qafter wakes0 wakes1
  pid="$(wd_pid)"
  echo "看门狗容器内 PID=$pid"

  log "1/3 纯空闲 5 秒（不发任何请求）"
  before="$(stat_state)"
  wakes0="$($DC logs gateway 2>/dev/null | grep -c '\[watchdog\] wake:' || true)"
  sleep 5
  after="$(stat_state)"
  wakes1="$($DC logs gateway 2>/dev/null | grep -c '\[watchdog\] wake:' || true)"
  echo "state.json inode:mtime:size  前=$before  后=$after"
  echo "看门狗 wake 日志行数 前=$wakes0 后=$wakes1"
  if [ "$before" = "$after" ] && [ "$wakes0" = "$wakes1" ]; then
    echo "PASS: 空闲期状态文件零写入、看门狗零唤醒（阻塞在 FIFO/到期 select 上）"
  else
    echo "FAIL: 空闲期仍有写盘或唤醒，请检查" >&2
    exit 1
  fi

  log "2/3 从 API 连续发起 6 个 GET 查询（等价于在 Swagger 里反复查询）"
  local i
  for i in 1 2 3; do
    curl -sf "$BASE/api/trials/current" >/dev/null
    curl -sf "$BASE/api/trials" >/dev/null
  done
  sleep 1
  qafter="$(stat_state)"
  wakes1="$($DC logs gateway 2>/dev/null | grep -c '\[watchdog\] wake:' || true)"
  echo "state.json inode:mtime:size  空闲前=$before  查询后=$qafter"
  echo "看门狗 wake 日志行数（查询后）=$wakes1"
  if [ "$before" = "$qafter" ] && [ "$wakes0" = "$wakes1" ]; then
    echo "PASS: 查询未回写状态文件，也未唤醒看门狗"
  else
    echo "FAIL: 查询触发了写盘或看门狗唤醒" >&2
    exit 1
  fi

  log "3/3 最近的逐次 IO 痕迹（API 进程应有 state read、无 state write；看门狗应停在 waiting/idle）"
  $DC logs --tail 30 gateway 2>/dev/null | grep -E '\[io pid=|\[watchdog\]' | tail -15 || true
}

cmd_fallback() {
  local dur="${1:-15}"
  wait_health
  log "发起 ${dur}s 试切（backup-good），随后立刻 SIGKILL 管理 API"
  local resp tid deadline
  resp="$(curl -sf -X POST "$BASE/api/trials" \
    -H 'Content-Type: application/json' \
    -d "{\"nexthop\":\"backup-good\",\"duration_sec\":${dur}}")"
  tid="$(printf '%s' "$resp" | python3 -c 'import json,sys;print(json.load(sys.stdin)["trial_id"])')"
  deadline="$(printf '%s' "$resp" | python3 -c 'import json,sys;print(json.load(sys.stdin)["deadline_ts"])')"
  echo "trial_id=$tid deadline_ts=$deadline"
  $DC exec -T gateway ip route get "$TARGET_IP" || true

  $DC exec -T gateway pkill -9 -f 'uvicorn app.main' || true
  echo "管理 API 已被 SIGKILL；看门狗仍存活，开始以 20ms 粒度轮询已落盘状态..."

  # 在容器内用 Python 读 /state/state.json：回退终态里的 finished_ts 与
  # 发起时返回的 deadline_ts 是同一时钟，差值即「相对保存期限的实际回退时点」。
  $DC exec -T \
    -e TID="$tid" -e DEADLINE="$deadline" -e TARGET_IP="$TARGET_IP" \
    -e DUR="$dur" \
    gateway python3 - <<'PY'
import json, os, sys, time
tid = os.environ["TID"]
deadline = float(os.environ["DEADLINE"])
give_up = time.time() + float(os.environ.get("DUR", "30")) + 12
last_route = ""
while time.time() < give_up:
    try:
        st = json.load(open("/state/state.json", encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        time.sleep(0.02); continue
    for t in st.get("history", []):
        if t.get("trial_id") == tid and t.get("status") == "rolled_back":
            finished = float(t["finished_ts"])
            print(f"rolled_back reason={t.get('rollback_reason')}")
            print(f"实际回退 finished_ts - 保存的 deadline_ts = "
                  f"{finished - deadline:+.3f}s（应约为 0，正数=略晚）")
            raise SystemExit(0)
    time.sleep(0.02)
print("未在预期时间内观察到 rolled_back", file=sys.stderr)
raise SystemExit(1)
PY

  log "容器内实际路由（应已恢复 via 172.30.0.11）"
  $DC exec -T gateway ip route get "$TARGET_IP" || true
  $DC logs --tail 20 gateway 2>/dev/null | grep -E '\[watchdog\]' | tail -8 || true
  echo
  echo "API 已被演示脚本杀掉。恢复：$DC restart gateway"
}

cmd_wdrestart() {
  local dur="${1:-10}"
  wait_health
  log "发起 ${dur}s 试切，只杀看门狗进程并由 entrypoint 在 2s 内重启"
  local resp tid deadline
  resp="$(curl -sf -X POST "$BASE/api/trials" \
    -H 'Content-Type: application/json' \
    -d "{\"nexthop\":\"backup-good\",\"duration_sec\":${dur}}")"
  tid="$(printf '%s' "$resp" | python3 -c 'import json,sys;print(json.load(sys.stdin)["trial_id"])')"
  deadline="$(printf '%s' "$resp" | python3 -c 'import json,sys;print(json.load(sys.stdin)["deadline_ts"])')"
  echo "trial_id=$tid；杀掉看门狗..."
  $DC exec -T gateway pkill -9 -f 'app.watchdog' || true
  echo "等待 entrypoint 重新拉起看门狗，并观察它按「剩余期限」等待..."
  sleep 3
  $DC logs --tail 30 gateway 2>/dev/null | grep '\[watchdog\] waiting' | tail -2 || true
  $DC exec -T \
    -e TID="$tid" -e DEADLINE="$deadline" -e DUR="$dur" \
    gateway python3 - <<'PY'
import json, os, time
tid = os.environ["TID"]; deadline = float(os.environ["DEADLINE"])
give_up = time.time() + float(os.environ.get("DUR", "10")) + 8
while time.time() < give_up:
    st = json.load(open("/state/state.json", encoding="utf-8"))
    for t in st.get("history", []):
        if t.get("trial_id") == tid and t.get("status") == "rolled_back":
            print(f"看门狗重启后回退时点 - 原期限 = "
                  f"{float(t['finished_ts']) - deadline:+.3f}s（应约为 0，而不是 +{os.environ.get('DUR','?')}s）")
            raise SystemExit(0)
    time.sleep(0.02)
raise SystemExit("未观察到回退")
PY
}

cmd_verify() {
  $DC exec -T gateway python /demo/verify_scenarios.py
}

case "${1:-}" in
  up) cmd_up ;;
  idle) cmd_idle ;;
  fallback) shift; cmd_fallback "${1:-15}" ;;
  wdrestart) shift; cmd_wdrestart "${1:-10}" ;;
  verify) cmd_verify ;;
  *)
    sed -n '2,12p' "$0"
    exit 2
    ;;
esac
