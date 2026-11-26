#!/usr/bin/env python3
"""关键场景自动核对（无需 Docker；使用 TSW_ROUTE_BACKEND=dryrun）。

覆盖任务要求：
  A. 空闲时看门狗不读不写状态文件；GET 查询不回写、不唤醒看门狗；
  B. 试切记录落盘后立刻 kill -9 管理 API，看门狗仍按保存的 deadline 回退；
  C. 保留上一笔后立刻开新笔：
       - 新期限更早 -> 看门狗被唤醒并按新期限回退；
       - 旧期限点到达时新试切仍存活 -> 旧定时不会提前撤回新试切；
  D. 看门狗单独重启：
       - 未到期：从已保存状态接着等剩余时间（不是重新等满时长）；
       - 已到期：启动后立即回退；
  E. 顺手核对 409（一次一个试切）与旧试切号 404。

驱动脚本本身只用标准库；子进程使用 sys.executable 启动，因此请用
「装好了 gateway/requirements.txt 的那个 Python」运行本脚本，例如：

    python3 demo/verify_scenarios.py

在 docker compose 容器里也可以（compose 已挂载 ./demo 到 /demo:ro）：

    docker compose exec gateway python /demo/verify_scenarios.py
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if (Path("/app/app/main.py")).exists():
    GATEWAY_DIR = Path("/app")
else:
    GATEWAY_DIR = REPO_ROOT / "gateway"

RESULTS: list[tuple[str, bool, str]] = []


def record(name: str, ok: bool, detail: str = "") -> None:
    RESULTS.append((name, ok, detail))
    mark = "PASS" if ok else "FAIL"
    print(f"\n=== [{mark}] {name} ===\n    {detail}", flush=True)


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def http(method: str, port: int, path: str, body: dict | None = None,
         timeout: float = 5.0):
    url = f"http://127.0.0.1:{port}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        try:
            payload = json.loads(exc.read().decode())
        except Exception:
            payload = None
        return exc.code, payload


def wait_health(port: int, proc: subprocess.Popen, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"API process exited early with {proc.returncode}")
        try:
            status, payload = http("GET", port, "/api/health", timeout=1.0)
            if status == 200:
                return
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(0.2)
    raise RuntimeError(f"API not healthy on :{port}: {last}")


def write_config(path: Path, probe_port: int) -> None:
    path.write_text(
        "target: 127.0.0.1\n"
        f"probe_port: {probe_port}\n"
        "probe_attempts: 3\n"
        "probe_timeout_sec: 0.5\n"
        "probe_interval_sec: 0.1\n"
        "nexthops:\n"
        "  primary: 192.0.2.1\n"
        "  backup-good: 192.0.2.2\n"
        "  backup-broken: 192.0.2.3\n",
        encoding="utf-8",
    )


def make_env(state_dir: Path, config: Path) -> dict:
    env = dict(os.environ)
    env.update({
        "TSW_STATE_DIR": str(state_dir),
        "TSW_CONFIG": str(config),
        "TSW_ROUTE_BACKEND": "dryrun",
        "TSW_IO_DEBUG": "1",
        "PYTHONUNBUFFERED": "1",
    })
    return env


def spawn_watchdog(env: dict, log_path: Path) -> subprocess.Popen:
    logf = open(log_path, "ab")
    return subprocess.Popen(
        [sys.executable, "-m", "app.watchdog"],
        cwd=str(GATEWAY_DIR), env=env, stdout=logf, stderr=subprocess.STDOUT,
    )


def spawn_api(env: dict, port: int, log_path: Path) -> subprocess.Popen:
    logf = open(log_path, "ab")
    return subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(port)],
        cwd=str(GATEWAY_DIR), env=env, stdout=logf, stderr=subprocess.STDOUT,
    )


def kill_proc(proc: subprocess.Popen, sig=signal.SIGKILL) -> None:
    if proc.poll() is None:
        proc.send_signal(sig)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def read_state(state_dir: Path) -> dict:
    with open(state_dir / "state.json", encoding="utf-8") as f:
        return json.load(f)


def wait_rollback(state_dir: Path, trial_id: str, timeout: float = 12.0):
    """轮询状态文件，等指定试切进入 rolled_back；返回 (trial, observed_ts)。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            st = read_state(state_dir)
        except (FileNotFoundError, json.JSONDecodeError):
            time.sleep(0.02)
            continue
        for t in st.get("history", []):
            if t.get("trial_id") == trial_id and t.get("status") == "rolled_back":
                return t, time.time()
        time.sleep(0.02)
    raise AssertionError(f"trial {trial_id} 未在 {timeout}s 内回退")


def wait_status_via_file(state_dir: Path, trial_id: str, status: str,
                         timeout: float = 8.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = read_state(state_dir)
        cur = st.get("current")
        if cur and cur.get("trial_id") == trial_id and cur.get("status") == status:
            return cur
        for t in st.get("history", []):
            if t.get("trial_id") == trial_id and t.get("status") == status:
                return t
        time.sleep(0.03)
    raise AssertionError(f"trial {trial_id} 未进入状态 {status}")


def log_tail(path: Path, start_pos: int) -> str:
    with open(path, "rb") as f:
        f.seek(start_pos)
        return f.read().decode(errors="replace")


def log_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except FileNotFoundError:
        return 0


def wait_log_contains(path: Path, needle: str, timeout: float = 1.5) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if needle in path.read_text(errors="replace"):
                return True
        except FileNotFoundError:
            pass
        time.sleep(0.03)
    return False


def route_nexthop(state_dir: Path):
    try:
        with open(state_dir / "dryrun_route.json", encoding="utf-8") as f:
            return json.load(f).get("nexthop")
    except FileNotFoundError:
        return "NO_SIM_FILE"


class Harness:
    def __init__(self, name: str, probe_port: int):
        self.dir = Path(tempfile.mkdtemp(prefix=f"tsw_verify_{name}_"))
        self.state_dir = self.dir / "state"
        self.state_dir.mkdir()
        self.config = self.dir / "config.yaml"
        write_config(self.config, probe_port)
        self.env = make_env(self.state_dir, self.config)
        self.wd_log = self.dir / "watchdog.log"
        self.api_log = self.dir / "api.log"
        self.wd: subprocess.Popen | None = None
        self.api: subprocess.Popen | None = None
        self.port = free_port()

    def start_wd(self):
        self.wd = spawn_watchdog(self.env, self.wd_log)
        # 等看门狗把初始状态文件建出来
        for _ in range(100):
            if (self.state_dir / "state.json").exists():
                break
            time.sleep(0.02)
        time.sleep(0.3)

    def start_api(self):
        self.api = spawn_api(self.env, self.port, self.api_log)
        wait_health(self.port, self.api)

    def cleanup(self, remove: bool = True) -> None:
        if self.api is not None:
            kill_proc(self.api)
        if self.wd is not None:
            kill_proc(self.wd)
        if remove:
            shutil.rmtree(self.dir, ignore_errors=True)
        else:
            print(f"    [保留现场日志] {self.dir}", flush=True)


def scenario_idle_and_queries(probe_port: int) -> None:
    h = Harness("idle", probe_port)
    try:
        h.start_wd()
        h.start_api()
        state_file = h.state_dir / "state.json"

        # ---- A1: 纯空闲 3s，看门狗日志里不应有新的状态读/写 ----
        pos = log_size(h.wd_log)
        st_before = state_file.stat()
        time.sleep(3.0)
        idle_log = log_tail(h.wd_log, pos)
        idle_reads = idle_log.count("state read")
        idle_writes = idle_log.count("state write")
        st_after = state_file.stat()
        no_file_change = (
            st_before.st_ino == st_after.st_ino
            and st_before.st_mtime_ns == st_after.st_mtime_ns
        )
        record(
            "A1 空闲 3s 看门狗零状态读/写",
            idle_reads == 0 and idle_writes == 0 and no_file_change,
            f"watchdog 新增 read={idle_reads} write={idle_writes}；"
            f"state.json inode/mtime 未变={no_file_change}",
        )

        # ---- A2: 连续查询：状态文件不回写、看门狗不被唤醒 ----
        pos_wd = log_size(h.wd_log)
        pos_api = log_size(h.api_log)
        snap_before = (state_file.stat().st_ino, state_file.stat().st_mtime_ns)
        for _ in range(3):
            http("GET", h.port, "/api/trials/current")
            http("GET", h.port, "/api/trials")
            http("GET", h.port, "/api/trials/t_does_not_exist")
            time.sleep(0.1)
        time.sleep(0.8)  # 给可能的误唤醒留时间
        snap_after = (state_file.stat().st_ino, state_file.stat().st_mtime_ns)
        wd_segment = log_tail(h.wd_log, pos_wd)
        api_segment = log_tail(h.api_log, pos_api)
        wd_wakes = wd_segment.count("wake:")
        api_writes = api_segment.count("state write")
        api_reads = api_segment.count("state read")
        record(
            "A2 GET 查询不回写状态文件",
            snap_before == snap_after and api_writes == 0,
            f"state.json inode+mtime 不变={snap_before == snap_after}；"
            f"API 日志 read={api_reads} write={api_writes}（读是允许的）",
        )
        record(
            "A3 查询不唤醒看门狗",
            wd_wakes == 0 and "state read" not in wd_segment
            and "state write" not in wd_segment,
            f"查询窗口内看门狗 wake 次数={wd_wakes}，"
            f"新增 IO 行数={wd_segment.count('[io pid=')}",
        )
    finally:
        h.cleanup(remove=not os.environ.get("TSW_VERIFY_KEEP"))


def scenario_api_killed(probe_port: int) -> None:
    h = Harness("apikill", probe_port)
    try:
        h.start_wd()
        h.start_api()
        t0 = time.time()
        status, trial = http("POST", h.port, "/api/trials",
                             {"nexthop": "backup-good", "duration_sec": 6})
        post_done = time.time()
        assert status == 201, (status, trial)
        tid = trial["trial_id"]
        deadline = float(trial["deadline_ts"])

        # 响应一回来立刻 SIGKILL 管理 API（模拟「立刻退出」）
        kill_proc(h.api)
        h.api = None
        api_lifetime = post_done - t0  # noqa: F841

        rolled, observed = wait_rollback(h.state_dir, tid, timeout=10)
        finished = float(rolled["finished_ts"])
        delta_vs_deadline = finished - deadline
        observe_lag = observed - finished
        has_wd_log = wait_log_contains(h.wd_log, "auto rolled back")
        ok = (
            rolled["rollback_reason"] == "deadline_exceeded"
            and -0.1 <= delta_vs_deadline <= 0.8
            and route_nexthop(h.state_dir) is None
            and has_wd_log
        )
        record(
            "B API 立即退出后看门狗按保存期限回退",
            ok,
            f"tid={tid}；回退时刻-期限={delta_vs_deadline:+.3f}s；"
            f"轮询观察滞后={observe_lag:.3f}s（仅测量脚本调度延迟）；"
            f"原因={rolled['rollback_reason']}；看门狗日志确认={has_wd_log}；"
            f"dryrun 路由已恢复(nexthop=None)={route_nexthop(h.state_dir) is None}",
        )
    finally:
        h.cleanup(remove=not os.environ.get("TSW_VERIFY_KEEP"))


def scenario_rapid_replacement(probe_port: int) -> None:
    h = Harness("replace", probe_port)
    try:
        h.start_wd()
        h.start_api()

        # ---- C1: 长试切 A 确认保留，立刻开更短的 B，必须按 B 期限回退 ----
        _, a = http("POST", h.port, "/api/trials",
                    {"nexthop": "backup-good", "duration_sec": 30})
        aid = a["trial_id"]
        a_deadline = float(a["deadline_ts"])
        st, _ = http("POST", h.port, f"/api/trials/{aid}/confirm")
        assert st == 202, st
        kept = wait_status_via_file(h.state_dir, aid, "kept", timeout=6)
        assert kept["status"] == "kept"

        _, b = http("POST", h.port, "/api/trials",
                    {"nexthop": "backup-broken", "duration_sec": 5})
        bid = b["trial_id"]
        b_deadline = float(b["deadline_ts"])
        assert b_deadline < a_deadline - 20  # 新期限显著更早

        b_rolled, _ = wait_rollback(h.state_dir, bid, timeout=9)
        b_delta = float(b_rolled["finished_ts"]) - b_deadline
        c1_ok = -0.1 <= b_delta <= 0.6
        record(
            "C1 保留 A 后立刻开更早到期的 B，按 B 新期限回退",
            c1_ok,
            f"B 回退- B 期限={b_delta:+.3f}s；B 期限比 A 旧期限早 "
            f"{a_deadline - b_deadline:.1f}s（不能等 A 的旧定时）",
        )

        # ---- C2: 短试切 C 保留后开更晚的 D；C 的旧期限点不能误撤 D ----
        _, c = http("POST", h.port, "/api/trials",
                    {"nexthop": "backup-good", "duration_sec": 5})
        cid = c["trial_id"]
        c_deadline = float(c["deadline_ts"])
        st, _ = http("POST", h.port, f"/api/trials/{cid}/confirm")
        assert st == 202, st
        wait_status_via_file(h.state_dir, cid, "kept", timeout=6)

        _, d = http("POST", h.port, "/api/trials",
                    {"nexthop": "backup-good", "duration_sec": 8})
        did = d["trial_id"]
        d_deadline = float(d["deadline_ts"])
        assert d_deadline > c_deadline

        # 睡到 C 的旧期限点之后 1s：D 必须仍是 trialing
        time.sleep(max(0.0, c_deadline - time.time()) + 1.0)
        st = read_state(h.state_dir)
        cur = st.get("current")
        d_alive_at_old_deadline = bool(
            cur and cur["trial_id"] == did and cur["status"] == "trialing"
        )
        route_ok = route_nexthop(h.state_dir) == "192.0.2.2"
        d_rolled, _ = wait_rollback(h.state_dir, did, timeout=10)
        d_delta = float(d_rolled["finished_ts"]) - d_deadline
        record(
            "C2 旧定时到点不误撤新试切 D",
            d_alive_at_old_deadline and route_ok and -0.1 <= d_delta <= 0.6,
            f"旧 C 期限+1s 时 D 仍 trialing={d_alive_at_old_deadline}，"
            f"路由仍在新下一跳={route_ok}；D 回退- D 期限={d_delta:+.3f}s",
        )
    finally:
        h.cleanup(remove=not os.environ.get("TSW_VERIFY_KEEP"))


def scenario_watchdog_restart(probe_port: int) -> None:
    h = Harness("wdrestart", probe_port)
    try:
        h.start_wd()
        h.start_api()

        # ---- E: 409 与旧号 404（顺带回归） ----
        _, t1 = http("POST", h.port, "/api/trials",
                     {"nexthop": "backup-good", "duration_sec": 9})
        tid = t1["trial_id"]
        code_conflict, _ = http("POST", h.port, "/api/trials",
                                {"nexthop": "backup-good", "duration_sec": 9})
        code_old, _ = http("POST", h.port, "/api/trials/t_deadbeef0000/confirm")
        record(
            "E 进行中再发起=409，陌生试切号 confirm=404",
            code_conflict == 409 and code_old == 404,
            f"second POST -> {code_conflict}；old confirm -> {code_old}",
        )

        # ---- D1: 看门狗杀掉 2s 后由「监督进程」重启，按原期限回退 ----
        deadline = float(t1["deadline_ts"])
        kill_proc(h.wd)
        h.wd = None
        time.sleep(2.0)
        h.wd = spawn_watchdog(h.env, h.wd_log)
        # 等它打印等待时长
        time.sleep(0.4)
        wd_text = h.wd_log.read_text(errors="replace")
        waiting_lines = [ln for ln in wd_text.splitlines() if "waiting" in ln]
        last_wait = waiting_lines[-1] if waiting_lines else ""
        rolled, _ = wait_rollback(h.state_dir, tid, timeout=10)
        delta = float(rolled["finished_ts"]) - deadline
        # 重启后剩余应约为 9-2.x ≈ 6~7s，而不是重新等 9s
        record(
            "D1 看门狗重启后按已保存 deadline 接着等",
            -0.1 <= delta <= 0.8 and "waiting" in last_wait,
            f"回退-原期限={delta:+.3f}s（杀进程 2s，故应贴近 0 而非 +9s）；"
            f"重启后日志: {last_wait.strip()}",
        )

        # ---- D2: 看门狗不在时让试切过期，再启动必须立即回退 ----
        _, t2 = http("POST", h.port, "/api/trials",
                     {"nexthop": "backup-good", "duration_sec": 5})
        tid2 = t2["trial_id"]
        deadline2 = float(t2["deadline_ts"])
        kill_proc(h.wd)
        h.wd = None
        time.sleep(6.0)  # 远超 5s 期限；没有看门狗、也不发查询 -> 无人回退
        st = read_state(h.state_dir)
        still_active = bool(st.get("current") and st["current"]["trial_id"] == tid2)
        start = time.time()
        h.wd = spawn_watchdog(h.env, h.wd_log)
        rolled, _ = wait_rollback(h.state_dir, tid2, timeout=4)
        reaction = float(rolled["finished_ts"]) - start
        past_by_start = start - deadline2

        # ---- D3: 新看门狗在运行，API 再开一笔：FIFO 通知必须送达新读端 ----
        wd_pos = log_size(h.wd_log)
        _, t3 = http("POST", h.port, "/api/trials",
                     {"nexthop": "backup-good", "duration_sec": 5})
        tid3 = t3["trial_id"]
        deadline3 = float(t3["deadline_ts"])
        rolled3, _ = wait_rollback(h.state_dir, tid3, timeout=9)
        delta3 = float(rolled3["finished_ts"]) - deadline3
        # 给看门狗一点时间打印 wake 日志，并只检查 D3 POST 之后的新增段
        deadline_wait = time.time() + 1.0
        wake_segment = ""
        while time.time() < deadline_wait:
            wake_segment = log_tail(h.wd_log, wd_pos)
            if "state-change notification" in wake_segment:
                break
            time.sleep(0.03)
        notified = "state-change notification" in wake_segment
        record(
            "D2 看门狗重启发现已到期立即回退",
            still_active and reaction < 1.0 and past_by_start > 0.5,
            f"看门狗启动前状态仍 active={still_active}（已过期 "
            f"{past_by_start:.1f}s）；启动到落终态耗时 {reaction:.3f}s (<1s)",
        )
        record(
            "D3 重启后的看门狗能收新试切通知并按新期限回退",
            -0.1 <= delta3 <= 0.8 and notified,
            f"新试切回退-期限={delta3:+.3f}s；日志见唤醒通知={notified}",
        )
    finally:
        h.cleanup(remove=not os.environ.get("TSW_VERIFY_KEEP"))


def main() -> int:
    if not (GATEWAY_DIR / "app" / "main.py").exists():
        print(f"找不到 gateway/app/main.py（推断 GATEWAY_DIR={GATEWAY_DIR}）",
              file=sys.stderr)
        return 2
    probe_port = free_port()
    server = subprocess.Popen(
        [sys.executable, "-m", "http.server", str(probe_port),
         "--bind", "127.0.0.1"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        time.sleep(0.8)
        scenario_idle_and_queries(probe_port)
        scenario_api_killed(probe_port)
        scenario_rapid_replacement(probe_port)
        scenario_watchdog_restart(probe_port)
    finally:
        kill_proc(server)

    print("\n" + "=" * 72)
    passed = sum(1 for _, ok, _ in RESULTS if ok)
    for name, ok, detail in RESULTS:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}")
    print("-" * 72)
    print(f"{passed}/{len(RESULTS)} 组断言通过")
    return 0 if passed == len(RESULTS) else 1


if __name__ == "__main__":
    raise SystemExit(main())
