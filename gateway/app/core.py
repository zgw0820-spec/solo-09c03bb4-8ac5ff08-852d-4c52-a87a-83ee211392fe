"""共享核心：配置加载、状态存储（flock 互斥）、路由操作、回退逻辑。

管理 API（app.main）与看门狗（app.watchdog）是两个相互独立的进程，
都通过本模块操作同一份状态文件。这样即使管理 API 进程停止，
看门狗仍能在试切到期时自行回退路由。

空闲行为（见 README「空闲 I/O 与唤醒机制」）：
  - StateStore.locked() 只在状态内容真正变化时写盘并触发 on_change；
  - StateStore.read() 是共享锁只读事务，绝不写盘、不发通知；
  - API 在提交变更后通过 notify_watchdog() 往 FIFO 写一个字节唤醒看门狗，
    查询路径不写盘也不通知；
  - 看门狗平时阻塞在 select() 上，等待「FIFO 可读」或「当前试切到期」。
"""
from __future__ import annotations

import contextlib
import copy
import errno
import fcntl
import json
import os
import subprocess
import time
from datetime import datetime, timezone

STATE_DIR = os.environ.get("TSW_STATE_DIR", "/state")
STATE_FILE = os.path.join(STATE_DIR, "state.json")
LOCK_FILE = os.path.join(STATE_DIR, "state.lock")
# API -> 看门狗的跨进程唤醒通道（FIFO）。看门狗以 O_RDWR 常驻打开，
# API 每次「提交了状态变更」后非阻塞写一个字节；通知可合并、可丢失，
# 因为看门狗唤醒后总是重新加锁读全量状态并按其中的 deadline 等待。
NOTIFY_FIFO = os.environ.get("TSW_NOTIFY_FIFO",
                             os.path.join(STATE_DIR, "watchdog.notify"))
CONFIG_FILE = os.environ.get("TSW_CONFIG", "/app/config.yaml")

# TSW_IO_DEBUG=1 时打印每一次状态文件读/写与看门狗通知，
# 便于演示时核对「空闲期间看门狗不读不写、查询不回写不唤醒」。
IO_DEBUG = os.environ.get("TSW_IO_DEBUG", "").lower() in ("1", "true", "yes", "on")


def io_trace(message: str) -> None:
    if IO_DEBUG:
        print(f"[io pid={os.getpid()}] {message}", flush=True)

ACTIVE_STATUSES = ("trialing", "probing")      # 进行中的试切（占用试切槽位）
TERMINAL_STATUSES = ("kept", "rolled_back")    # 终态
HISTORY_LIMIT = 50

REASON_PROBE_FAILED = "probe_failed"       # 确认后探测失败 -> 立即回退
REASON_DEADLINE = "deadline_exceeded"      # 到期仍未确认成功 -> 自动回退


def now_iso(ts: float | None = None) -> str:
    return datetime.fromtimestamp(
        ts if ts is not None else time.time(), tz=timezone.utc
    ).isoformat()


def load_config() -> dict:
    import yaml

    with open(CONFIG_FILE, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    # 环境变量覆盖（本地联调 / 容器注入用）
    env = os.environ
    if env.get("TSW_TARGET"):
        raw["target"] = env["TSW_TARGET"]
    for env_key, cfg_key, cast in (
        ("TSW_PROBE_PORT", "probe_port", int),
        ("TSW_PROBE_ATTEMPTS", "probe_attempts", int),
        ("TSW_PROBE_TIMEOUT_SEC", "probe_timeout_sec", float),
        ("TSW_PROBE_INTERVAL_SEC", "probe_interval_sec", float),
    ):
        if env.get(env_key):
            raw[cfg_key] = cast(env[env_key])
    return raw


class Config:
    def __init__(self, raw: dict):
        self.target = raw["target"]                       # 采集端 IPv4
        self.probe_port = int(raw.get("probe_port", 9000))
        self.probe_attempts = int(raw.get("probe_attempts", 3))
        self.probe_timeout = float(raw.get("probe_timeout_sec", 1.5))
        self.probe_interval = float(raw.get("probe_interval_sec", 0.3))
        self.nexthops = dict(raw.get("nexthops", {}))     # 预置下一跳：名称 -> IPv4
        # ip     = 真实执行 iproute2（容器内）
        # dryrun = 只打印命令并维护模拟路由表（本地无 NET_ADMIN 联调用）
        self.route_backend = os.environ.get("TSW_ROUTE_BACKEND", "ip")


class StateStore:
    """JSON 状态文件 + flock 互斥，供 API 与看门狗两个进程安全读写。

    - locked()：排他事务，退出时仅当状态内容发生变化才原子写盘，
      并在锁释放后调用 on_change（API 进程用它唤醒看门狗）。
      状态没变（例如一次普通查询顺带做的过期检查）时不写盘、不通知。
    - read()：共享锁只读事务，绝不写盘/通知，供查询接口使用。
    """

    def __init__(self, path: str = STATE_FILE, on_change=None, *,
                 lock_path: str | None = None):
        self.path = path
        # 默认构造（两个进程都用 STATE_FILE）沿用全局 LOCK_FILE；
        # 自定义 path（单元测试/多实例）且未显式给 lock_path 时，
        # 锁文件贴在状态文件旁边，保持自洽。
        self.lock_path = lock_path or (
            LOCK_FILE if os.path.abspath(path) == os.path.abspath(STATE_FILE)
            else path + ".lock"
        )
        self.on_change = on_change
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        if not os.path.exists(self.path):
            self._write({"current": None, "history": []})

    @contextlib.contextmanager
    def locked(self):
        """read-modify-write 临界区。

        正常退出且状态有变化时才落盘，并在释放锁后触发 on_change；
        状态未变化则完全不写。抛异常时丢弃改动、不写盘、不通知。
        """
        changed = False
        os.makedirs(os.path.dirname(self.lock_path), exist_ok=True)
        with open(self.lock_path, "w", encoding="utf-8") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            state = self._read()
            before = copy.deepcopy(state)
            try:
                yield state
            except Exception:
                fcntl.flock(lock, fcntl.LOCK_UN)
                raise
            else:
                if state != before:
                    self._write(state)
                    changed = True
                fcntl.flock(lock, fcntl.LOCK_UN)
        if changed and self.on_change is not None:
            try:
                self.on_change()
            except Exception as exc:  # noqa: BLE001 - 通知失败不能影响主流程
                io_trace(f"on_change callback failed: "
                         f"{type(exc).__name__}: {exc}")

    @contextlib.contextmanager
    def read(self):
        """共享锁只读临界区：只读取状态，退出时不写盘、不触发通知。"""
        os.makedirs(os.path.dirname(self.lock_path), exist_ok=True)
        with open(self.lock_path, "w", encoding="utf-8") as lock:
            fcntl.flock(lock, fcntl.LOCK_SH)
            try:
                yield self._read()
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def _read(self) -> dict:
        io_trace(f"state read  -> {self.path}")
        try:
            with open(self.path, encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"current": None, "history": []}

    def _write(self, state: dict) -> None:
        io_trace(f"state write -> {self.path}")
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())       # 「刚落盘」：返回前数据已下推
        os.replace(tmp, self.path)     # 原子替换，避免看门狗读到半写文件
        try:
            dfd = os.open(os.path.dirname(self.path) or ".", os.O_RDONLY)
            try:
                os.fsync(dfd)          # 让 rename 本身也持久（崩溃安全）
            finally:
                os.close(dfd)
        except OSError:
            # 某些文件系统不支持目录 fsync；不影响进程退出场景下的落盘语义
            pass


def ensure_notify_fifo(path: str = NOTIFY_FIFO) -> str:
    """创建 API->看门狗的唤醒 FIFO（已存在且确为 FIFO 则复用）。

    若同路径被普通文件等非 FIFO 对象占用（异常残留），替换成 FIFO，
    避免看门狗 select 普通文件永远可读而陷入忙等。
    """
    import stat

    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        st = os.stat(path)
        if not stat.S_ISFIFO(st.st_mode):
            os.unlink(path)
            raise FileNotFoundError  # 落到下面统一 mkfifo
    except FileNotFoundError:
        try:
            os.mkfifo(path, 0o600)
        except FileExistsError:
            pass
    return path


def notify_watchdog(path: str = NOTIFY_FIFO) -> bool:
    """状态已变更后唤醒看门狗。

    非阻塞：看门狗不在（FIFO 没有读端，ENXIO）或通知管道写满时直接放弃——
    通知只是「提示有变化」，看门狗启动/唤醒后都会全量重读状态，
    因此通知本身不需要可靠送达。返回是否成功写入了唤醒字节。
    """
    try:
        fd = os.open(path, os.O_WRONLY | os.O_NONBLOCK)
    except FileNotFoundError:
        io_trace("notify skipped: fifo missing (watchdog not started?)")
        return False
    except OSError as exc:
        if exc.errno == errno.ENXIO:
            io_trace("notify skipped: no fifo reader (watchdog down?)")
            return False
        io_trace(f"notify open failed: {type(exc).__name__}: {exc}")
        return False
    try:
        os.write(fd, b"\x01")
        io_trace("notify sent to watchdog")
    except OSError as exc:  # 管道满/EAGAIN：已有待处理通知，合并即可
        io_trace(f"notify coalesced/dropped: {type(exc).__name__}: {exc}")
    finally:
        os.close(fd)
    return True


def drain_notify_fifo(fd: int) -> None:
    """读空 FIFO 中积压的唤醒字节（通知可合并，字节内容无意义）。"""
    while True:
        try:
            chunk = os.read(fd, 65536)
        except BlockingIOError:
            return
        except OSError:
            return
        if not chunk:
            return


class RouteManager:
    """对「通往采集端的 IPv4 主机路由」的实际操作。"""

    def __init__(self, target: str, backend: str = "ip"):
        self.target = target
        self.backend = backend
        self.sim_file = os.path.join(STATE_DIR, "dryrun_route.json")

    def get(self) -> dict:
        """当前去往采集端的路由（nexthop 为 None 表示直连/无主机路由）。"""
        if self.backend == "dryrun":
            sim = self._sim_read()
            return {"target": self.target, "nexthop": sim.get("nexthop"),
                    "dev": "sim0", "backend": "dryrun"}
        out = subprocess.run(
            ["ip", "-j", "-4", "route", "get", self.target],
            capture_output=True, text=True, check=True,
        )
        data = json.loads(out.stdout)[0]
        return {"target": self.target, "nexthop": data.get("gateway"),
                "dev": data.get("dev"), "raw": data, "backend": "ip"}

    def install_host_route(self, nexthop: str) -> None:
        """试切换路：替换通往采集端的 /32 主机路由。"""
        if self.backend == "dryrun":
            print(f"[dryrun-route] ip -4 route replace {self.target}/32 via {nexthop}",
                  flush=True)
            self._sim_write({"target": self.target, "nexthop": nexthop})
            return
        subprocess.run(
            ["ip", "-4", "route", "replace", f"{self.target}/32", "via", nexthop],
            capture_output=True, text=True, check=True,
        )

    def restore(self, original_nexthop: str | None) -> None:
        """回退：恢复试切前的下一跳；原来没有主机路由则删除试切路由。"""
        if original_nexthop:
            if self.backend == "dryrun":
                print(f"[dryrun-route] ip -4 route replace {self.target}/32 "
                      f"via {original_nexthop}", flush=True)
                self._sim_write({"target": self.target, "nexthop": original_nexthop})
                return
            subprocess.run(
                ["ip", "-4", "route", "replace", f"{self.target}/32",
                 "via", original_nexthop],
                capture_output=True, text=True, check=True,
            )
        else:
            if self.backend == "dryrun":
                print(f"[dryrun-route] ip -4 route del {self.target}/32", flush=True)
                self._sim_write({"target": self.target, "nexthop": None})
                return
            subprocess.run(
                ["ip", "-4", "route", "del", f"{self.target}/32"],
                capture_output=True, text=True,  # 路由已不存在时忽略错误
            )

    def _sim_read(self) -> dict:
        try:
            with open(self.sim_file, encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"target": self.target, "nexthop": None}

    def _sim_write(self, sim: dict) -> None:
        os.makedirs(os.path.dirname(self.sim_file), exist_ok=True)
        with open(self.sim_file, "w", encoding="utf-8") as f:
            json.dump(sim, f)


# ---- 以下函数必须在 StateStore.locked() 临界区内调用 ----

def rollback_locked(state: dict, routes: RouteManager, reason: str) -> dict | None:
    """恢复原下一跳，把当前试切以 rolled_back 终态移入历史。幂等。"""
    cur = state.get("current")
    if not cur or cur["status"] not in ACTIVE_STATUSES:
        return None
    err = None
    try:
        routes.restore(cur.get("original_nexthop"))
    except Exception as exc:  # noqa: BLE001 - 回退路由失败也要落终态并记录
        err = f"{type(exc).__name__}: {exc}"
    cur["status"] = "rolled_back"
    cur["rollback_reason"] = reason
    cur["finished_at"] = now_iso()
    cur["finished_ts"] = time.time()
    if err:
        cur["route_restore_error"] = err
    state["history"].insert(0, cur)
    del state["history"][HISTORY_LIMIT:]
    state["current"] = None
    return cur


def keep_locked(state: dict) -> dict | None:
    """确认+探测均在期限内成功：保留新路由，试切以 kept 终态移入历史。"""
    cur = state.get("current")
    if not cur or cur["status"] not in ACTIVE_STATUSES:
        return None
    cur["status"] = "kept"
    cur["finished_at"] = now_iso()
    cur["finished_ts"] = time.time()
    state["history"].insert(0, cur)
    del state["history"][HISTORY_LIMIT:]
    state["current"] = None
    return cur


def expire_if_needed(state: dict, routes: RouteManager) -> dict | None:
    """到期仍未确认成功 -> 自动回退（无论探测是否仍在进行中）。"""
    cur = state.get("current")
    if cur and cur["status"] in ACTIVE_STATUSES and time.time() > cur["deadline_ts"]:
        return rollback_locked(state, routes, REASON_DEADLINE)
    return None
