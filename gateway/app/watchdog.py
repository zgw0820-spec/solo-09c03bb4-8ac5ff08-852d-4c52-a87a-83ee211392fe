"""独立看门狗进程：到期仍未确认成功的试切，自动恢复原下一跳。

与管理 API 分进程运行（同一容器内由 entrypoint 拉起）：
  - 管理 API 停止/崩溃时，回退依然按期限执行；
  - 覆盖「探测尚未返回但已到期」的场景：看门狗先回退，
    迟到的探测结果会被 API 侧按试切号+状态校验丢弃。

唤醒模型（不再每 0.5s 轮询状态文件）：
  看门狗平时阻塞在 select() 上，等待以下任一事件：
    1. API 提交状态变更后写入 TSW_NOTIFY_FIFO 的唤醒字节
       （发起试切 / 确认 / 探测定夺 / 查询触发的懒到期）；
    2. 当前试切自己的 deadline_ts 到期。
  被唤醒后总是重新加排他锁、全量重读状态，并用状态中的
  trial_id/status/deadline 再判定一次。因此：
    - 上一笔的旧定时不会误回退新试切；
    - 新试切期限更早时，FIFO 唤醒会打断旧等待，按新期限重算；
    - 看门狗单独重启后，首轮扫描即从已保存状态「接着等」，
      若发现已到期则立即回退。
"""
from __future__ import annotations

import os
import select
import time

from .core import (
    ACTIVE_STATUSES,
    Config,
    RouteManager,
    StateStore,
    drain_notify_fifo,
    ensure_notify_fifo,
    expire_if_needed,
    load_config,
)

# 仅在「读状态/等锁发生未预期异常」时的兜底重试间隔；
# 正常空闲没有任何周期性唤醒。
ERROR_RETRY_SEC = 1.0
# 仅当 FIFO 通知通道在特殊文件系统上建不起来时的降级轮询上限；
# 正常 Docker/Linux 环境走 FIFO，不会用到它（空闲仍零轮询）。
DEGRADED_POLL_SEC = 2.0


def next_deadline_timeout(state: dict, now: float | None = None) -> float | None:
    """根据当前已保存状态计算距离到期的等待秒数。

    返回 None 表示没有进行中的试切：无限等待状态变更通知。
    """
    cur = state.get("current")
    if not cur or cur.get("status") not in ACTIVE_STATUSES:
        return None
    return max(0.0, float(cur["deadline_ts"]) - (now if now is not None else time.time()))


def main() -> None:
    cfg = Config(load_config())
    # 看门狗自己的落盘不需要再通知自己，故不传 on_change。
    store = StateStore()
    routes = RouteManager(cfg.target, backend=cfg.route_backend)

    fifo_path = ensure_notify_fifo()
    # O_RDWR 常驻打开：自己持有读端+写端，避免外部写端全部关闭后
    # poll/select 持续报 POLLHUP 造成忙等。
    notify_fd: int | None
    degraded = False
    try:
        notify_fd = os.open(fifo_path, os.O_RDWR | os.O_NONBLOCK)
    except OSError as exc:  # noqa: BLE001 - 极端文件系统不支持 FIFO 时降级
        notify_fd = None
        degraded = True
        print(f"[watchdog] FIFO unavailable ({type(exc).__name__}: {exc}); "
              f"degraded to {DEGRADED_POLL_SEC}s cap polling", flush=True)

    print(
        f"[watchdog] started: target={cfg.target} backend={cfg.route_backend} "
        f"notify={fifo_path}",
        flush=True,
    )

    while True:
        wait_timeout: float | None = None
        rolled = None
        try:
            # 每轮（含启动首轮）都在锁内全量重读：
            # 重启恢复 / 到期执行 / 通知后重算，走的是同一条路径。
            with store.locked() as state:
                rolled = expire_if_needed(state, routes)
                wait_timeout = next_deadline_timeout(state)
        except Exception as exc:  # noqa: BLE001 - 看门狗不允许因单次异常退出
            print(f"[watchdog] error: {type(exc).__name__}: {exc}", flush=True)
            time.sleep(ERROR_RETRY_SEC)
            continue

        if rolled:
            print(
                f"[watchdog] trial {rolled['trial_id']} auto rolled back "
                f"(reason={rolled['rollback_reason']}, restored via "
                f"{rolled.get('original_nexthop') or 'direct'})",
                flush=True,
            )

        watch_fds = [notify_fd] if notify_fd is not None else []
        if degraded:
            # 没有 FIFO 时也保证到期能被发现：给等待时间加一个轮询上限。
            if wait_timeout is None or wait_timeout > DEGRADED_POLL_SEC:
                wait_timeout = DEGRADED_POLL_SEC

        if wait_timeout is None:
            print("[watchdog] idle: no active trial; "
                  "waiting for state change (no periodic state-file IO)",
                  flush=True)
        else:
            print(f"[watchdog] waiting {wait_timeout:.3f}s until deadline "
                  f"or state change", flush=True)

        try:
            ready, _, _ = select.select(watch_fds, [], [], wait_timeout)
        except InterruptedError:
            continue  # 收到信号（如调试用信号）：直接重新扫描
        except OSError as exc:  # noqa: BLE001
            print(f"[watchdog] select error: {type(exc).__name__}: {exc}",
                  flush=True)
            time.sleep(ERROR_RETRY_SEC)
            continue

        if notify_fd is not None and ready:
            drain_notify_fifo(notify_fd)
            print("[watchdog] wake: state-change notification", flush=True)
        elif watch_fds and not ready:
            # select 超时返回（ready 为空）：到期点到了，下一轮加锁复核并回退。
            # wait_timeout is None 且有 FIFO 时 select 会无限阻塞，走不到这里。
            print("[watchdog] wake: deadline reached", flush=True)
        elif not watch_fds:
            # 降级轮询：不打印 "deadline reached"（可能只是轮询上限到点），
            # 直接进入下一轮复核即可。
            pass


if __name__ == "__main__":
    main()
