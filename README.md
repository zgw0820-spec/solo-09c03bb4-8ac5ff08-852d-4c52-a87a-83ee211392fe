# 采集网关静态路由试切 API

现场采集网关的静态路由**试切**（trial switch）能力：值班员选择**预置备用下一跳**和**试用秒数**，
网关实际替换通往采集端的一条 Linux IPv4 主机路由（`/32` host route）并开始计时；凭本次试切号
发起确认后，网关经新下一跳对采集端做 TCP 探测。**确认与探测都在期限内成功才保留新路由**；
探测失败立即恢复原下一跳；到期仍未确认成功由**独立看门狗进程**自动回退——即使管理 API
已停止、或探测尚未返回。一次只允许一个试切；迟到的探测结果与旧试切号不影响当前试切。

> **本次整改重点（空闲 I/O / 唤醒机制）**：旧看门狗每 0.5s 醒一次，且状态事务无论是否变化
> 都把 `state.json` 整文件重写一遍；查询接口也会连带两次重写。现已改为：
> **看门狗阻塞等待「FIFO 状态变更通知」或「当前试切到期」**；状态内容没变就**不写盘、不通知**，
> 纯查询走共享锁只读事务。详见下文[「空闲 I/O 与唤醒机制」](#空闲-io-与唤醒机制本次整改)。

## 架构

```
┌──────────────────────────── gateway 容器（NET_ADMIN）────────────────────────┐
│  entrypoint.sh                                                               │
│   ├─ 启动时装初始主机路由: <采集端>/32 via <主用下一跳>                       │
│   ├─ python3 -m app.watchdog   ← 独立看门狗：select 等 FIFO 通知或 deadline   │
│   └─ uvicorn app.main:app      ← 管理 API（FastAPI，:8000）                  │
│  两进程通过 /state/state.json（flock 互斥 + 原子写+fsync）共享试切状态        │
│  变更唤醒通过 /state/watchdog.notify（FIFO，可合并/可丢失的 1 字节通知）       │
└──────────────────────────────────────────────────────────────────────────────┘
        │ 172.30.0.10            fieldnet 172.30.0.0/24（Docker bridge，隔离环境）
  ┌─────┼──────────────────┬───────────────────┬───────────────────┐
  ▼     ▼                  ▼                   ▼                   ▼
collector            nh-primary         nh-backup-good       nh-backup-broken
172.30.0.100:9000    172.30.0.11        172.30.0.12          172.30.0.13
（采集端，探测目标）  ip_forward=1        ip_forward=1         ip_forward=0（不转发）
```

关键设计：

- **回退不依赖管理 API**：看门狗是独立进程（entrypoint 拉起、退出会自动重启）。
  API 进程被杀掉后，到期回退仍由看门狗执行；状态落盘（原子 rename + fsync），API 重启后可查结果。
- **事件驱动、空闲零轮询**：看门狗不再每 0.5s 扫盘，平时阻塞在 `select()` 上，
  等待 API 的 FIFO 变更通知或当前试切自己的截止时刻；空闲期间没有任何周期性状态文件 I/O。
- **查询不回写、不唤醒**：`StateStore.locked()` 用前后内容比较做脏检查，未变化不写盘、
  不触发通知；GET 接口的读取段使用 `StateStore.read()` 共享锁只读事务。
- **期限永远以状态文件为准**：看门狗每次醒来都重新加锁、全量重读，按**当前 current 试切**的
  `trial_id/status/deadline_ts` 再判定。旧定时器只是一次 `select` 超时，不存在残留定时任务，
  因此上一笔的旧期限不可能误回退新试切；新试切期限更早时，FIFO 通知会打断旧等待并重算超时。
- **看门狗重启可续等**：启动首轮即扫描已保存状态——已到期立刻回退；未到期只等剩余时间。
- **一次一个试切**：`trialing`/`probing` 状态占用试切槽位，再发起返回 409。
- **迟到结果无效**：探测协程完成时校验「试切号一致 + 状态仍为 probing + 未超期」，
  三者任一不满足即丢弃；旧试切号确认返回 404，不影响当前试切。
- **跨进程互斥**：API 与看门狗的所有状态变更都在 `flock` 临界区内完成，
  到期回退在两个进程中是同一幂等逻辑（`expire_if_needed`），谁先抢到锁谁执行。
- **恢复原下一跳**：试切开始时用 `ip -j route get <采集端>` 记录原下一跳，
  回退时 `ip route replace` 恢复（原本无主机路由则删除试切路由）。

## 空闲 I/O 与唤醒机制（本次整改）

| 场景 | 旧实现 | 现实现 |
|---|---|---|
| 无试切、无请求 | 每 0.5s 加锁读 + **无条件整文件重写** `state.json`（约 2 写/s） | 打印一次 `idle ... waiting` 后无限阻塞在 `select`；**0 读 0 写** |
| GET 查询且状态未变 | `expire_current()` 与查询段各开一个写事务，**每次查询重写 2 次**并（若接通知）唤醒看门狗 | 过期检查事务脏检查不写；查询段用 `LOCK_SH` 只读事务；**0 写盘、0 通知、看门狗不醒** |
| POST/confirm/探测定夺 | 写状态 | 仅在内容真正变化时写盘，锁释放后向 FIFO 写 1 字节唤醒看门狗 |
| 试切到期 | 最多 0.5s 轮询延迟 | `select` 精确等到 `deadline_ts`，实测落终态滞后期限 **5–8ms**（本机 dryrun） |
| 看门狗重启 | 0.5s 内恢复轮询 | 首轮锁内扫描：过期立即回退（实测 38–41ms），未过期按剩余 deadline 等待 |

通知 FIFO 是**可合并、可丢失的提示**而非消息队列：通知可能因为看门狗未运行而 `ENXIO` 被丢弃，
但看门狗启动/被唤醒后都会全量重读状态，所以不漏事。

设置 `TSW_IO_DEBUG=1`（compose 里已默认打开）后，每次状态文件读写和通知都会打印
`[io pid=<进程号>] ...`，可在 `docker compose logs gateway` 里按 PID 区分 API 与看门狗。

## 试切状态机

```
POST /api/trials ──► trialing ──confirm──► probing ──探测成功且在期限内──► kept（保留新路由）
                      │  │                    │
                      │  │                    ├─探测失败────────► rolled_back / probe_failed（立即回退）
                      │  └─confirm 已超期────► rolled_back / deadline_exceeded
                      └─到期未确认（看门狗）─► rolled_back / deadline_exceeded（探测在途也回退，迟到结果丢弃）
```

## API 一览（Swagger: `http://localhost:8080/docs`）

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/trials` | 发起试切：`{"nexthop": "backup-good", "duration_sec": 60}`；进行中再发起返回 409 |
| POST | `/api/trials/{trial_id}/confirm` | 凭试切号确认，网关经新下一跳探测采集端（202 受理，异步探测） |
| GET | `/api/trials/current` | 当前进行中的试切（含剩余秒数） |
| GET | `/api/trials` | 当前试切 + 历史结果 |
| GET | `/api/trials/{trial_id}` | 试切结果与回退原因（`rollback_reason`: `probe_failed` / `deadline_exceeded`） |
| GET | `/api/route` | 当前通往采集端的路由（实测换路/回退的证据） |
| GET | `/api/config` | 预置下一跳与探测参数 |

`POST /api/trials` 在 Swagger 内置三个命名样例，可直接试出两种结局：

- **keep**：`backup-good` + 60s → 期限内 confirm → 探测成功 → `kept`，路由保留；
- **rollback_probe_failed**：`backup-broken` + 60s → confirm → 探测超时失败 → 立即回退；
- **rollback_deadline**：任意下一跳 + 15s → 不确认 → 看门狗到期自动回退。

## 启动（Docker Compose）

```bash
docker compose up -d --build
# 或： bash demo/docker_demo.sh up
# Swagger UI: http://localhost:8080/docs
docker compose exec gateway ip route get 172.30.0.100   # 初始：via 172.30.0.11
```

Swagger 操作：打开 `http://localhost:8080/docs` → `POST /api/trials` → **Try it out**，
请求体下拉可选 `keep` / `rollback_probe_failed` / `rollback_deadline` 三个样例 → **Execute**；
然后用响应里的 `trial_id` 在 `POST /api/trials/{trial_id}/confirm` 确认，
并用 `GET /api/trials/current`、`GET /api/trials/{trial_id}`、`GET /api/route` 查询核对。

### 演示 1：核对空闲时的状态文件读写（不写盘 / 看门狗不醒）

一键脚本（主机需有 `docker compose`、`curl`、`python3`）：

```bash
bash demo/docker_demo.sh idle
```

它会：记录 `state.json` 的 `inode:mtime:size` 和看门狗 wake 计数 → 纯空闲 5s →
再次比对（应完全不变）→ 连发 6 个 GET 查询 → 再比对（仍不变、看门狗 wake 不增加），
最后打印最近的 `[io pid=...]` / `[watchdog]` 日志。

等价手工命令：

```bash
WPID=$(docker compose exec -T gateway sh -c "pgrep -f app.watchdog | head -1" | tr -d '\r')
echo "watchdog pid=$WPID"

docker compose exec gateway stat -c '%i %y %s' /state/state.json
sleep 5                                                  # 期间不要在 Swagger 发请求
docker compose exec gateway stat -c '%i %y %s' /state/state.json   # inode/mtime/size 不变

for p in /api/trials/current /api/trials; do curl -s "http://localhost:8080$p" >/dev/null; done
docker compose exec gateway stat -c '%i %y %s' /state/state.json   # 查询后仍不变

docker compose logs --tail 80 gateway | grep -E "io pid=$WPID|\[watchdog\]"
# 期望：该看门狗 PID 没有新的 state read/write，没有新的 "wake:"；
#       API(uvicorn) PID 下只有 state read，没有 state write。
```

### 演示 2：API 立刻退出后，按保存期限核对实际回退时点

一键脚本（自动发 15s 试切、`SIGKILL` API、以 20ms 粒度读状态文件量时差、查真实路由）：

```bash
bash demo/docker_demo.sh fallback 15
# 结束后 API 处于被杀死状态，用下面命令恢复：
docker compose restart gateway
```

等价手工步骤：

```bash
# 1) Swagger 里用 rollback_deadline 样例（15s）发起，或：
curl -s -X POST http://localhost:8080/api/trials \
  -H 'Content-Type: application/json' \
  -d '{"nexthop":"backup-good","duration_sec":15}'
#    记下 trial_id 与 deadline_ts

# 2) 响应一回来立刻强杀管理 API（看门狗是独立进程，不受影响）
docker compose exec gateway pkill -9 -f 'uvicorn app.main'

# 3) 看到期后的真实路由与看门狗日志
sleep 16
docker compose exec gateway ip route get 172.30.0.100   # 已恢复 via 172.30.0.11
docker compose logs gateway | grep watchdog             # auto rolled back ...

# 4) 恢复管理 API
docker compose restart gateway
```

脚本额外打印 `finished_ts - deadline_ts`，即**实际回退时点相对保存期限的偏差**（应接近 0）。

### 演示 3：看门狗单独重启后接着等 / 过期立即回退

```bash
bash demo/docker_demo.sh wdrestart 10
```

手工等价：发起一个 10s 试切 → `docker compose exec gateway pkill -9 -f app.watchdog` →
entrypoint 约 2s 后自动重新拉起看门狗，日志出现
`[watchdog] waiting 7.x s until deadline or state change`（按**剩余**期限，不是重新等 10s），
到期仍精确回退。若看门狗停足够久、试切已过期，再启动时会在首轮扫描立即回退。

### 容器内一键关键场景自检（dryrun 路由后端，不碰真实路由）

`docker-compose.yml` 已把 `./demo` 只读挂进容器：

```bash
docker compose exec gateway python /demo/verify_scenarios.py
# 或在主机： bash demo/docker_demo.sh verify
```

该脚本自起独立的看门狗/API/本地探测端口（临时状态目录、180xx 端口、dryrun 路由），
与 compose 里正式运行的服务互不干扰，覆盖下面表格中的 10 组断言。

## 本地实测结果（本沙箱无 Docker，用 dryrun 路由后端真实双进程试跑）

本沙箱没有 `docker`/podman/`docker.sock`，进程 capability 为空，无法运行容器运行时；
因此 `docker compose up` 与容器内真实 `ip route`/跨容器转发未能在此执行。
代码内置的 `TSW_ROUTE_BACKEND=dryrun` 会打印等效 `ip` 命令并维护模拟路由表，
**状态机、FIFO 唤醒、看门狗双进程、flock 互斥、计时逻辑与容器内完全一致**。

### 整改前基线（同一沙箱实测）

旧看门狗空闲时 `state.json` 的 inode 每 1s 窗口都变化、3s 内 mtime 跳变 3 次，
即约每 0.5s 一次无条件原子重写。

### `demo/verify_scenarios.py` 实测：10/10 通过

运行方式：

```bash
pip install -r gateway/requirements.txt httpx pytest   # 验证脚本本身仅需标准库；子进程需要 requirements
python3 demo/verify_scenarios.py
```

| 编号 | 场景 | 实测结果（本次真实输出摘要） |
|---|---|---|
| A1 | 空闲 3s 看门狗零 I/O | `watchdog 新增 read=0 write=0`，`state.json` inode/mtime 未变 |
| A2 | 3 轮共 9 个 GET 查询不回写 | API 日志 `read=18 write=0`（每查询 1 次过期检查读 + 1 次只读事务读），inode+mtime 不变 |
| A3 | 查询不唤醒看门狗 | 查询窗口内看门狗 `wake` 次数 0、新增 I/O 行数 0 |
| B | 试切落盘后立刻 `SIGKILL` API | `deadline_exceeded`，`finished_ts - deadline_ts = +0.007s`，dryrun 路由恢复 `nexthop=None`，看门狗日志有 `auto rolled back` |
| C1 | 保留 30s 的 A 后立刻开 5s 的 B（新期限早 25s） | B 按自己的新期限回退，偏差 `+0.005s`（证明 FIFO 打断了旧的 30s 等待） |
| C2 | 保留 5s 的 C 后开 8s 的 D；跨过 C 的旧期限点 +1s | D 仍为 `trialing`、路由仍在新下一跳 `192.0.2.2`；D 到期偏差 `+0.008s` 回退，旧定时未误撤 |
| E | 一次一个试切 / 旧试切号 | 重复 POST → `409`；陌生 id confirm → `404` |
| D1 | 看门狗被杀 2s 后重启 | 日志 `waiting 6.953s ...`（9s 期限的剩余值），回退偏差 `+0.008s`，没有重新等满 9s |
| D2 | 看门狗缺席直到试切过期 1s 后再启动 | 启动前状态仍 active；启动到落终态 `0.038s`（<1s，立即回退） |
| D3 | 重启后的看门狗接收新试切通知 | 新 5s 试切回退偏差 `+0.005s`，新增日志段可见 `wake: state-change notification` |

另外手工补测：

- **探测失败立即回退**：无探测监听时 confirm `backup-broken` → 202 → 约 2s 后
  `status=rolled_back, reason=probe_failed, probe.state=failed`，模拟路由恢复；
  看门狗随后被终态变更唤醒一次并回到 idle。
- **保留成功路径**：验证脚本 C1/C2 中 confirm `backup-good` 后均进入 `kept`。
- **Swagger/OpenAPI**：`/docs` 返回 200；`/openapi.json` 中
  `keep` / `rollback_probe_failed` / `rollback_deadline` 三个命名样例齐全。
- **StateStore 单元级语义**：自定义路径实例上，空事务不写盘不回调；`read()` 中偷改内存不落盘；
  有变化的事务恰好写一次并回调一次。
- 全部 Python 文件 `compileall` 通过；`demo/docker_demo.sh` 通过 `bash -n`，
  两段内嵌 Python 通过语法编译；`docker-compose.yml` 通过 YAML 解析。

## 未完成项 / 限制（待补）

1. **未在真实 Docker 环境实跑（首要待补）**：本沙箱无容器运行时且无任何 capability，
   `docker compose up --build`、Swagger 点击、容器内真实 `ip route` 换路/回退时点、
   `backup-good`/`backup-broken` 的跨容器转发差异均未实际执行。已提供
   `demo/docker_demo.sh`（`idle`/`fallback`/`wdrestart`/`verify`）与容器内自检脚本，
   请在有 Docker 的主机按上文步骤验收；脚本本身只做过语法检查，未做 docker 实跑。
2. **容器/主机崩溃的耐久性边界**：状态写入已加文件 fsync 与目录 fsync；但 FIFO 通知是易失的。
   极端情况下「状态已写、通知丢失」时，若没有任何进行中试切则无影响；若有进行中试切，
   看门狗重启或下一次通知后会按 deadline 纠正——但看门狗不重启且通知全丢的纯理论窗口内，
   到期动作可能缺少一次唤醒。正常的「API 退出/被杀」不影响已写入 FIFO 缓冲的字节，
   且看门狗持有 FIFO 读端，本方案的演示与测试场景不受此限。
3. **容器重建不保留网络现场**：`docker compose restart` 只重启进程/容器、网络命名空间仍在；
   若 `down`/重建容器，netns 与路由复位而 `/state` 在容器可写层也会丢失（compose 未挂命名卷）。
   需要跨重建保留历史/期限时，可给 gateway 增加 `/state` 命名卷；注意清理旧的活动试切状态。
4. **无认证鉴权**：按内网管理面假设设计，未接认证；生产暴露前应加认证/鉴权。
5. **探测为 TCP 连接探测**：判活采集端 `probe_port`；如需 ICMP 或应用层探活可替换
   `tcp_probe` 实现。
6. **无手动取消接口**：值班员只能等期限到期或确认定夺，未提供主动 abort。
7. **entrypoint 的 API-DOWN 日志每 2s 重复一行**：沿用基线行为，仅影响日志观感，不影响功能；
   恢复 API 仍按基线约定使用 `docker compose restart gateway`（未改成自动拉起 API，
   以免改变「API 保持退出以供演示」的既有操作方式）。

## 本地复现（无需 Docker）

```bash
python3 -m pip install -r gateway/requirements.txt
python3 demo/verify_scenarios.py        # 约 1 分钟，10 组断言；用 TSW_VERIFY_KEEP=1 保留现场日志

# 或手工双进程（deadline/空闲类演示；探测成功需另在 19000 端口起 TCP 服务）：
export TSW_CONFIG=$PWD/gateway/config.yaml
export TSW_STATE_DIR=/tmp/tsw-state TSW_ROUTE_BACKEND=dryrun TSW_IO_DEBUG=1
cd gateway
python3 -m app.watchdog &
uvicorn app.main:app --port 18000 &
```
