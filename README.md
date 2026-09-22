# 本地异步任务编排服务

一个可在本地直接运行的异步任务编排服务：FastAPI 提交/查询/取消任务，
SQLite（WAL）做权威状态持久化，asyncio 执行器模拟带优先级与并发上限的调度。
目标是让**重复提交、并发执行、进程重启**三类场景下任务状态始终可预期。

## 快速开始

```bash
uv sync                       # 安装依赖（Python >= 3.14）
uv run uvicorn main:app --reload --port 8000
```

提交任务：

```bash
curl -X POST http://localhost:8000/tasks -H 'Content-Type: application/json' -d '{
  "idempotency_key": "order-123",
  "task_type": "echo",
  "payload": {"message": "hello"},
  "priority": 10,
  "scope": "tenant-a",
  "timeout_seconds": 5,
  "max_retries": 2
}'
# 201 -> {"task_id": "...", "status": "pending", "deduplicated": false, ...}

curl http://localhost:8000/tasks/<task_id>
curl -X POST http://localhost:8000/tasks/<task_id>/cancel
curl http://localhost:8000/metrics
```

内置任务类型：

| task_type | payload | 行为 |
| --- | --- | --- |
| `echo` | `{"message"?: ...}` | 回显，恒成功 |
| `flaky` | `{"fail_times": N}` | 前 N 次执行抛可重试错误，之后成功（验证重试） |
| `fail` | `{"retryable": bool, "message"?: str}` | 按类型失败（验证不可重试） |
| `sleep` | `{"seconds": float}` | 睡眠，响应取消与超时（验证取消/超时） |

## 任务状态机

```
            提交成功
              │
              ▼
         ┌────────┐  调度占用租约(CAS)  ┌─────────┐
         │PENDING │ ─────────────────▶ │ RUNNING │
         └────────┘                    └─────────┘
              ▲                            │  │  │  │
              │ (启动恢复/到期提升)          │  │  │  ├─ 处理器成功 ───────────▶ SUCCEEDED
         ┌────────────┐  到期重试           │  │  │  ├─ 不可重试失败/重试耗尽 ──▶ FAILED
         │ RETRY_WAIT │ ◀────可重试失败────┘  │  │  ├─ asyncio 硬超时 ─────────▶ TIMED_OUT
         └────────────┘                      │  │  └─ 取消请求先到（仲裁） ────▶ CANCELLED
                                             │  └─ 取消事件触发（handler 内 sleep 主动让出）
   PENDING / READY / RETRY_WAIT 任意等待态 ──▶ CANCELLED（直接 CAS 落终态）
```

- `PENDING`：提交落库后的初始态；
- `READY`：可被调度（含崩溃恢复回收的 `RUNNING`、到期的 `RETRY_WAIT`）；
- `RETRY_WAIT`：可重试失败后的退避等待，`not_before` 记录到期时刻；
- `RUNNING`：执行租约已被某执行器持有（`lease_owner`）；
- **终态** `SUCCEEDED / FAILED / TIMED_OUT / CANCELLED`：只进不出，
  所有终态写入都带 `version` 乐观锁（CAS），行不匹配则不写——**终态不可能被重复转换**。

取消语义：

- 等待态任务：`cancel` 直接 CAS 落 `CANCELLED` 并移出内存队列；
- 运行中任务：先持久化 `cancel_requested=1`（不改状态），同时唤醒执行协程；
  处理器在 `ctx.sleep()` 边界轮询到取消信号即抛 `CancelledError` 终止；
- **取消与完成竞速**：成功/失败/超时的终态写入对 `cancel_requested` 敏感，
  若取消先到则终态写入失败，由取消仲裁统一落 `CANCELLED`；反之完成先到，
  取消请求返回 `409 already_terminal`。两种情况下终态都恰好产生一次。

## 重试规则

- 处理器抛 `RetryableTaskError`：当 `attempts <= max_retries` 时
  `RUNNING -> RETRY_WAIT`，按**指数退避**等待后重回 `READY`；
- 退避公式：`delay = min(retry_base * 2**(attempts-1), retry_cap)`
  （默认 base=0.05s，cap=2s；确定性退避，无随机抖动，便于复现）；
- `attempts > max_retries`：落 `FAILED`，`last_error` 固定前缀
  `retries exhausted (N attempts): ...`，与普通失败原因可区分；
- `NonRetryableTaskError` 及未声明异常：**不消耗重试预算**，一次即 `FAILED`；
- `TIMED_OUT` 是确定终态（不自动重试），执行资源立即释放。

## 调度与并发控制

- **双重并发门控**：全局信号量（默认 4）+ 每作用域信号量（默认 2，
  可用 `SchedulerConfig.scope_concurrency` 按 scope 覆盖）。
  启动一个任务必须同时拿到两个名额，任一作用域上限绝不被突破；
- **优先级取用**：每个作用域一个就绪队列，排序分
  `effective = priority + aging_rate * 等待秒数`，同分 FIFO。
  优先级 `HIGH=10 / NORMAL=5 / LOW=1`；
- **防饥饿（老化）**：低优先级任务等待越久有效分越高，
  即使高优先级任务持续到达，低优先级任务也会在有限时间内被取到
  （默认每等 1 秒 +2 分，约 2.5 秒即可追平 HIGH）；
- **作用域间公平轮转**：调度循环每轮对作用域做轮询起点偏移，
  避免某作用域长期占满调度机会；
- **执行租约**：任务开始执行前以 `lease_for_run(id, version, owner)`
  原子地 `READY -> RUNNING`；CAS 失败（已取消/已恢复/状态已变）则不执行、不计数。

## 持久化与恢复语义

- SQLite 单库文件（默认 `data/tasks.db`），`PRAGMA journal_mode=WAL`
  + `synchronous=FULL`，每个状态转换在独立短事务内提交并落盘；
- `idempotency_key` 有 **UNIQUE 约束兜底**：并发重复提交时只有一个 INSERT 成功，
  其余读取既有任务并返回 `deduplicated=true` 与**相同 task_id**，
  因此重复提交不可能产生重复执行；
- 同键但载荷不同（规范化 JSON 的 SHA-256 不同）→
  `409 payload_conflict`，与参数非法的 `422 invalid_request` 可区分；
- **启动恢复**（`recover_stale_leases`）：
  1. 所有遗留 `RUNNING`（上个进程崩溃/被 kill）→ `READY`、清空 `lease_owner`；
  2. 已到期 `RETRY_WAIT` → `READY`；未到期保留，由执行器定时器到期再提升；
  3. 全部非终态任务重新入队；
- 崩溃窗口内正在执行的任务**没有提交任何终态**，恢复后最多多执行一次
  （at-least-once 执行、exactly-once 终态提交）；处理器应避免不可回滚的外部副作用；
- 优雅停机（`SIGTERM`/lifespan shutdown）会等待在执行任务收敛后再关库，
  此时终态已持久化，重启不会重复执行。

## HTTP 接口与错误码

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /tasks` | 提交任务（201；重复提交同键同载荷返回 `deduplicated=true`） |
| `GET /tasks/{id}` | 查询任务 |
| `GET /keys/{idempotency_key}` | 按业务唯一键查询 |
| `POST /tasks/{id}/cancel` | 请求取消 |
| `GET /metrics` | 运行中/排队计数 |
| `GET /health` | 健康检查 |

错误响应统一为 `{"error": <code>, "message": ...}`：

| HTTP | error code | 触发条件 |
| --- | --- | --- |
| 422 | `invalid_request` | 参数非法、未知任务类型、请求体校验失败 |
| 409 | `payload_conflict` | 同业务唯一键但载荷不一致 |
| 404 | `task_not_found` | 任务/唯一键不存在 |
| 409 | `already_terminal` | 对终态任务请求取消 |

## 本地验证

```bash
uv run pytest                      # 全部 37 个用例
uv run pytest -s -o log_cli=true   # 打印每个用例的输入与判定依据日志
```

测试覆盖（`tests/`）：

- `test_idempotency.py`：顺序/20 路并发重复提交只执行一次、
  载荷冲突、各类非法参数拒绝原因可区分；
- `test_scheduling.py`：作用域上限与全局上限不被突破（采样峰值）、
  高优先级先取用、持续高优先级下老化让低优先级不饥饿、超时转终态并释放名额；
- `test_lifecycle.py`：等待态取消、运行中取消及时生效（~毫秒级）、
  10 组取消与完成竞态只有单一终态、终态不可再转换；
- `test_retry_recovery.py`：可重试失败耗尽预算（attempts=N+1、错误前缀可区分）、
  不可重试单次失败、**子进程硬崩溃（os._exit）后重启恢复无丢失且终态只提交一次**、
  优雅重启后终态保持且 `RETRY_WAIT` 继续执行；
- `test_repository.py` / `test_ready_queue.py`：CAS 版本守卫、取消仲裁、
  租约回收、优先级队列与老化的单元级验证。

## 代码结构

```
task_orchestrator/
  models.py       # 状态/优先级/请求/记录/配置类型
  errors.py       # 可区分的业务错误码与处理器异常
  repository.py   # SQLite 仓储：UNIQUE 幂等 + version CAS 状态转换 + 启动恢复
  ready_queue.py  # 老化优先级就绪队列
  handlers.py     # 执行上下文（取消/超时可中断 sleep）与内置处理器
  executor.py     # 并发门控、调度循环、租约执行、退避重试、取消仲裁
  orchestrator.py # 门面：校验、幂等提交、查询、取消
  app.py          # FastAPI 装配与错误码映射
main.py           # uvicorn 入口
tests/
  crash_worker.py # 硬崩溃恢复测试的隔离子进程
```
