# 本地异步任务编排服务（Python / FastAPI / SQLite）

一套可在本地直接运行的异步任务编排服务：用 **SQLite 文件持久化** 模拟真实调度系统的
任务存储，用 **每作用域 asyncio 信号量** 模拟执行资源配额，重点保证三件事在
重复提交、并发执行与进程重启下始终可预期：

1. 同一业务幂等键的任务绝不重复执行；
2. 终态确定、可重试失败有退避、取消及时生效；
3. 进程异常退出后未完成任务不丢失、不重复执行。

## 快速开始

```bash
uv sync
uv run uvicorn main:app --reload          # 启动服务（默认 tasks.db）
uv run pytest -q                          # 运行全部测试（20 个用例）
uv run pytest -q --log-cli-level=INFO     # 含输入与判定依据的详细日志
```

可选环境变量：`TASK_DB_PATH`（数据库文件路径）、`TASK_DEFAULT_CONCURRENCY`
（默认作用域并发上限，默认 2）、`TASK_SCOPE_CONCURRENCY`（JSON 形式的按作用域配额）。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/tasks/submit` | 提交任务（201 新建 / 200 幂等重复） |
| GET | `/tasks/{task_id}` | 按任务 ID 查询状态 |
| GET | `/tasks/by-key/{idempotency_key}` | 按业务幂等键查询 |
| POST | `/tasks/{task_id}/cancel` | 请求取消（终态任务幂等返回） |
| GET | `/health` | 健康检查 |

提交请求示例：

```json
{
  "idempotency_key": "order-20260922-001",
  "task_type": "flaky",
  "payload": {"fail_times": 2},
  "priority": "normal",
  "scope": "default",
  "max_attempts": 3,
  "timeout_seconds": 10.0
}
```

### 拒绝原因（可区分）

| 场景 | HTTP | error.code |
| --- | --- | --- |
| 字段约束不通过（如 `max_attempts<1`、`timeout_seconds<=0`、未知枚举） | 422 | Pydantic `detail[].loc` 标明字段 |
| 任务类型未注册 | 422 | `unknown_task_type` |
| 幂等键为空 / scope 非法 等服务层校验 | 422 | `invalid_*` |
| 同一幂等键但载荷（或 task_type/scope/max_attempts）不一致 | 409 | `payload_conflict` |
| 任务不存在 | 404 | `not_found` |
| 同键同载荷重复提交 | 200 | 非错误，响应体 `already_exists=true` 且 `task_id` 与首次一致 |

内置任务类型：`echo`（回显，可 `sleep`）、`flaky`（前 `fail_times` 次调用抛
可重试错误）、`always_fail`（直接不可重试失败）、`slow`（按 `duration` 秒
分段睡眠、每 0.02s 响应取消）。

## 任务状态机

```
                 submit
                   │
                   ▼
              ┌─────────┐  调度领取(条件UPDATE, attempts+1)  ┌─────────┐
              │ pending │ ─────────────────────────────────▶ │ running │
              └─────────┘                                    └─────────┘
                ▲   │ 取消(未启动)              成功(且无取消请求)  │  │  │
   可重试失败   │   └──────────────┐   ┌───────────────────────┘  │  │
   (退避等待)  │                  ▼   ▼                            │  │
   ┌──────────┘            ┌───────────┐ 不可重试失败/重试耗尽     │  │
   └───────────────────────▶│ cancelled │◀── 运行中收到取消并到检查点│  │ 超时
                           └───────────┘                            │  │
                                ┌───────────┐ 可重试失败耗尽/超时耗尽│  │
                                │  failed   │◀───────────────────────┘  │
                                └───────────┘                           │
                                ┌───────────┐                           │
                                │ succeeded │◀──────────────────────────┘
                                └───────────┘
```

- 三个终态：`succeeded / failed / cancelled`，均由**带前置状态条件的 SQL UPDATE**
  落库（`WHERE status='running' ...`），因此终态不可能被重复转换。
- **取消与完成竞态**：成功落库要求 `cancel_requested=0`；若处理器完成时取消请求
  已先落库，则成功 UPDATE 命中 0 行，改由 `mark_cancelled_if_running` 落 cancelled。
  两种结果只可能赢一个，不会出现 “succeeded 且 cancel_requested=1” 的矛盾状态。
- pending 任务的取消直接进入终态（不再被领取）；终态任务上重复取消原样返回。

## 重试与超时规则

- 处理器可抛 `RetryableTaskError`（可重试）或 `FatalTaskError`（不可重试，
  直接 failed）；未预期异常按可重试处理，`error_code=handler_error`。
- **指数退避**：第 n 次失败后等待 `min(base_backoff × backoff_factor^(n-1),
  max_backoff)`，默认 0.2s × 2^k，上限 30s。等待期间任务保持 `pending`，
  `scheduled_at` 记录到点时刻，调度器只领取到点任务。
- 当 `attempts == max_attempts` 仍失败，进入 failed：
  - 重试耗尽：`error_code=retryable`（超时耗尽为 `timeout`，致命失败为 `fatal`），
    失败原因在查询结果中可区分。
- 每次领取执行 `attempts += 1`；`asyncio.wait_for` 强制任务超时，超时按可重试
  失败处理，因此超时任务一定会进入确定状态（重试或 failed），不会长期占用执行槽。

## 优先级调度与并发控制

- 每个 tick（默认 20ms）取出所有到点可运行任务，按排序键依次派发：
  `score = 优先级序数×1000 − 等待时长/aging_interval`，score 越小越先执行；
  同分时按 `scheduled_at`、再按任务 ID，保证派发顺序确定。
- 优先级序数：high=0、normal=1、low=2。
- **老化（aging）防饿死**：等待时间持续累积会不断降低 score。一个等待足够久的
  low 任务，其 score 会越过刚刚到达的 high 任务，因此“持续到达的高优先级任务”
  不会让低优先级任务无限期等待（有测试覆盖该性质）。
- **作用域并发上限**：每个 `scope` 持有独立的 `asyncio.Semaphore`
  （`TASK_SCOPE_CONCURRENCY` 可配，默认上限 2）。派发前先在调度循环中
  acquire、工作协程结束后 release；有测试用计数器验证任意时刻同 scope 在跑数
  恰好不超过配额。不同作用域互不阻塞。

## 持久化与恢复语义

- 所有任务状态保存在单个 SQLite 文件（WAL 模式，`busy_timeout=5s`）。
- **并发重复提交**：提交事务使用 `BEGIN IMMEDIATE` 先取写锁再按
  `idempotency_key UNIQUE` 查重插入，并发下恰好一个请求插入成功，其余拿到
  同一条记录；载荷用规范化 JSON（键排序、无空白）的 SHA-256 比较。
- **领取原子性**：`claim_task` 是
  `UPDATE ... SET status='running', attempts=attempts+1 WHERE id=? AND
  status='pending' AND cancel_requested=0 AND scheduled_at<=now`，
  配合 in-flight 集合，保证同一任务只被一个工作协程执行。
- **进程重启恢复**（`Executor.start` 时执行 `recover_on_startup`）：
  - 上次异常退出遗留的 `running` 任务：
    - 未请求取消 → 退回 `pending` 立即可重新调度，`attempts` 保留
      （崩溃时那一次已计数，体现 **at-least-once**，不会与成功结果重复）；
    - 已请求取消 → 直接 `cancelled`，取消请求不因重启丢失。
  - 退避中的 pending 任务保持原 `scheduled_at` 不变；终态任务原样保留。
- 语义为 at-least-once：崩溃窗口内的处理器可能已产生副作用，业务侧应通过
  幂等键与处理器自身幂等保证最终效果唯一。

## 代码结构

```
app/
  models.py    # 状态/优先级枚举、TaskRecord、领域异常
  store.py     # SQLite 持久层：条件 UPDATE 状态机、幂等插入、启动恢复
  handlers.py  # 处理器注册表、取消令牌、4 个内置处理器
  executor.py  # 调度循环、优先级老化、作用域信号量、重试/超时/取消执行
  service.py   # 提交校验、规范化哈希、幂等/冲突判定
  api.py       # FastAPI 路由与错误码映射
main.py        # 组装入口：uvicorn main:app
tests/         # 4 组 20 个用例
```

## 本地验证

```bash
# 1) 全量测试（每个用例日志打印输入与判定依据）
uv run pytest -q --log-cli-level=INFO

# 2) 手工联调
uv run uvicorn main:app --port 8000 &
curl -s -X POST localhost:8000/tasks/submit -H 'Content-Type: application/json' \
  -d '{"idempotency_key":"k1","task_type":"echo","payload":{"value":42}}'
curl -s localhost:8000/tasks/by-key/k1
```

测试覆盖的关键场景：

- `tests/test_idempotency.py`：重复提交、**20 路并发重复提交只执行一次**、
  同键不同载荷冲突、键序不同不算冲突、各类非法提交原因可区分；
- `tests/test_scheduling.py`：高优先级先执行、作用域并发上限不被突破且被打满、
  高优先级持续到达下低优先级不被饿死、超时进入确定终态并释放资源；
- `tests/test_lifecycle.py`：退避重试成功、重试耗尽 vs 致命失败错误码可区分、
  终态不重复转换、pending/running 取消、**取消与完成竞态**；
- `tests/test_recovery.py`：崩溃遗留 running 任务重启后续跑且计数正确、
  退避中 pending 与终态跨重启语义不变、取消请求跨重启生效。
