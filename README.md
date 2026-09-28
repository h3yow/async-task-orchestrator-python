# 本地异步任务编排服务（Python / FastAPI / SQLite）

一套可在本地直接运行的异步任务编排服务：用 **SQLite 文件持久化** 模拟真实调度系统的
任务存储，用 **每作用域 asyncio 信号量** 模拟执行资源配额，重点保证三件事在
重复提交、并发执行与进程重启下始终可预期：

1. 同一业务幂等键的任务绝不重复执行；
2. 终态确定、可重试失败有退避、取消及时生效；
3. 进程异常退出后未完成任务不丢失、不重复执行。

在此之上还提供一层**批量依赖编排**：一次提交多个声明了先后关系的任务
（如“先清洗 → 再聚合 → 最后导出”），批次整体幂等、非法批次整体拒绝、
失败/取消沿依赖边确定传播，且不改变单任务的任何既有行为。

## 快速开始

```bash
uv sync
uv run uvicorn main:app --reload          # 启动服务（默认 tasks.db）
uv run pytest -q                          # 运行全部测试（41 个用例）
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
| POST | `/batches/submit` | 批量提交带依赖任务（201 新建 / 200 幂等重复） |
| GET | `/batches/by-key/{batch_key}` | 按批次幂等键查询批次及全部任务 |
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

## 批量依赖编排

一次业务提交可以携带多个有先后关系的任务。每个任务仍有自己的**业务唯一键、
任务类型、参数、优先级、作用域**，并通过 `depends_on`（业务唯一键数组）声明
依赖；被依赖任务可以在同一批内，也可以是此前已提交过的单任务/历史批次任务。

```json
POST /batches/submit
{
  "idempotency_key": "pipeline-20260929-001",
  "tasks": [
    {"idempotency_key": "clean",  "task_type": "echo", "payload": {"step": 1}},
    {"idempotency_key": "agg",    "task_type": "echo", "payload": {"step": 2},
     "depends_on": ["clean"], "priority": "high"},
    {"idempotency_key": "export", "task_type": "echo", "payload": {"step": 3},
     "depends_on": ["agg"]}
  ]
}
```

响应按提交位置有序返回每个任务的 `task_id / status / depends_on / already_exists`，
以及批次的 `batch_id` 与批次级 `already_exists`。

### 批次幂等与一致性

- 批次自身有业务幂等键：**同一批次重复提交（含 20 路并发重复提交）只会产生
  同一组任务与同一套依赖关系，返回的 batch_id / task_id 完全一致**，每个任务
  仍只执行一次。批次写入是单连接上的单个 `BEGIN IMMEDIATE` 事务
  （批次行 + 任务行 + 成员关系 + 依赖边），并发下恰好一方插入成功。
- **批次内容指纹**：按提交位置排列的每个任务的规范化声明
  （载荷哈希、类型、作用域、优先级、max_attempts、timeout、**排序后的依赖集合**）。
  即依赖的声明顺序不影响指纹，但任务集合/参数/依赖集合/提交顺序不同则是另一批。
- 同批次键但内容不同 → `409 batch_conflict`；批次中某业务唯一键已被历史任务
  占用且任务参数或依赖集合不一致 → `409 payload_conflict`。冲突在任何写入之前
  判定，**不会只写进去一半任务**。

### 批量提交的拒绝原因（提交时整体拒绝，422 可区分）

| error.code | 含义 |
| --- | --- |
| `invalid_batch_key` / `empty_batch` | 批次键为空，或批次不含任务 |
| `duplicate_task_in_batch` | 批次内同一业务唯一键重复声明 |
| `unknown_task_type` / `invalid_payload` / `invalid_priority` / `invalid_scope` / `invalid_max_attempts` / `invalid_timeout` | 单任务字段校验（与单任务提交同一套规则） |
| `invalid_dependency` / `duplicate_dependency` | `depends_on` 不是字符串数组、或同一依赖重复声明 |
| `cycle_dependency` | 批内存在循环依赖（含任务依赖自身），Kahn 拓扑校验 |
| `dependency_not_found` | 依赖键在同批与历史任务中均不存在 |
| `cross_scope_dependency` | 依赖了其他作用域的任务（依赖必须与任务同 scope） |

跨批次循环不可能出现：历史任务的依赖边在其提交时已固定，新批只能追加
“新任务 → 旧任务”的边，旧任务不可能反向依赖尚未创建的新任务。

### 依赖状态流转

新增两个状态（单任务提交永远不会进入它们）：

- `blocked`：任务已提交但至少一个依赖尚未成功结束，**不会出现在调度候选集中**
  （`list_runnable` 只取 pending），因此不可能被提前领取执行；
- `skipped`（**终态**）：依赖未满足且确定不再可能满足，任务从未执行
  （`attempts` 恒为 0），与处理器真正执行失败的 `failed` 终态明确区分。

```
 提交(有依赖)                 所有依赖均 succeeded
 ┌─────────┐  ─────────────────────────────────────▶ ┌─────────┐
 │ blocked │                                         │ pending │ →（原单任务状态机）
 └─────────┘                                         └─────────┘
     │  任一依赖终态为 failed/cancelled/skipped
     │  （或等待期间被用户取消）
     ▼
 ┌─────────┐
 │ skipped │  终态（error_code 见下），attempts 恒为 0
 └─────────┘
```

### 失败与取消的传播规则

- **全部前置成功**：blocked → pending，`scheduled_at` 置为当前时刻，下一个
  调度 tick 即可参与派发；随后的优先级/老化/作用域并发规则与普通任务完全一致。
- **任一前置为 `failed`**：任务立即 `skipped`，`error_code=dependency_failed`。
- **任一前置为 `cancelled` 或 `skipped`（且不存在失败链）**：任务立即
  `skipped`，`error_code=dependency_cancelled`。失败原因沿依赖链传递——
  若某条 skipped 链的源头是执行失败，传递下游仍标记 `dependency_failed`，
  因此“多个前置一部分成功、一部分失败”时结论与依赖声明顺序无关、稳定一致。
- 传播是**固定点迭代**：一个任务落终态会在**同一个数据库事务内**级联重算其
  全部下游（含多层扇入/扇出），不会出现“父已 skipped、孙还 blocked”的中间态。
- **等待依赖期间被取消**：blocked 上的取消请求立即落 `cancelled` 终态
  （不挂起、不等 tick），并在同一事务内向其下游传播 `dependency_cancelled`；
  之后即使前置成功，任务也不会恢复执行。
- 对一个正在重试（pending 退避中）的依赖，下游继续 blocked 等待；只有依赖
  重试耗尽进入 failed 才传播 skipped。

### 与幂等、优先级、并发、重启恢复的兼容范围

- **幂等**：批次幂等建立在既有任务幂等键唯一约束之上；批次内任务可与历史任务
  共享键（此时复用该任务，参数/依赖必须一致），单任务 `/tasks/submit` 的行为、
  返回与冲突规则保持不变。
- **优先级与老化**：blocked 任务不参与排序；释放为 pending 后按既有
  `优先级序数×1000 − 老化值` 规则参与派发，老化自任务 `created_at`（批次提交
  时刻）起算，因此长时间等待依赖的低优先级任务就绪后不会被持续到达的新任务
  饿死。
- **作用域并发**：依赖释放只改变“可否参与派发”，真正执行仍领取同 scope 的
  semaphore，作用域上限不会因大量后置同时就绪而被突破。
- **重启恢复**：依赖边与批次关系持久化在 `task_dependencies / batches /
  batch_tasks` 表中。`recover_on_startup` 在恢复 running 残留的同一事务内做一次
  全量 blocked 对齐：已成功前置不会重复执行（claim 仍是条件 UPDATE），
  blocked 后置在依赖未满足前不会提前执行；崩溃期间落定的 failed/cancelled
  前置会在重启后立即把下游收敛到确定终态，状态与依赖关系不丢不重。

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

- 单任务三个终态：`succeeded / failed / cancelled`，均由**带前置状态条件的 SQL
  UPDATE** 落库（`WHERE status='running' ...`），因此终态不可能被重复转换。
  批量依赖任务额外使用 `blocked`（等待依赖的非终态）与 `skipped`
  （依赖失败/取消被传播的终态，未执行），流转规则见上文“依赖状态流转”。
- **取消与完成竞态**：成功落库要求 `cancel_requested=0`；若处理器完成时取消请求
  已先落库，则成功 UPDATE 命中 0 行，改由 `mark_cancelled_if_running` 落 cancelled。
  两种结果只可能赢一个，不会出现 “succeeded 且 cancel_requested=1” 的矛盾状态。
- pending / blocked 任务的取消直接进入终态（不再被领取/等待）；终态任务上重复
  取消原样返回。

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
- **依赖的持久化与传播**：`batches`（批次键 + 内容指纹）、`batch_tasks`
  （批次→任务成员及提交位置）、`task_dependencies`（任务→依赖业务键）三张表与
  任务在同一事务内写入。成功/失败/取消的终态流转在**同一事务内**执行 blocked
  下游的固定点传播；`recover_on_startup` 也在恢复事务内对全部 blocked 做一次
  幂等对齐，保证传播与崩溃不出现中间态。
- 语义为 at-least-once：崩溃窗口内的处理器可能已产生副作用，业务侧应通过
  幂等键与处理器自身幂等保证最终效果唯一。

## 代码结构

```
app/
  models.py    # 状态/优先级枚举（含 blocked/skipped）、TaskRecord/BatchRecord、领域异常
  store.py     # SQLite 持久层：条件 UPDATE 状态机、幂等插入、批次原子提交、
               #   依赖传播固定点、启动恢复（含 blocked 全量对齐）
  handlers.py  # 处理器注册表、取消令牌、4 个内置处理器
  executor.py  # 调度循环、优先级老化、作用域信号量、重试/超时/取消执行（未因批量改动）
  service.py   # 单任务提交校验、规范化哈希、幂等/冲突判定
  batch.py     # 批量依赖编排：批次校验（循环/引用/跨作用域）、内容指纹、原子提交
  api.py       # FastAPI 路由与错误码映射（/tasks/* 与 /batches/*）
main.py        # 组装入口：uvicorn main:app
tests/         # 5 组 41 个用例
```

## 本地验证

```bash
# 1) 全量测试（每个用例日志打印输入与判定依据）
uv run pytest -q --log-cli-level=INFO

# 2) 只跑批量依赖编排用例
uv run pytest tests/test_batch.py -q --log-cli-level=INFO

# 3) 手工联调
uv run uvicorn main:app --port 8000 &
curl -s -X POST localhost:8000/tasks/submit -H 'Content-Type: application/json' \
  -d '{"idempotency_key":"k1","task_type":"echo","payload":{"value":42}}'
curl -s localhost:8000/tasks/by-key/k1

# 批量提交一条 clean -> agg -> export 依赖链并查询批次
curl -s -X POST localhost:8000/batches/submit -H 'Content-Type: application/json' -d '{
  "idempotency_key": "pipe-1",
  "tasks": [
    {"idempotency_key": "clean", "task_type": "echo", "payload": {}},
    {"idempotency_key": "agg", "task_type": "echo", "payload": {}, "depends_on": ["clean"]},
    {"idempotency_key": "export", "task_type": "echo", "payload": {}, "depends_on": ["agg"]}
  ]}'
curl -s localhost:8000/batches/by-key/pipe-1
```

测试覆盖的关键场景：

- `tests/test_idempotency.py`：重复提交、**20 路并发重复提交只执行一次**、
  同键不同载荷冲突、键序不同不算冲突、各类非法提交原因可区分；
- `tests/test_scheduling.py`：高优先级先执行、作用域并发上限不被突破且被打满、
  高优先级持续到达下低优先级不被饿死、超时进入确定终态并释放资源；
- `tests/test_lifecycle.py`：退避重试成功、重试耗尽 vs 致命失败错误码可区分、
  终态不重复转换、pending/running 取消、**取消与完成竞态**；
- `tests/test_recovery.py`：崩溃遗留 running 任务重启后续跑且计数正确、
  退避中 pending 与终态跨重启语义不变、取消请求跨重启生效；
- `tests/test_batch.py`（21 个用例）：批次重复/**20 路并发重复提交标识一致**、
  批次内容冲突与单键冲突、循环依赖（自环/两环/三环）与引用不存在/跨作用域/
  批次内重复键的整体拒绝且不写一半、链式 gating 严格按序、**多前置部分失败的
  skipped 终态与 dependency_failed/dependency_cancelled 原因稳定可区分**、
  等待依赖期间取消立即落定并向下游传播、**依赖释放时作用域并发上限不被突破**、
  同时就绪任务的优先级生效、**异常重启后已完成前置不重复执行且 blocked 不提前
  执行**、崩溃后传播对齐、单批次 150 任务 6 层菱形依赖的规模场景。
