# 本地异步任务编排服务（Python / FastAPI / SQLite）

一套可在本地直接运行的异步任务编排服务：用 **SQLite 文件持久化** 模拟真实调度系统的
任务存储，用 **每作用域 asyncio 信号量** 模拟执行资源配额，重点保证三件事在
重复提交、并发执行与进程重启下始终可预期：

1. 同一业务幂等键的任务绝不重复执行；
2. 终态确定、可重试失败有退避、取消及时生效；
3. 进程异常退出后未完成任务不丢失、不重复执行。

在单任务能力之上，还提供一层**批量依赖编排**：一次提交一串带先后关系的任务
（如“清洗 → 聚合 → 导出”），后继任务只在其声明的全部前置任务**成功**后才允许
执行；前置失败/取消/被跳过时后继进入独立的 `skipped` 终态并沿依赖链级联传播。
批量提交整体幂等、整体校验、整体落库，并完整复用单任务的优先级、作用域并发、
重试/超时/取消与重启恢复语义。

## 快速开始

```bash
uv sync
uv run uvicorn main:app --reload          # 启动服务（默认 tasks.db）
uv run pytest -q                          # 运行全部测试（44 个用例）
uv run pytest -q --log-cli-level=INFO     # 含输入与判定依据的详细日志
```

可选环境变量：`TASK_DB_PATH`（数据库文件路径）、`TASK_DEFAULT_CONCURRENCY`
（默认作用域并发上限，默认 2）、`TASK_SCOPE_CONCURRENCY`（JSON 形式的按作用域配额）。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/tasks/submit` | 提交单个任务（201 新建 / 200 幂等重复） |
| POST | `/tasks/batch-submit` | 批量提交带依赖的任务（201 新建 / 200 幂等重复） |
| GET | `/tasks/{task_id}` | 按任务 ID 查询状态 |
| GET | `/tasks/by-key/{idempotency_key}` | 按业务幂等键查询 |
| GET | `/batches/{batch_key}` | 按批次幂等键查询整批任务（按提交顺序） |
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

### 批量依赖提交

`POST /tasks/batch-submit` 的请求体带一个批次幂等键和一组任务，每个任务用
`depends_on` 声明**业务幂等键**形式的前置依赖；被依赖的任务既可以在同批中，
也可以是此前通过单任务/批量接口已经提交过的任务：

```json
{
  "idempotency_key": "pipeline-20260929-001",
  "tasks": [
    {"idempotency_key": "clean",  "task_type": "echo", "payload": {"step": "clean"},
     "priority": "high", "scope": "etl", "depends_on": []},
    {"idempotency_key": "agg",    "task_type": "echo", "payload": {"step": "agg"},
     "priority": "normal", "scope": "etl", "depends_on": ["clean"]},
    {"idempotency_key": "export", "task_type": "echo", "payload": {"step": "export"},
     "priority": "normal", "scope": "etl", "depends_on": ["agg", "clean"]}
  ]
}
```

响应（201 新建 / 200 重复）：

```json
{
  "batch_idempotency_key": "pipeline-20260929-001",
  "already_exists": false,
  "tasks": [ {"task_id": "...", "idempotency_key": "clean", "status": "pending", "...": "..."} ]
}
```

批量提交在持久层的**单个事务**内完成“校验 + 写入 + 首次传播”，任何校验失败都
整体回滚，不会只写进去一半任务。新增的可区分拒绝原因：

| 场景 | HTTP | error.code |
| --- | --- | --- |
| 批次键为空 / 批次为空或超限（>1000） | 422 | `invalid_batch_key` / `invalid_batch_tasks` |
| 批次内出现重复业务键 | 422 | `duplicate_task_key` |
| `depends_on` 不是字符串列表 / 含空键 | 422 | `invalid_dependency` |
| 依赖引用了不存在的任务 | 422 | `dependency_not_found` |
| 依赖了不同 scope 的任务 | 422 | `cross_scope_dependency` |
| 依赖关系形成环 | 422 | `cyclic_dependency` |
| 同 batch_key 但内容/依赖关系不一致 | 409 | `batch_conflict` |
| 批次内某键与**已存在单任务**的载荷/参数不一致，或试图给已存在任务改写依赖 | 409 | `payload_conflict` |
| 批次不存在（查询时） | 404 | `batch_not_found` |
| 同 batch_key 同内容重复（含并发）提交 | 200 | 非错误，`already_exists=true`，任务标识与首次一致 |

- **批次幂等的内容指纹**：对每个任务的 `task_type / 规范化载荷哈希 / priority /
  scope / max_attempts / timeout_seconds / 排序后的 depends_on`，按业务键排序后
  整体做 SHA-256。因此重复提交时打乱任务顺序或依赖声明顺序，仍视为同一批次。
- **一致性**：批次内容一致时，重复（包括并发）提交返回的 `task_id` 集合与首次
  完全一致；并发重复提交由 `BEGIN IMMEDIATE` 串行化，恰好一方新建，其余拿到同一批。
- 单批次规模上限 1000 个任务，环检测与拓扑在提交时一次性完成；
  自动化测试覆盖了 120 个任务、4 层依赖的链路。

## 任务状态机

```
                 submit
                   │
                   ▼
              ┌─────────┐  调度领取(条件UPDATE, attempts+1)  ┌─────────┐
              │ pending │ ─────────────────────────────────▶ │ running │
              └─────────┘  依赖未满足时停留 pending(不可领取) └─────────┘
                ▲   │ 取消(未启动)              成功(且无取消请求)  │  │  │
   可重试失败   │   ├──────────────┐   ┌───────────────────────┘  │  │
   (退避等待)  │   │              ▼   ▼                            │  │
   ┌──────────┘   │       ┌───────────┐ 运行中收到取消并到检查点    │  │
   └──────────────┼──────▶│ cancelled │                  不可重试  │  │
                  │       └───────────┘                  失败/超时 │  │
                  │       ┌───────────┐ 可重试失败耗尽/超时耗尽     │  │
                  ├──────▶│  failed   │◀───────────────────────────┘  │
                  │       └───────────┘                               │
                  │       ┌───────────┐                               │
                  ├──────▶│ succeeded │◀──────────────────────────────┘
                  │       └───────────┘
                  │       ┌───────────┐ 前置 failed/cancelled/skipped
                  └──────▶│  skipped  │  （任务从未执行，级联传播）
                          └───────────┘
```

- 四个终态：`succeeded / failed / cancelled / skipped`，均由**带前置状态条件的
  SQL UPDATE** 落库，终态不可能被重复转换。
- `failed` 表示任务**真正执行过**并失败（错误码 `retryable/fatal/timeout/
  handler_error`）；`skipped` 表示任务**从未被领取执行**（`attempts=0`），只因
  依赖无法满足而被确定，二者可明确区分。
- **取消与完成竞态**：成功落库要求 `cancel_requested=0`；若处理器完成时取消请求
  已先落库，则成功 UPDATE 命中 0 行，改由 `mark_cancelled_if_running` 落 cancelled。
  两种结果只可能赢一个，不会出现 “succeeded 且 cancel_requested=1” 的矛盾状态。
- pending 任务的取消直接进入终态（不再被领取）；终态任务上重复取消原样返回。

## 批量依赖的状态流转与传播规则

- **调度门禁**：任务被领取执行的条件是「pending、未取消、已到 `scheduled_at`」
  且它在 `task_dependencies` 中的**每一条**前置都已是 `succeeded`。门禁直接写在
  `list_runnable` 与 `claim_task` 的 SQL 里（`NOT EXISTS ... 前置 status <>
  'succeeded'`），因此依赖未满足的任务即使到点也绝不会被提前领取。
- **全成功才放行**：多个前置中只要还有一个未成功（pending/running/退避中），后继
  就继续等待；全部成功后立即进入正常调度集合。
- **一个坏前置即跳过**：任一前置进入 `failed` / `cancelled` / `skipped`，后继即
  不可能再满足，进入 `skipped` 终态。多个前置一部分成功、一部分失败时结果稳定：
  `error_code` 按 `dependency_failed > dependency_cancelled > dependency_skipped`
  的固定优先级报告，原因任务取其业务键最小者（确定性，不依赖完成先后）。
- **传递级联**：`skipped` 任务自身也会继续阻塞它的后继——递归闭包把所有（传递地）
  依赖到坏终态的 pending 任务一次性确定下来。例如 `A failed → B skipped →
  C skipped`，其中 B 的原因是 `dependency_failed`，C 是 `dependency_skipped`。
- **等待期间取消立即确定**：任务还在等依赖时被取消，`request_cancel` 在同一事务
  内把它置为 `cancelled` 并立即级联，其后继当场变 `skipped`，不会一直挂着。
- **传播原子性**：失败落库、取消落库、级联传播在**同一个 SQLite 事务**内完成，
  外部不可能观察到“前置已坏、后继仍 pending”的中间态。
- running 任务在被领取时其全部前置必然已经成功，而成功是不可翻转的终态，所以
  正在执行的任务不存在“前置中途变坏”的情况；running 转 failed/cancelled 时同样
  会触发对其后继的级联。

## 与幂等、优先级、并发、重启恢复的兼容范围

- **幂等**：批量任务仍以各自的业务唯一键写入 `tasks`（唯一约束），单任务接口
  `/tasks/submit` 的行为完全不变；同一任务可被多个批次引用为成员，但依赖边去重。
- **优先级与老化**：依赖释放后，就绪任务进入与普通任务相同的派发排序
  （`score = 优先级序数×1000 − 等待时长/aging_interval`）。等待依赖所经过的时间
  同样计入老化，因此等待很久的低优先级后继在依赖满足后，不会因持续到达的新高
  优先级任务而无限期得不到机会。
- **作用域并发**：依赖只允许在**同 scope** 内声明（跨作用域提交即拒绝）；依赖
  释放后任务的执行仍由原每作用域信号量配额约束，大量后置同时就绪时不突破上限。
- **重试/超时**：依赖门禁只放行前置全成功的任务；任务自身的重试、指数退避、
  超时语义与单任务完全一致。退避中的任务对其后继仍表现为“未成功”，后继继续
  等待；只有重试耗尽进入 failed 才会触发后继 skipped。
- **重启恢复**（`Executor.start` → `recover_on_startup`，同一事务内恢复 + 传播）：
  - 已 `succeeded` 的前置不会被重复执行（claim 仅对 pending 生效）；
  - 崩溃遗留的 `running` 任务按原规则恢复：未取消→回 pending 重新调度（attempts
    保留），已取消→cancelled；恢复为 cancelled 时其等待中的后继当场级联 skipped；
  - 依赖关系持久化在 `task_dependencies`，重启后门禁继续生效，后继不会在前置
    未成功时提前执行；恢复流程末尾再跑一次级联传播，修复任何历史不一致；
  - 退避中的 pending、已终态（含 skipped）任务原样保留，语义不丢不重。
- 语义仍为 at-least-once：崩溃窗口内的处理器可能已产生副作用，业务侧需以幂等键
  与处理器自身幂等保证最终效果唯一。

## 持久化结构

结构由 `PRAGMA user_version` 标记版本（当前为 2），启动时按版本逐段自动迁移：

- v1：`tasks` 基础表（幂等键唯一、状态机字段）；
- v2：批量依赖编排——`tasks` 增加 `batch_id` 列，新增下述三张关系表。

批量依赖编排新增的关系表：

- `batches`：批次幂等键唯一、内容指纹 `content_hash`；
- `batch_members`：批次 → 任务的成员关系与提交顺序 `position`；
- `task_dependencies`：`(task_id, depends_on_task_id)` 依赖边（外键、联合主键），
  带双向索引以支撑“查前置”（调度门禁）与“查后继”（级联传播）。

级联传播用 SQLite **递归 CTE** 从当前所有坏终态出发沿依赖边求后继闭包，再在
同一事务内把闭包中的 pending 任务更新为 skipped，天然幂等、可在重启时安全重放。

## 从旧版库升级

- **兼容范围**：上一版（仅 `tasks` 表、无批量依赖表）的 `tasks.db` 可直接用
  本版程序启动，无需手工迁移。启动时 `TaskStore.connect` 在**单个事务**内完成：
  给 `tasks` 补 `batch_id` 列、创建 `batches` / `batch_members` /
  `task_dependencies` 及索引、把 `user_version` 推进到 2。已有任务与状态
  原样保留，升级后即可正常提交/查询带依赖的批量任务。
- **幂等与反复重启**：迁移按 `user_version` 逐段执行且每段幂等，同一个库
  反复启动不会重复迁移，也不会再报结构相关的错。
- **失败行为**：整个迁移要么全部生效、要么整体回滚，不会留下半新半旧的库。
  迁移后做结构自检（必需的表与列齐全）；以下情况启动即失败并抛出带原因的
  `MigrationError`，库文件不被改动：
  - 库的 `user_version` 高于本程序支持的版本（提示升级程序或更换库文件）；
  - 库缺少必需的表或列、无法安全补齐（提示具体缺失项）。
- **本地验证**：

```bash
# 用上一版程序生成旧库（或直接沿用已有的 tasks.db），然后：
uv run uvicorn main:app            # 旧库启动：自动升级，日志无结构错误
# 校验升级结果（期望版本 2，且含三张批量表）：
uv run python -c "import sqlite3; c=sqlite3.connect('tasks.db'); \
print('user_version =', c.execute('PRAGMA user_version').fetchone()[0]); \
print('tables =', sorted(r[0] for r in c.execute(\
\"SELECT name FROM sqlite_master WHERE type='table'\")))"
# 自动化覆盖见 tests/test_migration.py：
uv run pytest tests/test_migration.py -v --log-cli-level=INFO
```

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
  status='pending' AND cancel_requested=0 AND scheduled_at<=now AND NOT EXISTS
  (未成功的前置依赖)`，配合 in-flight 集合，保证同一任务只被一个工作协程执行，
  且绝不会在依赖未满足时被提前领取。
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
  models.py    # 状态/优先级枚举（含 SKIPPED）、TaskRecord/BatchTaskSpec/BatchRecord、领域异常
  store.py     # SQLite 持久层：版本化结构迁移（user_version + 单事务 + 自检）、
               #            条件 UPDATE 状态机、幂等插入、批量事务、
               #            依赖门禁、递归 CTE 级联 skipped、启动恢复
  handlers.py  # 处理器注册表、取消令牌、4 个内置处理器
  executor.py  # 调度循环（仅派发依赖已满足任务）、优先级老化、作用域信号量、重试/超时/取消
  service.py   # 单任务/批量提交校验、规范化内容哈希、幂等/冲突判定、批次查询
  api.py       # FastAPI 路由与错误码映射（含 /tasks/batch-submit、/batches/{key}）
main.py        # 组装入口：uvicorn main:app
tests/         # 6 组 44 个用例（含 test_migration.py 旧库升级与反复重启）
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

# 3) 批量依赖联调（清洗 -> 聚合 -> 导出）
curl -s -X POST localhost:8000/tasks/batch-submit \
  -H 'Content-Type: application/json' -d '{
    "idempotency_key":"pipeline-1",
    "tasks":[
      {"idempotency_key":"clean","task_type":"echo","payload":{"v":"clean"}},
      {"idempotency_key":"agg","task_type":"flaky","payload":{"fail_times":0},
       "depends_on":["clean"]},
      {"idempotency_key":"export","task_type":"echo","payload":{},"depends_on":["agg"]}
    ]}'
curl -s localhost:8000/batches/pipeline-1
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
- `tests/test_batch.py`（19 个）：依赖链拓扑顺序、依赖此前已提交（完成/在跑）
  任务、批次重复与 **10 路并发重复提交**只产生一组任务、内容不一致
  `batch_conflict`、环 / 非法引用 / 跨作用域 / 批内键重复的**整体拒绝**、
  多前置部分失败与跳过原因分层、跳过的传递闭包、前置取消传播、**等待依赖期间
  取消立即终态**、依赖释放后作用域并发上限与优先级顺序、等待中老化不被新高任务
  长期插队、**异常重启后已完成前置不重复执行/后继不提前执行/skipped 不丢**，
  以及 **120 个任务 4 层依赖**的规模场景。
