"""SQLite 持久层：所有状态变更均为单连接 + 串行化事务。

进程内所有读写通过一把 asyncio.Lock 串行化，保证“状态判断 + 写入”原子，
从而支持并发重复提交的幂等保证；所有状态流转均为带前置状态条件的 UPDATE，
终态天然不可被重复转换。跨进程重启场景由 BEGIN IMMEDIATE + WAL 提供串行写。
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from .models import BatchRecord, Priority, TaskRecord, TaskStatus, utcnow

# 依赖传播时视为“坏终态”的状态：其下游不允许执行
_BAD_TERMINAL = ("failed", "cancelled", "skipped")


@dataclass
class BatchEntry:
    """批次原子写入时的单条任务：预生成 ID 的记录 + 有序依赖键。

    所有任务先以 pending 插入，依赖边写齐后在同一事务内统一做传播计算，
    使历史依赖已终态的任务在提交时立即得到正确初态。
    """

    record: TaskRecord
    depends_on: list[str]


@dataclass
class BatchPersistenceResult:
    """commit_batch 的结果。

    outcome:
    - created：批次首次写入
    - exists：批次键命中且内容一致（幂等重复提交）
    - batch_conflict：批次键命中但内容指纹不同
    - task_conflict：批次内某幂等键已被占用且参数/依赖不一致
    """

    outcome: str
    batch: BatchRecord | None = None
    tasks: list[tuple[TaskRecord, bool]] = field(default_factory=list)
    conflict_key: str | None = None

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT UNIQUE NOT NULL,
    task_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    priority TEXT NOT NULL,
    scope TEXT NOT NULL,
    max_attempts INTEGER NOT NULL,
    timeout_seconds REAL NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    scheduled_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    result TEXT,
    error_code TEXT,
    error_message TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_tasks_dispatch
    ON tasks (status, scheduled_at);
CREATE INDEX IF NOT EXISTS idx_tasks_scope ON tasks (scope, status);

CREATE TABLE IF NOT EXISTS batches (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT UNIQUE NOT NULL,
    content_hash TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batch_tasks (
    batch_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    position INTEGER NOT NULL,
    PRIMARY KEY (batch_id, task_id)
);
CREATE INDEX IF NOT EXISTS idx_batch_tasks_task ON batch_tasks (task_id);

CREATE TABLE IF NOT EXISTS task_dependencies (
    task_id TEXT NOT NULL,
    depends_on_key TEXT NOT NULL,
    position INTEGER NOT NULL,
    PRIMARY KEY (task_id, depends_on_key)
);
CREATE INDEX IF NOT EXISTS idx_deps_downstream
    ON task_dependencies (depends_on_key);
CREATE INDEX IF NOT EXISTS idx_deps_task ON task_dependencies (task_id);
"""


def _dt(value: datetime) -> str:
    return value.isoformat()


def _parse_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


class TaskStore:
    """任务表的持久化门面。"""

    def __init__(self, db_path: str = "tasks.db") -> None:
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None

    def connect(self) -> None:
        """打开连接（WAL + 忙等）并建表。"""
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_SCHEMA)
        self._conn = conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("TaskStore 尚未 connect()")
        return self._conn

    def _row_to_record(self, row: sqlite3.Row) -> TaskRecord:
        return TaskRecord(
            id=row["id"],
            idempotency_key=row["idempotency_key"],
            task_type=row["task_type"],
            payload=json.loads(row["payload"]),
            payload_hash=row["payload_hash"],
            priority=Priority(row["priority"]),
            scope=row["scope"],
            max_attempts=row["max_attempts"],
            timeout_seconds=row["timeout_seconds"],
            status=TaskStatus(row["status"]),
            attempts=row["attempts"],
            created_at=_parse_dt(row["created_at"]),  # type: ignore[arg-type]
            updated_at=_parse_dt(row["updated_at"]),  # type: ignore[arg-type]
            scheduled_at=_parse_dt(row["scheduled_at"]),  # type: ignore[arg-type]
            started_at=_parse_dt(row["started_at"]),
            finished_at=_parse_dt(row["finished_at"]),
            result=json.loads(row["result"]) if row["result"] is not None else None,
            error_code=row["error_code"],
            error_message=row["error_message"],
            cancel_requested=bool(row["cancel_requested"]),
        )

    async def insert_task_if_absent(
        self, record: TaskRecord
    ) -> tuple[bool, TaskRecord | None]:
        """按幂等键原子插入；若已存在返回 (False, 已有记录)。

        BEGIN IMMEDIATE 先拿写锁再查重，保证并发重复提交下只有一方插入成功。
        """
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM tasks WHERE idempotency_key = ?",
                (record.idempotency_key,),
            ).fetchone()
            if row is not None:
                conn.execute("COMMIT")
                return False, self._row_to_record(row)
            conn.execute(
                """
                INSERT INTO tasks (id, idempotency_key, task_type, payload,
                    payload_hash, priority, scope, max_attempts, timeout_seconds,
                    status, attempts, created_at, updated_at, scheduled_at,
                    started_at, finished_at, result, error_code, error_message,
                    cancel_requested)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    record.id,
                    record.idempotency_key,
                    record.task_type,
                    json.dumps(record.payload, ensure_ascii=False, sort_keys=True),
                    record.payload_hash,
                    record.priority.value,
                    record.scope,
                    record.max_attempts,
                    record.timeout_seconds,
                    record.status.value,
                    record.attempts,
                    _dt(record.created_at),
                    _dt(record.updated_at),
                    _dt(record.scheduled_at),
                    _dt(record.started_at) if record.started_at else None,
                    _dt(record.finished_at) if record.finished_at else None,
                    json.dumps(record.result) if record.result is not None else None,
                    record.error_code,
                    record.error_message,
                    int(record.cancel_requested),
                ),
            )
            conn.execute("COMMIT")
            return True, record
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    # -- 批量依赖：原子提交与传播 -------------------------------------------

    @staticmethod
    def _insert_task_sql(conn: sqlite3.Connection, record: TaskRecord) -> None:
        conn.execute(
            """
            INSERT INTO tasks (id, idempotency_key, task_type, payload,
                payload_hash, priority, scope, max_attempts, timeout_seconds,
                status, attempts, created_at, updated_at, scheduled_at,
                started_at, finished_at, result, error_code, error_message,
                cancel_requested)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                record.id,
                record.idempotency_key,
                record.task_type,
                json.dumps(record.payload, ensure_ascii=False, sort_keys=True),
                record.payload_hash,
                record.priority.value,
                record.scope,
                record.max_attempts,
                record.timeout_seconds,
                record.status.value,
                record.attempts,
                _dt(record.created_at),
                _dt(record.updated_at),
                _dt(record.scheduled_at),
                _dt(record.started_at) if record.started_at else None,
                _dt(record.finished_at) if record.finished_at else None,
                json.dumps(record.result) if record.result is not None else None,
                record.error_code,
                record.error_message,
                int(record.cancel_requested),
            ),
        )

    @staticmethod
    def _dep_vectors(depends_on: list[str]) -> set[str]:
        """依赖集合的规范化表示（排序后去重），用于重复提交一致性比较。"""
        return set(depends_on)

    def _propagate(self, conn: sqlite3.Connection, seed_keys: list[str]) -> int:
        """在当前事务内做 blocked -> pending/skipped 的依赖传播（固定点）。

        规则（幂等、可重复执行）：
        - 对每个 blocked 任务，任一依赖处于 failed/cancelled/skipped -> skipped，
          error_code 按 failed 优先于 cancelled/skipped 聚合（确定性）；
        - 全部依赖均 succeeded -> pending，scheduled_at 置为现在，立即可调度；
        - 其余（依赖仍 pending/blocked/running 或边尚未齐）保持 blocked。
        新产生的 pending/skipped 任务会继续触发其下游重算，直到无变化。
        返回新落到 pending/skipped 的任务数。
        """
        now_dt = utcnow()
        now = _dt(now_dt)
        frontier = list(seed_keys)
        settled = 0
        while frontier:
            key = frontier.pop()
            rows = conn.execute(
                """
                SELECT t.* FROM tasks t
                JOIN task_dependencies d ON d.task_id = t.id
                WHERE d.depends_on_key = ? AND t.status = 'blocked'
                """,
                (key,),
            ).fetchall()
            for row in rows:
                task_id = row["id"]
                deps = conn.execute(
                    "SELECT depends_on_key FROM task_dependencies "
                    "WHERE task_id = ? ORDER BY position",
                    (task_id,),
                ).fetchall()
                dep_keys = [d["depends_on_key"] for d in deps]
                if not dep_keys:
                    continue
                marks = ",".join("?" for _ in dep_keys)
                dep_rows = conn.execute(
                    f"SELECT id, status, error_code FROM tasks "
                    f"WHERE idempotency_key IN ({marks})",
                    tuple(dep_keys),
                ).fetchall()
                dep_info = [(r["status"], r["error_code"]) for r in dep_rows]
                # 引用完整性由提交校验保证；缺边时保持 blocked 等待，不做误判
                if len(dep_info) != len(dep_keys):
                    continue
                # 坏原因沿 error_code 传播：直接或传递性地存在“执行失败”上游则
                # 记 dependency_failed；否则（取消/跳过链）记 dependency_cancelled。
                # 多前置部分成功部分失败时结论稳定一致，不依赖依赖声明顺序。
                bad_deps = [
                    (status, code)
                    for status, code in dep_info
                    if status in _BAD_TERMINAL
                ]
                if bad_deps:
                    failed_chain = any(
                        status == "failed" or code == "dependency_failed"
                        for status, code in bad_deps
                    )
                    if failed_chain:
                        code = "dependency_failed"
                        msg = "上游任务执行失败，依赖传播跳过"
                    else:
                        code = "dependency_cancelled"
                        msg = "上游任务被取消或跳过，依赖传播跳过"
                    cur = conn.execute(
                        """
                        UPDATE tasks
                           SET status = 'skipped', finished_at = ?,
                               error_code = ?, error_message = ?, updated_at = ?
                         WHERE id = ? AND status = 'blocked'
                        """,
                        (now, code, msg, now, task_id),
                    )
                    if cur.rowcount == 1:
                        settled += 1
                        frontier.append(row["idempotency_key"])
                elif all(status == "succeeded" for status, _ in dep_info):
                    cur = conn.execute(
                        """
                        UPDATE tasks
                           SET status = 'pending', scheduled_at = ?, updated_at = ?
                         WHERE id = ? AND status = 'blocked'
                        """,
                        (now, now, task_id),
                    )
                    if cur.rowcount == 1:
                        settled += 1
                        frontier.append(row["idempotency_key"])
        return settled

    async def commit_batch(
        self,
        *,
        batch_id: str,
        batch_key: str,
        content_hash: str,
        entries: list[BatchEntry],
    ) -> BatchPersistenceResult:
        """在单个 BEGIN IMMEDIATE 事务内原子提交一个批次。

        - 批次键已存在：内容指纹一致 -> 幂等返回既有任务；不一致 -> batch_conflict，
          不写任何数据；
        - 批次键不存在：逐个幂等键核对既有任务（参数/载荷/依赖必须一致，
          否则 task_conflict），随后一次性写入批次、任务、成员、依赖边，
          并在同一事务内传播初态。任何冲突都在写入前判定，不会写进去一半。
        """
        conn = self.conn
        now_dt = utcnow()
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing_batch_row = conn.execute(
                "SELECT * FROM batches WHERE idempotency_key = ?", (batch_key,)
            ).fetchone()
            if existing_batch_row is not None:
                existing_batch = self._row_to_batch(existing_batch_row)
                if existing_batch.content_hash != content_hash:
                    conn.execute("COMMIT")
                    return BatchPersistenceResult(
                        outcome="batch_conflict", batch=existing_batch
                    )
                task_rows = conn.execute(
                    """
                    SELECT t.* FROM tasks t
                    JOIN batch_tasks b ON b.task_id = t.id
                    WHERE b.batch_id = ? ORDER BY b.position
                    """,
                    (existing_batch.id,),
                ).fetchall()
                tasks = [(self._row_to_record(r), False) for r in task_rows]
                conn.execute("COMMIT")
                return BatchPersistenceResult(
                    outcome="exists", batch=existing_batch, tasks=tasks
                )

            keys = [e.record.idempotency_key for e in entries]
            marks = ",".join("?" for _ in keys)
            existing_rows = conn.execute(
                f"SELECT * FROM tasks WHERE idempotency_key IN ({marks})",
                tuple(keys),
            ).fetchall()
            existing = {r["idempotency_key"]: r for r in existing_rows}

            for entry in entries:
                rec = entry.record
                row = existing.get(rec.idempotency_key)
                if row is None:
                    continue
                want_deps = self._dep_vectors(entry.depends_on)
                got_dep_rows = conn.execute(
                    "SELECT depends_on_key FROM task_dependencies WHERE task_id = ?",
                    (row["id"],),
                ).fetchall()
                got_deps = {r["depends_on_key"] for r in got_dep_rows}
                mismatch = (
                    row["payload_hash"] != rec.payload_hash
                    or row["task_type"] != rec.task_type
                    or row["scope"] != rec.scope
                    or row["priority"] != rec.priority.value
                    or row["max_attempts"] != rec.max_attempts
                    or row["timeout_seconds"] != rec.timeout_seconds
                    or got_deps != want_deps
                )
                if mismatch:
                    conn.execute("COMMIT")
                    return BatchPersistenceResult(
                        outcome="task_conflict", conflict_key=rec.idempotency_key
                    )

            batch = BatchRecord(
                id=batch_id,
                idempotency_key=batch_key,
                content_hash=content_hash,
                created_at=now_dt,
            )
            conn.execute(
                "INSERT INTO batches (id, idempotency_key, content_hash, created_at) "
                "VALUES (?,?,?,?)",
                (batch.id, batch.idempotency_key, batch.content_hash,
                 _dt(batch.created_at)),
            )

            results: list[tuple[TaskRecord, bool]] = []
            # 种子说明：新任务自身（作为别人依赖时释放下游）+ 新任务引用到的
            # 全部历史键（其终态决定新任务初态）。批内级联由固定点迭代完成。
            seed_keys: list[str] = []
            for position, entry in enumerate(entries):
                rec = entry.record
                row = existing.get(rec.idempotency_key)
                if row is not None:
                    task = self._row_to_record(row)
                    results.append((task, False))  # 已存在：created=False
                    # 复用历史任务也要登记为本次批次成员（重复登记被主键忽略），
                    # 否则批次查询/幂等重放会漏掉这些任务
                    conn.execute(
                        "INSERT OR IGNORE INTO batch_tasks "
                        "(batch_id, task_id, position) VALUES (?,?,?)",
                        (batch_id, task.id, position),
                    )
                    continue
                self._insert_task_sql(conn, rec)
                conn.execute(
                    "INSERT INTO batch_tasks (batch_id, task_id, position) "
                    "VALUES (?,?,?)",
                    (batch_id, rec.id, position),
                )
                for dep_pos, dep_key in enumerate(entry.depends_on):
                    conn.execute(
                        "INSERT OR IGNORE INTO task_dependencies "
                        "(task_id, depends_on_key, position) VALUES (?,?,?)",
                        (rec.id, dep_key, dep_pos),
                    )
                results.append((rec, True))  # 本次新建：created=True
                seed_keys.append(rec.idempotency_key)
                seed_keys.extend(entry.depends_on)

            self._propagate(conn, seed_keys)

            final_rows = conn.execute(
                f"SELECT * FROM tasks WHERE idempotency_key IN ({marks})",
                tuple(keys),
            ).fetchall()
            final_by_key = {r["idempotency_key"]: self._row_to_record(r)
                            for r in final_rows}
            # created 标志按幂等键映射回提交位置（复用分支会打乱 results 顺序，
            # 不能与 entries 按位置 zip）
            created_by_key = {rec.idempotency_key: created
                              for rec, created in results}
            tasks = [
                (final_by_key[e.record.idempotency_key],
                 created_by_key[e.record.idempotency_key])
                for e in entries
            ]
            conn.execute("COMMIT")
            return BatchPersistenceResult(
                outcome="created", batch=batch, tasks=tasks
            )
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    async def propagate_dependencies(self, seed_keys: list[str] | None = None) -> int:
        """对外的传播入口（执行器终态流转 / 重启恢复后调用）。

        seed_keys 为 None 时对全部 blocked 任务做一次全量对齐（重启恢复用）。
        """
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            if seed_keys is None:
                # 全量对齐（重启恢复）：以所有被依赖键为种子扫描下游 blocked，
                # 固定点逻辑会把状态漂移的任务全部收敛
                dep_rows = conn.execute(
                    "SELECT DISTINCT depends_on_key FROM task_dependencies"
                ).fetchall()
                seed_keys = [r["depends_on_key"] for r in dep_rows]
            settled = self._propagate(conn, list(seed_keys))
            conn.execute("COMMIT")
            return settled
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    async def get_task(self, task_id: str) -> TaskRecord | None:
        row = self.conn.execute(
            "SELECT * FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()
        return self._row_to_record(row) if row is not None else None

    async def get_by_idempotency_key(self, key: str) -> TaskRecord | None:
        row = self.conn.execute(
            "SELECT * FROM tasks WHERE idempotency_key = ?", (key,)
        ).fetchone()
        return self._row_to_record(row) if row is not None else None

    async def count_active(self) -> int:
        """统计非终态任务数（pending 含退避中 + running）。"""
        row = self.conn.execute(
            "SELECT COUNT(*) AS n FROM tasks "
            "WHERE status NOT IN ('succeeded', 'failed', 'cancelled', 'skipped')"
        ).fetchone()
        return int(row["n"])

    async def list_runnable(self) -> list[TaskRecord]:
        """列出 pending 且已到调度时间的任务（未取消）。"""
        now = _dt(utcnow())
        rows = self.conn.execute(
            """
            SELECT * FROM tasks
            WHERE status = 'pending' AND cancel_requested = 0
              AND scheduled_at <= ?
            """,
            (now,),
        ).fetchall()
        return [self._row_to_record(r) for r in rows]

    async def recover_on_startup(self) -> int:
        """重启恢复上次异常退出遗留的运行态任务。

        - running 且未请求取消：退回 pending 并立即可调度（尝试次数保留，
          即崩溃那一次已计 attempts，体现 at-least-once）；
        - running 且已请求取消：直接 cancelled 终态，取消请求不因重启丢失；
        - 同一事务内对全部 blocked 任务做依赖对齐：崩溃期间已落终态的前置
          （含本次新落 cancelled 的任务）在重启后立即释放或跳过下游，
          保证“依赖没满足绝不提前执行、状态与依赖关系不丢不重”。
        """
        conn = self.conn
        now = _dt(utcnow())
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                """
                UPDATE tasks
                   SET status = CASE WHEN cancel_requested = 1
                                    THEN 'cancelled' ELSE 'pending' END,
                       finished_at = CASE WHEN cancel_requested = 1
                                         THEN ? ELSE finished_at END,
                       error_code = CASE WHEN cancel_requested = 1
                                         THEN 'cancelled' ELSE error_code END,
                       scheduled_at = ?,
                       updated_at = ?
                 WHERE status = 'running'
                """,
                (now, now, now),
            )
            recovered = cur.rowcount
            # 以所有被依赖键为种子做一次固定点传播，幂等地对齐所有 blocked
            dep_rows = conn.execute(
                "SELECT DISTINCT depends_on_key FROM task_dependencies"
            ).fetchall()
            self._propagate(conn, [r["depends_on_key"] for r in dep_rows])
            conn.execute("COMMIT")
            return recovered
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    async def claim_task(self, task_id: str) -> TaskRecord | None:
        """原子领取：pending(未取消, 到点) -> running，attempts+1。

        条件 UPDATE 保证同一任务只会被一个工作协程领取成功。
        """
        conn = self.conn
        now = _dt(utcnow())
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                """
                UPDATE tasks
                   SET status = 'running',
                       attempts = attempts + 1,
                       started_at = COALESCE(started_at, ?),
                       updated_at = ?
                 WHERE id = ? AND status = 'pending'
                   AND cancel_requested = 0 AND scheduled_at <= ?
                """,
                (now, now, task_id, now),
            )
            if cur.rowcount != 1:
                conn.execute("COMMIT")
                return None
            row = conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            conn.execute("COMMIT")
            return self._row_to_record(row)
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    async def mark_succeeded(
        self, task_id: str, result: dict
    ) -> TaskRecord | None:
        """running -> succeeded（终态）。仅未请求取消时成立。"""
        conn = self.conn
        now = _dt(utcnow())
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                """
                UPDATE tasks
                   SET status = 'succeeded', result = ?, finished_at = ?,
                       updated_at = ?, error_code = NULL, error_message = NULL
                 WHERE id = ? AND status = 'running' AND cancel_requested = 0
                """,
                (json.dumps(result, ensure_ascii=False), now, now, task_id),
            )
            if cur.rowcount != 1:
                conn.execute("COMMIT")
                return None
            row = conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            # 成功终态在同一事务内释放下游 blocked 任务，保证“依赖一满足即可调度”
            self._propagate(conn, [row["idempotency_key"]])
            conn.execute("COMMIT")
            return self._row_to_record(row)
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    async def mark_failed(
        self,
        task_id: str,
        error_code: str,
        error_message: str,
        retryable: bool,
        backoff_seconds: float,
    ) -> TaskRecord | None:
        """失败落库。

        可重试且 attempts < max_attempts：回到 pending，scheduled_at 推迟
        backoff_seconds（指数退避）；否则进入 failed 终态。
        所有流转仅对 status='running' 的行生效，终态不会被重复转换。
        """
        conn = self.conn
        now_dt = utcnow()
        now = _dt(now_dt)
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if row is None or row["status"] != "running":
                conn.execute("COMMIT")
                return None
            should_retry = retryable and row["attempts"] < row["max_attempts"]
            if should_retry:
                scheduled = _dt(now_dt + timedelta(seconds=backoff_seconds))
                conn.execute(
                    """
                    UPDATE tasks
                       SET status = 'pending', scheduled_at = ?,
                           error_code = ?, error_message = ?, updated_at = ?
                     WHERE id = ?
                    """,
                    (scheduled, error_code, error_message, now, task_id),
                )
            else:
                conn.execute(
                    """
                    UPDATE tasks
                       SET status = 'failed', finished_at = ?,
                           error_code = ?, error_message = ?, updated_at = ?
                     WHERE id = ?
                    """,
                    (now, error_code, error_message, now, task_id),
                )
                # failed 终态在同一事务内向 blocked 下游传播 skipped
                self._propagate(conn, [row["idempotency_key"]])
            row = conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            conn.execute("COMMIT")
            return self._row_to_record(row)
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    async def request_cancel(self, task_id: str) -> TaskRecord | None:
        """请求取消。

        - 终态：原样返回（取消幂等，不产生新流转）；
        - pending / blocked：直接 cancelled 终态，不会再被领取/等待；
          blocked 的取消在同一事务内向其下游传播 skipped；
        - running：仅置 cancel_requested=1，由执行器及时检测并落终态。
        """
        conn = self.conn
        now = _dt(utcnow())
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            status = row["status"]
            if status in {"succeeded", "failed", "cancelled", "skipped"}:
                conn.execute("COMMIT")
                return self._row_to_record(row)
            if status in {"pending", "blocked"}:
                conn.execute(
                    """
                    UPDATE tasks
                       SET status = 'cancelled', cancel_requested = 1,
                           finished_at = ?, error_code = 'cancelled',
                           updated_at = ?
                     WHERE id = ? AND status IN ('pending', 'blocked')
                    """,
                    (now, now, task_id),
                )
                # blocked/pending 任务都可能是他人依赖：终态后传播 skipped
                self._propagate(conn, [row["idempotency_key"]])
            else:  # running
                conn.execute(
                    "UPDATE tasks SET cancel_requested = 1, updated_at = ? "
                    "WHERE id = ? AND status = 'running'",
                    (now, task_id),
                )
            row = conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            conn.execute("COMMIT")
            return self._row_to_record(row)
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    async def mark_cancelled_if_running(
        self, task_id: str, reason: str
    ) -> TaskRecord | None:
        """running 且已收到取消请求 -> cancelled（终态）。"""
        conn = self.conn
        now = _dt(utcnow())
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                """
                UPDATE tasks
                   SET status = 'cancelled', finished_at = ?,
                       error_code = 'cancelled',
                       error_message = ?, updated_at = ?
                 WHERE id = ? AND status = 'running' AND cancel_requested = 1
                """,
                (now, reason, now, task_id),
            )
            if cur.rowcount != 1:
                conn.execute("COMMIT")
                return None
            row = conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            self._propagate(conn, [row["idempotency_key"]])
            conn.execute("COMMIT")
            return self._row_to_record(row)
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    async def mark_timed_out(
        self, task_id: str, backoff_seconds: float
    ) -> TaskRecord | None:
        """执行超时：按可重试失败处理（退避重试），重试耗尽则 failed 终态。"""
        return await self.mark_failed(
            task_id,
            error_code="timeout",
            error_message="任务执行超过 timeout_seconds 限制",
            retryable=True,
            backoff_seconds=backoff_seconds,
        )

    # -- 批量依赖：只读查询 -------------------------------------------------

    def _row_to_batch(self, row: sqlite3.Row) -> BatchRecord:
        return BatchRecord(
            id=row["id"],
            idempotency_key=row["idempotency_key"],
            content_hash=row["content_hash"],
            created_at=_parse_dt(row["created_at"]),  # type: ignore[arg-type]
        )

    async def get_batch_by_key(self, key: str) -> BatchRecord | None:
        row = self.conn.execute(
            "SELECT * FROM batches WHERE idempotency_key = ?", (key,)
        ).fetchone()
        return self._row_to_batch(row) if row is not None else None

    async def get_batch_by_id(self, batch_id: str) -> BatchRecord | None:
        row = self.conn.execute(
            "SELECT * FROM batches WHERE id = ?", (batch_id,)
        ).fetchone()
        return self._row_to_batch(row) if row is not None else None

    async def list_batch_task_ids(self, batch_id: str) -> list[str]:
        """按提交位置顺序返回批次内任务 ID。"""
        rows = self.conn.execute(
            "SELECT task_id FROM batch_tasks WHERE batch_id = ? ORDER BY position",
            (batch_id,),
        ).fetchall()
        return [r["task_id"] for r in rows]

    async def list_dependencies(self, task_id: str) -> list[str]:
        """按声明位置返回任务依赖的业务幂等键。"""
        rows = self.conn.execute(
            "SELECT depends_on_key FROM task_dependencies "
            "WHERE task_id = ? ORDER BY position",
            (task_id,),
        ).fetchall()
        return [r["depends_on_key"] for r in rows]

    async def get_many_by_idempotency_keys(
        self, keys: list[str] | tuple[str, ...]
    ) -> dict[str, TaskRecord]:
        if not keys:
            return {}
        marks = ",".join("?" for _ in keys)
        rows = self.conn.execute(
            f"SELECT * FROM tasks WHERE idempotency_key IN ({marks})",
            tuple(keys),
        ).fetchall()
        return {r["idempotency_key"]: self._row_to_record(r) for r in rows}

    async def list_blocked(self) -> list[TaskRecord]:
        """列出所有 blocked 任务（重启恢复依赖关系时使用）。"""
        rows = self.conn.execute(
            "SELECT * FROM tasks WHERE status = 'blocked'"
        ).fetchall()
        return [self._row_to_record(r) for r in rows]
