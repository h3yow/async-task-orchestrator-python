"""SQLite 持久层：所有状态变更均为单连接 + 串行化事务。

进程内所有读写通过一把 asyncio.Lock 串行化，保证“状态判断 + 写入”原子，
从而支持并发重复提交的幂等保证；所有状态流转均为带前置状态条件的 UPDATE，
终态天然不可被重复转换。跨进程重启场景由 BEGIN IMMEDIATE + WAL 提供串行写。
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timedelta

from .models import (
    BatchConflict,
    BatchRecord,
    BatchTaskSpec,
    PayloadConflict,
    Priority,
    TaskRecord,
    TaskStatus,
    ValidationFailure,
    utcnow,
)

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
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    batch_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_tasks_dispatch
    ON tasks (status, scheduled_at);
CREATE INDEX IF NOT EXISTS idx_tasks_scope ON tasks (scope, status);
CREATE INDEX IF NOT EXISTS idx_tasks_batch ON tasks (batch_id);

CREATE TABLE IF NOT EXISTS batches (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT UNIQUE NOT NULL,
    content_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- 批次成员（按提交时的位置保存，含复用的已存在任务），用于按批次查询。
CREATE TABLE IF NOT EXISTS batch_members (
    batch_id TEXT NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    position INTEGER NOT NULL,
    PRIMARY KEY (batch_id, task_id)
);
CREATE INDEX IF NOT EXISTS idx_batch_members_task
    ON batch_members (task_id);

-- 依赖边：task_id 必须等 depends_on_task_id 成功后才允许执行。
-- 边只在批量提交事务内整体写入，写入前已完成存在性/作用域/环校验。
CREATE TABLE IF NOT EXISTS task_dependencies (
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    depends_on_task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    PRIMARY KEY (task_id, depends_on_task_id)
);
CREATE INDEX IF NOT EXISTS idx_deps_downstream
    ON task_dependencies (depends_on_task_id);
CREATE INDEX IF NOT EXISTS idx_deps_upstream
    ON task_dependencies (task_id);
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
        self._migrate(conn)
        self._conn = conn

    @staticmethod
    def _migrate(conn: sqlite3.Connection) -> None:
        """对旧版库做幂等加列（CREATE TABLE IF NOT EXISTS 不会补列）。"""
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(tasks)")}
        if "batch_id" not in cols:
            conn.execute("ALTER TABLE tasks ADD COLUMN batch_id TEXT")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_batch ON tasks (batch_id)"
            )
        conn.commit()

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
        """统计非终态任务数（pending 含退避/等待依赖中 + running）。"""
        row = self.conn.execute(
            "SELECT COUNT(*) AS n FROM tasks "
            "WHERE status NOT IN ('succeeded', 'failed', 'cancelled', 'skipped')"
        ).fetchone()
        return int(row["n"])

    async def list_runnable(self) -> list[TaskRecord]:
        """列出 pending、已到调度时间、且全部依赖都已成功的任务（未取消）。

        依赖门禁直接在 SQL 内完成：只要还存在一条依赖边指向的前置任务不是
        succeeded，该任务就不会出现在调度集合里（等待中 / 坏终态前置都挡住）。
        """
        now = _dt(utcnow())
        rows = self.conn.execute(
            """
            SELECT t.* FROM tasks t
            WHERE t.status = 'pending' AND t.cancel_requested = 0
              AND t.scheduled_at <= ?
              AND NOT EXISTS (
                  SELECT 1 FROM task_dependencies d
                  JOIN tasks p ON p.id = d.depends_on_task_id
                  WHERE d.task_id = t.id AND p.status <> 'succeeded'
              )
            """,
            (now,),
        ).fetchall()
        return [self._row_to_record(r) for r in rows]

    async def recover_on_startup(self) -> int:
        """重启恢复上次异常退出遗留的运行态任务。

        - running 且未请求取消：退回 pending 并立即可调度（尝试次数保留，
          即崩溃那一次已计 attempts，体现 at-least-once）；
        - running 且已请求取消：直接 cancelled 终态，取消请求不因重启丢失。
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
            # 恢复后兜底再做一次依赖传播：
            # - 恢复为 cancelled 的 running 任务，其等待中的后继必须立即 skipped；
            # - 同时修复任何历史遗留的“前置已坏、后继仍 pending”的不一致。
            self._propagate_skipped(conn)
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
                   AND NOT EXISTS (
                       SELECT 1 FROM task_dependencies d
                       JOIN tasks p ON p.id = d.depends_on_task_id
                       WHERE d.task_id = tasks.id AND p.status <> 'succeeded'
                   )
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
                # 依赖传播：进入 failed 终态时，把所有（传递地）等待它的
                # pending 后继在同一事务内确定为 skipped，状态变更对外部原子可见。
                self._propagate_skipped(conn)
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
        - pending：直接 cancelled 终态，不会再被领取；
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
            if status in {"succeeded", "failed", "cancelled"}:
                conn.execute("COMMIT")
                return self._row_to_record(row)
            if status == "pending":
                conn.execute(
                    """
                    UPDATE tasks
                       SET status = 'cancelled', cancel_requested = 1,
                           finished_at = ?, error_code = 'cancelled',
                           updated_at = ?
                     WHERE id = ? AND status = 'pending'
                    """,
                    (now, now, task_id),
                )
                # 取消在等待依赖的 pending 任务：立即确定终态，并把它的
                # pending 后继一并级联为 skipped，不允许链路一直挂着。
                self._propagate_skipped(conn)
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
            # 后继任务可能在该任务运行期间才提交：取消落库后同样要级联确定。
            self._propagate_skipped(conn)
            row = conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
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
            error_message=f"任务执行超过 timeout_seconds 限制",
            retryable=True,
            backoff_seconds=backoff_seconds,
        )

    # ------------------------------------------------------------------
    # 批量依赖编排
    # ------------------------------------------------------------------

    @staticmethod
    def _raise_store_error(code: str, message: str) -> None:
        """按错误码抛出对应的服务层异常（store 与 service 共用一套错误码）。"""
        if code == "batch_conflict":
            raise BatchConflict(f"{code}:{message}")
        if code == "payload_conflict":
            raise PayloadConflict(f"{code}:{message}")
        raise ValidationFailure(f"{code}:{message}")

    @staticmethod
    def _propagate_skipped(conn: sqlite3.Connection) -> int:
        """在当前事务内把“依赖已不可满足”的所有 pending 任务级联标记为 skipped。

        - 只处理 pending：running 任务在领取时其全部前置必已 succeeded，
          其前置不可能再变坏（终态不可翻转），因此无需处理；
        - 部分成功、部分失败：一条坏前置即足以让后继进入 skipped；
        - 递归闭包：skipped 任务自身也会继续阻塞它的后继，直到所有可达的
          pending 后继都被确定下来（fixpoint）；
        - 幂等：已是坏终态的任务不会再被改写，重复执行安全。

        返回本轮新进入 skipped 的任务数。
        """
        conn.execute("CREATE TEMP TABLE IF NOT EXISTS _skip_reached (task_id TEXT)")
        conn.execute("DELETE FROM _skip_reached")
        # 递归闭包：从当前所有坏终态（failed/cancelled/skipped）出发，沿依赖边
        # 反向找到“后继”任务（边中 depends_on_task_id -> task_id）。
        conn.execute(
            """
            INSERT INTO _skip_reached (task_id)
            WITH RECURSIVE reach(task_id) AS (
                SELECT id FROM tasks
                WHERE status IN ('failed', 'cancelled', 'skipped')
                UNION
                SELECT d.task_id
                FROM task_dependencies d JOIN reach r ON r.task_id = d.depends_on_task_id
            )
            SELECT task_id FROM reach
            """
        )
        now = _dt(utcnow())
        cur = conn.execute(
            """
            UPDATE tasks
               SET status = 'skipped', finished_at = ?,
                   error_code = 'dependency_failed',
                   error_message = '存在未成功结束的前置依赖',
                   updated_at = ?
             WHERE status = 'pending'
               AND id IN (SELECT task_id FROM _skip_reached)
            """,
            (now, now),
        )
        marked = cur.rowcount
        if marked:
            # 区分跳过原因：优先报告 failed 的前置，其次 cancelled，最后 skipped。
            # 若有多个，选择 idempotency_key 最小者，保证结果稳定可预期。
            conn.execute(
                """
                UPDATE tasks
                   SET error_code = CASE
                           WHEN EXISTS (SELECT 1 FROM task_dependencies d2
                                        JOIN tasks p2 ON p2.id = d2.depends_on_task_id
                                        WHERE d2.task_id = tasks.id AND p2.status='failed')
                                THEN 'dependency_failed'
                           WHEN EXISTS (SELECT 1 FROM task_dependencies d2
                                        JOIN tasks p2 ON p2.id = d2.depends_on_task_id
                                        WHERE d2.task_id = tasks.id AND p2.status='cancelled')
                                THEN 'dependency_cancelled'
                           ELSE 'dependency_skipped'
                       END,
                       error_message = '存在未成功结束的前置依赖: ' || COALESCE(
                           (SELECT '前置失败 ' || p3.idempotency_key FROM task_dependencies d3
                              JOIN tasks p3 ON p3.id = d3.depends_on_task_id
                             WHERE d3.task_id = tasks.id AND p3.status='failed'
                             ORDER BY p3.idempotency_key LIMIT 1),
                           (SELECT '前置取消 ' || p3.idempotency_key FROM task_dependencies d3
                              JOIN tasks p3 ON p3.id = d3.depends_on_task_id
                             WHERE d3.task_id = tasks.id AND p3.status='cancelled'
                             ORDER BY p3.idempotency_key LIMIT 1),
                           (SELECT '前置跳过 ' || p3.idempotency_key FROM task_dependencies d3
                              JOIN tasks p3 ON p3.id = d3.depends_on_task_id
                             WHERE d3.task_id = tasks.id AND p3.status='skipped'
                             ORDER BY p3.idempotency_key LIMIT 1))
                 WHERE status = 'skipped' AND error_code = 'dependency_failed'
                   AND finished_at = ?
                """,
                (now,),
            )
        return int(marked)

    async def propagate_skipped(self) -> int:
        """对外的级联传播入口（重启恢复后调用）。"""
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            marked = self._propagate_skipped(conn)
            conn.execute("COMMIT")
            return marked
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    async def submit_batch(
        self,
        *,
        batch_id: str,
        batch_key: str,
        content_hash: str,
        specs: list[BatchTaskSpec],
        payload_hashes: list[str],
    ) -> tuple[bool, list[TaskRecord], BatchRecord | None]:
        """在单个事务内整体提交一批带依赖的任务。

        返回 (是否新建, 按提交顺序排列的任务记录, 批次记录（仅新建时非空)）。
        任何校验失败都在写入前抛出（ValidationFailure / PayloadConflict），
        事务回滚，保证“不会只写进去一半任务”。
        BEGIN IMMEDIATE 串行化并发提交：重复批次只有一方新建，其余拿到同一批。
        """
        conn = self.conn
        now_dt = utcnow()
        now = _dt(now_dt)
        conn.execute("BEGIN IMMEDIATE")
        try:
            # 1) 批次幂等：同键批次内容必须完全一致
            brow = conn.execute(
                "SELECT * FROM batches WHERE idempotency_key = ?", (batch_key,)
            ).fetchone()
            if brow is not None:
                if brow["content_hash"] != content_hash:
                    self._raise_store_error(
                        "batch_conflict",
                        "相同批次幂等键的批次内容或依赖关系与已存在批次不一致",
                    )
                rows = conn.execute(
                    """
                    SELECT t.* FROM batch_members m JOIN tasks t ON t.id = m.task_id
                    WHERE m.batch_id = ? ORDER BY m.position
                    """,
                    (brow["id"],),
                ).fetchall()
                conn.execute("COMMIT")
                return False, [self._row_to_record(r) for r in rows], None

            # 2) 同批幂等键唯一
            keys_in_batch = [s.idempotency_key for s in specs]
            if len(set(keys_in_batch)) != len(keys_in_batch):
                dup = next(k for k in keys_in_batch if keys_in_batch.count(k) > 1)
                self._raise_store_error(
                    "duplicate_task_key",
                    f"批次内业务唯一键重复: {dup!r}",
                )

            # 3) 解析每个键对应的任务（同批待插入或已存在），并校验已存在任务
            records: dict[str, TaskRecord] = {}
            new_specs: list[tuple[int, BatchTaskSpec, str]] = []
            order: list[str] = []
            for pos, (spec, phash) in enumerate(zip(specs, payload_hashes)):
                row = conn.execute(
                    "SELECT * FROM tasks WHERE idempotency_key = ?",
                    (spec.idempotency_key,),
                ).fetchone()
                if row is None:
                    new_specs.append((pos, spec, phash))
                    order.append(spec.idempotency_key)
                    continue
                existing = self._row_to_record(row)
                if existing.payload_hash != phash:
                    self._raise_store_error(
                        "payload_conflict",
                        f"业务唯一键 {spec.idempotency_key!r} 的载荷与已存在任务不一致",
                    )
                if (
                    existing.task_type != spec.task_type
                    or existing.scope != spec.scope
                    or existing.max_attempts != spec.max_attempts
                ):
                    self._raise_store_error(
                        "payload_conflict",
                        f"业务唯一键 {spec.idempotency_key!r} 的任务参数与已存在任务不一致",
                    )
                records[spec.idempotency_key] = existing
                order.append(spec.idempotency_key)

            # 4) 构建依赖邻接（key -> 前置 key 集合），覆盖同批任务的声明；
            #    复用的已存在任务不得通过新批次改写其依赖。
            graph: dict[str, set[str]] = {}
            for spec in specs:
                deps: set[str] = set()
                for dep in spec.depends_on:
                    if not dep:
                        self._raise_store_error(
                            "invalid_dependency",
                            f"任务 {spec.idempotency_key!r} 声明了空的依赖键",
                        )
                    if dep not in deps and dep not in keys_in_batch:
                        dep_row = conn.execute(
                            "SELECT id FROM tasks WHERE idempotency_key = ?",
                            (dep,),
                        ).fetchone()
                        if dep_row is None:
                            self._raise_store_error(
                                "dependency_not_found",
                                f"任务 {spec.idempotency_key!r} 引用了不存在的任务 {dep!r}",
                            )
                    deps.add(dep)
                graph[spec.idempotency_key] = deps

            # 复用任务：比对其现有依赖与本次声明是否一致
            for spec in specs:
                if spec.idempotency_key not in records:
                    continue
                existing_rows = conn.execute(
                    """
                    SELECT p.idempotency_key AS k FROM task_dependencies d
                    JOIN tasks p ON p.id = d.depends_on_task_id
                    WHERE d.task_id = ?
                    """,
                    (records[spec.idempotency_key].id,),
                ).fetchall()
                existing_deps = {r["k"] for r in existing_rows}
                if existing_deps != set(spec.depends_on):
                    self._raise_store_error(
                        "payload_conflict",
                        f"业务唯一键 {spec.idempotency_key!r} 已存在且其依赖关系与本批声明不一致",
                    )

            # 5) 跨作用域依赖校验（含引用的已存在任务）
            scope_of = {s.idempotency_key: s.scope for s in specs}
            for spec in specs:
                for dep in graph[spec.idempotency_key]:
                    dep_scope = scope_of.get(dep)
                    if dep_scope is None:
                        dep_row = conn.execute(
                            "SELECT scope FROM tasks WHERE idempotency_key = ?",
                            (dep,),
                        ).fetchone()
                        dep_scope = dep_row["scope"] if dep_row else None
                    if dep_scope is not None and dep_scope != spec.scope:
                        self._raise_store_error(
                            "cross_scope_dependency",
                            f"任务 {spec.idempotency_key!r}(scope={spec.scope!r}) 不允许"
                            f"依赖不同作用域的任务 {dep!r}(scope={dep_scope!r})",
                        )

            # 6) 环检测：只有同批任务之间可能形成新环（旧任务不可能反向依赖
            #    本批新任务），对本批子图做 Kahn 拓扑判定即可。
            indegree = {k: 0 for k in keys_in_batch}
            children: dict[str, list[str]] = {k: [] for k in keys_in_batch}
            for k in keys_in_batch:
                for dep in graph[k]:
                    if dep in indegree:
                        indegree[k] += 1
                        children[dep].append(k)
            queue = [k for k, d in indegree.items() if d == 0]
            seen = 0
            while queue:
                node = queue.pop()
                seen += 1
                for child in children[node]:
                    indegree[child] -= 1
                    if indegree[child] == 0:
                        queue.append(child)
            if seen != len(keys_in_batch):
                cycle_keys = sorted(k for k, d in indegree.items() if d > 0)
                self._raise_store_error(
                    "cyclic_dependency",
                    f"依赖关系存在环，涉及任务: {cycle_keys}",
                )

            # 7) 写入批次
            conn.execute(
                "INSERT INTO batches (id, idempotency_key, content_hash, "
                "created_at, updated_at) VALUES (?,?,?,?,?)",
                (batch_id, batch_key, content_hash, now, now),
            )

            # 8) 写入新任务（拓扑序无关：外键引用的行在边写入前都会存在，
            #    因为依赖要么是同批新任务（本循环全部先插入）、要么是旧任务）
            for pos, spec, phash in new_specs:
                rec = TaskRecord(
                    id=uuid.uuid4().hex,
                    idempotency_key=spec.idempotency_key,
                    task_type=spec.task_type,
                    payload=spec.payload,
                    payload_hash=phash,
                    priority=spec.priority,
                    scope=spec.scope,
                    max_attempts=spec.max_attempts,
                    timeout_seconds=spec.timeout_seconds,
                    status=TaskStatus.PENDING,
                    attempts=0,
                    created_at=now_dt,
                    updated_at=now_dt,
                    scheduled_at=now_dt,
                )
                records[spec.idempotency_key] = rec
                conn.execute(
                    """
                    INSERT INTO tasks (id, idempotency_key, task_type, payload,
                        payload_hash, priority, scope, max_attempts, timeout_seconds,
                        status, attempts, created_at, updated_at, scheduled_at,
                        started_at, finished_at, result, error_code, error_message,
                        cancel_requested, batch_id)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        rec.id, rec.idempotency_key, rec.task_type,
                        json.dumps(rec.payload, ensure_ascii=False, sort_keys=True),
                        rec.payload_hash, rec.priority.value, rec.scope,
                        rec.max_attempts, rec.timeout_seconds, rec.status.value,
                        rec.attempts, _dt(rec.created_at), _dt(rec.updated_at),
                        _dt(rec.scheduled_at), None, None, None, None, None, 0,
                        batch_id,
                    ),
                )
                conn.execute(
                    "INSERT INTO batch_members (batch_id, task_id, position) "
                    "VALUES (?,?,?)",
                    (batch_id, rec.id, pos),
                )
            new_keys = {spec.idempotency_key for _, spec, _ in new_specs}
            for pos, spec in enumerate(specs):
                if spec.idempotency_key in new_keys:
                    continue
                conn.execute(
                    "INSERT OR IGNORE INTO batch_members (batch_id, task_id, position) "
                    "VALUES (?,?,?)",
                    (batch_id, records[spec.idempotency_key].id, pos),
                )

            # 9) 写入依赖边（去重）。依赖若指向批外已存在任务，先解析进 records。
            for spec in specs:
                task_id = records[spec.idempotency_key].id
                for dep in sorted(graph[spec.idempotency_key]):
                    if dep not in records:
                        dep_row = conn.execute(
                            "SELECT * FROM tasks WHERE idempotency_key = ?",
                            (dep,),
                        ).fetchone()
                        # 校验阶段已保证引用存在，这里取行只是解析任务 ID
                        records[dep] = self._row_to_record(dep_row)
                    conn.execute(
                        "INSERT OR IGNORE INTO task_dependencies "
                        "(task_id, depends_on_task_id) VALUES (?,?)",
                        (task_id, records[dep].id),
                    )

            # 10) 提交前先做一次级联传播：引用的前置若已失败/取消/跳过，
            #     新提交的后继在返回时就已经是确定的 skipped 终态。
            self._propagate_skipped(conn)

            result_rows = [
                conn.execute(
                    "SELECT * FROM tasks WHERE idempotency_key = ?", (k,)
                ).fetchone()
                for k in order
            ]
            result = [self._row_to_record(r) for r in result_rows]
            conn.execute("COMMIT")
            batch_record = BatchRecord(
                id=batch_id, idempotency_key=batch_key, content_hash=content_hash,
                created_at=now_dt, updated_at=now_dt,
            )
            return True, result, batch_record
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    async def get_batch_by_key(self, key: str) -> BatchRecord | None:
        row = self.conn.execute(
            "SELECT * FROM batches WHERE idempotency_key = ?", (key,)
        ).fetchone()
        if row is None:
            return None
        return BatchRecord(
            id=row["id"], idempotency_key=row["idempotency_key"],
            content_hash=row["content_hash"],
            created_at=_parse_dt(row["created_at"]),  # type: ignore[arg-type]
            updated_at=_parse_dt(row["updated_at"]),  # type: ignore[arg-type]
        )

    async def list_batch_tasks(self, batch_id: str) -> list[TaskRecord]:
        rows = self.conn.execute(
            """
            SELECT t.* FROM batch_members m JOIN tasks t ON t.id = m.task_id
            WHERE m.batch_id = ? ORDER BY m.position
            """,
            (batch_id,),
        ).fetchall()
        return [self._row_to_record(r) for r in rows]

    async def list_dependencies(self, task_id: str) -> list[str]:
        """返回任务直接依赖的前置任务 idempotency_key 列表（稳定排序）。"""
        rows = self.conn.execute(
            """
            SELECT p.idempotency_key AS k FROM task_dependencies d
            JOIN tasks p ON p.id = d.depends_on_task_id
            WHERE d.task_id = ? ORDER BY p.idempotency_key
            """,
            (task_id,),
        ).fetchall()
        return [r["k"] for r in rows]
