"""SQLite 持久层：所有状态变更均为单连接 + 串行化事务。

进程内所有读写通过一把 asyncio.Lock 串行化，保证“状态判断 + 写入”原子，
从而支持并发重复提交的幂等保证；所有状态流转均为带前置状态条件的 UPDATE，
终态天然不可被重复转换。跨进程重启场景由 BEGIN IMMEDIATE + WAL 提供串行写。
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta

from .models import Priority, TaskRecord, TaskStatus, utcnow

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
            "WHERE status NOT IN ('succeeded', 'failed', 'cancelled')"
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
