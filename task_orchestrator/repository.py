"""SQLite 任务仓储：单连接 + 线程锁；状态转换一律走 CAS(version)。

关键不变量：
- idempotency_key 唯一（DB 约束兜底并发重复提交）；
- 每次状态转换必须带 expected_version，行不匹配则转换失败（返回 None），
  从而"终态不重复转换"、"取消与完成竞速"都有确定结果；
- 所有写操作在提交前即落盘（WAL + 短事务），进程崩溃不丢已确认状态。
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time

from .models import TaskRecord, TaskStatus, TERMINAL_STATUSES, WAITING_STATUSES

_WAITING_SQL = "('pending','ready','retry_wait')"


class TaskRepository:
    """任务持久化仓储。"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None

    # ---- 生命周期 -------------------------------------------------------

    async def init(self) -> None:
        with self._lock:
            if self._conn is not None:
                return
            directory = os.path.dirname(os.path.abspath(self.db_path))
            os.makedirs(directory, exist_ok=True)
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    task_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    scope TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_retries INTEGER NOT NULL DEFAULT 0,
                    timeout_seconds REAL NOT NULL,
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    not_before REAL NOT NULL DEFAULT 0,
                    lease_owner TEXT,
                    last_error TEXT,
                    result_json TEXT,
                    version INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            conn.commit()
            self._conn = conn

    async def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.commit()
                self._conn.close()
                self._conn = None

    # ---- 内部工具 -------------------------------------------------------

    def _c(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("repository not initialised")
        return self._conn

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> TaskRecord:
        return TaskRecord(
            id=row["id"],
            idempotency_key=row["idempotency_key"],
            task_type=row["task_type"],
            payload_json=row["payload_json"],
            payload_hash=row["payload_hash"],
            priority=row["priority"],
            scope=row["scope"],
            status=TaskStatus(row["status"]),
            attempts=row["attempts"],
            max_retries=row["max_retries"],
            timeout_seconds=row["timeout_seconds"],
            cancel_requested=bool(row["cancel_requested"]),
            not_before=row["not_before"],
            lease_owner=row["lease_owner"],
            last_error=row["last_error"],
            result_json=row["result_json"],
            version=row["version"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _fetch_one(self, sql: str, params: tuple) -> TaskRecord | None:
        cur = self._c().execute(sql, params)
        row = cur.fetchone()
        return self._row_to_record(row) if row is not None else None

    # ---- 查询 -----------------------------------------------------------

    async def get(self, task_id: str) -> TaskRecord | None:
        with self._lock:
            return self._fetch_one("SELECT * FROM tasks WHERE id = ?", (task_id,))

    async def get_by_idempotency_key(self, key: str) -> TaskRecord | None:
        with self._lock:
            return self._fetch_one(
                "SELECT * FROM tasks WHERE idempotency_key = ?", (key,)
            )

    async def list_active(self) -> list[TaskRecord]:
        """所有非终态任务（启动恢复用），按 created_at 排序。"""
        placeholders = ",".join("?" for _ in TERMINAL_STATUSES)
        values = tuple(s.value for s in TERMINAL_STATUSES)
        with self._lock:
            cur = self._c().execute(
                f"SELECT * FROM tasks WHERE status NOT IN ({placeholders})"
                " ORDER BY created_at",
                values,
            )
            return [self._row_to_record(r) for r in cur.fetchall()]

    # ---- 写入 -----------------------------------------------------------

    async def insert(self, record: TaskRecord) -> bool:
        """插入新任务；idempotency_key 冲突返回 False（并发重复提交兜底）。"""
        with self._lock:
            try:
                self._c().execute(
                    """
                    INSERT INTO tasks (
                        id, idempotency_key, task_type, payload_json, payload_hash,
                        priority, scope, status, attempts, max_retries,
                        timeout_seconds, cancel_requested, not_before, lease_owner,
                        last_error, result_json, version, created_at, updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        record.id,
                        record.idempotency_key,
                        record.task_type,
                        record.payload_json,
                        record.payload_hash,
                        record.priority,
                        record.scope,
                        record.status.value,
                        record.attempts,
                        record.max_retries,
                        record.timeout_seconds,
                        int(record.cancel_requested),
                        record.not_before,
                        record.lease_owner,
                        record.last_error,
                        record.result_json,
                        record.version,
                        record.created_at,
                        record.updated_at,
                    ),
                )
                self._c().commit()
                return True
            except sqlite3.IntegrityError:
                # 2069 旧 sqlite 的 UNIQUE 冲突也归这里
                return False

    async def lease_for_run(
        self, task_id: str, expected_version: int, owner: str
    ) -> TaskRecord | None:
        """CAS 占用执行租约：等待态 -> RUNNING。

        取消请求已置位、或已到期条件不满足（not_before）则拒绝。
        """
        now = time.time()
        with self._lock:
            cur = self._c().execute(
                f"""
                UPDATE tasks
                   SET status = 'running', lease_owner = ?, version = version + 1,
                       updated_at = ?
                 WHERE id = ? AND version = ?
                   AND status IN {_WAITING_SQL}
                   AND cancel_requested = 0
                   AND not_before <= ?
                """,
                (owner, now, task_id, expected_version, now),
            )
            if cur.rowcount == 0:
                return None
            self._c().commit()
            return await self.get(task_id)

    async def schedule_retry(
        self,
        task_id: str,
        expected_version: int,
        attempts: int,
        not_before: float,
        error: str,
    ) -> TaskRecord | None:
        """CAS：RUNNING -> RETRY_WAIT，释放租约。"""
        now = time.time()
        with self._lock:
            cur = self._c().execute(
                """
                UPDATE tasks
                   SET status = 'retry_wait', lease_owner = NULL,
                       attempts = ?, not_before = ?, last_error = ?,
                       version = version + 1, updated_at = ?
                 WHERE id = ? AND version = ? AND status = 'running'
                """,
                (attempts, not_before, error, now, task_id, expected_version),
            )
            if cur.rowcount == 0:
                return None
            self._c().commit()
            return await self.get(task_id)

    async def finish(
        self,
        task_id: str,
        expected_version: int,
        status: TaskStatus,
        *,
        attempts: int,
        result_json: str | None = None,
        error: str | None = None,
        cancel_sensitive: bool = True,
    ) -> TaskRecord | None:
        """CAS 落终态。

        cancel_sensitive=True（成功/失败/超时）时，若 cancel_requested 已置位，
        本方法返回 None，交由调用方走取消仲裁，保证取消及时生效且终态唯一。
        """
        if status not in TERMINAL_STATUSES:
            raise ValueError(f"finish() requires a terminal status, got {status}")
        now = time.time()
        guard = "AND cancel_requested = 0" if cancel_sensitive else ""
        with self._lock:
            cur = self._c().execute(
                f"""
                UPDATE tasks
                   SET status = ?, attempts = ?, result_json = COALESCE(?, result_json),
                       last_error = ?, lease_owner = NULL,
                       version = version + 1, updated_at = ?
                 WHERE id = ? AND version = ? {guard}
                """,
                (
                    status.value,
                    attempts,
                    result_json,
                    error,
                    now,
                    task_id,
                    expected_version,
                ),
            )
            if cur.rowcount == 0:
                return None
            self._c().commit()
            return await self.get(task_id)

    async def request_cancel(self, task_id: str) -> TaskRecord | None:
        """对 RUNNING 任务打 cancel_requested 标记（不改状态、不占版本转换）。"""
        now = time.time()
        with self._lock:
            cur = self._c().execute(
                """
                UPDATE tasks
                   SET cancel_requested = 1, updated_at = ?
                 WHERE id = ? AND status = 'running'
                """,
                (now, task_id),
            )
            if cur.rowcount == 0:
                return None
            self._c().commit()
            return await self.get(task_id)

    async def promote_retry_wait(
        self, task_id: str, expected_version: int
    ) -> TaskRecord | None:
        """CAS：到期 RETRY_WAIT -> READY（取消已请求则拒绝）。"""
        now = time.time()
        with self._lock:
            cur = self._c().execute(
                """
                UPDATE tasks
                   SET status = 'ready', version = version + 1, updated_at = ?
                 WHERE id = ? AND version = ?
                   AND status = 'retry_wait' AND cancel_requested = 0
                """,
                (now, task_id, expected_version),
            )
            if cur.rowcount == 0:
                return None
            self._c().commit()
            return await self.get(task_id)

    async def cancel_waiting(self, task_id: str) -> TaskRecord | None:
        """等待态任务 CAS 落 CANCELLED。"""
        now = time.time()
        with self._lock:
            cur = self._c().execute(
                f"""
                UPDATE tasks
                   SET status = 'cancelled', lease_owner = NULL,
                       version = version + 1, updated_at = ?
                 WHERE id = ? AND status IN {_WAITING_SQL}
                """,
                (now, task_id),
            )
            if cur.rowcount == 0:
                return None
            self._c().commit()
            return await self.get(task_id)

    async def force_cancel_running(
        self, task_id: str, expected_version: int, error: str
    ) -> TaskRecord | None:
        """运行中任务在取消竞速中胜出时落 CANCELLED（不检查 cancel_requested）。"""
        now = time.time()
        with self._lock:
            cur = self._c().execute(
                """
                UPDATE tasks
                   SET status = 'cancelled', last_error = ?, lease_owner = NULL,
                       version = version + 1, updated_at = ?
                 WHERE id = ? AND version = ? AND status = 'running'
                """,
                (error, now, task_id, expected_version),
            )
            if cur.rowcount == 0:
                return None
            self._c().commit()
            return await self.get(task_id)

    async def recover_stale_leases(self) -> list[TaskRecord]:
        """启动恢复：

        1) RUNNING 记录（上个进程崩溃在执行中）-> READY、清空租约，任务不丢失；
        2) 已到期 RETRY_WAIT -> READY（未到期保留，由执行器定时器唤醒）；
        3) 返回全部非终态任务，由执行器重新入队。
        崩溃窗口内的执行不提交终态，因此不会产生重复结果，最多重试一次。
        """
        now = time.time()
        with self._lock:
            self._c().execute(
                "UPDATE tasks SET status = 'ready', lease_owner = NULL,"
                " version = version + 1, updated_at = ? WHERE status = 'running'",
                (now,),
            )
            self._c().execute(
                "UPDATE tasks SET status = 'ready', version = version + 1,"
                " updated_at = ? WHERE status = 'retry_wait' AND not_before <= ?",
                (now, now),
            )
            self._c().commit()
            return await self.list_active()
