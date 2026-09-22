"""应用服务层：参数校验、幂等提交、状态查询、取消。

拒绝原因通过 error_code 区分：
- invalid_idempotency_key / invalid_payload / unknown_task_type /
  invalid_max_attempts / invalid_timeout -> ValidationFailure（422）
- payload_conflict -> PayloadConflict（409）：幂等键相同但载荷不一致
- not_found -> TaskNotFound（404）
重复提交（幂等命中且载荷一致）不报错，返回已有任务记录供上层标记 already_exists。
"""

from __future__ import annotations

import hashlib
import json
import uuid

from .executor import Executor
from .handlers import HandlerRegistry
from .models import (
    PayloadConflict,
    Priority,
    TaskNotFound,
    TaskRecord,
    TaskStatus,
    ValidationFailure,
    utcnow,
)
from .store import TaskStore


def canonical_payload_hash(payload: dict) -> str:
    """载荷的规范化哈希：JSON 键排序、无空白、ensure_ascii=False。

    语义相同（键顺序不同）的载荷哈希一致；任何值差异都会产生不同哈希。
    """
    blob = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


class TaskService:
    """对外的用例服务。"""

    def __init__(
        self,
        store: TaskStore,
        registry: HandlerRegistry,
        executor: Executor | None = None,
    ) -> None:
        self._store = store
        self._registry = registry
        self._executor = executor

    def set_executor(self, executor: Executor) -> None:
        self._executor = executor

    async def submit(
        self,
        *,
        idempotency_key: str,
        task_type: str,
        payload: dict,
        priority: str = "normal",
        scope: str = "default",
        max_attempts: int = 3,
        timeout_seconds: float = 10.0,
    ) -> tuple[TaskRecord, bool]:
        """提交任务，返回 (任务记录, 是否新建)。

        并发重复提交由持久层 BEGIN IMMEDIATE + 唯一约束保证：只有一个请求
        插入成功，其余拿到同一记录；载荷不一致则拒绝并给出 payload_conflict。
        """
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise ValidationFailure("invalid_idempotency_key:幂等键不能为空")
        if not isinstance(payload, dict):
            raise ValidationFailure("invalid_payload:payload 必须是 JSON 对象")
        try:
            prio = Priority(priority)
        except ValueError:
            raise ValidationFailure(f"invalid_priority:未知优先级 {priority!r}")
        if not isinstance(scope, str) or not scope.strip():
            raise ValidationFailure("invalid_scope:scope 不能为空")
        if not isinstance(max_attempts, int) or isinstance(max_attempts, bool) \
                or max_attempts < 1:
            raise ValidationFailure(
                "invalid_max_attempts:max_attempts 必须为 >=1 的整数"
            )
        if not isinstance(timeout_seconds, (int, float)) or isinstance(
            timeout_seconds, bool
        ) or timeout_seconds <= 0:
            raise ValidationFailure(
                "invalid_timeout:timeout_seconds 必须为 >0 的数值"
            )
        if not self._registry.contains(task_type):
            raise ValidationFailure(
                f"unknown_task_type:未注册的任务类型 {task_type!r}"
            )

        payload_hash = canonical_payload_hash(payload)
        now = utcnow()
        record = TaskRecord(
            id=uuid.uuid4().hex,
            idempotency_key=idempotency_key,
            task_type=task_type,
            payload=payload,
            payload_hash=payload_hash,
            priority=prio,
            scope=scope,
            max_attempts=max_attempts,
            timeout_seconds=float(timeout_seconds),
            status=TaskStatus.PENDING,
            attempts=0,
            created_at=now,
            updated_at=now,
            scheduled_at=now,
        )

        created, existing = await self._store.insert_task_if_absent(record)
        if created:
            return record, True

        # 幂等命中：载荷哈希必须一致，否则是“同键不同载荷”冲突
        if existing.payload_hash != payload_hash:
            raise PayloadConflict(
                "payload_conflict:相同 idempotency_key 的载荷与已存在任务不一致"
            )
        if (
            existing.task_type != task_type
            or existing.scope != scope
            or existing.max_attempts != max_attempts
        ):
            # task_type/scope/max_attempts 也属于提交载荷的一部分
            raise PayloadConflict(
                "payload_conflict:相同 idempotency_key 的任务参数与已存在任务不一致"
            )
        return existing, False

    async def get_task(self, task_id: str) -> TaskRecord:
        record = await self._store.get_task(task_id)
        if record is None:
            raise TaskNotFound(f"not_found:任务 {task_id!r} 不存在")
        return record

    async def get_by_key(self, idempotency_key: str) -> TaskRecord:
        record = await self._store.get_by_idempotency_key(idempotency_key)
        if record is None:
            raise TaskNotFound(
                f"not_found:幂等键 {idempotency_key!r} 下无任务"
            )
        return record

    async def cancel(self, task_id: str) -> TaskRecord:
        """请求取消：终态任务原样返回（取消幂等）。

        运行中的任务先落 cancel_requested=1，再通过执行器令牌即时打断其
        可取消等待点，保证“运行中及时生效”。
        """
        record = await self._store.request_cancel(task_id)
        if record is None:
            raise TaskNotFound(f"not_found:任务 {task_id!r} 不存在")
        if record.status.value == "running" and record.cancel_requested:
            if self._executor is not None:
                self._executor.signal_cancel(task_id)
        return record
