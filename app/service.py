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
    BatchNotFound,
    BatchRecord,
    BatchTaskSpec,
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

    def _validate_task_fields(
        self,
        *,
        idempotency_key: str,
        task_type: str,
        payload: object,
        priority: str,
        scope: str,
        max_attempts: int,
        timeout_seconds: float,
    ) -> Priority:
        """单个任务规格的字段校验；批量与单任务共用，保证错误码一致。"""
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
        return prio

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
        prio = self._validate_task_fields(
            idempotency_key=idempotency_key,
            task_type=task_type,
            payload=payload,
            priority=priority,
            scope=scope,
            max_attempts=max_attempts,
            timeout_seconds=timeout_seconds,
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

    async def submit_batch(
        self,
        *,
        batch_key: str,
        tasks: list[dict],
    ) -> tuple[list[TaskRecord], bool, BatchRecord | None]:
        """批量提交带依赖的任务，返回 (按提交顺序的任务记录, 是否新建, 批次记录)。

        - 每个任务含 idempotency_key/task_type/payload/priority/scope/
          max_attempts/timeout_seconds/depends_on（依赖键列表）；
        - 全部校验（字段、同批键重复、引用不存在、跨作用域、环）与写入在持久层
          单事务内完成，失败整体回滚；
        - 批次幂等：同 batch_key 重复（含并发）提交得到同一组任务与依赖，
          内容不一致则 batch_conflict 拒绝。
        """
        if not isinstance(batch_key, str) or not batch_key.strip():
            raise ValidationFailure("invalid_batch_key:批次幂等键不能为空")
        if not isinstance(tasks, list) or not tasks:
            raise ValidationFailure(
                "invalid_batch_tasks:批次至少要包含 1 个任务"
            )
        if len(tasks) > 1000:
            raise ValidationFailure(
                "invalid_batch_tasks:单批次任务数超过上限 1000"
            )

        specs: list[BatchTaskSpec] = []
        payload_hashes: list[str] = []
        keys_in_batch: set[str] = set()
        for index, item in enumerate(tasks):
            if not isinstance(item, dict):
                raise ValidationFailure(
                    f"invalid_batch_task:第 {index} 个任务规格必须是对象"
                )
            key = item.get("idempotency_key")
            task_type = item.get("task_type")
            payload = item.get("payload", {})
            priority = item.get("priority", "normal")
            scope = item.get("scope", "default")
            max_attempts = item.get("max_attempts", 3)
            timeout_seconds = item.get("timeout_seconds", 10.0)
            depends_on = item.get("depends_on", [])
            prio = self._validate_task_fields(
                idempotency_key=key if isinstance(key, str) else "",
                task_type=task_type if isinstance(task_type, str) else "",
                payload=payload,
                priority=priority if isinstance(priority, str) else "",
                scope=scope if isinstance(scope, str) else "",
                max_attempts=max_attempts
                if isinstance(max_attempts, int) and not isinstance(max_attempts, bool)
                else 0,
                timeout_seconds=timeout_seconds
                if isinstance(timeout_seconds, (int, float))
                and not isinstance(timeout_seconds, bool) else 0.0,
            )
            if not isinstance(depends_on, list) or not all(
                isinstance(d, str) for d in depends_on
            ):
                raise ValidationFailure(
                    f"invalid_dependency:任务 {key!r} 的 depends_on 必须是字符串列表"
                )
            if key in keys_in_batch:
                raise ValidationFailure(
                    f"duplicate_task_key:批次内业务唯一键重复: {key!r}"
                )
            keys_in_batch.add(key)
            specs.append(
                BatchTaskSpec(
                    idempotency_key=key,
                    task_type=task_type,
                    payload=payload,
                    priority=prio,
                    scope=scope,
                    max_attempts=max_attempts,
                    timeout_seconds=float(timeout_seconds),
                    depends_on=tuple(depends_on),
                )
            )
            payload_hashes.append(canonical_payload_hash(payload))

        # 批次内容哈希：按业务键排序后逐项规范化，任务顺序不影响内容一致性；
        # 依赖列表也排序，消除声明顺序差异。
        canonical_entries = sorted(
            (
                {
                    "idempotency_key": s.idempotency_key,
                    "task_type": s.task_type,
                    "payload_hash": h,
                    "priority": s.priority.value,
                    "scope": s.scope,
                    "max_attempts": s.max_attempts,
                    "timeout_seconds": s.timeout_seconds,
                    "depends_on": sorted(s.depends_on),
                }
                for s, h in zip(specs, payload_hashes)
            ),
            key=lambda e: e["idempotency_key"],
        )
        content_blob = json.dumps(
            canonical_entries, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"),
        )
        content_hash = hashlib.sha256(content_blob.encode("utf-8")).hexdigest()

        created, records, batch_record = await self._store.submit_batch(
            batch_id=uuid.uuid4().hex,
            batch_key=batch_key,
            content_hash=content_hash,
            specs=specs,
            payload_hashes=payload_hashes,
        )
        return records, created, batch_record

    async def get_batch(self, batch_key: str) -> tuple[BatchRecord, list[TaskRecord]]:
        batch = await self._store.get_batch_by_key(batch_key)
        if batch is None:
            raise BatchNotFound(f"batch_not_found:批次 {batch_key!r} 不存在")
        members = await self._store.list_batch_tasks(batch.id)
        return batch, members

    async def get_task(self, task_id: str) -> TaskRecord:
        record = await self._store.get_task(task_id)
        if record is None:
            raise TaskNotFound(f"not_found:任务 {task_id!r} 不存在")
        return record

    async def get_dependencies(self, task_id: str) -> list[str]:
        """返回任务直接前置的业务键列表；任务不存在时按 404 处理。"""
        record = await self._store.get_task(task_id)
        if record is None:
            raise TaskNotFound(f"not_found:任务 {task_id!r} 不存在")
        return await self._store.list_dependencies(task_id)

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
