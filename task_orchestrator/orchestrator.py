"""编排服务门面：参数校验、幂等提交、查询与取消。

幂等语义：
- idempotency_key 有 DB 唯一约束；并发重复提交时只有一个 INSERT 成功，
  其余按既有任务返回（deduplicated=True，task_id 相同）；
- 同键载荷不同 -> PayloadConflict(409 payload_conflict)；
- 参数非法 / 未知任务类型 -> InvalidRequest(422 invalid_request)，原因可区分；
- 取消：等待态直接落 CANCELLED；运行中先打 cancel_requested 标记，
  执行循环在 sleep 边界/取消事件处及时终止；已终态 -> AlreadyTerminal。
"""

from __future__ import annotations

import json
import time
import uuid

from .errors import (
    AlreadyTerminal,
    InvalidRequest,
    PayloadConflict,
    TaskNotFound,
)
from .executor import Executor
from .handlers import HandlerRegistry, default_registry
from .models import (
    Priority,
    SchedulerConfig,
    SubmitRequest,
    SubmitResult,
    TaskRecord,
    TaskStatus,
    TERMINAL_STATUSES,
)
from .repository import TaskRepository

_KEY_MAX_LEN = 200
_TIMEOUT_RANGE = (0.05, 300.0)
_MAX_RETRIES_RANGE = (0, 20)


def canonical_payload_hash(payload: dict) -> str:
    """规范化载荷哈希：键排序、separators 压缩，同语义载荷哈希一致。"""
    import hashlib

    canonical = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _validate(req: SubmitRequest, registry: HandlerRegistry) -> None:
    if not isinstance(req.idempotency_key, str) or not req.idempotency_key.strip():
        raise InvalidRequest("idempotency_key must be a non-empty string")
    if len(req.idempotency_key) > _KEY_MAX_LEN:
        raise InvalidRequest(f"idempotency_key must be <= {_KEY_MAX_LEN} chars")
    if not isinstance(req.task_type, str) or not req.task_type:
        raise InvalidRequest("task_type must be a non-empty string")
    if not registry.known(req.task_type):
        raise InvalidRequest(f"unknown task_type: {req.task_type}")
    if not isinstance(req.payload, dict):
        raise InvalidRequest("payload must be a JSON object")
    try:
        json.dumps(req.payload, ensure_ascii=False)
    except (TypeError, ValueError) as exc:
        raise InvalidRequest(f"payload not JSON serializable: {exc}") from exc
    if not isinstance(req.scope, str) or not req.scope.strip():
        raise InvalidRequest("scope must be a non-empty string")
    if not isinstance(req.priority, Priority) and req.priority not in (1, 5, 10):
        raise InvalidRequest("priority must be one of 1/5/10 (LOW/NORMAL/HIGH)")
    lo, hi = _TIMEOUT_RANGE
    if not isinstance(req.timeout_seconds, (int, float)) or not (
        lo <= float(req.timeout_seconds) <= hi
    ):
        raise InvalidRequest(f"timeout_seconds must be within [{lo}, {hi}]")
    lo, hi = _MAX_RETRIES_RANGE
    if not isinstance(req.max_retries, int) or not (lo <= req.max_retries <= hi):
        raise InvalidRequest(f"max_retries must be an int within [{lo}, {hi}]")


class TaskOrchestrator:
    def __init__(
        self,
        repo: TaskRepository,
        registry: HandlerRegistry | None = None,
        config: SchedulerConfig | None = None,
    ):
        self.repo = repo
        self.registry = registry or default_registry()
        self.config = config or SchedulerConfig()
        self.executor = Executor(repo, self.registry, self.config)

    async def start(self) -> None:
        await self.repo.init()
        await self.executor.start()

    async def shutdown(self) -> None:
        await self.executor.stop()

    async def submit(self, request: SubmitRequest) -> SubmitResult:
        _validate(request, self.registry)
        now = time.time()
        payload_json = json.dumps(
            request.payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        payload_hash = canonical_payload_hash(request.payload)
        record = TaskRecord(
            id=uuid.uuid4().hex,
            idempotency_key=request.idempotency_key,
            task_type=request.task_type,
            payload_json=payload_json,
            payload_hash=payload_hash,
            priority=int(request.priority),
            scope=request.scope,
            status=TaskStatus.PENDING,
            attempts=0,
            max_retries=request.max_retries,
            timeout_seconds=float(request.timeout_seconds),
            cancel_requested=False,
            not_before=now,
            lease_owner=None,
            last_error=None,
            result_json=None,
            version=0,
            created_at=now,
            updated_at=now,
        )
        inserted = await self.repo.insert(record)
        if not inserted:
            # 唯一键冲突（含并发重复提交）：读取既有任务比对载荷
            existing = await self.repo.get_by_idempotency_key(
                request.idempotency_key
            )
            if existing is None:  # 极端情况：事务尚未可见，重试一次
                existing = await self.repo.get_by_idempotency_key(
                    request.idempotency_key
                )
            if existing is None:
                raise InvalidRequest("idempotency key conflict unresolved")
            if existing.payload_hash != payload_hash:
                raise PayloadConflict(
                    "idempotency key already exists with a different payload"
                )
            # 同键同载荷：返回既有任务，调用方可凭 task_id 区分"同一任务"
            return SubmitResult(record=existing, deduplicated=True)

        await self.executor.admit(record, now)
        return SubmitResult(record=record, deduplicated=False)

    async def get_task(self, task_id: str) -> TaskRecord | None:
        return await self.repo.get(task_id)

    async def get_by_key(self, idempotency_key: str) -> TaskRecord | None:
        return await self.repo.get_by_idempotency_key(idempotency_key)

    async def cancel(self, task_id: str) -> TaskRecord:
        record = await self.repo.get(task_id)
        if record is None:
            raise TaskNotFound(f"task not found: {task_id}")
        if record.status in TERMINAL_STATUSES:
            # 终态不得被重复转换
            raise AlreadyTerminal(f"task already terminal: {record.status.value}")
        if record.status is TaskStatus.RUNNING:
            marked = await self.repo.request_cancel(task_id)
            if marked is None:
                # 与完成竞速输掉：读取最新状态
                fresh = await self.repo.get(task_id)
                if fresh is not None and fresh.status in TERMINAL_STATUSES:
                    raise AlreadyTerminal(
                        f"task already terminal: {fresh.status.value}"
                    )
                if fresh is None:
                    raise TaskNotFound(task_id)
                record = fresh
            else:
                record = marked
            # 尽快通知执行协程（handler 在 sleep 边界观察）
            await self.executor.request_cancel(task_id)
            # 同步从就绪队列移除并不适用（运行中不在队列）；
            # 若恰好在 RETRY_WAIT 等待中，下面分支处理。
            return record
        # 等待态（pending/ready/retry_wait）：尝试直接落 CANCELLED
        cancelled = await self.repo.cancel_waiting(task_id)
        if cancelled is not None:
            # 从内存队列移除，避免再被调度
            for q in self.executor._queues.values():
                if q.remove(task_id) is not None:
                    break
            return cancelled
        fresh = await self.repo.get(task_id)
        if fresh is None:
            raise TaskNotFound(task_id)
        if fresh.status in TERMINAL_STATUSES:
            raise AlreadyTerminal(f"task already terminal: {fresh.status.value}")
        if fresh.status is TaskStatus.RUNNING:
            return await self.cancel(task_id)
        return fresh
