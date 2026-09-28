"""领域模型：任务状态、优先级、记录结构与异常。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime, timezone


def utcnow() -> datetime:
    """返回带时区的 UTC 当前时间（单一入口，便于测试理解）。"""
    return datetime.now(timezone.utc)


class TaskStatus(str, enum.Enum):
    """任务状态机枚举（值即持久化字符串）。"""

    PENDING = "pending"        # 已提交，等待调度（可能在退避等待中）
    BLOCKED = "blocked"        # 已提交，但尚有依赖未成功结束，不可被调度领取
    RUNNING = "running"        # 已被某个工作协程领取执行
    SUCCEEDED = "succeeded"    # 终态：成功
    FAILED = "failed"          # 终态：重试耗尽或不可重试失败
    CANCELLED = "cancelled"    # 终态：被取消（含等待依赖期间被取消）
    SKIPPED = "skipped"        # 终态：依赖失败/取消而被传播跳过，从未执行


TERMINAL_STATUSES: frozenset[TaskStatus] = frozenset(
    {
        TaskStatus.SUCCEEDED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
        TaskStatus.SKIPPED,
    }
)


class Priority(str, enum.Enum):
    """任务优先级：high > normal > low。"""

    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


PRIORITY_RANK: dict[Priority, int] = {
    Priority.HIGH: 0,
    Priority.NORMAL: 1,
    Priority.LOW: 2,
}


@dataclass
class TaskRecord:
    """tasks 表的一行；任务的权威状态全部以此结构为准。"""

    id: str
    idempotency_key: str
    task_type: str
    payload: dict
    payload_hash: str
    priority: Priority
    scope: str
    max_attempts: int
    timeout_seconds: float
    status: TaskStatus
    attempts: int = 0
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)
    scheduled_at: datetime = field(default_factory=utcnow)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    result: dict | None = None
    error_code: str | None = None
    error_message: str | None = None
    cancel_requested: bool = False


@dataclass
class BatchRecord:
    """batches 表的一行：记录一次批量提交的幂等键与其规范化内容指纹。"""

    id: str
    idempotency_key: str
    content_hash: str
    created_at: datetime = field(default_factory=utcnow)


class OrchestratorError(Exception):
    """服务层异常基类。"""


class ValidationFailure(OrchestratorError):
    """提交参数非法（HTTP 422 语义，独立于框架校验，便于区分错误码）。"""


class PayloadConflict(OrchestratorError):
    """幂等键相同但载荷不一致（HTTP 409 语义）。"""


class BatchConflict(OrchestratorError):
    """批次幂等键相同但批次内容不一致（HTTP 409 语义）。"""


class TaskNotFound(OrchestratorError):
    """任务不存在（HTTP 404 语义）。"""


class BatchNotFound(OrchestratorError):
    """批次不存在（HTTP 404 语义）。"""
