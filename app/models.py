"""领域模型：任务状态、优先级、记录结构与异常。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def utcnow() -> datetime:
    """返回带时区的 UTC 当前时间（单一入口，便于测试理解）。"""
    return datetime.now(timezone.utc)


class TaskStatus(str, enum.Enum):
    """任务状态机枚举（值即持久化字符串）。"""

    PENDING = "pending"        # 已提交，等待调度（可能在退避等待中，也可能在等依赖）
    RUNNING = "running"        # 已被某个工作协程领取执行
    SUCCEEDED = "succeeded"    # 终态：成功
    FAILED = "failed"          # 终态：重试耗尽或不可重试失败（任务真正执行过并失败）
    CANCELLED = "cancelled"    # 终态：被取消
    SKIPPED = "skipped"        # 终态：依赖未满足（前置 failed/cancelled/skipped），
                               # 任务从未执行；与 failed 严格区分


TERMINAL_STATUSES: frozenset[TaskStatus] = frozenset(
    {TaskStatus.SUCCEEDED, TaskStatus.FAILED,
     TaskStatus.CANCELLED, TaskStatus.SKIPPED}
)

# “坏终态”：凡依赖链上出现这类状态，后继任务一律不能再执行。
UNSATISFIABLE_STATUSES: frozenset[TaskStatus] = frozenset(
    {TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.SKIPPED}
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


# SKIPPED 终态的 error_code：说明“因什么依赖结果被跳过”，可与 failed 的
# retryable/fatal/timeout/handler_error 明确区分。
SKIP_REASON_DEP_FAILED = "dependency_failed"
SKIP_REASON_DEP_CANCELLED = "dependency_cancelled"
SKIP_REASON_DEP_SKIPPED = "dependency_skipped"


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


@dataclass(frozen=True)
class BatchTaskSpec:
    """批量提交中的单个任务规格；依赖以业务幂等键声明。

    depends_on 中引用的键可以是同批任务，也可以是此前已经单独/批量提交过的
    任务；持久层负责在提交时验证引用存在、作用域一致且不形成环。
    """

    idempotency_key: str
    task_type: str
    payload: dict[str, Any] = field(default_factory=dict)
    priority: Priority = Priority.NORMAL
    scope: str = "default"
    max_attempts: int = 3
    timeout_seconds: float = 10.0
    depends_on: tuple[str, ...] = ()


@dataclass
class BatchRecord:
    """一次批量提交的权威记录（batches 表一行）。"""

    id: str
    idempotency_key: str
    content_hash: str
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)


class OrchestratorError(Exception):
    """服务层异常基类。"""


class ValidationFailure(OrchestratorError):
    """提交参数非法（HTTP 422 语义，独立于框架校验，便于区分错误码）。"""


class PayloadConflict(OrchestratorError):
    """幂等键相同但载荷不一致（HTTP 409 语义）。"""


class BatchConflict(OrchestratorError):
    """批次幂等键相同但批次内容/依赖关系不一致（HTTP 409 语义）。"""


class TaskNotFound(OrchestratorError):
    """任务不存在（HTTP 404 语义）。"""


class BatchNotFound(OrchestratorError):
    """批次不存在（HTTP 404 语义）。"""


class SchemaMigrationError(OrchestratorError):
    """数据库结构无法安全升级到当前版本（启动即失败，不留下半新半旧的库）。"""
