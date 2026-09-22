"""领域类型：任务状态机、提交请求、持久化记录与调度配置。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class Priority(enum.IntEnum):
    """任务优先级；数值越大越优先被调度。"""

    LOW = 1
    NORMAL = 5
    HIGH = 10


class TaskStatus(str, enum.Enum):
    """任务状态机：

    PENDING -> READY -> RUNNING -> SUCCEEDED / FAILED / TIMED_OUT / CANCELLED
                                   RUNNING -> RETRY_WAIT -> READY  (可重试失败)
    任意等待态(PENDING/READY/RETRY_WAIT) -> CANCELLED
    RUNNING 上的取消先打 cancel_requested 标记，由运行循环尽快终止。
    """

    PENDING = "pending"
    READY = "ready"
    RETRY_WAIT = "retry_wait"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"


TERMINAL_STATUSES = frozenset(
    {
        TaskStatus.SUCCEEDED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
        TaskStatus.TIMED_OUT,
    }
)

# 等待执行的状态
WAITING_STATUSES = frozenset(
    {TaskStatus.PENDING, TaskStatus.READY, TaskStatus.RETRY_WAIT}
)


@dataclass(frozen=True)
class SubmitRequest:
    """任务提交入参。"""

    task_type: str
    payload: dict
    idempotency_key: str
    priority: Priority = Priority.NORMAL
    scope: str = "default"
    timeout_seconds: float = 5.0
    max_retries: int = 0


@dataclass(frozen=True)
class TaskRecord:
    """tasks 表的一行（任务的权威状态）。"""

    id: str
    idempotency_key: str
    task_type: str
    payload_json: str
    payload_hash: str
    priority: int
    scope: str
    status: TaskStatus
    attempts: int
    max_retries: int
    timeout_seconds: float
    cancel_requested: bool
    not_before: float
    lease_owner: str | None
    last_error: str | None
    result_json: str | None
    version: int
    created_at: float
    updated_at: float


@dataclass(frozen=True)
class SubmitResult:
    """提交结果：deduplicated=True 表示命中了同一业务唯一键的既有任务。"""

    record: TaskRecord
    deduplicated: bool


@dataclass(frozen=True)
class SchedulerConfig:
    """调度与执行资源配置。"""

    global_concurrency: int = 4
    default_scope_concurrency: int = 2
    scope_concurrency: dict[str, int] = field(default_factory=dict)
    # 老化速度：每等待 1 秒，有效优先级提升 aging_rate 分（防饥饿）
    aging_rate: float = 2.0
    # 调度循环 tick
    poll_interval: float = 0.01
    # 退避：base * 2**(attempt-1)，上限 cap
    retry_base_delay: float = 0.05
    retry_cap_delay: float = 2.0
