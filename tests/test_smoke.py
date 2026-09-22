"""冒烟测试：骨架所有模块可导入、核心枚举值正确。"""

from task_orchestrator import models, errors, repository, ready_queue, handlers, executor, orchestrator
from task_orchestrator.app import create_app  # noqa: F401
from task_orchestrator.models import TaskStatus, Priority, TERMINAL_STATUSES


def test_imports_and_state_values():
    assert TaskStatus.SUCCEEDED.value == "succeeded"
    assert Priority.HIGH > Priority.LOW
    assert len(TERMINAL_STATUSES) == 4
    assert errors.PayloadConflict.error_code == "payload_conflict"
