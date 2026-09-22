"""就绪队列单测：优先级、FIFO 与老化防饥饿。"""

import time

from task_orchestrator.models import Priority, TaskRecord, TaskStatus
from task_orchestrator.ready_queue import ReadyQueue


def rec(task_id: str, priority: int, not_before: float = 0.0) -> TaskRecord:
    now = time.time()
    return TaskRecord(
        id=task_id, idempotency_key=task_id, task_type="echo",
        payload_json="{}", payload_hash="h", priority=priority, scope="s",
        status=TaskStatus.READY, attempts=0, max_retries=0, timeout_seconds=1.0,
        cancel_requested=False, not_before=not_before, lease_owner=None,
        last_error=None, result_json=None, version=0, created_at=now, updated_at=now,
    )


def test_higher_priority_first_and_fifo():
    q = ReadyQueue(aging_rate=0.0)
    t0 = 1000.0
    q.add(rec("low", Priority.LOW), t0)
    q.add(rec("high", Priority.HIGH), t0)
    q.add(rec("normal", Priority.NORMAL), t0)
    assert q.pop_due(t0).id == "high"
    assert q.pop_due(t0).id == "normal"
    assert q.pop_due(t0).id == "low"
    assert q.pop_due(t0) is None


def test_aging_lets_old_low_priority_win():
    # aging_rate=2：LOW(1) 等待 5 秒 -> 11 分，超过刚入队 HIGH(10)
    q = ReadyQueue(aging_rate=2.0)
    t0 = 1000.0
    q.add(rec("low", Priority.LOW), t0)
    q.add(rec("high", Priority.HIGH), t0 + 5.0)
    assert q.pop_due(t0 + 5.0).id == "low"


def test_not_before_holds_then_releases():
    q = ReadyQueue(aging_rate=0.0)
    q.add(rec("future", Priority.HIGH, not_before=10.0), 0.0)
    assert q.pop_due(5.0) is None
    assert q.pop_due(10.0).id == "future"


def test_remove():
    q = ReadyQueue()
    q.add(rec("a", 1), 0.0)
    assert q.remove("a").id == "a"
    assert q.remove("a") is None
    assert len(q) == 0
