"""仓储层单测：插入幂等、租约 CAS、终态 CAS、取消仲裁与启动恢复。"""

import time

import pytest

from task_orchestrator.models import TaskRecord, TaskStatus
from task_orchestrator.repository import TaskRepository


def make_record(key: str = "k1", status: TaskStatus = TaskStatus.PENDING, **kw) -> TaskRecord:
    now = time.time()
    defaults = dict(
        id=f"t-{key}", idempotency_key=key, task_type="echo",
        payload_json='{"x":1}', payload_hash="h1", priority=5, scope="default",
        status=status, attempts=0, max_retries=2, timeout_seconds=1.0,
        cancel_requested=False, not_before=0.0, lease_owner=None,
        last_error=None, result_json=None, version=0,
        created_at=now, updated_at=now,
    )
    defaults.update(kw)
    return TaskRecord(**defaults)


@pytest.fixture
async def repo(tmp_path):
    r = TaskRepository(str(tmp_path / "t.db"))
    await r.init()
    yield r
    await r.close()


async def test_insert_duplicate_returns_false(repo):
    assert await repo.insert(make_record()) is True
    assert await repo.insert(make_record()) is False


async def test_lease_cas_version_guard(repo):
    await repo.insert(make_record())
    rec = await repo.get("t-k1")
    leased = await repo.lease_for_run(rec.id, rec.version, "owner-1")
    assert leased is not None and leased.status is TaskStatus.RUNNING
    # 旧版本号不得再次占用
    assert await repo.lease_for_run(rec.id, rec.version, "owner-2") is None


async def test_finish_terminal_cas_and_cancel_arbitration(repo):
    await repo.insert(make_record())
    rec = await repo.get("t-k1")
    leased = await repo.lease_for_run(rec.id, rec.version, "o")
    # 取消先到：cancel_sensitive 的终态写入必须失败
    assert await repo.request_cancel(leased.id) is not None
    assert await repo.finish(leased.id, leased.version, TaskStatus.SUCCEEDED,
                            attempts=1, result_json='{"ok":true}') is None
    fresh = await repo.get(leased.id)
    # 取消仲裁落 CANCELLED
    done = await repo.force_cancel_running(fresh.id, fresh.version, "cancelled by user")
    assert done.status is TaskStatus.CANCELLED
    # 终态不得被重复转换
    assert await repo.force_cancel_running(done.id, done.version, "again") is None


async def test_cancel_waiting(repo):
    await repo.insert(make_record("cw"))
    rec = await repo.get("t-cw")
    out = await repo.cancel_waiting(rec.id)
    assert out.status is TaskStatus.CANCELLED
    assert await repo.cancel_waiting(rec.id) is None


async def test_recover_stale_running_lease(repo):
    await repo.insert(make_record("stale", status=TaskStatus.RUNNING, lease_owner="dead"))
    recovered = await repo.recover_stale_leases()
    assert len(recovered) == 1
    assert recovered[0].status is TaskStatus.READY and recovered[0].lease_owner is None
