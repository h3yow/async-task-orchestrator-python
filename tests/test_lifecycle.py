"""生命周期：等待态取消、运行中取消及时生效、取消与完成竞速、终态不可重复转换。"""

import asyncio
import logging
import time

import pytest

from task_orchestrator.errors import AlreadyTerminal
from task_orchestrator.models import (
    Priority,
    SchedulerConfig,
    SubmitRequest,
    TaskStatus,
)
from task_orchestrator.orchestrator import TaskOrchestrator
from task_orchestrator.repository import TaskRepository

logger = logging.getLogger(__name__)


@pytest.fixture
async def orch(tmp_path):
    config = SchedulerConfig(
        global_concurrency=1,
        default_scope_concurrency=1,
        aging_rate=10.0,
    )
    o = TaskOrchestrator(
        TaskRepository(str(tmp_path / "life.db")), config=config
    )
    await o.start()
    yield o
    await o.shutdown()


async def _submit(orch, key, scope="s", **kw):
    defaults = dict(task_type="sleep", payload={"seconds": 5.0},
                    idempotency_key=key, timeout_seconds=10.0)
    defaults.update(kw)
    return (await orch.submit(SubmitRequest(**defaults))).record


async def _wait_status(orch, task_id, statuses, timeout=3.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        rec = await orch.get_task(task_id)
        if rec.status in statuses:
            return rec
        await asyncio.sleep(0.005)
    raise AssertionError(f"{task_id} never reached {statuses}")


async def test_cancel_waiting_task(orch):
    # 全局并发=1：第一个任务占住，后续任务保持在队列中可被取消
    running = await _submit(orch, "run-1")
    await _wait_status(orch, running.id, {TaskStatus.RUNNING})
    waiting = await _submit(orch, "wait-1")
    await asyncio.sleep(0.05)
    result = await orch.cancel(waiting.id)
    logger.info("cancel waiting task -> status=%s (expect cancelled)",
                result.status.value)
    assert result.status is TaskStatus.CANCELLED
    # 再次取消终态必须被拒绝，原因可区分
    with pytest.raises(AlreadyTerminal) as ei:
        await orch.cancel(waiting.id)
    logger.info("duplicate cancel rejected -> %s", ei.value.error_code)
    assert ei.value.error_code == "already_terminal"


async def test_cancel_running_takes_effect_promptly(orch):
    rec = await _submit(orch, "run-cancel")
    await _wait_status(orch, rec.id, {TaskStatus.RUNNING})
    start = time.time()
    cancelled = await orch.cancel(rec.id)
    logger.info("cancel requested at %.3f, flag=%s status=%s",
                time.time() - start, cancelled.cancel_requested,
                cancelled.status.value)
    final = await _wait_status(
        orch, rec.id, {TaskStatus.CANCELLED}, timeout=2.0)
    elapsed = time.time() - start
    logger.info("running task reached %s in %.3fs (sleep was 5s)",
                final.status.value, elapsed)
    assert final.status is TaskStatus.CANCELLED
    assert elapsed < 1.0  # 及时生效：不必等 5s 睡眠结束


async def test_cancel_and_completion_race_has_single_outcome(orch):
    # 极短任务 + 取消请求并发；无论谁先，终态必须唯一且不被重复转换
    outcomes = {"cancelled": 0, "succeeded": 0, "other": 0}
    for i in range(10):
        rec = await _submit(
            orch, f"race-{i}",
            task_type="sleep", payload={"seconds": 0.01 + 0.002 * i},
            timeout_seconds=5.0,
        )

        async def cancel_soon(task_id):
            await asyncio.sleep(0.001 * (i % 5))
            try:
                await orch.cancel(task_id)
            except AlreadyTerminal:
                pass  # 完成先到：拒绝原因明确，允许

        await asyncio.gather(cancel_soon(rec.id))
        final = await _wait_status(
            orch, rec.id,
            {TaskStatus.CANCELLED, TaskStatus.SUCCEEDED,
             TaskStatus.FAILED, TaskStatus.TIMED_OUT},
            timeout=3.0,
        )
        outcomes[final.status.value if final.status.value in outcomes else "other"] += 1
        # 终态版本必须稳定：再等一会状态/版本不变
        v1 = final.version
        await asyncio.sleep(0.02)
        again = await orch.get_task(rec.id)
        assert again.status == final.status and again.version == v1
    logger.info("race outcomes over 10 tasks: %s (only cancelled/succeeded allowed)",
                outcomes)
    assert outcomes["other"] == 0
    assert outcomes["cancelled"] + outcomes["succeeded"] == 10


async def test_terminal_not_transitioned_again(orch):
    rec = await _submit(orch, "term-1", task_type="echo",
                        payload={"message": "x"}, timeout_seconds=2.0)
    final = await _wait_status(
        orch, rec.id,
        {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.TIMED_OUT})
    with pytest.raises(AlreadyTerminal):
        await orch.cancel(rec.id)
    fresh = await orch.get_task(rec.id)
    logger.info("terminal=%s stays terminal after cancel attempt",
                fresh.status.value)
    assert fresh.status == final.status
