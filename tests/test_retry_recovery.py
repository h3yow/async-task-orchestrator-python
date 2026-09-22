"""重试次数耗尽与进程重启恢复：状态、执行计数一致，失败原因可区分。"""

import asyncio
import json
import logging
import subprocess
import sys
import time

import pytest

from task_orchestrator.models import (
    SchedulerConfig,
    SubmitRequest,
    TaskStatus,
)
from task_orchestrator.orchestrator import TaskOrchestrator
from task_orchestrator.repository import TaskRepository

logger = logging.getLogger(__name__)


def make_config():
    return SchedulerConfig(
        global_concurrency=2,
        default_scope_concurrency=2,
        aging_rate=2.0,
        retry_base_delay=0.02,
        retry_cap_delay=0.2,
    )


async def _wait_terminal(orch, task_id, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        rec = await orch.get_task(task_id)
        if rec.status in (TaskStatus.SUCCEEDED, TaskStatus.FAILED,
                          TaskStatus.CANCELLED, TaskStatus.TIMED_OUT):
            return rec
        await asyncio.sleep(0.005)
    raise AssertionError(f"{task_id} not terminal")


async def test_retryable_failure_exhausts_retries(tmp_path):
    db = str(tmp_path / "r.db")
    orch = TaskOrchestrator(TaskRepository(db), config=make_config())
    await orch.start()
    res = await orch.submit(SubmitRequest(
        task_type="flaky", payload={"fail_times": 99},
        idempotency_key="retry-exhaust", max_retries=2, timeout_seconds=2.0,
    ))
    final = await _wait_terminal(orch, res.record.id)
    logger.info("input fail_times=99 max_retries=2 -> status=%s attempts=%s error=%s",
                final.status.value, final.attempts, final.last_error)
    assert final.status is TaskStatus.FAILED
    assert final.attempts == 3
    assert "retries exhausted" in (final.last_error or "")
    await orch.shutdown()


async def test_non_retryable_distinct_reason_and_single_attempt(tmp_path):
    db = str(tmp_path / "nr.db")
    orch = TaskOrchestrator(TaskRepository(db), config=make_config())
    await orch.start()
    res = await orch.submit(SubmitRequest(
        task_type="fail", payload={"retryable": False, "message": "permanent-bug"},
        idempotency_key="non-retry", max_retries=5, timeout_seconds=2.0,
    ))
    final = await _wait_terminal(orch, res.record.id)
    logger.info("input non-retryable max_retries=5 -> status=%s attempts=%s error=%s",
                final.status.value, final.attempts, final.last_error)
    assert final.status is TaskStatus.FAILED
    assert final.attempts == 1
    assert "permanent-bug" in (final.last_error or "")
    await orch.shutdown()


def test_hard_crash_mid_execution_recovers_without_duplicate_commit(tmp_path):
    """硬崩溃恢复：用独立子进程崩溃，主测试进程扮演重启后的新进程。"""
    db = str(tmp_path / "rec.db")
    marker = tmp_path / "crashed"
    progress = tmp_path / "side_effects.log"

    # 进程 1（子进程）：执行到一半硬崩溃
    worker = subprocess.run(
        [sys.executable, "tests/crash_worker.py", db, str(marker), str(progress)],
        cwd=".", timeout=20,
    )
    logger.info("crash worker exit code=%s (negative=signal; 0=os._exit)",
                worker.returncode)
    assert marker.exists()

    # 直接读 DB 验证崩溃现场：任务停在 RUNNING 租约上
    import asyncio as _aio

    async def inspect_running():
        repo = TaskRepository(db)
        await repo.init()
        rec = await repo.get_by_idempotency_key("crash-k")
        await repo.close()
        return rec

    crashed_rec = _aio.run(inspect_running())
    logger.info("crash scene: status=%s lease=%s result=%s",
                crashed_rec.status.value, crashed_rec.lease_owner,
                crashed_rec.result_json)
    assert crashed_rec.status is TaskStatus.RUNNING
    assert crashed_rec.result_json is None

    # 进程 2（主测试事件循环）：同 DB 启动 -> 回收租约 -> 重新执行恰好一次
    async def second_life():
        async def crash_once(payload, ctx):
            with open(progress, "a") as f:
                f.write(f"start:{ctx.record.idempotency_key}\n")
            return {"survived": True}

        orch = TaskOrchestrator(TaskRepository(db), config=make_config())
        orch.registry.register("crash_once", crash_once)
        await orch.start()
        final = await _wait_terminal(
            orch, crashed_rec.id, timeout=4.0)
        await orch.shutdown()
        return final

    final = asyncio.run(second_life())
    logger.info("after restart: status=%s attempts=%s result=%s",
                final.status.value, final.attempts, final.result_json)
    assert final.status is TaskStatus.SUCCEEDED
    assert json.loads(final.result_json) == {"survived": True}
    starts = progress.read_text().count("start:")
    logger.info("handler side-effect starts=%s (1 aborted by crash + 1 committed)",
                starts)
    assert starts == 2  # 崩溃前 1 次未提交 + 恢复后 1 次提交；终态只提交一次


async def test_restart_preserves_terminal_and_continues_retry_wait(tmp_path):
    db = str(tmp_path / "term.db")
    config = make_config()

    orch = TaskOrchestrator(TaskRepository(db), config=config)
    await orch.start()
    done = (await orch.submit(SubmitRequest(
        task_type="echo", payload={"m": 1}, idempotency_key="done-k",
        timeout_seconds=2.0,
    ))).record
    waiting = (await orch.submit(SubmitRequest(
        task_type="flaky", payload={"fail_times": 2},
        idempotency_key="waiting-k", max_retries=3, timeout_seconds=2.0,
    ))).record
    await _wait_terminal(orch, done.id)
    deadline = time.time() + 3.0
    while time.time() < deadline:
        rec = await orch.get_task(waiting.id)
        if rec.status is TaskStatus.RETRY_WAIT:
            break
        await asyncio.sleep(0.01)
    rec = await orch.get_task(waiting.id)
    logger.info("before restart: done=%s waiting=%s attempts=%s",
                (await orch.get_task(done.id)).status.value,
                rec.status.value, rec.attempts)
    await orch.shutdown()

    orch2 = TaskOrchestrator(TaskRepository(db), config=config)
    await orch2.start()
    done2 = await orch2.get_task(done.id)
    assert done2.status is TaskStatus.SUCCEEDED and done2.attempts == 1
    final2 = await _wait_terminal(orch2, waiting.id, timeout=5.0)
    logger.info("after restart: waiting-k -> %s attempts=%s result=%s",
                final2.status.value, final2.attempts, final2.result_json)
    assert final2.status is TaskStatus.SUCCEEDED
    assert final2.attempts == 3
    await orch2.shutdown()
