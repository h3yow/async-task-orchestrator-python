"""调度场景：作用域并发上限、全局上限、优先级取用、老化防饥饿、超时转终态。"""

import asyncio
import logging
import time

import pytest

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
        global_concurrency=3,
        default_scope_concurrency=2,
        aging_rate=50.0,          # 测试加速老化：等 0.2s 即 +10 分
        retry_base_delay=0.02,
    )
    o = TaskOrchestrator(
        TaskRepository(str(tmp_path / "sched.db")), config=config
    )
    await o.start()
    yield o
    await o.shutdown()


async def _submit(orch, key, scope, priority=Priority.NORMAL,
                  task_type="sleep", payload=None, timeout=5.0, max_retries=0):
    req = SubmitRequest(
        task_type=task_type, payload=payload or {"seconds": 0.2},
        idempotency_key=key, priority=priority, scope=scope,
        timeout_seconds=timeout, max_retries=max_retries,
    )
    return (await orch.submit(req)).record


async def _wait_terminal(orch, task_id, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        rec = await orch.get_task(task_id)
        if rec.status in (TaskStatus.SUCCEEDED, TaskStatus.FAILED,
                          TaskStatus.CANCELLED, TaskStatus.TIMED_OUT):
            return rec
        await asyncio.sleep(0.005)
    raise AssertionError(f"{task_id} not terminal")


async def _running_ids(orch, records):
    ids = set()
    for r in records:
        cur = await orch.get_task(r.id)
        if cur.status is TaskStatus.RUNNING:
            ids.add(r.id)
    return ids


async def test_scope_concurrency_limit_never_exceeded(orch):
    records = [await _submit(orch, f"s-{i}", "sc-a") for i in range(5)]
    observations = []
    for _ in range(60):
        observations.append(len(await _running_ids(orch, records)))
        await asyncio.sleep(0.01)
    logger.info("scope concurrency samples=%s -> max=%s (limit=2)",
                observations, max(observations))
    assert max(observations) <= 2
    finals = await asyncio.gather(*[_wait_terminal(orch, r.id) for r in records])
    assert all(f.status is TaskStatus.SUCCEEDED for f in finals)


async def test_global_concurrency_limit(orch):
    records = [await _submit(orch, f"g-a-{i}", "sc-b") for i in range(3)]
    records += [await _submit(orch, f"g-b-{i}", "sc-c") for i in range(3)]
    peak = 0
    for _ in range(60):
        peak = max(peak, len(await _running_ids(orch, records)))
        await asyncio.sleep(0.005)
    logger.info("global concurrency peak=%s (limit=3)", peak)
    assert peak <= 3
    finals = await asyncio.gather(*[_wait_terminal(orch, r.id) for r in records])
    assert len(finals) == 6


async def test_priority_order_within_scope(orch):
    # 自定义处理器记录每个任务"实际开始执行"的时刻
    start_times: dict[str, float] = {}

    async def timed_sleep(payload, ctx):
        start_times[ctx.record.idempotency_key] = time.time()
        await ctx.sleep(float(payload["seconds"]))
        return {"started": start_times[ctx.record.idempotency_key]}

    orch.registry.register("timed_sleep", timed_sleep)

    await _submit(orch, "b1", "sc-p", task_type="timed_sleep",
                  payload={"seconds": 0.3})
    await _submit(orch, "b2", "sc-p", task_type="timed_sleep",
                  payload={"seconds": 0.3})
    await asyncio.sleep(0.05)  # 确保两个 blocker 已占满作用域名额
    await _submit(orch, "low", "sc-p", Priority.LOW,
                  task_type="timed_sleep", payload={"seconds": 0.05})
    await _submit(orch, "high", "sc-p", Priority.HIGH,
                  task_type="timed_sleep", payload={"seconds": 0.05})
    # 等待全部完成
    for key in ("b1", "b2", "low", "high"):
        rec = await orch.get_by_key(key)
        await _wait_terminal(orch, rec.id)
    logger.info("actual start times low=%.4f high=%.4f (expect high earlier)",
                start_times["low"], start_times["high"])
    assert start_times["high"] < start_times["low"]


async def test_aging_prevents_starvation_under_continuous_high_priority(orch):
    await _submit(orch, "ab1", "sc-age", payload={"seconds": 0.2})
    await _submit(orch, "ab2", "sc-age", payload={"seconds": 0.2})
    low = await _submit(orch, "aged-low", "sc-age", Priority.LOW,
                        task_type="echo", payload={"message": "low"})
    highs = []
    for i in range(6):
        await asyncio.sleep(0.03)
        highs.append(await _submit(
            orch, f"high-{i}", "sc-age", Priority.HIGH,
            task_type="sleep", payload={"seconds": 0.1}))
    low_final = await _wait_terminal(orch, low.id, timeout=3.0)
    logger.info("aged low task final=%s attempts=%s (aging_rate=50)",
                low_final.status.value, low_final.attempts)
    assert low_final.status is TaskStatus.SUCCEEDED


async def test_timeout_becomes_terminal_and_releases_slot(orch):
    rec = await _submit(orch, "to", "sc-to", task_type="sleep",
                        payload={"seconds": 5.0}, timeout=0.1)
    start = time.time()
    final = await _wait_terminal(orch, rec.id, timeout=2.0)
    logger.info("timeout input sleep=5s timeout=0.1 -> final=%s in %.2fs error=%s",
                final.status.value, time.time() - start, final.last_error)
    assert final.status is TaskStatus.TIMED_OUT
    follower = await _submit(orch, "to-follower", "sc-to",
                             task_type="echo", payload={"message": "ok"})
    f2 = await _wait_terminal(orch, follower.id, timeout=2.0)
    assert f2.status is TaskStatus.SUCCEEDED
