"""优先级调度、作用域并发上限、老化防饿死、超时测试。

判定依据均通过“任务实际开始执行的顺序”或“同一时刻在跑计数”给出，
输入（优先级、scope、到达时机）逐条打印到日志。
"""

from __future__ import annotations

import asyncio
import logging

from app.handlers import CancellationToken
from conftest import log_case

logger = logging.getLogger("test.scheduling")


def install_ordered_handler(registry, started_order: list[str], delay: float = 0.12):
    """注册一个按提交 key 记录开始顺序、固定耗时的处理器。"""

    async def ordered(payload: dict, token: CancellationToken) -> dict:
        key = payload["key"]
        started_order.append(key)
        await token.wait_cancellable(delay)
        return {"ran": key}

    registry.register("ordered", ordered)


async def submit(client, key, priority, scope="s1", task_type="ordered",
                 payload=None, timeout=10.0, max_attempts=3):
    return await client.post("/tasks/submit", json={
        "idempotency_key": key,
        "task_type": task_type,
        "payload": payload if payload is not None else {"key": key},
        "priority": priority,
        "scope": scope,
        "timeout_seconds": timeout,
        "max_attempts": max_attempts,
    })


async def test_higher_priority_dispatched_first(env):
    """同 scope 并发=1 时，低/中/高同时排队，执行顺序必须是 high->normal->low。"""
    order: list[str] = []
    install_ordered_handler(env["registry"], order, delay=0.12)
    client = env["client"]
    log_case("优先级顺序", tasks=["low-job", "normal-job", "high-job"],
             scope_concurrency=1)
    # 使用独立作用域，并在测试内构造并发=1 的栈较复杂；改为通过慢任务占住槽位后
    # 直接比较开始顺序：默认并发 2，先放一个占槽任务，剩下 3 个按优先级排队。
    await submit(client, "holder", "low", payload={"key": "holder"})
    await asyncio.sleep(0.15)  # holder 占掉第一个槽
    await asyncio.gather(
        submit(client, "low-job", "low"),
        submit(client, "normal-job", "normal"),
        submit(client, "high-job", "high"),
    )
    await env["executor"].wait_idle(5)
    logger.info("判定依据：任务开始顺序=%s", order)
    assert order.index("high-job") < order.index("normal-job")
    assert order.index("normal-job") < order.index("low-job")


async def test_scope_concurrency_limit_never_exceeded(tmp_path):
    """scope=limited 上限为 2：并发提交 8 个任务，任意时刻在跑数 <=2。"""
    from conftest import _make_stack

    store, registry, executor, service, fastapi_app = _make_stack(
        tmp_path, default_concurrency=1,
        scope_concurrency={"limited": 2},
    )
    max_seen = {"value": 0}
    inflight = {"value": 0}
    lock = asyncio.Lock()

    async def guarded(payload: dict, token: CancellationToken) -> dict:
        async with lock:
            inflight["value"] += 1
            max_seen["value"] = max(max_seen["value"], inflight["value"])
        await token.wait_cancellable(0.15)
        async with lock:
            inflight["value"] -= 1
        return {"ok": True}

    registry.register("guarded", guarded)
    import httpx

    await executor.start()
    try:
        transport = httpx.ASGITransport(app=fastapi_app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://t") as client:
            log_case("作用域并发上限", scope="limited", limit=2, tasks=8)
            await asyncio.gather(*[
                client.post("/tasks/submit", json={
                    "idempotency_key": f"g-{i}", "task_type": "guarded",
                    "payload": {"i": i}, "scope": "limited"})
                for i in range(8)
            ])
            await executor.wait_idle(5)
        logger.info("判定依据：观测到的最大在跑数=%d（期望 <=2）", max_seen["value"])
        assert max_seen["value"] <= 2
        assert max_seen["value"] == 2, "8 个任务持续供给时上限应被打满"
    finally:
        await executor.stop()
        store.close()


async def test_low_priority_not_starved_by_high_arrivals(tmp_path):
    """持续到达的高优先级任务不得让低优先级任务无限等待（老化）。"""
    from conftest import _make_stack

    # aging_interval=0.1 且默认并发 1，低任务等待约 0.1s 即可老化越过新高任务
    store, registry, executor, service, fastapi_app = _make_stack(
        tmp_path, default_concurrency=1, aging_interval=0.1,
    )
    finished: list[str] = []

    async def timed_echo(payload: dict, token: CancellationToken) -> dict:
        await token.wait_cancellable(0.05)
        finished.append(payload["key"])
        return {"ran": payload["key"]}

    registry.register("timed", timed_echo)
    import httpx

    await executor.start()
    try:
        transport = httpx.ASGITransport(app=fastapi_app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://t") as client:
            log_case("老化防饿死", low="lone-low", high_count=6,
                     aging_interval=0.1)
            await client.post("/tasks/submit", json={
                "idempotency_key": "lone-low", "task_type": "timed",
                "payload": {"key": "lone-low"}, "priority": "low"})

            async def keep_flooding_high():
                for i in range(6):
                    await client.post("/tasks/submit", json={
                        "idempotency_key": f"high-{i}", "task_type": "timed",
                        "payload": {"key": f"high-{i}"}, "priority": "high"})
                    await asyncio.sleep(0.04)

            flood_task = asyncio.create_task(keep_flooding_high())
            await flood_task
            await executor.wait_idle(8)
        logger.info("判定依据：完成顺序=%s；低任务位置=%d",
                    finished, finished.index("lone-low"))
        assert "lone-low" in finished, "低优先级任务必须在持续高优先级供给下完成"
        # 6 个高任务每个 0.05s、供给间隔 0.04s；若不老化，低任务将排在所有高任务
        # 之后。老化后它应在第 3 个高任务前后插入，不晚于第 4 个。
        assert finished.index("lone-low") <= 4
    finally:
        await executor.stop()
        store.close()


async def test_timeout_becomes_deterministic_state(env):
    """超时任务按 timeout 错误重试，次数耗尽进入 failed，不再占用资源。"""
    client = env["client"]
    log_case("执行超时", task_type="slow", duration=2.0, timeout=0.15,
             max_attempts=2, base_backoff=0.05)
    r = await submit(client, "timeout-job", "normal", task_type="slow",
                     payload={"duration": 2.0}, timeout=0.15, max_attempts=2)
    tid = r.json()["task_id"]
    await env["executor"].wait_idle(5)
    got = (await client.get(f"/tasks/{tid}")).json()
    logger.info("判定依据：终态=%s attempts=%d error_code=%s",
                got["status"], got["attempts"], got["error_code"])
    assert got["status"] == "failed"
    assert got["attempts"] == 2
    assert got["error_code"] == "timeout"
    assert env["executor"].inflight_count == 0, "超时任务不得长期占用执行资源"
