"""进程重启恢复测试：用同一个 SQLite 文件构造“新进程”的全新服务栈。

恢复语义：
- 上次进程异常退出时遗留的 running 任务在新进程启动时重新入队，
  不会丢失；claims 是条件 UPDATE，不会与已完成任务重复执行；
- attempts 保留（崩溃那一次已计数），体现 at-least-once；
- 退避中的 pending 任务继续按原 scheduled_at 等待；
- 已请求取消但仍在 running 的任务，重启后直接 cancelled。
"""

from __future__ import annotations

import asyncio
import logging

import httpx

from conftest import _make_stack, log_case

logger = logging.getLogger("test.recovery")


async def test_running_task_recovered_after_restart(tmp_path):
    """进程在任务 running 期间被“杀死”：新进程启动后任务继续执行且只成功一次。"""
    store1, registry1, executor1, service1, app1 = _make_stack(
        tmp_path, base_backoff=0.05)

    hang = asyncio.Event()

    async def block_then_ok(payload, token):
        # 处理器在“执行中”挂起，由测试在模拟崩溃后才放行，
        # 从而精确制造“进程死亡时任务仍为 running”的现场。
        await hang.wait()
        return {"ok": True, "attempt": payload["_attempt"]}

    registry1.register("block_ok", block_then_ok)
    db = str(tmp_path / "tasks.db")

    await executor1.start()
    transport1 = httpx.ASGITransport(app=app1)
    async with httpx.AsyncClient(transport=transport1,
                                 base_url="http://old") as client:
        log_case("重启恢复-running", key="recover-running")
        r = await client.post("/tasks/submit", json={
            "idempotency_key": "recover-running",
            "task_type": "block_ok", "payload": {}, "max_attempts": 3})
        tid = r.json()["task_id"]
        await asyncio.sleep(0.12)
        mid = (await client.get(f"/tasks/{tid}")).json()
        assert mid["status"] == "running" and mid["attempts"] == 1
        logger.info("判定依据：旧进程被杀前状态=%s attempts=%d",
                    mid["status"], mid["attempts"])
    # 模拟进程崩溃：取消调度循环、直接关闭 DB 连接，挂起中的工作协程被整个
    # 事件循环抛弃，不执行任何优雅落库
    executor1._scheduler.cancel()  # noqa: SLF001 - 模拟强杀
    await asyncio.sleep(0.02)
    store1.close()

    # --- 新进程：全新对象图、同一个 db 文件 ---
    store2, registry2, executor2, service2, app2 = _make_stack(
        tmp_path, base_backoff=0.05)

    async def block_ok_again(payload, token):
        return {"ok": True, "attempt": payload["_attempt"]}

    registry2.register("block_ok", block_ok_again)
    recovered = await store2.recover_on_startup()
    logger.info("判定依据：新进程启动恢复条数=%d", recovered)
    assert recovered == 1
    rec = await store2.get_task(tid)
    assert rec.status.value == "pending" and rec.attempts == 1, \
        "崩溃前的尝试计数必须保留，任务重新入队"

    await executor2.start()
    try:
        await executor2.wait_idle(5)
        got = await store2.get_task(tid)
        logger.info("判定依据：恢复后终态=%s attempts=%d result=%s",
                    got.status.value, got.attempts, got.result)
        assert got.status.value == "succeeded"
        assert got.attempts == 2, "崩溃时计 1 次 + 恢复后执行 1 次，不重复不丢失"
        assert got.result == {"ok": True, "attempt": 2}
    finally:
        await executor2.stop()
        store2.close()


async def test_backoff_pending_and_terminal_survive_restart(tmp_path):
    """退避等待中的 pending 与已终态任务，重启后语义不变。"""
    store1, registry1, executor1, _, app1 = _make_stack(
        tmp_path, base_backoff=10.0)  # 超长退避，重启后仍在等待
    transport1 = httpx.ASGITransport(app=app1)

    await executor1.start()
    async with httpx.AsyncClient(transport=transport1,
                                 base_url="http://old") as client:
        log_case("重启恢复-退避与终态", flaky_key="recover-backoff",
                 fatal_key="recover-fatal", backoff=10.0)
        r1 = await client.post("/tasks/submit", json={
            "idempotency_key": "recover-backoff", "task_type": "flaky",
            "payload": {"fail_times": 1}, "max_attempts": 3})
        t_backoff = r1.json()["task_id"]
        r2 = await client.post("/tasks/submit", json={
            "idempotency_key": "recover-fatal", "task_type": "always_fail",
            "payload": {}})
        t_fatal = r2.json()["task_id"]
        await asyncio.sleep(0.2)
        b = await store1.get_task(t_backoff)
        f = await store1.get_task(t_fatal)
        assert b.status.value == "pending" and b.attempts == 1
        assert f.status.value == "failed"
        scheduled_before = b.scheduled_at
    for w in list(executor1._workers):  # noqa: SLF001 - 模拟强杀
        w.cancel()
    executor1._scheduler.cancel()  # noqa: SLF001
    await asyncio.sleep(0.05)
    store1.close()

    store2, registry2, executor2, _, app2 = _make_stack(
        tmp_path, base_backoff=10.0)
    await executor2.start()
    try:
        recovered = await store2.recover_on_startup()
        assert recovered == 0, "没有 running 残留时恢复条数应为 0"
        b = await store2.get_task(t_backoff)
        f = await store2.get_task(t_fatal)
        logger.info("判定依据：退避任务 status=%s scheduled_at 保持=%s；"
                    "终态任务 status=%s error_code=%s",
                    b.status.value, b.scheduled_at == scheduled_before,
                    f.status.value, f.error_code)
        assert b.status.value == "pending" and b.attempts == 1
        assert b.scheduled_at == scheduled_before, "退避时刻不得被重置"
        assert f.status.value == "failed" and f.error_code == "fatal"
    finally:
        await executor2.stop()
        store2.close()


async def test_cancel_request_survives_restart(tmp_path):
    """running 任务已收到取消请求后进程崩溃：重启直接 cancelled，不再执行。"""
    store1, registry1, executor1, _, app1 = _make_stack(tmp_path)
    hang = asyncio.Event()

    async def forever(payload, token):
        # 阻塞在非取消检查点上：cancel_requested 已落库但处理器尚未感知，
        # 此刻进程死亡，用于验证取消请求的持久化语义。
        await hang.wait()
        return {}

    registry1.register("forever", forever)
    await executor1.start()
    transport1 = httpx.ASGITransport(app=app1)
    async with httpx.AsyncClient(transport=transport1,
                                 base_url="http://old") as client:
        log_case("重启恢复-取消不丢", key="recover-cancel")
        r = await client.post("/tasks/submit", json={
            "idempotency_key": "recover-cancel", "task_type": "forever",
            "payload": {}})
        tid = r.json()["task_id"]
        await asyncio.sleep(0.12)
        # 直接经持久层置取消标志，绕过执行器的内存令牌
        rec = await store1.request_cancel(tid)
        assert rec.status.value == "running" and rec.cancel_requested
    # 模拟进程崩溃：仅停止调度循环（不派新活），随后直接关闭 DB 连接，
    # 正在 await hang 事件的工作协程被整个事件循环抛弃，没有任何优雅落库。
    executor1._scheduler.cancel()  # noqa: SLF001
    await asyncio.sleep(0.02)
    store1.close()

    store2, registry2, executor2, _, _ = _make_stack(tmp_path)
    recovered = await store2.recover_on_startup()
    got = await store2.get_task(tid)
    logger.info("判定依据：恢复条数=%d 终态=%s error_code=%s attempts=%d",
                recovered, got.status.value, got.error_code, got.attempts)
    assert recovered == 1
    assert got.status.value == "cancelled"
    assert got.error_code == "cancelled"
    assert got.attempts == 1
    store2.close()
