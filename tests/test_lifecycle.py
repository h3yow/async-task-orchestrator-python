"""任务生命周期：重试/终态/取消/取消与完成竞态。"""

from __future__ import annotations

import asyncio
import logging

from app.handlers import CancellationToken
from conftest import log_case

logger = logging.getLogger("test.lifecycle")


async def submit(client, key, task_type, payload, **extra):
    body = {"idempotency_key": key, "task_type": task_type, "payload": payload}
    body.update(extra)
    r = await client.post("/tasks/submit", json=body)
    assert r.status_code == 201, r.text
    return r.json()["task_id"]


async def test_retryable_retries_with_backoff_then_succeeds(env):
    client = env["client"]
    log_case("可重试后成功", fail_times=2, max_attempts=3)
    tid = await submit(client, "life-retry", "flaky",
                       {"fail_times": 2}, max_attempts=3)
    await env["executor"].wait_idle(5)
    got = (await client.get(f"/tasks/{tid}")).json()
    logger.info("判定依据：终态=%s attempts=%d result=%s",
                got["status"], got["attempts"], got["result"])
    assert got["status"] == "succeeded"
    assert got["attempts"] == 3
    assert got["result"]["attempt"] == 3


async def test_retry_exhaustion_distinguishes_error_codes(env):
    """重试耗尽（retryable）与不可重试失败（fatal）必须可区分。"""
    client = env["client"]
    log_case("重试耗尽", fail_times=9, max_attempts=2)
    tid = await submit(client, "life-exhaust", "flaky",
                       {"fail_times": 9}, max_attempts=2)
    await env["executor"].wait_idle(5)
    got = (await client.get(f"/tasks/{tid}")).json()
    logger.info("判定依据：终态=%s attempts=%d error_code=%s",
                got["status"], got["attempts"], got["error_code"])
    assert got["status"] == "failed"
    assert got["attempts"] == 2
    assert got["error_code"] == "retryable"

    log_case("不可重试直接终态", task_type="always_fail", max_attempts=3)
    tid2 = await submit(client, "life-fatal", "always_fail", {}, max_attempts=3)
    await env["executor"].wait_idle(5)
    got2 = (await client.get(f"/tasks/{tid2}")).json()
    logger.info("判定依据：终态=%s attempts=%d error_code=%s（不可重试必须只执行 1 次）",
                got2["status"], got2["attempts"], got2["error_code"])
    assert got2["status"] == "failed"
    assert got2["attempts"] == 1
    assert got2["error_code"] == "fatal"


async def test_terminal_state_is_not_transitioned_again(env):
    """已失败任务再次取消/再次执行均不得改变终态。"""
    client = env["client"]
    log_case("终态不可重复转换", task_type="always_fail")
    tid = await submit(client, "life-terminal", "always_fail", {})
    await env["executor"].wait_idle(5)
    before = (await client.get(f"/tasks/{tid}")).json()
    assert before["status"] == "failed"
    r = await client.post(f"/tasks/{tid}/cancel")
    after = r.json()
    logger.info("判定依据：取消前=%s 取消后=%s error_code=%s",
                before["status"], after["status"], after["error_code"])
    assert after["status"] == "failed", "终态任务的取消请求必须幂等且不改状态"
    assert after["error_code"] == "fatal"
    await asyncio.sleep(0.2)
    again = (await client.get(f"/tasks/{tid}")).json()
    assert again["attempts"] == 1, "终态任务不得被再次调度执行"


async def test_cancel_pending_never_runs(env):
    """排队中的 pending 任务被取消：直接 cancelled，执行计数保持 0。"""
    client = env["client"]
    log_case("取消排队任务", task_type="echo", max_attempts=1)
    tid = await submit(client, "life-cancel-pending", "echo", {"value": 1})
    r = await client.post(f"/tasks/{tid}/cancel")
    logger.info("判定依据：取消响应 status=%s", r.json()["status"])
    assert r.json()["status"] == "cancelled"
    await asyncio.sleep(0.3)
    got = (await client.get(f"/tasks/{tid}")).json()
    assert got["status"] == "cancelled"
    assert got["attempts"] == 0, "未开始的任务不得产生执行次数"


async def test_cancel_running_takes_effect_promptly(env):
    """运行中的慢任务收到取消后，在检查点上及时进入 cancelled。"""
    client = env["client"]
    log_case("取消运行中", duration=5.0)
    tid = await submit(client, "life-cancel-running", "slow",
                       {"duration": 5.0}, timeout_seconds=10)
    await asyncio.sleep(0.15)
    running = (await client.get(f"/tasks/{tid}")).json()
    assert running["status"] == "running"
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    r = await client.post(f"/tasks/{tid}/cancel")
    assert r.json()["cancel_requested"] is True
    for _ in range(50):
        await asyncio.sleep(0.02)
        got = (await client.get(f"/tasks/{tid}")).json()
        if got["status"] == "cancelled":
            elapsed = loop.time() - t0
            break
    else:
        got = (await client.get(f"/tasks/{tid}")).json()
        elapsed = -1
    logger.info("判定依据：终态=%s 取消生效耗时=%.3fs attempts=%d",
                got["status"], elapsed, got["attempts"])
    assert got["status"] == "cancelled"
    assert elapsed < 0.5, "取消必须在运行中及时生效（检查点粒度 0.02s）"
    assert got["attempts"] == 1


async def test_cancel_and_completion_race(env):
    """处理器在“即将成功”时收到取消：终态必须确定（二者只赢一个），
    不允许出现 succeeded+cancel_requested 的矛盾状态。"""
    client = env["client"]
    release = asyncio.Event()

    async def race_handler(payload: dict, token: CancellationToken) -> dict:
        await release.wait()
        # 不检查取消直接返回，模拟“成功落库与取消请求”竞争
        return {"done": True}

    env["registry"].register("race", race_handler)
    log_case("取消与完成竞态", task_type="race")
    tid = await submit(client, "life-race", "race", {})
    await asyncio.sleep(0.1)
    cr = await client.post(f"/tasks/{tid}/cancel")
    release.set()
    logger.info("判定依据：取消时任务状态=%s", cr.json()["status"])
    await asyncio.sleep(0.2)
    got = (await client.get(f"/tasks/{tid}")).json()
    logger.info("判定依据：竞态终态=%s cancel_requested=%s result=%s",
                got["status"], got["cancel_requested"], got["result"])
    assert got["status"] in {"cancelled", "succeeded"}
    if got["status"] == "succeeded":
        assert got["cancel_requested"] is False
    else:
        assert got["result"] is None
    # 再多等确认终态稳定不翻转
    await asyncio.sleep(0.2)
    stable = (await client.get(f"/tasks/{tid}")).json()
    assert stable["status"] == got["status"]
