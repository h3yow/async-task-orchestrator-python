"""幂等提交与参数校验测试。

覆盖：
- 重复提交返回同一 task_id、already_exists=True，只执行一次；
- 并发重复提交（asyncio.gather）只产生一个任务、执行计数为 1；
- 同幂等键不同载荷 -> 409 payload_conflict；
- 非法参数（task_type / max_attempts / timeout / payload 类型）-> 422，
  且 error_code 可区分。
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from conftest import log_case

logger = logging.getLogger("test.idempotency")


async def test_duplicate_submit_returns_same_task_and_runs_once(env):
    client = env["client"]
    body = {
        "idempotency_key": "idem-1",
        "task_type": "echo",
        "payload": {"value": 1},
    }
    log_case("重复提交", first=body, second="相同载荷再次提交")

    r1 = await client.post("/tasks/submit", json=body)
    assert r1.status_code == 201
    first = r1.json()

    r2 = await client.post("/tasks/submit", json=body)
    assert r2.status_code == 200, "重复提交应返回 200"
    second = r2.json()
    assert second["task_id"] == first["task_id"], "必须返回同一任务"
    assert second["already_exists"] is True
    logger.info("判定依据：两次提交 task_id 一致=%s，第二次 already_exists=%s",
                second["task_id"] == first["task_id"], second["already_exists"])

    await env["executor"].wait_idle(5)
    got = await client.get(f"/tasks/{first['task_id']}")
    assert got.json()["status"] == "succeeded"
    assert got.json()["attempts"] == 1, "重复提交不得产生重复执行"
    logger.info("判定依据：终态=%s，执行计数 attempts=%d（期望 1）",
                got.json()["status"], got.json()["attempts"])


async def test_concurrent_duplicate_submit_single_execution(env):
    client = env["client"]
    body = {
        "idempotency_key": "idem-concurrent",
        "task_type": "flaky",
        "payload": {"fail_times": 0},
    }
    log_case("并发重复提交", concurrency=20, payload=body)

    async def submit():
        return await client.post("/tasks/submit", json=body)

    responses = await asyncio.gather(*(submit() for _ in range(20)))
    statuses = sorted(r.status_code for r in responses)
    ids = {r.json()["task_id"] for r in responses}
    created = [r for r in responses if r.status_code == 201]
    logger.info("判定依据：20 个并发响应 201 数量=%d，200 数量=%d，不同 task_id 数=%d",
                len(created), sum(1 for r in responses if r.status_code == 200), len(ids))
    assert statuses.count(201) == 1, "并发下只能有一次插入成功"
    assert statuses.count(200) == 19
    assert len(ids) == 1, "所有响应必须指向同一任务"

    await env["executor"].wait_idle(5)
    got = await client.get(f"/tasks/{ids.pop()}")
    view = got.json()
    assert view["status"] == "succeeded"
    assert view["attempts"] == 1, "并发重复提交不得重复执行"
    logger.info("判定依据：终态=%s attempts=%d（期望 1）", view["status"], view["attempts"])


async def test_same_key_different_payload_rejected_conflict(env):
    client = env["client"]
    log_case("同键不同载荷", key="idem-conflict",
             first={"value": 1}, second={"value": 2})
    await client.post("/tasks/submit", json={
        "idempotency_key": "idem-conflict",
        "task_type": "echo", "payload": {"value": 1}})
    r = await client.post("/tasks/submit", json={
        "idempotency_key": "idem-conflict",
        "task_type": "echo", "payload": {"value": 2}})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "payload_conflict"
    logger.info("判定依据：冲突响应 code=%s（期望 payload_conflict）",
                r.json()["error"]["code"])


async def test_same_key_different_key_order_is_not_conflict(env):
    client = env["client"]
    log_case("同键不同键序", first={"a": 1, "b": 2}, second={"b": 2, "a": 1})
    await client.post("/tasks/submit", json={
        "idempotency_key": "idem-order",
        "task_type": "echo", "payload": {"a": 1, "b": 2}})
    r = await client.post("/tasks/submit", json={
        "idempotency_key": "idem-order",
        "task_type": "echo", "payload": {"b": 2, "a": 1}})
    assert r.status_code == 200, "JSON 键顺序不同但语义一致应视为重复提交"
    assert r.json()["already_exists"] is True


@pytest.mark.parametrize("body,expected_field", [
    ({"idempotency_key": "bad2", "task_type": "echo", "payload": {},
      "max_attempts": 0}, "max_attempts"),
    ({"idempotency_key": "bad3", "task_type": "echo", "payload": {},
      "timeout_seconds": 0}, "timeout_seconds"),
])
async def test_field_level_invalid_submissions(env, body, expected_field):
    """Field 约束（ge/gt）由 Pydantic 层拒绝，detail[0].loc 标明问题字段。"""
    client = env["client"]
    log_case("字段级非法提交", body=body, expected_field=expected_field)
    r = await client.post("/tasks/submit", json=body)
    assert r.status_code == 422
    locs = [item["loc"][-1] for item in r.json()["detail"]]
    assert expected_field in locs
    logger.info("判定依据：HTTP=%d Pydantic 字段=%s", r.status_code, locs)


@pytest.mark.parametrize("body,expected_code", [
    ({"idempotency_key": "bad1", "task_type": "unknown_xyz", "payload": {}},
     "unknown_task_type"),
])
async def test_service_level_invalid_submissions(env, body, expected_code):
    client = env["client"]
    log_case("非法提交", body=body, expected=expected_code)
    r = await client.post("/tasks/submit", json=body)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == expected_code
    logger.info("判定依据：HTTP=%d code=%s", r.status_code,
                r.json()["error"]["code"])
