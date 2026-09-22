"""幂等场景：顺序重复提交、20 路并发重复提交、载荷冲突、非法参数可区分。"""

import asyncio
import logging

import pytest
from httpx import ASGITransport, AsyncClient

from task_orchestrator.app import create_app
from task_orchestrator.models import SchedulerConfig

logger = logging.getLogger(__name__)


@pytest.fixture
async def client(tmp_path):
    app = create_app(
        str(tmp_path / "idem.db"),
        config=SchedulerConfig(retry_base_delay=0.02),
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://t") as ac:
        async with app.router.lifespan_context(app):
            yield ac


async def _terminal(client, task_id, timeout=3.0):
    import time
    deadline = time.time() + timeout
    while time.time() < deadline:
        body = (await client.get(f"/tasks/{task_id}")).json()
        if body["status"] in ("succeeded", "failed", "cancelled", "timed_out"):
            return body
        await asyncio.sleep(0.01)
    raise AssertionError("not terminal")


async def test_sequential_duplicate_returns_same_task(client):
    body = {"idempotency_key": "dup-1", "task_type": "echo",
            "payload": {"a": 1}}
    r1 = await client.post("/tasks", json=body)
    r2 = await client.post("/tasks", json=body)
    b1, b2 = r1.json(), r2.json()
    logger.info("input identical x2 -> id1=%s dedup1=%s | id2=%s dedup2=%s",
                b1["task_id"], b1["deduplicated"], b2["task_id"], b2["deduplicated"])
    assert r1.status_code == r2.status_code == 201
    assert b1["task_id"] == b2["task_id"]
    assert b1["deduplicated"] is False
    assert b2["deduplicated"] is True


async def test_concurrent_duplicates_execute_once(client):
    n = 20
    body = {"idempotency_key": "conc-1", "task_type": "echo",
            "payload": {"v": 1}, "max_retries": 0}

    async def submit():
        return await client.post("/tasks", json=body)

    responses = await asyncio.gather(*[submit() for _ in range(n)])
    ids = {r.json()["task_id"] for r in responses}
    dedups = sum(1 for r in responses if r.json()["deduplicated"])
    logger.info("input %s concurrent identical submissions -> unique ids=%s deduplicated=%s",
                n, len(ids), dedups)
    assert len(ids) == 1
    assert dedups == n - 1
    final = await _terminal(client, ids.pop())
    # 判定依据：并发重复提交不得产生重复执行
    assert final["status"] == "succeeded"
    assert final["attempts"] == 1


async def test_same_key_different_payload_conflict(client):
    await client.post("/tasks", json={
        "idempotency_key": "k", "task_type": "echo", "payload": {"v": 1}})
    r = await client.post("/tasks", json={
        "idempotency_key": "k", "task_type": "echo", "payload": {"v": 2}})
    logger.info("same key different payload -> status=%s error=%s",
                r.status_code, r.json()["error"])
    assert r.status_code == 409
    assert r.json()["error"] == "payload_conflict"


@pytest.mark.parametrize("body,reason", [
    ({"task_type": "echo", "payload": {}}, "missing key"),
    ({"idempotency_key": "k", "task_type": "", "payload": {}}, "empty type"),
    ({"idempotency_key": "k", "task_type": "nope", "payload": {}}, "unknown type"),
    ({"idempotency_key": "k", "task_type": "echo", "payload": {},
      "timeout_seconds": 0.0}, "bad timeout"),
    ({"idempotency_key": "k", "task_type": "echo", "payload": {},
      "priority": 99}, "bad priority"),
])
async def test_invalid_requests_distinguishable(client, body, reason):
    r = await client.post("/tasks", json=body)
    logger.info("invalid input (%s) -> status=%s error=%s msg=%s",
                reason, r.status_code, r.json().get("error"), r.json().get("message"))
    assert r.status_code == 422
    assert r.json()["error"] == "invalid_request"


async def test_key_route_distinguishes_missing(client):
    r = await client.get("/keys/does-not-exist")
    assert r.status_code == 404 and r.json()["error"] == "task_not_found"
