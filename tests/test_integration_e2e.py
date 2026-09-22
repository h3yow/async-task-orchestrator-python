"""端到端主链路：成功执行、重试后成功、不可重试失败、HTTP 层健康检查。"""

import logging

import pytest
from httpx import ASGITransport, AsyncClient

from task_orchestrator.app import create_app
from task_orchestrator.models import SchedulerConfig, TaskStatus

logger = logging.getLogger(__name__)


@pytest.fixture
async def client(tmp_path):
    config = SchedulerConfig(
        global_concurrency=2, default_scope_concurrency=2,
        retry_base_delay=0.02, retry_cap_delay=0.1,
    )
    app = create_app(str(tmp_path / "e2e.db"), config=config)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://t") as ac:
        async with app.router.lifespan_context(app):
            yield ac


async def _wait_terminal(client, task_id, timeout=3.0):
    import time
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = await client.get(f"/tasks/{task_id}")
        body = r.json()
        logger.info("poll task=%s -> status=%s attempts=%s error=%s",
                    task_id, body["status"], body["attempts"], body["last_error"])
        if body["status"] in ("succeeded", "failed", "cancelled", "timed_out"):
            return body
        import asyncio
        await asyncio.sleep(0.01)
    raise AssertionError("task did not reach terminal state")


async def test_health(client):
    r = await client.get("/health")
    assert r.status_code == 200 and r.json() == {"status": "ok"}


async def test_echo_succeeds(client):
    r = await client.post("/tasks", json={
        "idempotency_key": "k-echo", "task_type": "echo",
        "payload": {"message": "hi"}, "timeout_seconds": 2.0,
    })
    assert r.status_code == 201, r.text
    body = r.json()
    logger.info("submit input=%s -> task_id=%s deduplicated=%s",
                "k-echo", body["task_id"], body["deduplicated"])
    final = await _wait_terminal(client, body["task_id"])
    assert final["status"] == "succeeded"
    assert final["attempts"] == 1
    assert final["result"] == {"echo": "hi"}


async def test_flaky_retries_then_succeeds(client):
    r = await client.post("/tasks", json={
        "idempotency_key": "k-flaky", "task_type": "flaky",
        "payload": {"fail_times": 2}, "max_retries": 3, "timeout_seconds": 2.0,
    })
    task_id = r.json()["task_id"]
    logger.info("flaky submit fail_times=2 max_retries=3 task=%s", task_id)
    final = await _wait_terminal(client, task_id)
    # 判定依据：前 2 次可重试失败 -> attempts=3 成功
    assert final["status"] == "succeeded"
    assert final["attempts"] == 3
    assert final["result"]["ok"] is True


async def test_non_retryable_fails_immediately(client):
    r = await client.post("/tasks", json={
        "idempotency_key": "k-fail", "task_type": "fail",
        "payload": {"message": "boom", "retryable": False},
        "max_retries": 5, "timeout_seconds": 2.0,
    })
    final = await _wait_terminal(client, r.json()["task_id"])
    logger.info("non-retryable final=%s attempts=%s error=%s",
                final["status"], final["attempts"], final["last_error"])
    assert final["status"] == "failed"
    assert final["attempts"] == 1
    assert "boom" in final["last_error"]


async def test_unknown_type_rejected_with_distinct_reason(client):
    r = await client.post("/tasks", json={
        "idempotency_key": "k-x", "task_type": "nope", "payload": {},
    })
    assert r.status_code == 422
    assert r.json()["error"] == "invalid_request"
