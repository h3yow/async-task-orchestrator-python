"""FastAPI 装配：生命周期管理、HTTP 接口与业务错误码映射。"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .errors import (
    InvalidRequest,
    OrchestratorError,
    TaskNotFound,
)
from .models import Priority, SchedulerConfig, SubmitRequest
from .orchestrator import TaskOrchestrator
from .repository import TaskRepository


class SubmitBody(BaseModel):
    idempotency_key: str = Field(..., min_length=1, max_length=200)
    task_type: str = Field(..., min_length=1)
    payload: dict[str, Any]
    priority: int = 5
    scope: str = "default"
    timeout_seconds: float = 5.0
    max_retries: int = 0


def _record_dict(rec, deduplicated: bool | None = None) -> dict:
    data = {
        "task_id": rec.id,
        "idempotency_key": rec.idempotency_key,
        "task_type": rec.task_type,
        "status": rec.status.value,
        "priority": rec.priority,
        "scope": rec.scope,
        "attempts": rec.attempts,
        "max_retries": rec.max_retries,
        "cancel_requested": rec.cancel_requested,
        "last_error": rec.last_error,
        "result": json.loads(rec.result_json) if rec.result_json else None,
        "created_at": rec.created_at,
        "updated_at": rec.updated_at,
    }
    if deduplicated is not None:
        data["deduplicated"] = deduplicated
    return data


def create_app(
    db_path: str = "data/tasks.db",
    config: SchedulerConfig | None = None,
) -> FastAPI:
    orchestrator = TaskOrchestrator(
        TaskRepository(db_path), config=config or SchedulerConfig()
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await orchestrator.start()
        try:
            yield
        finally:
            await orchestrator.shutdown()

    app = FastAPI(title="async-task-orchestrator", lifespan=lifespan)
    app.state.orchestrator = orchestrator

    def fail(exc: OrchestratorError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.http_status,
            content={"error": exc.error_code, "message": str(exc)},
        )

    @app.exception_handler(RequestValidationError)
    async def on_validation_error(request, exc: RequestValidationError):
        # 框架层参数非法统一映射为可区分的 invalid_request
        return JSONResponse(
            status_code=422,
            content={
                "error": "invalid_request",
                "message": "request validation failed",
                "details": exc.errors(),
            },
        )

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.post("/tasks", status_code=201)
    async def submit(body: SubmitBody):
        try:
            priority = Priority(body.priority)
        except ValueError as exc:
            return fail(InvalidRequest(f"invalid priority: {body.priority}"))
        req = SubmitRequest(
            task_type=body.task_type,
            payload=body.payload,
            idempotency_key=body.idempotency_key,
            priority=priority,
            scope=body.scope,
            timeout_seconds=body.timeout_seconds,
            max_retries=body.max_retries,
        )
        try:
            result = await orchestrator.submit(req)
        except OrchestratorError as exc:
            return fail(exc)
        return JSONResponse(
            status_code=201,
            content=_record_dict(result.record, deduplicated=result.deduplicated),
        )

    @app.get("/tasks/{task_id}")
    async def get_task(task_id: str):
        rec = await orchestrator.get_task(task_id)
        if rec is None:
            return fail(TaskNotFound(f"task not found: {task_id}"))
        return _record_dict(rec)

    @app.get("/keys/{idempotency_key:path}")
    async def get_by_key(idempotency_key: str):
        rec = await orchestrator.get_by_key(idempotency_key)
        if rec is None:
            return fail(TaskNotFound(f"key not found: {idempotency_key}"))
        return _record_dict(rec)

    @app.post("/tasks/{task_id}/cancel")
    async def cancel(task_id: str):
        try:
            rec = await orchestrator.cancel(task_id)
        except OrchestratorError as exc:
            return fail(exc)
        return _record_dict(rec)

    @app.get("/metrics")
    async def metrics():
        return {
            "running": orchestrator.executor.running_count(),
            "queued": orchestrator.executor.queued_count(),
        }

    return app
