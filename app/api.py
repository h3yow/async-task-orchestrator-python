"""HTTP 接口层：提交 / 查询 / 按幂等键查询 / 取消 / 健康检查。

状态码约定：
- 201 新建任务；200 幂等重复提交（already_exists=true，task_id 与首次一致）
- 422 参数非法（error.code 以 invalid_/unknown_ 开头）
- 409 同幂等键载荷冲突（error.code=payload_conflict）
- 404 任务不存在
"""

from __future__ import annotations

from typing import Any, Literal

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from pydantic.config import ConfigDict

from .batch import BatchService, BatchTaskSpec
from .executor import Executor
from .models import (
    BatchConflict,
    BatchNotFound,
    OrchestratorError,
    PayloadConflict,
    TaskNotFound,
    TaskRecord,
    ValidationFailure,
)
from .service import TaskService


class SubmitRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    idempotency_key: str = Field(min_length=1, max_length=128)
    task_type: str = Field(min_length=1, max_length=64)
    payload: dict[str, Any]
    priority: Literal["high", "normal", "low"] = "normal"
    scope: str = Field(default="default", min_length=1, max_length=64)
    max_attempts: int = Field(default=3, ge=1, le=20)
    timeout_seconds: float = Field(default=10.0, gt=0, le=3600.0)


class TaskView(BaseModel):
    task_id: str
    idempotency_key: str
    task_type: str
    payload: dict[str, Any]
    priority: str
    scope: str
    status: str
    attempts: int
    max_attempts: int
    result: dict[str, Any] | None = None
    error_code: str | None = None
    error_message: str | None = None
    cancel_requested: bool
    already_exists: bool = False


class BatchTaskIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    idempotency_key: str = Field(min_length=1, max_length=128)
    task_type: str = Field(min_length=1, max_length=64)
    payload: dict[str, Any]
    depends_on: list[str] = Field(default_factory=list, max_length=1000)
    priority: Literal["high", "normal", "low"] = "normal"
    scope: str = Field(default="default", min_length=1, max_length=64)
    max_attempts: int = Field(default=3, ge=1, le=20)
    timeout_seconds: float = Field(default=10.0, gt=0, le=3600.0)


class BatchSubmitRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    idempotency_key: str = Field(min_length=1, max_length=128)
    tasks: list[BatchTaskIn] = Field(min_length=1, max_length=1000)


class BatchTaskView(BaseModel):
    task_id: str
    idempotency_key: str
    task_type: str
    payload: dict[str, Any]
    priority: str
    scope: str
    status: str
    attempts: int
    max_attempts: int
    depends_on: list[str] = []
    result: dict[str, Any] | None = None
    error_code: str | None = None
    error_message: str | None = None
    cancel_requested: bool = False
    already_exists: bool = False


class BatchView(BaseModel):
    batch_id: str
    idempotency_key: str
    already_exists: bool
    tasks: list[BatchTaskView]


def task_to_view(task: TaskRecord, already_exists: bool = False) -> TaskView:
    return TaskView(
        task_id=task.id,
        idempotency_key=task.idempotency_key,
        task_type=task.task_type,
        payload=task.payload,
        priority=task.priority.value,
        scope=task.scope,
        status=task.status.value,
        attempts=task.attempts,
        max_attempts=task.max_attempts,
        result=task.result,
        error_code=task.error_code,
        error_message=task.error_message,
        cancel_requested=task.cancel_requested,
        already_exists=already_exists,
    )


def _error_body(exc: OrchestratorError) -> dict[str, Any]:
    text = str(exc)
    code, _, message = text.partition(":")
    return {"error": {"code": code, "message": message or text}}


def create_app(
    service: TaskService,
    executor: Executor | None = None,
    batch_service: BatchService | None = None,
) -> FastAPI:
    """构造 FastAPI 应用（依赖注入，便于测试替换服务实例与执行器）。"""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if executor is not None:
            await executor.start()
        try:
            yield
        finally:
            if executor is not None:
                await executor.stop()

    app = FastAPI(
        title="async-task-orchestrator", version="1.1.0", lifespan=lifespan
    )

    if executor is not None:
        service.set_executor(executor)

    @app.exception_handler(ValidationFailure)
    async def _validation_handler(_request, exc: ValidationFailure):
        return JSONResponse(status_code=422, content=_error_body(exc))

    @app.exception_handler(PayloadConflict)
    async def _conflict_handler(_request, exc: PayloadConflict):
        return JSONResponse(status_code=409, content=_error_body(exc))

    @app.exception_handler(BatchConflict)
    async def _batch_conflict_handler(_request, exc: BatchConflict):
        return JSONResponse(status_code=409, content=_error_body(exc))

    @app.exception_handler(TaskNotFound)
    async def _not_found_handler(_request, exc: TaskNotFound):
        return JSONResponse(status_code=404, content=_error_body(exc))

    @app.exception_handler(BatchNotFound)
    async def _batch_not_found_handler(_request, exc: BatchNotFound):
        return JSONResponse(status_code=404, content=_error_body(exc))

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/tasks/submit", status_code=201)
    async def submit_task(req: SubmitRequest):
        record, created = await service.submit(
            idempotency_key=req.idempotency_key,
            task_type=req.task_type,
            payload=req.payload,
            priority=req.priority,
            scope=req.scope,
            max_attempts=req.max_attempts,
            timeout_seconds=req.timeout_seconds,
        )
        view = task_to_view(record, already_exists=not created).model_dump()
        return JSONResponse(status_code=201 if created else 200, content=view)

    @app.get("/tasks/{task_id}")
    async def get_task(task_id: str) -> dict[str, Any]:
        return task_to_view(await service.get_task(task_id)).model_dump()

    @app.get("/tasks/by-key/{idempotency_key}")
    async def get_task_by_key(idempotency_key: str) -> dict[str, Any]:
        return task_to_view(await service.get_by_key(idempotency_key)).model_dump()

    @app.post("/tasks/{task_id}/cancel")
    async def cancel_task(task_id: str) -> dict[str, Any]:
        return task_to_view(await service.cancel(task_id)).model_dump()

    # -- 批量依赖编排 -------------------------------------------------------

    async def _batch_to_view(result, *, include_deps: bool = True) -> BatchView:
        views: list[BatchTaskView] = []
        for task, task_created in result.tasks:
            depends_on = []
            if include_deps and batch_service is not None:
                depends_on = await batch_service.list_dependencies(task.id)
            views.append(
                BatchTaskView(
                    task_id=task.id,
                    idempotency_key=task.idempotency_key,
                    task_type=task.task_type,
                    payload=task.payload,
                    priority=task.priority.value,
                    scope=task.scope,
                    status=task.status.value,
                    attempts=task.attempts,
                    max_attempts=task.max_attempts,
                    depends_on=depends_on,
                    result=task.result,
                    error_code=task.error_code,
                    error_message=task.error_message,
                    cancel_requested=task.cancel_requested,
                    already_exists=not task_created,
                )
            )
        return BatchView(
            batch_id=result.batch.id,
            idempotency_key=result.batch.idempotency_key,
            already_exists=not result.created,
            tasks=views,
        )

    @app.post("/batches/submit", status_code=201)
    async def submit_batch(req: BatchSubmitRequest):
        if batch_service is None:
            return JSONResponse(
                status_code=503,
                content={"error": {"code": "batch_unavailable",
                                   "message": "批量编排服务未装配"}},
            )
        specs = [
            BatchTaskSpec(
                idempotency_key=t.idempotency_key,
                task_type=t.task_type,
                payload=t.payload,
                depends_on=tuple(t.depends_on),
                priority=t.priority,
                scope=t.scope,
                max_attempts=t.max_attempts,
                timeout_seconds=t.timeout_seconds,
            )
            for t in req.tasks
        ]
        result = await batch_service.submit_batch(req.idempotency_key, specs)
        view = await _batch_to_view(result)
        return JSONResponse(
            status_code=201 if result.created else 200, content=view.model_dump()
        )

    @app.get("/batches/by-key/{batch_key}")
    async def get_batch(batch_key: str) -> dict[str, Any]:
        if batch_service is None:
            return JSONResponse(
                status_code=503,
                content={"error": {"code": "batch_unavailable",
                                   "message": "批量编排服务未装配"}},
            )
        result = await batch_service.get_batch(batch_key)
        view = await _batch_to_view(result)
        return view.model_dump()

    return app
