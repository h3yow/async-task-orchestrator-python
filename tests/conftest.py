"""pytest 公共夹具：每个用例独立的 SQLite 文件、执行器与 ASGI 客户端。

日志：以 INFO 级别打印每个用例的输入参数与判定依据，满足
“单测日志中需要打印输入与判定依据”的要求。
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator

import httpx
import pytest_asyncio

from app.api import create_app
from app.batch import BatchService
from app.executor import Executor
from app.handlers import HandlerRegistry, register_builtin_handlers
from app.service import TaskService
from app.store import TaskStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)


def _make_stack(
    tmp_path,
    *,
    default_concurrency: int = 2,
    scope_concurrency: dict | None = None,
    base_backoff: float = 0.05,
    aging_interval: float = 0.2,
    tick_interval: float = 0.01,
):
    db = str(tmp_path / "tasks.db")
    store = TaskStore(db)
    store.connect()
    registry = HandlerRegistry()
    register_builtin_handlers(registry)
    executor = Executor(
        store,
        registry,
        default_concurrency=default_concurrency,
        scope_concurrency=scope_concurrency,
        base_backoff=base_backoff,
        aging_interval=aging_interval,
        tick_interval=tick_interval,
    )
    service = TaskService(store, registry, executor)
    batch_service = BatchService(store, registry)
    fastapi_app = create_app(service, executor, batch_service)
    # 保持 5 元组返回签名以兼容既有用例；批量服务挂在 app.state 上供新用例取用
    fastapi_app.state.batch_service = batch_service
    return store, registry, executor, service, fastapi_app


@pytest_asyncio.fixture
async def env(tmp_path) -> AsyncIterator[dict]:
    """启动执行器并提供 ASGI 客户端；用例结束自动停止。"""
    store, registry, executor, service, fastapi_app = _make_stack(tmp_path)
    batch_service = fastapi_app.state.batch_service
    await executor.start()
    transport = httpx.ASGITransport(app=fastapi_app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test"
    ) as client:
        yield {
            "store": store,
            "registry": registry,
            "executor": executor,
            "service": service,
            "batch_service": batch_service,
            "client": client,
            "db_path": str(tmp_path / "tasks.db"),
        }
    await executor.stop()
    store.close()


def log_case(title: str, **inputs) -> None:
    """统一打印用例输入，便于在日志中核对判定依据。"""
    logging.getLogger("test").info("用例[%s] 输入=%s", title, inputs)
