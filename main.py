"""本地启动入口：uv run uvicorn main:app。

环境变量：
- TASK_DB_PATH：SQLite 文件位置（默认 tasks.db）
- TASK_DEFAULT_CONCURRENCY：默认作用域并发上限（默认 2）
"""

from __future__ import annotations

import json
import os

from app.api import create_app
from app.executor import Executor
from app.handlers import HandlerRegistry, register_builtin_handlers
from app.service import TaskService
from app.store import TaskStore


def build_service(db_path: str | None = None) -> tuple[TaskService, Executor, TaskStore]:
    """组装默认服务：内置处理器 + SQLite 文件持久化 + 调度执行器。"""
    store = TaskStore(db_path or os.environ.get("TASK_DB_PATH", "tasks.db"))
    store.connect()
    registry = HandlerRegistry()
    register_builtin_handlers(registry)
    executor = Executor(
        store,
        registry,
        default_concurrency=int(os.environ.get("TASK_DEFAULT_CONCURRENCY", "2")),
        scope_concurrency=json.loads(os.environ.get("TASK_SCOPE_CONCURRENCY", "{}")),
    )
    service = TaskService(store, registry, executor)
    return service, executor, store


service, executor, _store = build_service()
app = create_app(service, executor)
