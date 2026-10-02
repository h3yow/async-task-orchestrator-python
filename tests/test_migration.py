"""旧版库结构（v1，无批量能力）升级到当前版本的兼容性与稳定性测试。

背景：批量依赖编排（v2 结构）上线前，线上库只有 tasks 表（无 batch_id 列、
无 batches/batch_members/task_dependencies 表）。本组用例覆盖：
- 用 v1 结构的库启动服务：正常升级、旧任务与状态原样保留、可照常查询；
- 升级后批量依赖功能（提交/门禁/级联/批次查询）立即可用；
- 同一个库反复重启稳定，不重复迁移、不丢数据；
- 无法安全升级的库：启动即给出明确失败，且不留下半新半旧的结构；
- 默认启动入口（main:app，TASK_DB_PATH 指向旧库）覆盖从旧库启动的路径。
"""

from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from conftest import log_case

from app.executor import Executor
from app.handlers import HandlerRegistry, register_builtin_handlers
from app.models import SchemaMigrationError
from app.service import TaskService, canonical_payload_hash
from app.store import _SCHEMA_VERSION, TaskStore
from app.api import create_app

logger = logging.getLogger("test.migration")

# 与上一轮（v1）线上库完全一致的结构：只有 tasks 表，无 batch_id 列。
_V1_SCHEMA = """
CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT UNIQUE NOT NULL,
    task_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    priority TEXT NOT NULL,
    scope TEXT NOT NULL,
    max_attempts INTEGER NOT NULL,
    timeout_seconds REAL NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    scheduled_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    result TEXT,
    error_code TEXT,
    error_message TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX idx_tasks_dispatch ON tasks (status, scheduled_at);
CREATE INDEX idx_tasks_scope ON tasks (scope, status);
"""


def _dt(value: datetime) -> str:
    return value.isoformat()


def _insert_v1_task(
    conn: sqlite3.Connection,
    *,
    key: str,
    status: str,
    task_type: str = "echo",
    payload: dict | None = None,
    scope: str = "default",
    attempts: int = 0,
) -> str:
    """按 v1 结构直接写入一条任务（模拟上一轮服务产生的数据）。"""
    payload = payload if payload is not None else {"key": key}
    now = _dt(datetime.now(timezone.utc))
    task_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO tasks (id, idempotency_key, task_type, payload, payload_hash,
            priority, scope, max_attempts, timeout_seconds, status, attempts,
            created_at, updated_at, scheduled_at, started_at, finished_at,
            result, error_code, error_message, cancel_requested)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            task_id, key, task_type,
            json.dumps(payload, ensure_ascii=False, sort_keys=True),
            canonical_payload_hash(payload),
            "normal", scope, 3, 10.0, status, attempts,
            now, now, now,
            now if status == "running" else None,
            now if status in {"succeeded", "failed", "cancelled"} else None,
            json.dumps({"echo": payload}) if status == "succeeded" else None,
            "fatal" if status == "failed" else None,
            "模拟旧库失败" if status == "failed" else None,
            0,
        ),
    )
    return task_id


def _build_v1_db(db_path: str) -> dict[str, str]:
    """创建一个 v1 结构的旧库，含各状态任务，返回 {业务键: 任务ID}。"""
    conn = sqlite3.connect(db_path)
    conn.executescript(_V1_SCHEMA)
    ids = {
        "old-succeeded": _insert_v1_task(conn, key="old-succeeded",
                                         status="succeeded", attempts=1),
        "old-failed": _insert_v1_task(conn, key="old-failed", status="failed",
                                      attempts=3),
        "old-pending": _insert_v1_task(conn, key="old-pending", status="pending"),
    }
    conn.commit()
    conn.close()
    return ids


def _make_stack_at(db_path: str, **executor_kwargs):
    """与 conftest._make_stack 相同，但使用指定的已有库文件。"""
    store = TaskStore(db_path)
    store.connect()
    registry = HandlerRegistry()
    register_builtin_handlers(registry)
    executor = Executor(
        store, registry,
        default_concurrency=2,
        base_backoff=executor_kwargs.get("base_backoff", 0.05),
        aging_interval=0.2,
        tick_interval=0.01,
    )
    service = TaskService(store, registry, executor)
    return store, registry, executor, service, create_app(service, executor)


async def test_old_schema_db_upgrades_and_preserves_data(tmp_path):
    """v1 旧库启动：结构升到 v2，旧任务逐字段原样保留、可照常查询。"""
    db = str(tmp_path / "old.db")
    ids = _build_v1_db(db)
    log_case("旧库升级-数据保留", db=db, keys=sorted(ids))

    store, _, _, service, _ = _make_stack_at(db)
    try:
        version = store.conn.execute("PRAGMA user_version").fetchone()[0]
        cols = {r["name"] for r in store.conn.execute("PRAGMA table_info(tasks)")}
        tables = {r["name"] for r in store.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        logger.info("判定依据：user_version=%d batch_id列=%s 新表=%s",
                    version, "batch_id" in cols,
                    sorted(tables - {"tasks"}))
        assert version == _SCHEMA_VERSION
        assert "batch_id" in cols
        assert {"batches", "batch_members", "task_dependencies"} <= tables

        # 旧数据未被清空、重建或改写：逐字段比对
        succeeded = await service.get_task(ids["old-succeeded"])
        failed = await service.get_task(ids["old-failed"])
        pending = await service.get_task(ids["old-pending"])
        logger.info("判定依据：succeeded=%s/%d failed=%s/%s pending=%s",
                    succeeded.status.value, succeeded.attempts,
                    failed.status.value, failed.error_code,
                    pending.status.value)
        assert succeeded.status.value == "succeeded" and succeeded.attempts == 1
        assert succeeded.result == {"echo": {"key": "old-succeeded"}}
        assert failed.status.value == "failed" and failed.error_code == "fatal"
        assert failed.attempts == 3
        assert pending.status.value == "pending"
        # 按业务键查询通道同样可用
        by_key = await service.get_by_key("old-succeeded")
        assert by_key.id == ids["old-succeeded"]
    finally:
        store.close()


async def test_batch_submit_and_query_after_upgrade(tmp_path):
    """升级后的库上：带依赖批量提交可用，可依赖旧任务，批次可查询。"""
    db = str(tmp_path / "old.db")
    ids = _build_v1_db(db)
    log_case("旧库升级-批量依赖", 依赖旧任务="old-succeeded",
             批次=["new-a(依赖old-succeeded)", "new-b(依赖new-a)"])

    store, _, executor, service, app = _make_stack_at(db)
    await executor.start()
    try:
        records, created, batch = await service.submit_batch(
            batch_key="pipeline-after-upgrade",
            tasks=[
                {"idempotency_key": "new-a", "task_type": "echo",
                 "payload": {"step": "a"}, "depends_on": ["old-succeeded"]},
                {"idempotency_key": "new-b", "task_type": "echo",
                 "payload": {"step": "b"}, "depends_on": ["new-a"]},
            ],
        )
        assert created and batch is not None
        assert [r.idempotency_key for r in records] == ["new-a", "new-b"]

        await executor.wait_idle(5)
        a = await service.get_by_key("new-a")
        b = await service.get_by_key("new-b")
        old = await service.get_by_key("old-succeeded")
        logger.info("判定依据：new-a=%s new-b=%s 旧任务attempts=%d(不重复执行)",
                    a.status.value, b.status.value, old.attempts)
        assert a.status.value == "succeeded"
        assert b.status.value == "succeeded"
        assert old.attempts == 1, "旧库已成功的前置不得被重复执行"

        # 依赖关系与批次查询在新结构上正常工作
        assert await service.get_dependencies(a.id) == ["old-succeeded"]
        got_batch, members = await service.get_batch("pipeline-after-upgrade")
        assert got_batch.id == batch.id
        assert [m.idempotency_key for m in members] == ["new-a", "new-b"]

        # 依赖已失败旧任务的新提交：提交即确定为 skipped（级联传播可用）
        records2, created2, _ = await service.submit_batch(
            batch_key="pipeline-on-failed",
            tasks=[
                {"idempotency_key": "new-c", "task_type": "echo",
                 "payload": {}, "depends_on": ["old-failed"]},
            ],
        )
        logger.info("判定依据：依赖旧failed任务的新任务状态=%s error=%s",
                    records2[0].status.value, records2[0].error_code)
        assert created2
        assert records2[0].status.value == "skipped"
        assert records2[0].error_code == "dependency_failed"
    finally:
        await executor.stop()
        store.close()


async def test_repeated_restarts_on_upgraded_db_are_stable(tmp_path):
    """同一个升级后的库反复重启：不再迁移、不报错、数据与批量功能稳定。"""
    db = str(tmp_path / "old.db")
    ids = _build_v1_db(db)
    log_case("旧库升级-反复重启", 重启次数=3)

    # 第一次启动：完成升级，并提交一批带依赖任务
    store, _, executor, service, _ = _make_stack_at(db)
    await executor.start()
    await service.submit_batch(
        batch_key="restart-batch",
        tasks=[
            {"idempotency_key": "rb-1", "task_type": "echo", "payload": {}},
            {"idempotency_key": "rb-2", "task_type": "echo", "payload": {},
             "depends_on": ["rb-1"]},
        ],
    )
    await executor.wait_idle(5)
    await executor.stop()
    store.close()

    # 后续反复重启：结构版本稳定、旧数据与批次数据都在、恢复流程正常
    for round_no in (2, 3, 4):
        store, _, executor, service, _ = _make_stack_at(db)
        await executor.start()
        try:
            version = store.conn.execute("PRAGMA user_version").fetchone()[0]
            total = store.conn.execute("SELECT COUNT(*) AS n FROM tasks") \
                .fetchone()["n"]
            rb2 = await service.get_by_key("rb-2")
            old = await service.get_by_key("old-succeeded")
            _, members = await service.get_batch("restart-batch")
            logger.info("判定依据：第%d次启动 version=%d 任务数=%d "
                        "rb-2=%s 旧任务=%s 批次成员=%d",
                        round_no, version, total, rb2.status.value,
                        old.status.value, len(members))
            assert version == _SCHEMA_VERSION
            assert total == 5, "3 个旧任务 + 2 个批量任务，重启不得丢数据"
            assert rb2.status.value == "succeeded"
            assert old.id == ids["old-succeeded"]
            assert [m.idempotency_key for m in members] == ["rb-1", "rb-2"]
        finally:
            await executor.stop()
            store.close()


async def test_incompatible_db_fails_loudly_without_partial_migration(tmp_path):
    """无法安全升级的库：启动即明确失败，且不留下半新半旧的结构。"""
    db = str(tmp_path / "bad.db")
    conn = sqlite3.connect(db)
    # 形似 tasks 但缺少核心列（payload_hash 等），不是本服务可兼容的库
    conn.execute("CREATE TABLE tasks (id TEXT PRIMARY KEY, status TEXT NOT NULL)")
    conn.commit()
    conn.close()
    log_case("旧库升级-不兼容拒绝", db=db, 缺少列="payload_hash 等")

    store = TaskStore(db)
    with pytest.raises(SchemaMigrationError) as exc_info:
        store.connect()
    logger.info("判定依据：异常=%s", exc_info.value)
    assert "schema_incompatible" in str(exc_info.value)

    # 失败后库内不得有任何半完成的迁移痕迹
    probe = sqlite3.connect(db)
    version = probe.execute("PRAGMA user_version").fetchone()[0]
    tables = {r[0] for r in probe.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    probe.close()
    logger.info("判定依据：失败后 user_version=%d 表集合=%s", version, tables)
    assert version == 0
    assert tables == {"tasks"}, "不得创建 batches 等新表，保持原样"


async def test_future_schema_version_fails_loudly(tmp_path):
    """结构版本比服务还新的库：明确拒绝启动，而不是误降级。"""
    db = str(tmp_path / "newer.db")
    conn = sqlite3.connect(db)
    conn.execute(f"PRAGMA user_version={_SCHEMA_VERSION + 1}")
    conn.commit()
    conn.close()
    log_case("旧库升级-版本过新", user_version=_SCHEMA_VERSION + 1)

    with pytest.raises(SchemaMigrationError) as exc_info:
        TaskStore(db).connect()
    logger.info("判定依据：异常=%s", exc_info.value)
    assert "schema_too_new" in str(exc_info.value)


async def test_default_entrypoint_starts_from_old_db(tmp_path, monkeypatch):
    """默认启动方式（uvicorn main:app + TASK_DB_PATH）覆盖从旧库启动的路径。"""
    db = str(tmp_path / "old.db")
    ids = _build_v1_db(db)
    monkeypatch.setenv("TASK_DB_PATH", db)
    log_case("旧库升级-默认入口", TASK_DB_PATH=db)

    import main  # 模块级即执行默认组装：build_service() -> store.connect()

    try:
        version = main._store.conn.execute("PRAGMA user_version").fetchone()[0]  # noqa: SLF001
        logger.info("判定依据：默认入口启动后 user_version=%d", version)
        assert version == _SCHEMA_VERSION

        transport = httpx.ASGITransport(app=main.app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://default"
        ) as client:
            r = await client.get(f"/tasks/{ids['old-succeeded']}")
            assert r.status_code == 200
            logger.info("判定依据：默认入口查询旧任务=%s", r.json()["status"])
            assert r.json()["status"] == "succeeded"
            assert r.json()["idempotency_key"] == "old-succeeded"
            # 默认入口上批量接口同样可用
            r2 = await client.post("/tasks/batch-submit", json={
                "idempotency_key": "default-entry-batch",
                "tasks": [
                    {"idempotency_key": "de-1", "task_type": "echo",
                     "payload": {}, "depends_on": ["old-succeeded"]},
                ],
            })
            assert r2.status_code == 201
    finally:
        main._store.close()  # noqa: SLF001
