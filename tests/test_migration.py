"""旧版库升级迁移与反复重启稳定性测试。

覆盖：
- 老结构（v1，无 batch_id / 无批量三表、user_version=0）的库启动升级：
  服务正常起来，旧任务数据原样保留、可查询；
- 升级后继续提交带依赖的批量任务（含依赖旧库中已存在任务）并查询批次；
- 同一个库反复重启：迁移幂等、结构版本稳定、数据持续累积不丢；
- 无法安全升级的库（更高版本号 / 缺列的损坏库）：明确报出原因的失败，
  且迁移整体回滚，不留下半新半旧的库；
- 默认启动入口 main.build_service 从旧库启动的完整路径。

用例日志均打印输入与判定依据。
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import tempfile
import uuid

import pytest

# main 模块在 import 时会按默认路径组装一次服务（模块级副作用）；
# 把默认库路径指到临时文件，避免测试污染工作目录下的 tasks.db。
os.environ.setdefault(
    "TASK_DB_PATH", os.path.join(tempfile.mkdtemp(), "import-side-effect.db")
)

from app.models import utcnow
from app.store import MigrationError, TaskStore
from conftest import _make_stack, log_case
from main import build_service

logger = logging.getLogger("test.migration")

# 上一版（批量依赖编排之前）的 tasks 表结构：无 batch_id 列、无批量三表。
_LEGACY_SCHEMA = """
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


def _insert_legacy_task(
    conn: sqlite3.Connection,
    key: str,
    status: str,
    *,
    payload: dict | None = None,
    result: dict | None = None,
    scope: str = "default",
) -> str:
    """按老结构直接写入一条任务，返回任务 id。"""
    task_id = uuid.uuid4().hex
    # 与旧版程序写入格式一致：带时区的 UTC ISO 字符串
    now = utcnow().isoformat()
    body = json.dumps(payload or {}, ensure_ascii=False, sort_keys=True)
    conn.execute(
        """
        INSERT INTO tasks (id, idempotency_key, task_type, payload, payload_hash,
            priority, scope, max_attempts, timeout_seconds, status, attempts,
            created_at, updated_at, scheduled_at, started_at, finished_at,
            result, error_code, error_message, cancel_requested)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            task_id, key, "echo", body, f"hash-{key}", "normal", scope, 3, 60.0,
            status, 1, now, now, now, now if status == "succeeded" else None,
            now if status in {"succeeded", "failed"} else None,
            json.dumps(result) if result is not None else None,
            None, None, 0,
        ),
    )
    return task_id


def _create_legacy_db(db_path: str) -> dict[str, str]:
    """造一个老结构的库：两条已终态任务 + 一条 pending，返回 key -> task_id。"""
    conn = sqlite3.connect(db_path)
    conn.executescript(_LEGACY_SCHEMA)
    ids = {
        "legacy-done": _insert_legacy_task(
            conn, "legacy-done", "succeeded", result={"echo": {"v": 1}}
        ),
        "legacy-failed": _insert_legacy_task(
            conn, "legacy-failed", "failed"
        ),
        "legacy-pending": _insert_legacy_task(
            conn, "legacy-pending", "pending"
        ),
    }
    conn.commit()
    conn.close()
    return ids


def _batch_body(key: str, tasks: list[dict]) -> dict:
    return {"idempotency_key": key, "tasks": tasks}


def _task(key: str, payload=None, *, depends_on=None) -> dict:
    return {
        "idempotency_key": key, "task_type": "echo",
        "payload": payload if payload is not None else {},
        "scope": "default", "priority": "normal",
        "depends_on": depends_on or [],
    }


async def test_legacy_db_upgrade_preserves_data_and_serves_batches(tmp_path):
    """老库升级：服务正常启动，旧任务原样可查，可继续提交/查询带依赖的批次。"""
    ids = _create_legacy_db(str(tmp_path / "tasks.db"))
    log_case("老库升级", legacy_tasks=ids)
    store, registry, executor, service, app = _make_stack(tmp_path)
    await executor.start()
    try:
        # 1) 结构版本升级到位，旧数据未被清空或改写
        version = store.conn.execute("PRAGMA user_version").fetchone()[0]
        logger.info("判定依据：user_version=%s（期望 2）", version)
        assert version == 2
        done = await store.get_task(ids["legacy-done"])
        failed = await store.get_task(ids["legacy-failed"])
        assert done is not None and done.status.value == "succeeded"
        assert done.result == {"echo": {"v": 1}}
        assert failed is not None and failed.status.value == "failed"
        logger.info(
            "判定依据：旧任务状态保持不变 done=%s failed=%s",
            done.status.value, failed.status.value,
        )

        # 2) 升级后批量依赖功能可用：新任务依赖旧库中已成功的任务
        import httpx

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            r = await client.post("/tasks/batch-submit", json=_batch_body(
                "after-upgrade-1",
                [_task("new-child", {"v": 2}, depends_on=["legacy-done"])],
            ))
            assert r.status_code == 201, r.text
            child_id = r.json()["tasks"][0]["task_id"]
            await executor.wait_idle(5)
            child = (await client.get(f"/tasks/{child_id}")).json()
            logger.info("判定依据：依赖旧任务的新任务状态=%s", child["status"])
            assert child["status"] == "succeeded", "前置已成功，后继应正常执行"

            # 3) 批次查询可用，成员含新任务
            b = await client.get("/batches/after-upgrade-1")
            assert b.status_code == 200, b.text
            member_keys = [x["idempotency_key"] for x in b.json()["tasks"]]
            logger.info("判定依据：批次成员=%s", member_keys)
            assert member_keys == ["new-child"]
    finally:
        await executor.stop()
        store.close()


async def test_repeated_restarts_on_same_db_are_stable(tmp_path):
    """同一个（老结构升上来的）库反复重启：每次都正常升级、数据累积不丢。"""
    ids = _create_legacy_db(str(tmp_path / "tasks.db"))
    log_case("反复重启", rounds=3, legacy_tasks=ids)
    for round_no in range(1, 4):
        store, registry, executor, service, app = _make_stack(tmp_path)
        await executor.start()
        try:
            version = store.conn.execute("PRAGMA user_version").fetchone()[0]
            assert version == 2, f"第 {round_no} 轮重启结构版本应为 2"
            # 每轮提交一个批次，依赖上一轮的任务，验证跨重启依赖可用
            depends = [f"round-{round_no - 1}"] if round_no > 1 else []
            batch_key = f"restart-batch-{round_no}"
            tasks, created, _record = await service.submit_batch(
                batch_key=batch_key,
                tasks=[{
                    "idempotency_key": f"round-{round_no}",
                    "task_type": "echo", "payload": {"round": round_no},
                    "scope": "default", "priority": "normal",
                    "depends_on": depends,
                }],
            )
            assert created
            await executor.wait_idle(5)
            got = await store.get_task(tasks[0].id)
            logger.info(
                "判定依据：第 %s 轮任务状态=%s（依赖=%s）",
                round_no, got.status.value, depends,
            )
            assert got.status.value == "succeeded"
            # 老库任务与历轮任务都还在
            assert (await store.get_task(ids["legacy-done"])).status.value == "succeeded"
            total = store.conn.execute("SELECT COUNT(*) AS n FROM tasks").fetchone()["n"]
            logger.info("判定依据：第 %s 轮后任务总数=%s", round_no, total)
            assert total == 3 + round_no
        finally:
            await executor.stop()
            store.close()


def test_future_schema_version_fails_with_clear_reason(tmp_path):
    """结构版本高于程序支持版本：明确报错且不改动库。"""
    db = str(tmp_path / "tasks.db")
    ids = _create_legacy_db(db)
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA user_version = 99")
    conn.commit()
    conn.close()
    log_case("高版本库拒绝启动", user_version=99)
    store = TaskStore(db)
    with pytest.raises(MigrationError) as exc:
        store.connect()
    logger.info("判定依据：异常消息=%s", exc.value)
    assert "99" in str(exc.value) and "2" in str(exc.value)
    # 库未被改动：版本号与表结构原样
    conn = sqlite3.connect(db)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 99
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert "batches" not in tables
    assert conn.execute(
        "SELECT COUNT(*) FROM tasks WHERE id = ?", (ids["legacy-done"],)
    ).fetchone()[0] == 1
    conn.close()


def test_corrupt_db_missing_column_fails_and_rolls_back(tmp_path):
    """缺必需列的库：迁移自检失败、消息指出缺列，且已做的变更整体回滚。"""
    db = str(tmp_path / "tasks.db")
    conn = sqlite3.connect(db)
    # 手工造一个缺 payload_hash 列的 tasks 表
    conn.execute(
        """
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY, idempotency_key TEXT UNIQUE NOT NULL,
            task_type TEXT NOT NULL, payload TEXT NOT NULL,
            priority TEXT NOT NULL, scope TEXT NOT NULL,
            max_attempts INTEGER NOT NULL, timeout_seconds REAL NOT NULL,
            status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
            scheduled_at TEXT NOT NULL, cancel_requested INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.commit()
    conn.close()
    log_case("损坏库拒绝启动", missing_column="payload_hash")
    store = TaskStore(db)
    with pytest.raises(MigrationError) as exc:
        store.connect()
    logger.info("判定依据：异常消息=%s", exc.value)
    assert "payload_hash" in str(exc.value)
    # 迁移整体回滚：不留半新半旧（版本号未推进、批量表未残留）
    conn = sqlite3.connect(db)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 0
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    logger.info("判定依据：回滚后表集合=%s", sorted(tables))
    assert tables == {"tasks"}
    conn.close()


async def test_default_entrypoint_boots_from_legacy_db(tmp_path):
    """默认启动入口 main.build_service 直接吃旧库：能起来、能跑批量依赖。"""
    db = str(tmp_path / "tasks.db")
    ids = _create_legacy_db(db)
    log_case("默认入口启动旧库", db_path=db, legacy_tasks=ids)
    service, executor, store = build_service(db_path=db)
    await executor.start()
    try:
        tasks, created, _record = await service.submit_batch(
            batch_key="entrypoint-batch",
            tasks=[{
                "idempotency_key": "entry-child", "task_type": "echo",
                "payload": {}, "scope": "default", "priority": "normal",
                "depends_on": ["legacy-done"],
            }],
        )
        assert created
        await executor.wait_idle(5)
        got = await service.get_task(tasks[0].id)
        logger.info("判定依据：默认入口提交的任务状态=%s", got.status.value)
        assert got.status.value == "succeeded"
        # 旧任务仍可查询
        old = await store.get_task(ids["legacy-done"])
        assert old is not None and old.status.value == "succeeded"
    finally:
        await executor.stop()
        store.close()
