"""批量依赖编排测试。

覆盖：
- 重复 / 并发批量提交：同一组任务、同一套依赖、一致的任务标识；
- 批次内容不一致 / 单键冲突 -> 409，错误码可区分；
- 循环依赖、引用不存在、跨作用域依赖、批次内重复键 -> 提交时整体拒绝（422），
  且不会写入一半任务；
- 依赖 gating：前置未全部成功不执行；链式 clean->agg->export 严格按序；
- 多前置部分失败：skipped 终态及 error_code（dependency_failed /
  dependency_cancelled）稳定一致，且与真正 failed 区分；
- 等待依赖期间取消立即确定为 cancelled，并向下游传播 skipped；
- 依赖释放后仍受作用域并发上限约束，优先级对同时就绪的任务生效；
- 异常重启后：已完成前置不重复执行，blocked 任务不提前执行，依赖关系恢复；
- 单批次 150 个任务、6 层依赖的规模场景。

每个用例日志均打印输入与判定依据。
"""

from __future__ import annotations

import asyncio
import logging

import httpx
import pytest

from conftest import _make_stack, log_case

logger = logging.getLogger("test.batch")


def submit_body(batch_key: str, tasks: list[dict]) -> dict:
    return {"idempotency_key": batch_key, "tasks": tasks}


def task(key: str, task_type: str = "echo", payload=None, *,
         depends_on=None, priority="normal", scope="default",
         max_attempts=3, timeout_seconds=10.0) -> dict:
    body = {
        "idempotency_key": key,
        "task_type": task_type,
        "payload": payload if payload is not None else {},
        "priority": priority,
        "scope": scope,
        "max_attempts": max_attempts,
        "timeout_seconds": timeout_seconds,
    }
    if depends_on is not None:
        body["depends_on"] = list(depends_on)
    return body


async def status_map(client, batch_key: str) -> dict[str, dict]:
    r = await client.get(f"/batches/by-key/{batch_key}")
    assert r.status_code == 200, r.text
    return {t["idempotency_key"]: t for t in r.json()["tasks"]}


# ---------------------------------------------------------------------------
# 1. 重复与并发批量提交
# ---------------------------------------------------------------------------


async def test_duplicate_batch_submit_same_tasks_and_deps(env):
    """同批次重复提交：返回同一批 ID、同一组任务 ID、already_exists=True。"""
    client = env["client"]
    body = submit_body("bp-dup", [
        task("c", payload={"v": 1}),
        task("a", payload={"v": 2}, depends_on=["c"]),
        task("e", payload={"v": 3}, depends_on=["a"]),
    ])
    log_case("批次重复提交", batch_key="bp-dup", task_count=3)

    r1 = await client.post("/batches/submit", json=body)
    assert r1.status_code == 201, r1.text
    first = r1.json()

    # 依赖声明顺序不同但集合相同，仍是同一批次
    body2 = submit_body("bp-dup", [
        task("c", payload={"v": 1}),
        task("a", payload={"v": 2}, depends_on=["c"]),
        task("e", payload={"v": 3}, depends_on=["a"]),
    ])
    r2 = await client.post("/batches/submit", json=body2)
    assert r2.status_code == 200
    second = r2.json()
    logger.info("判定依据：batch_id 一致=%s，任务 ID 一致=%s，already_exists=%s",
                second["batch_id"] == first["batch_id"],
                [t["task_id"] for t in second["tasks"]]
                == [t["task_id"] for t in first["tasks"]],
                second["already_exists"])
    assert second["batch_id"] == first["batch_id"]
    assert [t["task_id"] for t in second["tasks"]] == \
        [t["task_id"] for t in first["tasks"]]
    assert second["already_exists"] is True
    deps = {t["idempotency_key"]: t["depends_on"] for t in second["tasks"]}
    assert deps == {"c": [], "a": ["c"], "e": ["a"]}

    await env["executor"].wait_idle(5)
    states = await status_map(client, "bp-dup")
    assert {t["status"] for t in states.values()} == {"succeeded"}
    total_attempts = sum(t["attempts"] for t in states.values())
    assert total_attempts == 3, "重复提交不得产生重复执行"
    logger.info("判定依据：全部 succeeded，总 attempts=%d（期望 3）", total_attempts)


async def test_concurrent_duplicate_batch_submit(env):
    """20 路并发提交同一批次：恰好一批、一组任务，每个任务只执行一次。"""
    client = env["client"]
    body = submit_body("bp-concurrent", [
        task(f"t-{i}", task_type="flaky", payload={"fail_times": 0},
             depends_on=[f"t-{i - 1}"] if i > 0 else None)
        for i in range(6)
    ])
    log_case("批次并发重复提交", concurrency=20, task_count=6)

    async def one():
        return await client.post("/batches/submit", json=body)

    responses = await asyncio.gather(*(one() for _ in range(20)))
    codes = [r.status_code for r in responses]
    batch_ids = {r.json()["batch_id"] for r in responses}
    task_id_sets = {
        tuple(t["task_id"] for t in r.json()["tasks"]) for r in responses
    }
    logger.info("判定依据：201 数量=%d，200 数量=%d，不同 batch_id=%d，不同任务序列=%d",
                codes.count(201), codes.count(200), len(batch_ids), len(task_id_sets))
    assert codes.count(201) == 1
    assert codes.count(200) == 19
    assert len(batch_ids) == 1
    assert len(task_id_sets) == 1, "并发重复提交必须返回同一组任务标识"

    await env["executor"].wait_idle(8)
    states = await status_map(client, "bp-concurrent")
    assert all(t["status"] == "succeeded" for t in states.values())
    assert all(t["attempts"] == 1 for t in states.values())
    logger.info("判定依据：6 个任务全部 succeeded 且 attempts 全为 1")


async def test_batch_content_mismatch_rejected(env):
    """同批次键内容不一致 -> 409 batch_conflict，且差异任务未被写入。"""
    client = env["client"]
    log_case("批次内容冲突", batch_key="bp-conflict")
    r1 = await client.post("/batches/submit", json=submit_body("bp-conflict", [
        task("x", payload={"v": 1}),
    ]))
    assert r1.status_code == 201
    r2 = await client.post("/batches/submit", json=submit_body("bp-conflict", [
        task("x", payload={"v": 2}),  # 载荷不同
    ]))
    assert r2.status_code == 409
    assert r2.json()["error"]["code"] == "batch_conflict"
    logger.info("判定依据：冲突 HTTP=%d code=%s",
                r2.status_code, r2.json()["error"]["code"])

    # 依赖集合不同也属于批次内容不一致
    r3 = await client.post("/batches/submit", json=submit_body("bp-conflict", [
        task("x", payload={"v": 1}),
        task("y", payload={}),
        task("z", payload={}, depends_on=["x", "y"]),
    ]))
    assert r3.status_code == 409
    # 原批次内容不受影响：仍是单任务批次
    states = await status_map(client, "bp-conflict")
    assert list(states) == ["x"]
    logger.info("判定依据：冲突后原批次任务集合=%s（未被部分写入污染）", list(states))


async def test_batch_task_key_conflict_with_existing(env):
    """批次内键与历史单任务参数不一致 -> 409 payload_conflict，整批不写入。"""
    client = env["client"]
    log_case("批次单键冲突", key="shared-key")
    r = await client.post("/tasks/submit", json={
        "idempotency_key": "shared-key", "task_type": "echo",
        "payload": {"v": 1}})
    assert r.status_code == 201
    rb = await client.post("/batches/submit", json=submit_body("bp-key-conflict", [
        task("shared-key", payload={"v": 999}),
        task("other", payload={}, depends_on=["shared-key"]),
    ]))
    assert rb.status_code == 409
    assert rb.json()["error"]["code"] == "payload_conflict"
    logger.info("判定依据：HTTP=%d code=%s",
                rb.status_code, rb.json()["error"]["code"])
    r404 = await client.get("/batches/by-key/bp-key-conflict")
    assert r404.status_code == 404, "冲突批次不得落库"


# ---------------------------------------------------------------------------
# 2. 非法依赖：整体拒绝、原因可区分、不写一半
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("tasks,expected_code", [
    # 直接自环
    ([("a", ["a"])], "cycle_dependency"),
    # 两环
    ([("a", ["b"]), ("b", ["a"])], "cycle_dependency"),
    # 三环
    ([("a", ["c"]), ("b", ["a"]), ("c", ["b"])], "cycle_dependency"),
    # 引用缺失
    ([("a", ["ghost"])], "dependency_not_found"),
    # 跨作用域：同批跨 scope
    ([("a", [], "s1"), ("b", ["a"], "s2")], "cross_scope_dependency"),
], )
async def test_invalid_batch_rejected_whole(env, tasks, expected_code):
    client = env["client"]
    specs = []
    for name, deps, *scope in tasks:
        specs.append(task(name, depends_on=list(deps),
                          scope=scope[0] if scope else "default"))
    log_case("非法批次整体拒绝", tasks=tasks, expected=expected_code)
    r = await client.post("/batches/submit",
                          json=submit_body(f"bp-invalid-{expected_code}", specs))
    assert r.status_code == 422, r.text
    assert r.json()["error"]["code"] == expected_code
    logger.info("判定依据：HTTP=422 code=%s", r.json()["error"]["code"])
    rb = await client.get(f"/batches/by-key/bp-invalid-{expected_code}")
    assert rb.status_code == 404, "非法批次不得写入任何任务"
    for name, _deps, *_rest in tasks:
        got = await client.get(f"/tasks/by-key/{name}")
        assert got.status_code == 404, f"非法批次的任务 {name} 被部分写入"


async def test_cross_scope_dependency_on_history_rejected(env):
    """依赖历史任务但作用域不同：422 cross_scope_dependency。"""
    client = env["client"]
    log_case("跨作用域历史依赖", history_scope="sA", batch_scope="sB")
    await client.post("/tasks/submit", json={
        "idempotency_key": "hist-scoped", "task_type": "echo",
        "payload": {}, "scope": "sA"})
    r = await client.post("/batches/submit", json=submit_body("bp-xscope", [
        task("down", scope="sB", depends_on=["hist-scoped"]),
    ]))
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "cross_scope_dependency"


async def test_duplicate_task_key_in_batch_rejected(env):
    """批次内业务唯一键重复声明：422 duplicate_task_in_batch。"""
    client = env["client"]
    log_case("批次内重复键", key="dup-me")
    r = await client.post("/batches/submit", json=submit_body("bp-dupkey", [
        task("dup-me", payload={"i": 1}),
        task("dup-me", payload={"i": 2}),
    ]))
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "duplicate_task_in_batch"


async def test_empty_batch_rejected(env):
    client = env["client"]
    log_case("空批次")
    r = await client.post("/batches/submit", json={"idempotency_key": "bp-empty",
                                                   "tasks": []})
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# 3. 依赖满足后的调度语义
# ---------------------------------------------------------------------------


async def test_dependency_gating_chain_runs_in_order(env):
    """链式批次：后任务必须等前置成功后才开始；提交后立即看是 blocked。"""
    client = env["client"]
    order: list[str] = []

    async def ordered(payload, token):
        order.append(payload["key"])
        await token.wait_cancellable(0.06)
        return {"ran": payload["key"]}

    env["registry"].register("btype", ordered)
    log_case("依赖 gating 链式", chain=["clean", "agg", "export"])
    r = await client.post("/batches/submit", json=submit_body("bp-chain", [
        task("clean", "btype", {"key": "clean"}),
        task("agg", "btype", {"key": "agg"}, depends_on=["clean"], priority="high"),
        task("export", "btype", {"key": "export"}, depends_on=["agg"]),
    ]))
    initial = {t["idempotency_key"]: t["status"] for t in r.json()["tasks"]}
    logger.info("判定依据：提交后初态=%s", initial)
    assert initial["clean"] == "pending"
    assert initial["agg"] == "blocked"
    assert initial["export"] == "blocked"

    await env["executor"].wait_idle(5)
    states = await status_map(client, "bp-chain")
    assert {k: v["status"] for k, v in states.items()} == {
        "clean": "succeeded", "agg": "succeeded", "export": "succeeded"}
    logger.info("判定依据：实际开始顺序=%s（期望严格 clean<agg<export）", order)
    assert order == ["clean", "agg", "export"]


async def test_multi_deps_partial_failure_stable_skipped(env):
    """多个前置一部分成功、一部分失败：下游 skipped 且原因稳定；
    failed 链与 cancelled 链的 error_code 可区分；skipped 与 failed 区分。"""
    client = env["client"]
    log_case("多前置部分失败", deps=("root-fail", "root-ok"))
    await client.post("/batches/submit", json=submit_body("bp-partial", [
        task("root-ok", "echo", {"v": 1}),
        task("root-fail", "always_fail", {}),
        task("root-cancel", "echo", {}),
        # 两个前置：一个成功一个失败 -> dependency_failed
        task("mix-fail", "echo", {}, depends_on=["root-ok", "root-fail"]),
        # failed 的传递下游 -> 仍记 dependency_failed
        task("grand", "echo", {}, depends_on=["mix-fail"]),
        # 一个成功一个被取消 -> dependency_cancelled
        task("mix-cancel", "echo", {}, depends_on=["root-ok", "root-cancel"]),
        # 依赖 skipped 链 -> dependency_failed
        task("skip-chain", "echo", {}, depends_on=["grand", "root-ok"]),
    ]))
    # root-cancel 尚未被领取前立即取消（pending 取消）
    states0 = await status_map(client, "bp-partial")
    tid_cancel = states0["root-cancel"]["task_id"]
    await client.post(f"/tasks/{tid_cancel}/cancel")

    await env["executor"].wait_idle(5)
    states = await status_map(client, "bp-partial")
    logger.info("判定依据：终态=%s",
                {k: (v["status"], v["error_code"]) for k, v in states.items()})
    assert states["root-ok"]["status"] == "succeeded"
    assert states["root-fail"]["status"] == "failed", "真正执行失败必须是 failed"
    assert states["root-fail"]["error_code"] == "fatal"
    assert states["root-cancel"]["status"] == "cancelled"
    for k in ("mix-fail", "grand", "skip-chain"):
        assert states[k]["status"] == "skipped", f"{k} 必须是 skipped 而非 failed"
        assert states[k]["error_code"] == "dependency_failed"
        assert states[k]["attempts"] == 0, "被跳过的任务从未被执行"
    assert states["mix-cancel"]["status"] == "skipped"
    assert states["mix-cancel"]["error_code"] == "dependency_cancelled"


async def test_cancel_blocked_task_settles_immediately(env):
    """等待依赖期间被取消：立即 cancelled，不挂起，并向其下游传播 skipped。"""
    client = env["client"]
    release = asyncio.Event()

    async def gate(payload, token):
        await release.wait()  # 前置一直不完成，waiting 保持 blocked
        return {"ok": True}

    env["registry"].register("gate", gate)
    log_case("等待中取消", waiting="waiting", down="down-of-waiting")
    await client.post("/batches/submit", json=submit_body("bp-cancel-blocked", [
        task("root", "gate", {}),
        task("waiting", "echo", {}, depends_on=["root"]),
        task("down", "echo", {}, depends_on=["waiting"]),
    ]))
    states = await status_map(client, "bp-cancel-blocked")
    waiting_id = states["waiting"]["task_id"]
    down_id = states["down"]["task_id"]
    assert states["waiting"]["status"] == "blocked"

    r = await client.post(f"/tasks/{waiting_id}/cancel")
    assert r.json()["status"] == "cancelled"
    logger.info("判定依据：取消响应立即给出 cancelled")

    # 不依赖执行器 tick：取消事务内已级联传播
    for _ in range(30):
        s = (await client.get(f"/tasks/{down_id}")).json()
        if s["status"] == "skipped":
            break
        await asyncio.sleep(0.02)
    assert s["status"] == "skipped"
    assert s["error_code"] == "dependency_cancelled"
    assert s["attempts"] == 0
    logger.info("判定依据：waiting=cancelled，下游 down=%s/%s，attempts=%d（未执行）",
                s["status"], s["error_code"], s["attempts"])

    # 放行前置后，waiting 不得恢复执行；整条链确定不翻转
    release.set()
    await asyncio.sleep(0.2)
    states2 = await status_map(client, "bp-cancel-blocked")
    assert states2["waiting"]["status"] == "cancelled"
    assert states2["down"]["status"] == "skipped"


async def test_dependency_released_respects_scope_concurrency(tmp_path):
    """大量后置任务同时就绪：仍受 scope 并发上限约束（上限被打满但不突破）。"""
    store, registry, executor, service, fastapi_app = _make_stack(
        tmp_path, default_concurrency=4, scope_concurrency={"limited": 2})
    max_seen = {"v": 0}
    inflight = {"v": 0}
    lock = asyncio.Lock()

    async def guarded(payload, token):
        async with lock:
            inflight["v"] += 1
            max_seen["v"] = max(max_seen["v"], inflight["v"])
        await token.wait_cancellable(0.1)
        async with lock:
            inflight["v"] -= 1
        return {"i": payload.get("i")}

    async def gate(payload, token):
        await gate_evt.wait()
        return {"gate": True}

    registry.register("guarded", guarded)
    registry.register("gate", gate)
    gate_evt = asyncio.Event()

    await executor.start()
    try:
        transport = httpx.ASGITransport(app=fastapi_app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://t") as client:
            log_case("释放后并发上限", scope="limited", limit=2, downstream=10)
            tasks_spec = [task("g", "gate", {}, scope="limited")]
            tasks_spec += [
                task(f"d-{i}", "guarded", {"i": i}, scope="limited",
                     depends_on=["g"])
                for i in range(10)
            ]
            r = await client.post("/batches/submit",
                                  json=submit_body("bp-scope", tasks_spec))
            assert r.status_code == 201
            before = {t["idempotency_key"]: t["status"]
                      for t in r.json()["tasks"]}
            assert all(v == "blocked" for k, v in before.items() if k != "g")
            await asyncio.sleep(0.15)  # gate 开始执行
            gate_evt.set()             # 10 个后置同时就绪
            await executor.wait_idle(8)
        logger.info("判定依据：观测最大同 scope 在跑数=%d（期望恰为 2）",
                    max_seen["v"])
        assert max_seen["v"] <= 2
        assert max_seen["v"] == 2, "持续供给下上限应被打满"
    finally:
        await executor.stop()
        store.close()


async def test_priority_takes_effect_when_dependencies_release(tmp_path):
    """同一前置扇出 3 个后置任务、scope 并发=1：优先级决定就绪后的执行次序。"""
    store, registry, executor, service, fastapi_app = _make_stack(
        tmp_path, default_concurrency=1, aging_interval=10.0, tick_interval=0.01)
    started: list[str] = []
    gate_evt = asyncio.Event()

    async def gate(payload, token):
        await gate_evt.wait()
        return {}

    async def ordered(payload, token):
        started.append(payload["key"])
        await token.wait_cancellable(0.05)
        return {}

    registry.register("gate", gate)
    registry.register("ordered", ordered)
    await executor.start()
    try:
        transport = httpx.ASGITransport(app=fastapi_app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://t") as client:
            log_case("就绪后优先级", downstream=["low-d", "normal-d", "high-d"])
            await client.post("/batches/submit", json=submit_body("bp-prio", [
                task("g", "gate", {}),
                task("low-d", "ordered", {"key": "low-d"},
                     depends_on=["g"], priority="low"),
                task("normal-d", "ordered", {"key": "normal-d"},
                     depends_on=["g"], priority="normal"),
                task("high-d", "ordered", {"key": "high-d"},
                     depends_on=["g"], priority="high"),
            ]))
            await asyncio.sleep(0.12)
            gate_evt.set()
            await executor.wait_idle(5)
        logger.info("判定依据：后置开始顺序=%s（期望 high<normal<low）", started)
        assert started.index("high-d") < started.index("normal-d")
        assert started.index("normal-d") < started.index("low-d")
    finally:
        await executor.stop()
        store.close()


# ---------------------------------------------------------------------------
# 4. 异常重启后的依赖恢复
# ---------------------------------------------------------------------------


async def test_dependency_state_recovered_after_restart(tmp_path):
    """崩溃重启：已成功前置不重复执行；blocked 后置不提前执行；
    running 前置恢复后续跑，成功后正常释放下游。"""
    store1, registry1, executor1, _, app1 = _make_stack(
        tmp_path, tick_interval=0.01)
    hang = asyncio.Event()

    async def block_then_ok(payload, token):
        await hang.wait()
        return {"attempt": payload["_attempt"]}

    registry1.register("block_ok", block_then_ok)
    await executor1.start()
    transport1 = httpx.ASGITransport(app=app1)
    async with httpx.AsyncClient(transport=transport1,
                                 base_url="http://old") as client:
        log_case("重启依赖恢复", batch="bp-restart", tasks=("pre", "post"))
        r = await client.post("/batches/submit", json=submit_body("bp-restart", [
            task("pre", "block_ok", {}),
            task("post", "echo", {}, depends_on=["pre"]),
        ]))
        ids = {t["idempotency_key"]: t["task_id"] for t in r.json()["tasks"]}
        await asyncio.sleep(0.12)
        pre = (await client.get(f"/tasks/{ids['pre']}")).json()
        post = (await client.get(f"/tasks/{ids['post']}")).json()
        assert pre["status"] == "running" and pre["attempts"] == 1
        assert post["status"] == "blocked", "依赖未满足时后置不得提前执行"
        logger.info("判定依据：崩溃前 pre=%s attempts=1, post=%s",
                    pre["status"], post["status"])
    # 模拟强杀：调度循环取消、DB 直接关闭，运行协程被抛弃
    executor1._scheduler.cancel()  # noqa: SLF001
    await asyncio.sleep(0.02)
    store1.close()

    store2, registry2, executor2, _, app2 = _make_stack(
        tmp_path, tick_interval=0.01)

    async def block_ok_again(payload, token):
        return {"attempt": payload["_attempt"]}

    registry2.register("block_ok", block_ok_again)
    await executor2.start()
    try:
        pre2 = await store2.get_task(ids["pre"])
        post2 = await store2.get_task(ids["post"])
        logger.info("判定依据：恢复后 pre=%s attempts=%d；post=%s（仍不得提前跑）",
                    pre2.status.value, pre2.attempts, post2.status.value)
        assert pre2.status.value == "pending" and pre2.attempts == 1
        assert post2.status.value == "blocked"

        hang.set()
        await executor2.wait_idle(5)
        pre3 = await store2.get_task(ids["pre"])
        post3 = await store2.get_task(ids["post"])
        logger.info("判定依据：最终 pre=%s attempts=%d；post=%s attempts=%d",
                    pre3.status.value, pre3.attempts,
                    post3.status.value, post3.attempts)
        assert pre3.status.value == "succeeded" and pre3.attempts == 2
        assert post3.status.value == "succeeded" and post3.attempts == 1
    finally:
        await executor2.stop()
        store2.close()


async def test_failed_dependency_skip_recovered_after_restart(tmp_path):
    """前置 failed 落库后、传播前崩溃：重启全量对齐让下游稳定进入 skipped。"""
    store1, registry1, executor1, _, app1 = _make_stack(tmp_path)
    await executor1.start()
    transport1 = httpx.ASGITransport(app=app1)
    async with httpx.AsyncClient(transport=transport1,
                                 base_url="http://old") as client:
        log_case("重启恢复-传播对齐", tasks=("bad", "down"))
        r = await client.post("/batches/submit", json=submit_body("bp-rec-skip", [
            task("bad", "always_fail", {}),
            task("down", "echo", {}, depends_on=["bad"]),
            task("grand", "echo", {}, depends_on=["down"]),
        ]))
        ids = {t["idempotency_key"]: t["task_id"] for t in r.json()["tasks"]}
        await executor1.wait_idle(5)
        bad = await store1.get_task(ids["bad"])
        assert bad.status.value == "failed"
    # 正常 stop 后模拟“另一次崩溃”：手工把 down/grand 重置为 blocked，
    # 模拟传播事务与崩溃交错后、启动恢复负责收敛的现场
    store1.conn.execute(
        "UPDATE tasks SET status='blocked', error_code=NULL, error_message=NULL, "
        "finished_at=NULL WHERE idempotency_key IN ('down','grand')")
    store1.conn.commit()
    executor1._scheduler.cancel()  # noqa: SLF001
    await asyncio.sleep(0.02)
    store1.close()

    store2, registry2, executor2, _, _ = _make_stack(tmp_path)
    recovered = await store2.recover_on_startup()
    logger.info("判定依据：启动恢复条数=%d（无 running 残留，传播对齐独立生效）",
                recovered)
    down = await store2.get_task(ids["down"])
    grand = await store2.get_task(ids["grand"])
    logger.info("判定依据：down=%s/%s grand=%s/%s",
                down.status.value, down.error_code,
                grand.status.value, grand.error_code)
    assert recovered == 0
    assert down.status.value == "skipped"
    assert down.error_code == "dependency_failed"
    assert grand.status.value == "skipped"
    assert grand.error_code == "dependency_failed"
    store2.close()


# ---------------------------------------------------------------------------
# 5. 规模场景
# ---------------------------------------------------------------------------


async def test_batch_with_150_tasks_multiple_levels(env):
    """单批次 150 个任务、6 层菱形依赖：提交幂等、全部成功、执行无重复。"""
    client = env["client"]
    n_per_layer = 25
    layers = 6
    specs: list[dict] = []
    for layer in range(layers):
        for i in range(n_per_layer):
            deps = None
            if layer > 0:
                # 每层任务依赖上一层的两个任务，形成多层菱形扇入/扇出
                deps = [
                    f"L{layer - 1}-{i % n_per_layer}",
                    f"L{layer - 1}-{(i + 1) % n_per_layer}",
                ]
            specs.append(task(f"L{layer}-{i}", "echo",
                              {"layer": layer, "i": i},
                              depends_on=deps))
    log_case("批次规模", layers=layers, per_layer=n_per_layer,
             total=len(specs))
    r = await client.post("/batches/submit", json=submit_body("bp-scale", specs))
    assert r.status_code == 201, r.text
    blocked_initial = sum(
        1 for t in r.json()["tasks"] if t["status"] == "blocked")
    logger.info("判定依据：提交 201，首层 pending=%d，其余 blocked=%d",
                len(specs) - blocked_initial, blocked_initial)
    assert blocked_initial == n_per_layer * (layers - 1)

    # 重复提交仍返回同一批
    r2 = await client.post("/batches/submit", json=submit_body("bp-scale", specs))
    assert r2.status_code == 200
    assert r2.json()["batch_id"] == r.json()["batch_id"]

    await env["executor"].wait_idle(15)
    states = await status_map(client, "bp-scale")
    assert len(states) == layers * n_per_layer
    assert all(v["status"] == "succeeded" for v in states.values())
    assert all(v["attempts"] == 1 for v in states.values()), "规模批次不得重复执行"
    logger.info("判定依据：%d 个任务全部 succeeded 且 attempts 均为 1", len(states))


async def test_new_batch_reuses_history_task_per_task_flag(env):
    """新批次可把历史已提交任务作为成员：新建任务 already_exists=false，
    历史任务 already_exists=true，批次级 created=true；重复提交后全部 true。"""
    client = env["client"]
    log_case("批次复用历史任务", history="hist-task")
    r0 = await client.post("/tasks/submit", json={
        "idempotency_key": "hist-task", "task_type": "echo", "payload": {"v": 7}})
    assert r0.status_code == 201
    hist_id = r0.json()["task_id"]

    body = submit_body("bp-reuse", [
        task("hist-task", payload={"v": 7}),  # 完全一致地复用
        task("new-down", payload={}, depends_on=["hist-task"]),
    ])
    r = await client.post("/batches/submit", json=body)
    assert r.status_code == 201, r.text
    by_key = {t["idempotency_key"]: t for t in r.json()["tasks"]}
    logger.info("判定依据：批次 created，hist 复用同 id=%s flags(hist=%s,new=%s)",
                by_key["hist-task"]["task_id"] == hist_id,
                by_key["hist-task"]["already_exists"],
                by_key["new-down"]["already_exists"])
    assert by_key["hist-task"]["task_id"] == hist_id
    assert by_key["hist-task"]["already_exists"] is True
    assert by_key["new-down"]["already_exists"] is False

    r2 = await client.post("/batches/submit", json=body)
    assert r2.status_code == 200
    flags = {t["idempotency_key"]: t["already_exists"]
             for t in r2.json()["tasks"]}
    assert flags == {"hist-task": True, "new-down": True}

    await env["executor"].wait_idle(5)
    got = await client.get(f"/tasks/{hist_id}")
    assert got.json()["attempts"] == 1, "历史任务被复用进批次不得重复执行"
