"""批量依赖编排测试。

覆盖：
- 依赖链式执行：全部前置成功才开始，按拓扑顺序推进；
- 批次重复提交与并发重复提交：同一组任务标识、同一套依赖、只执行一次；
- 批次内容不一致 -> 409 batch_conflict；
- 循环依赖 / 引用不存在 / 跨作用域依赖 -> 422 且 error_code 可区分，整体拒绝；
- 多前置部分成功部分失败/取消：后继 skipped，跳过原因稳定可区分，传递闭包一致；
- 依赖释放后仍受作用域并发上限约束；释放瞬间优先级生效；等待期间老化不饿死；
- 等待依赖期间取消：立即终态，后继级联 skipped；
- 依赖此前已提交（已完成/在跑）任务的批次；
- 异常重启：已完成前置不重复执行、依赖未满足不提前执行、跨重启状态一致；
- 单批次 120 个任务、4 层依赖的规模场景。

用例日志均打印输入与判定依据。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os

import httpx

from app.handlers import CancellationToken
from conftest import _make_stack, log_case

logger = logging.getLogger("test.batch")


def _batch_body(key: str, tasks: list[dict]) -> dict:
    return {"idempotency_key": key, "tasks": tasks}


def t(key: str, task_type: str = "echo", payload=None, *,
      scope="default", priority="normal", depends_on=None, **extra) -> dict:
    body = {
        "idempotency_key": key, "task_type": task_type,
        "payload": payload if payload is not None else {},
        "scope": scope, "priority": priority,
        "depends_on": depends_on or [],
    }
    body.update(extra)
    return body


# ---------------------------------------------------------------------------
# 1. 基本依赖语义
# ---------------------------------------------------------------------------

async def test_dependency_chain_runs_in_topological_order(env):
    """清洗 -> 聚合 -> 导出：每个任务必须在其全部前置成功后才执行。"""
    order: list[str] = []

    async def ordered(payload: dict, token: CancellationToken) -> dict:
        await token.wait_cancellable(0.03)
        order.append(payload["key"])
        return {"ran": payload["key"]}

    env["registry"].register("ordered", ordered)
    client = env["client"]
    log_case("依赖链顺序", layers=["clean", "agg", "export"])
    r = await client.post("/tasks/batch-submit", json=_batch_body("chain-1", [
        t("clean", "ordered", {"key": "clean"}),
        t("agg", "ordered", {"key": "agg"}, depends_on=["clean"]),
        t("export", "ordered", {"key": "export"}, depends_on=["agg", "clean"]),
    ]))
    assert r.status_code == 201, r.text
    ids = {x["idempotency_key"]: x["task_id"] for x in r.json()["tasks"]}
    await env["executor"].wait_idle(5)
    logger.info("判定依据：实际开始顺序=%s", order)
    assert order == ["clean", "agg", "export"], "后继不得早于前置执行"
    for key in ("clean", "agg", "export"):
        got = (await client.get(f"/tasks/{ids[key]}")).json()
        assert got["status"] == "succeeded" and got["attempts"] == 1


async def test_batch_can_depend_on_previously_submitted_tasks(env):
    """被依赖任务可以是此前单独提交的：已完成的立即放行，在跑的等其成功后放行。"""
    client = env["client"]
    log_case("依赖已提交任务", old_done="prev-done", old_running="prev-running")
    # 已完成任务
    r1 = await client.post("/tasks/submit", json={
        "idempotency_key": "prev-done", "task_type": "echo", "payload": {"v": 1}})
    done_id = r1.json()["task_id"]
    await env["executor"].wait_idle(5)
    # 在跑（慢速）任务
    await client.post("/tasks/submit", json={
        "idempotency_key": "prev-running", "task_type": "slow",
        "payload": {"duration": 0.3}, "timeout_seconds": 5})
    await asyncio.sleep(0.12)
    r2 = await client.post("/tasks/batch-submit", json=_batch_body("after-prev", [
        t("child-done", depends_on=["prev-done"]),
        t("child-run", depends_on=["prev-running", "prev-done"]),
    ]))
    assert r2.status_code == 201, r2.text
    cdone, crun = r2.json()["tasks"]
    await asyncio.sleep(0.05)
    mid = (await client.get(f"/tasks/{cdone['task_id']}")).json()
    mid_run = (await client.get(f"/tasks/{crun['task_id']}")).json()
    logger.info("判定依据：前置已完成的后继 status=%s；前置在跑的后继 status=%s",
                mid["status"], mid_run["status"])
    assert mid["status"] == "succeeded", "只依赖已完成任务的后继应立即执行"
    assert mid_run["status"] == "pending", "前置仍在跑时后继必须等待"
    await env["executor"].wait_idle(5)
    final = (await client.get(f"/tasks/{crun['task_id']}")).json()
    assert final["status"] == "succeeded"
    # 前置任务本身不受影响，只执行一次
    assert (await client.get(f"/tasks/{done_id}")).json()["attempts"] == 1
    # 外部已提交任务是“依赖”而非“批次成员”：批次只含声明的两个任务
    batch = (await client.get("/batches/after-prev")).json()
    member_keys = {x["idempotency_key"] for x in batch["tasks"]}
    logger.info("判定依据：批次成员=%s（外部前置 prev-done/prev-running 不并入）",
                member_keys)
    assert member_keys == {"child-done", "child-run"}
    deps = await env["service"].get_dependencies(crun["task_id"])
    assert deps == ["prev-done", "prev-running"] or set(deps) == {
        "prev-done", "prev-running"}


# ---------------------------------------------------------------------------
# 2. 批次幂等
# ---------------------------------------------------------------------------

async def test_duplicate_batch_returns_same_ids_and_runs_once(env):
    """同 batch_key 重复提交（任务顺序打乱）返回同一组任务标识，执行只发生一次。"""
    client = env["client"]
    first = [
        t("bk-clean", depends_on=[]),
        t("bk-agg", depends_on=["bk-clean"]),
        t("bk-export", depends_on=["bk-agg"]),
    ]
    log_case("批次重复提交", batch_key="dup-batch", task_count=3)
    r1 = await client.post("/tasks/batch-submit", json=_batch_body("dup-batch", first))
    assert r1.status_code == 201
    ids_first = {x["idempotency_key"]: x["task_id"] for x in r1.json()["tasks"]}

    # 打乱任务与依赖声明顺序，语义仍相同
    second = [
        t("bk-export", depends_on=["bk-agg"]),
        t("bk-clean"),
        t("bk-agg", depends_on=["bk-clean"]),
    ]
    r2 = await client.post("/tasks/batch-submit", json=_batch_body("dup-batch", second))
    assert r2.status_code == 200, "批次重复提交应返回 200"
    assert r2.json()["already_exists"] is True
    ids_second = {x["idempotency_key"]: x["task_id"] for x in r2.json()["tasks"]}
    logger.info("判定依据：两次任务标识一致=%s", ids_first == ids_second)
    assert ids_first == ids_second, "重复批次必须返回同一组任务标识"

    await env["executor"].wait_idle(5)
    for key, tid in ids_first.items():
        got = (await client.get(f"/tasks/{tid}")).json()
        assert got["status"] == "succeeded" and got["attempts"] == 1, \
            f"{key} 因批次重复提交被重复执行"


async def test_concurrent_duplicate_batch_single_set(env):
    """10 路并发重复提交同一批次：恰好一次 201，任务标识集合唯一，各任务只执行一次。"""
    client = env["client"]
    body = _batch_body("concurrent-batch", [
        t("cc-1", "flaky", {"fail_times": 0}),
        t("cc-2", "flaky", {"fail_times": 0}, depends_on=["cc-1"]),
        t("cc-3", "flaky", {"fail_times": 0}, depends_on=["cc-2"]),
    ])
    log_case("并发重复批次", concurrency=10, task_count=3)
    responses = await asyncio.gather(*(
        client.post("/tasks/batch-submit", json=body) for _ in range(10)
    ))
    statuses = [r.status_code for r in responses]
    id_sets = {
        frozenset((x["idempotency_key"], x["task_id"]) for x in r.json()["tasks"])
        for r in responses
    }
    logger.info("判定依据：201 数量=%d 200 数量=%d 不同任务集合数=%d",
                statuses.count(201), statuses.count(200), len(id_sets))
    assert statuses.count(201) == 1 and statuses.count(200) == 9
    assert len(id_sets) == 1, "并发重复提交必须指向同一组任务"

    await env["executor"].wait_idle(5)
    only = next(iter(id_sets))
    for _key, tid in only:
        got = (await client.get(f"/tasks/{tid}")).json()
        assert got["attempts"] == 1, "并发重复批次不得造成重复执行"


async def test_batch_content_mismatch_rejected(env):
    """同 batch_key 但内容（载荷/依赖）不一致：409 batch_conflict，拒绝且不新增任务。"""
    client = env["client"]
    log_case("批次内容冲突", batch_key="conflict-batch")
    await client.post("/tasks/batch-submit", json=_batch_body("conflict-batch", [
        t("bc-1", payload={"v": 1}),
        t("bc-2", depends_on=["bc-1"]),
    ]))
    # 载荷不同
    r1 = await client.post("/tasks/batch-submit", json=_batch_body("conflict-batch", [
        t("bc-1", payload={"v": 2}),
        t("bc-2", depends_on=["bc-1"]),
    ]))
    # 依赖关系不同
    r2 = await client.post("/tasks/batch-submit", json=_batch_body("conflict-batch", [
        t("bc-1", payload={"v": 1}),
        t("bc-2"),
    ]))
    logger.info("判定依据：载荷冲突 HTTP=%d code=%s；依赖冲突 HTTP=%d code=%s",
                r1.status_code, r1.json()["error"]["code"],
                r2.status_code, r2.json()["error"]["code"])
    assert (r1.status_code, r1.json()["error"]["code"]) == (409, "batch_conflict")
    assert (r2.status_code, r2.json()["error"]["code"]) == (409, "batch_conflict")
    # 批次查询仍只包含原始两个任务
    got = (await client.get("/batches/conflict-batch")).json()
    assert len(got["tasks"]) == 2


# ---------------------------------------------------------------------------
# 3. 提交时整体拒绝
# ---------------------------------------------------------------------------

async def test_cyclic_dependency_rejected(env):
    client = env["client"]
    log_case("循环依赖", cycle=["cy-a", "cy-b", "cy-c"])
    r = await client.post("/tasks/batch-submit", json=_batch_body("cyc-batch", [
        t("cy-a", depends_on=["cy-c"]),
        t("cy-b", depends_on=["cy-a"]),
        t("cy-c", depends_on=["cy-b"]),
    ]))
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "cyclic_dependency"
    logger.info("判定依据：HTTP=%d code=%s", r.status_code,
                r.json()["error"]["code"])
    # 整体拒绝：任务一个都不能落库
    assert (await client.get("/tasks/by-key/cy-a")).status_code == 404


async def test_dependency_not_found_rejected(env):
    client = env["client"]
    log_case("引用不存在", missing="ghost")
    r = await client.post("/tasks/batch-submit", json=_batch_body("missing-batch", [
        t("mf-1", depends_on=["ghost"]),
    ]))
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "dependency_not_found"
    logger.info("判定依据：HTTP=%d code=%s", r.status_code,
                r.json()["error"]["code"])
    assert (await client.get("/tasks/by-key/mf-1")).status_code == 404


async def test_cross_scope_dependency_rejected(env):
    client = env["client"]
    log_case("跨作用域依赖", scopes=["scope-a", "scope-b"])
    r = await client.post("/tasks/batch-submit", json=_batch_body("cross-batch", [
        t("cs-a", scope="scope-a"),
        t("cs-b", scope="scope-b", depends_on=["cs-a"]),
    ]))
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "cross_scope_dependency"
    logger.info("判定依据：HTTP=%d code=%s", r.status_code,
                r.json()["error"]["code"])
    assert (await client.get("/tasks/by-key/cs-a")).status_code == 404
    assert (await client.get("/tasks/by-key/cs-b")).status_code == 404


async def test_duplicate_key_inside_batch_rejected(env):
    client = env["client"]
    log_case("批次内键重复", key="dup-inner")
    r = await client.post("/tasks/batch-submit", json=_batch_body("inner-dup", [
        t("dup-inner"), t("dup-inner"),
    ]))
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "duplicate_task_key"
    logger.info("判定依据：HTTP=%d code=%s", r.status_code,
                r.json()["error"]["code"])


# ---------------------------------------------------------------------------
# 4. 失败/取消传播与 skipped 终态
# ---------------------------------------------------------------------------

async def test_partial_dependency_failure_makes_skipped_terminal(env):
    """两个前置一个成功一个失败：后继进入 skipped（区别于 failed），且永不执行。"""
    client = env["client"]
    log_case("多前置部分失败", deps=["ok-1", "bad-1"])
    r = await client.post("/tasks/batch-submit", json=_batch_body("partial-fail", [
        t("ok-1"),
        t("bad-1", "always_fail"),
        t("join-1", depends_on=["ok-1", "bad-1"]),
    ]))
    ids = {x["idempotency_key"]: x["task_id"] for x in r.json()["tasks"]}
    await env["executor"].wait_idle(5)
    ok = (await client.get(f"/tasks/{ids['ok-1']}")).json()
    bad = (await client.get(f"/tasks/{ids['bad-1']}")).json()
    join = (await client.get(f"/tasks/{ids['join-1']}")).json()
    logger.info("判定依据：ok=%s bad=%s join=%s error_code=%s attempts=%d",
                ok["status"], bad["status"], join["status"],
                join["error_code"], join["attempts"])
    assert join["status"] == "skipped", "任一前置失败后继必须 skipped"
    assert join["error_code"] == "dependency_failed"
    assert join["attempts"] == 0, "skipped 任务从未被领取执行"
    assert bad["status"] == "failed", "真正执行失败的任务仍是 failed，二者可区分"
    await asyncio.sleep(0.2)
    stable = (await client.get(f"/tasks/{ids['join-1']}")).json()
    assert stable["status"] == "skipped", "skipped 是终态，不得再翻转执行"


async def test_skipped_propagates_transitively_with_distinct_reasons(env):
    """failed -> skipped -> skipped 链：传递闭包全部确定，跳过原因分层可区分。"""
    client = env["client"]
    log_case("跳过传递闭包", chain=["root-fail", "mid", "leaf"])
    r = await client.post("/tasks/batch-submit", json=_batch_body("skip-chain", [
        t("root-fail", "always_fail"),
        t("mid", depends_on=["root-fail"]),
        t("leaf", depends_on=["mid"]),
    ]))
    ids = {x["idempotency_key"]: x["task_id"] for x in r.json()["tasks"]}
    await env["executor"].wait_idle(5)
    mid = (await client.get(f"/tasks/{ids['mid']}")).json()
    leaf = (await client.get(f"/tasks/{ids['leaf']}")).json()
    logger.info("判定依据：mid=%s/%s leaf=%s/%s",
                mid["status"], mid["error_code"], leaf["status"], leaf["error_code"])
    assert (mid["status"], mid["error_code"]) == ("skipped", "dependency_failed")
    assert (leaf["status"], leaf["error_code"]) == ("skipped", "dependency_skipped")


async def test_cancelled_dependency_makes_dependency_cancelled_skip(env):
    """前置被取消（从未运行）：后继 skipped，原因 dependency_cancelled。"""
    client = env["client"]

    async def hang_no_check(payload: dict, token: CancellationToken) -> dict:
        # 阻塞在非取消检查点：测试取消“仍在跑的前置”时任务确定为 running
        await asyncio.Event().wait()
        return {}

    env["registry"].register("hang_no_check", hang_no_check)
    log_case("前置取消传播", cancelled="cancel-root")
    r = await client.post("/tasks/batch-submit", json=_batch_body("cancel-dep", [
        t("cancel-root", "hang_no_check", {}, timeout_seconds=30),
        t("cancel-child", depends_on=["cancel-root"]),
    ]))
    ids = {x["idempotency_key"]: x["task_id"] for x in r.json()["tasks"]}
    await asyncio.sleep(0.12)
    running = (await client.get(f"/tasks/{ids['cancel-root']}")).json()
    assert running["status"] == "running"
    cr = await client.post(f"/tasks/{ids['cancel-root']}/cancel")
    assert cr.json()["cancel_requested"] is True
    # 停止执行器以中断阻塞处理器，中断后由执行器走“取消已生效”落 cancelled
    await env["executor"].stop()
    await asyncio.sleep(0.05)
    root = (await client.get(f"/tasks/{ids['cancel-root']}")).json()
    child = (await client.get(f"/tasks/{ids['cancel-child']}")).json()
    logger.info("判定依据：root=%s child=%s/%s",
                root["status"], child["status"], child["error_code"])
    assert root["status"] == "cancelled"
    assert (child["status"], child["error_code"]) == (
        "skipped", "dependency_cancelled")
    assert child["attempts"] == 0


async def test_cancel_waiting_task_settles_immediately(env):
    """任务还在等依赖时被取消：立刻确定终态，其后继立即级联 skipped，不挂起。"""
    client = env["client"]
    log_case("等待中取消", chain=["wait-root", "wait-mid", "wait-leaf"])
    r = await client.post("/tasks/batch-submit", json=_batch_body("waiting-cancel", [
        t("wait-root", "slow", {"duration": 5}, timeout_seconds=10),
        t("wait-mid", depends_on=["wait-root"]),
        t("wait-leaf", depends_on=["wait-mid"]),
    ]))
    ids = {x["idempotency_key"]: x["task_id"] for x in r.json()["tasks"]}
    await asyncio.sleep(0.12)
    root_mid = (await client.get(f"/tasks/{ids['wait-root']}")).json()
    assert root_mid["status"] == "running"
    t0 = asyncio.get_running_loop().time()
    cr = await client.post(f"/tasks/{ids['wait-mid']}/cancel")
    assert cr.json()["status"] == "cancelled", "等待中的取消必须同步落终态"
    leaf = (await client.get(f"/tasks/{ids['wait-leaf']}")).json()
    elapsed = asyncio.get_running_loop().time() - t0
    logger.info("判定依据：wait-mid=%s wait-leaf=%s/%s 级联耗时=%.3fs",
                cr.json()["status"], leaf["status"], leaf["error_code"], elapsed)
    assert leaf["status"] == "skipped", "取消的等待任务其后继必须立即确定"
    assert leaf["error_code"] == "dependency_cancelled"
    assert elapsed < 0.2, "等待依赖期间取消不能挂起"
    # 清理：取消仍在跑的根任务
    await client.post(f"/tasks/{ids['wait-root']}/cancel")
    await env["executor"].wait_idle(5)


# ---------------------------------------------------------------------------
# 5. 与调度能力兼容：并发上限 / 优先级 / 老化
# ---------------------------------------------------------------------------

async def test_released_tasks_respect_scope_concurrency(tmp_path):
    """前置释放瞬间 4 个后继同时就绪：同 scope 在跑数始终不超过上限 2。"""
    store, registry, executor, _svc, app = _make_stack(
        tmp_path, scope_concurrency={"rel": 2}, default_concurrency=1)
    max_seen = {"v": 0}
    inflight = {"v": 0}
    lock = asyncio.Lock()

    async def guarded(payload: dict, token: CancellationToken) -> dict:
        async with lock:
            inflight["v"] += 1
            max_seen["v"] = max(max_seen["v"], inflight["v"])
        await token.wait_cancellable(0.2)
        async with lock:
            inflight["v"] -= 1
        return {"ok": True}

    registry.register("guarded", guarded)
    await executor.start()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://t") as client:
            log_case("释放后并发上限", scope="rel", limit=2, children=4)
            await client.post("/tasks/batch-submit", json=_batch_body("rel-batch", [
                t("rel-root", "guarded", scope="rel"),
                *[t(f"rel-c{i}", "guarded", scope="rel",
                    depends_on=["rel-root"]) for i in range(4)],
            ]))
            await executor.wait_idle(8)
        logger.info("判定依据：依赖释放后观测到的最大在跑数=%d（期望恰好 2）",
                    max_seen["v"])
        assert max_seen["v"] == 2, "后继就绪后必须仍受作用域配额约束"
    finally:
        await executor.stop()
        store.close()


async def test_priority_effective_when_dependencies_release(tmp_path):
    """同一批后置任务同时就绪：high 先于 normal 先于 low（并发=1 下严格有序）。"""
    store, registry, executor, _svc, app = _make_stack(
        tmp_path, default_concurrency=1)
    order: list[str] = []

    async def ordered(payload: dict, token: CancellationToken) -> dict:
        order.append(payload["key"])
        await token.wait_cancellable(0.05)
        return {}

    registry.register("ordered", ordered)
    await executor.start()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://t") as client:
            log_case("释放时优先级", children=["p-low", "p-mid", "p-high"])
            await client.post("/tasks/batch-submit", json=_batch_body("prio-batch", [
                t("p-root", "ordered", {"key": "p-root"}),
                t("p-low", "ordered", {"key": "p-low"}, priority="low",
                  depends_on=["p-root"]),
                t("p-mid", "ordered", {"key": "p-mid"}, priority="normal",
                  depends_on=["p-root"]),
                t("p-high", "ordered", {"key": "p-high"}, priority="high",
                  depends_on=["p-root"]),
            ]))
            await executor.wait_idle(8)
        children = [k for k in order if k != "p-root"]
        logger.info("判定依据：后继实际开始顺序=%s", children)
        assert children == ["p-high", "p-mid", "p-low"]
    finally:
        await executor.stop()
        store.close()


async def test_waiting_task_aging_not_starved_after_release(tmp_path):
    """等待依赖期间累积的等待时间参与老化：low 后继带着老化值就绪后，
    相对“释放之后才到达”的新高优先级任务不会无限靠后。"""
    # aging_interval 极小：依赖等待 0.5s 即可累积 500 老化分，显著改变就绪时的
    # 排序位置（score = 优先级序数×1000 − 等待秒数/aging_interval）。
    store, registry, executor, _svc, app = _make_stack(
        tmp_path, default_concurrency=2, aging_interval=0.0001)
    finished: list[str] = []
    release = asyncio.Event()

    async def gated(payload: dict, token: CancellationToken) -> dict:
        await release.wait()
        return {}

    async def timed(payload: dict, token: CancellationToken) -> dict:
        await token.wait_cancellable(0.05)
        finished.append(payload["key"])
        return {}

    registry.register("gated", gated)
    registry.register("timed", timed)
    await executor.start()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://t") as client:
            log_case("释放后老化防饿死", wait=0.5, aging_interval=0.0001,
                     late_highs=8)
            await client.post("/tasks/batch-submit", json=_batch_body("age-batch", [
                t("age-root", "gated", {}),
                t("age-low", "timed", {"key": "age-low"}, priority="low",
                  depends_on=["age-root"]),
            ]))
            await asyncio.sleep(0.5)  # low 累积 ~5000 老化分，越过 high 的 2000 基础分
            release.set()
            # 释放后到达 8 个全新高优先级任务（无老化值）
            for i in range(8):
                await client.post("/tasks/submit", json={
                    "idempotency_key": f"late-high-{i}", "task_type": "timed",
                    "payload": {"key": f"late-high-{i}"}, "priority": "high"})
            await executor.wait_idle(8)
        pos = finished.index("age-low")
        late_before = [k for k in finished[:pos] if k.startswith("late-high")]
        logger.info(
            "判定依据：完成顺序=%s；age-low 位置=%d，排在它前面的迟到高任务数=%d"
            "（无老化时会排在全部 8 个之后，即位置 8）",
            finished, pos, len(late_before))
        assert len(late_before) < 8, \
            "依赖等待期间累积的老化必须让已就绪后继前移，而非永远排在新高任务之后"
    finally:
        release.set()
        await executor.stop()
        store.close()


# ---------------------------------------------------------------------------
# 6. 异常重启后的依赖恢复
# ---------------------------------------------------------------------------

async def test_dependencies_consistent_across_restart(tmp_path):
    """重启后：已完成前置不重复执行；后继在前置未成功前绝不提前执行；
    跨重启链路最终一致完成。"""
    counter_file = str(tmp_path / "runs.json")

    def _counts() -> dict:
        if not os.path.exists(counter_file):
            return {}
        with open(counter_file, encoding="utf-8") as fh:
            return json.load(fh)

    def _bump(key: str) -> None:
        counts = _counts()
        counts[key] = counts.get(key, 0) + 1
        with open(counter_file, "w", encoding="utf-8") as fh:
            json.dump(counts, fh)

    store1, registry1, executor1, _svc1, app1 = _make_stack(
        tmp_path, base_backoff=0.05)
    hang = asyncio.Event()

    async def count_ok(payload: dict, token: CancellationToken) -> dict:
        _bump(payload["key"])
        return {"ran": payload["key"]}

    async def count_then_hang(payload: dict, token: CancellationToken) -> dict:
        _bump(payload["key"])
        await hang.wait()
        return {"ran": payload["key"]}

    registry1.register("counted", count_ok)
    registry1.register("counted_hang", count_then_hang)
    db = str(tmp_path / "tasks.db")

    await executor1.start()
    transport1 = httpx.ASGITransport(app=app1)
    async with httpx.AsyncClient(transport=transport1,
                                 base_url="http://old") as client:
        log_case("重启依赖恢复", chain=["rc-a", "rc-b", "rc-c"])
        r = await client.post("/tasks/batch-submit", json=_batch_body("rc-batch", [
            t("rc-a", "counted", {"key": "rc-a"}),
            t("rc-b", "counted_hang", {"key": "rc-b"}, depends_on=["rc-a"]),
            t("rc-c", "counted", {"key": "rc-c"}, depends_on=["rc-b"]),
        ]))
        ids = {x["idempotency_key"]: x["task_id"] for x in r.json()["tasks"]}
        # 等 rc-a 完成、rc-b 已在依赖满足后被领取并挂起；rc-c 必须仍 pending
        for _ in range(50):
            await asyncio.sleep(0.02)
            b = await store1.get_task(ids["rc-b"])
            if b.status.value == "running":
                break
        a = await store1.get_task(ids["rc-a"])
        b = await store1.get_task(ids["rc-b"])
        c = await store1.get_task(ids["rc-c"])
        logger.info("判定依据：崩溃现场 a=%s b=%s c=%s 执行计数=%s",
                    a.status.value, b.status.value, c.status.value, _counts())
        assert a.status.value == "succeeded"
        assert b.status.value == "running"
        assert c.status.value == "pending", "依赖未满足的 rc-c 不得提前执行"
        assert c.attempts == 0

    # 模拟进程强杀
    executor1._scheduler.cancel()  # noqa: SLF001
    for w in list(executor1._workers):  # noqa: SLF001
        w.cancel()
    await asyncio.sleep(0.05)
    store1.close()

    # --- 新进程 ---
    store2, registry2, executor2, _svc2, app2 = _make_stack(
        tmp_path, base_backoff=0.05)
    registry2.register("counted", count_ok)

    async def count_plain(payload: dict, token: CancellationToken) -> dict:
        _bump(payload["key"])
        return {"ran": payload["key"]}

    registry2.register("counted_hang", count_plain)

    recovered = await store2.recover_on_startup()
    logger.info("判定依据：新进程恢复 running 条数=%d", recovered)
    assert recovered == 1
    a2 = await store2.get_task(ids["rc-a"])
    c2 = await store2.get_task(ids["rc-c"])
    assert a2.status.value == "succeeded" and a2.attempts == 1
    assert c2.status.value == "pending", "重启后 rc-c 仍须等待 rc-b 重新成功"

    await executor2.start()
    try:
        await executor2.wait_idle(8)
        final = {k: await store2.get_task(tid) for k, tid in ids.items()}
        logger.info("判定依据：终态 a=%s b=%s c=%s 跨进程执行计数=%s",
                    final["rc-a"].status.value, final["rc-b"].status.value,
                    final["rc-c"].status.value, _counts())
        assert all(v.status.value == "succeeded" for v in final.values())
        counts = _counts()
        assert counts.get("rc-a") == 1, "已完成前置跨重启不得重复执行"
        assert counts.get("rc-b") == 2, "崩溃时执行 1 次 + 恢复后执行 1 次"
        assert counts.get("rc-c") == 1, "rc-c 只能在依赖满足后执行一次"
        assert final["rc-c"].attempts == 1
    finally:
        await executor2.stop()
        store2.close()


async def test_skip_state_survives_restart(tmp_path):
    """崩溃前已确定的 skipped 与依赖边在重启后保持，不会重新执行。"""
    store1, registry1, executor1, _svc1, app1 = _make_stack(tmp_path)
    await executor1.start()
    transport1 = httpx.ASGITransport(app=app1)
    async with httpx.AsyncClient(transport=transport1,
                                 base_url="http://old") as client:
        log_case("skipped 跨重启", chain=["sx-bad", "sx-child"])
        r = await client.post("/tasks/batch-submit", json=_batch_body("sx-batch", [
            t("sx-bad", "always_fail"),
            t("sx-child", depends_on=["sx-bad"]),
        ]))
        ids = {x["idempotency_key"]: x["task_id"] for x in r.json()["tasks"]}
        await executor1.wait_idle(5)
        pre = await store1.get_task(ids["sx-child"])
        assert pre.status.value == "skipped"
    executor1._scheduler.cancel()  # noqa: SLF001
    await asyncio.sleep(0.02)
    store1.close()

    store2, registry2, executor2, _svc2, _app2 = _make_stack(tmp_path)
    recovered = await store2.recover_on_startup()
    await store2.propagate_skipped()
    got = await store2.get_task(ids["sx-child"])
    deps = await store2.list_dependencies(ids["sx-child"])
    logger.info("判定依据：恢复条数=%d 重启后 child=%s/%s 依赖边=%s",
                recovered, got.status.value, got.error_code, deps)
    assert recovered == 0
    assert got.status.value == "skipped"
    assert got.attempts == 0
    assert deps == ["sx-bad"]
    store2.close()


# ---------------------------------------------------------------------------
# 7. 规模场景
# ---------------------------------------------------------------------------

async def test_batch_with_120_tasks_and_multiple_layers(env):
    """单批次 120 个任务、4 层菱形依赖：全部成功且只执行一次，依赖顺序不被违反。"""
    client = env["client"]
    layers, width, depth = 120, 30, 4
    started: dict[str, float] = {}
    lock = asyncio.Lock()

    async def layered(payload: dict, token: CancellationToken) -> dict:
        async with lock:
            started[payload["key"]] = asyncio.get_running_loop().time()
        return {}

    env["registry"].register("layered", layered)
    tasks = []
    for layer in range(depth):
        for i in range(width):
            key = f"L{layer}-{i}"
            deps = [f"L{layer-1}-{i}"] if layer > 0 else []
            tasks.append(t(key, "layered", {"key": key}, depends_on=deps))
    log_case("批次规模", total=layers, width=width, depth=depth)
    r = await client.post("/tasks/batch-submit", json=_batch_body("big-batch", tasks))
    assert r.status_code == 201, r.text
    assert len(r.json()["tasks"]) == layers
    ids = {x["idempotency_key"]: x["task_id"] for x in r.json()["tasks"]}
    await env["executor"].wait_idle(15)
    statuses = {}
    for key, tid in ids.items():
        statuses[key] = (await client.get(f"/tasks/{tid}")).json()
    logger.info("判定依据：终态分布=%s",
                {s: sum(1 for v in statuses.values() if v["status"] == s)
                 for s in {"succeeded", "failed", "skipped", "pending", "running"}})
    assert all(v["status"] == "succeeded" for v in statuses.values())
    assert all(v["attempts"] == 1 for v in statuses.values())
    # 每条纵向链严格按层启动
    for layer in range(1, depth):
        for i in range(width):
            assert started[f"L{layer}-{i}"] >= started[f"L{layer-1}-{i}"] - 1e-6
