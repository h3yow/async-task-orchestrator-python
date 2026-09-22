"""调度执行器：优先级调度、作用域并发控制、超时与重试。

调度规则：
- 每个 tick 从持久层取所有可运行任务（pending 且到 scheduled_at），按
  “优先级权重 - 老化值”排序，权重越小越先执行；
- 老化：任务自创建起每 aging_interval 秒等待，权重减 1。低优先级任务等待
  足够久后权重会越过新到达的高优先级任务，因此持续到达的高优先级任务不会
  让低优先级任务无限期等待（无饥饿）；
- 每个 scope 持有独立 asyncio.Semaphore，保证同作用域并发上限不被突破；
- 领取（pending->running，attempts+1）是持久层条件 UPDATE，同一任务只会
  被一个协程领取；进程重启后由 recover_on_startup 兜底，不丢失不重复。
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable

from .handlers import (
    CancellationToken,
    FatalTaskError,
    HandlerRegistry,
    RetryableTaskError,
)
from .models import PRIORITY_RANK, TaskRecord, TaskStatus, utcnow
from .store import TaskStore

logger = logging.getLogger("orchestrator.executor")


class Executor:
    """后台调度器 + 每 scope 并发池。"""

    def __init__(
        self,
        store: TaskStore,
        registry: HandlerRegistry,
        scope_concurrency: dict[str, int] | None = None,
        default_concurrency: int = 2,
        tick_interval: float = 0.02,
        base_backoff: float = 0.2,
        backoff_factor: float = 2.0,
        max_backoff: float = 30.0,
        aging_interval: float = 1.0,
        stop_grace: float = 5.0,
    ) -> None:
        self._store = store
        self._registry = registry
        self._scope_concurrency = dict(scope_concurrency or {})
        self._default_concurrency = default_concurrency
        self._tick_interval = tick_interval
        self._base_backoff = base_backoff
        self._backoff_factor = backoff_factor
        self._max_backoff = max_backoff
        self._aging_interval = aging_interval
        self._stop_grace = stop_grace

        self._semaphores: dict[str, asyncio.Semaphore] = {}
        self._tokens: dict[str, CancellationToken] = {}
        self._inflight: set[str] = set()
        self._workers: set[asyncio.Task] = set()
        self._scheduler: asyncio.Task | None = None
        self._stopping = False

    # -- 生命周期 -----------------------------------------------------------

    async def start(self) -> None:
        """启动调度循环；先做重启恢复。"""
        recovered = await self._store.recover_on_startup()
        if recovered:
            logger.warning("启动恢复：%d 个未完成任务已重新入队", recovered)
        self._stopping = False
        self._scheduler = asyncio.create_task(self._run_loop(), name="scheduler")

    async def stop(self) -> None:
        """优雅停止：停止派发；在宽限期内等待在跑任务落库，超时则中断
        （中断的任务保持 running，下次启动恢复，状态仍可预期）。"""
        self._stopping = True
        if self._scheduler is not None:
            self._scheduler.cancel()
            try:
                await self._scheduler
            except asyncio.CancelledError:
                pass
        if self._workers:
            _, pending = await asyncio.wait(
                list(self._workers), timeout=self._stop_grace
            )
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

    # -- 取消信号 -----------------------------------------------------------

    def signal_cancel(self, task_id: str) -> bool:
        """置位运行中任务的取消令牌。返回是否找到在跑令牌。"""
        token = self._tokens.get(task_id)
        if token is not None:
            token.event.set()
            return True
        return False

    @property
    def inflight_count(self) -> int:
        return len(self._inflight)

    async def wait_idle(self, timeout: float = 5.0) -> None:
        """等待直到没有在跑任务且没有非终态任务（含退避等待中；测试辅助）。"""
        deadline = utcnow().timestamp() + timeout
        while utcnow().timestamp() < deadline:
            if not self._inflight and await self._store.count_active() == 0:
                return
            await asyncio.sleep(self._tick_interval)
        raise TimeoutError("等待执行器空闲超时")

    # -- 调度主循环 ---------------------------------------------------------

    def _semaphore_for(self, scope: str) -> asyncio.Semaphore:
        sem = self._semaphores.get(scope)
        if sem is None:
            limit = self._scope_concurrency.get(scope, self._default_concurrency)
            sem = asyncio.Semaphore(limit)
            self._semaphores[scope] = sem
        return sem

    def _dispatch_order(self, tasks: Iterable[TaskRecord]) -> list[TaskRecord]:
        now = utcnow()

        def score(task: TaskRecord) -> tuple[float, str, str]:
            waited = (now - task.created_at).total_seconds()
            aging = waited / self._aging_interval
            # 权重越小越优先；同权重按入队时间、再按任务 ID，保证确定性
            return (
                PRIORITY_RANK[task.priority] * 1000 - aging,
                task.scheduled_at.isoformat(),
                task.id,
            )

        return sorted(tasks, key=score)

    async def _run_loop(self) -> None:
        while not self._stopping:
            try:
                runnable = await self._store.list_runnable()
                for task in self._dispatch_order(runnable):
                    if task.id in self._inflight or self._stopping:
                        continue
                    sem = self._semaphore_for(task.scope)
                    # 调度循环是唯一的信号量消费者且整个派发段内无 await（领取
                    # 发生在工作协程中），locked() 与后续之间不会有其他协程插入，
                    # 因此作用域上限不会被突破；额度占满时下个 tick 再按老化重排。
                    if sem.locked():
                        continue
                    await sem.acquire()
                    self._inflight.add(task.id)
                    worker = asyncio.create_task(
                        self._run_task(task.id), name=f"worker-{task.id}"
                    )
                    self._workers.add(worker)
                    worker.add_done_callback(self._on_worker_done(worker, task.id, sem))
            except Exception:
                logger.exception("调度 tick 异常")
            await asyncio.sleep(self._tick_interval)

    def _on_worker_done(self, worker: asyncio.Task, task_id: str, sem: asyncio.Semaphore):
        def _cb(_: asyncio.Task) -> None:
            self._workers.discard(worker)
            self._inflight.discard(task_id)
            self._tokens.pop(task_id, None)
            sem.release()

        return _cb

    # -- 单任务执行 ---------------------------------------------------------

    def _backoff_for(self, attempts: int) -> float:
        delay = self._base_backoff * self._backoff_factor ** max(0, attempts - 1)
        return min(delay, self._max_backoff)

    async def _run_task(self, task_id: str) -> None:
        record = await self._store.claim_task(task_id)
        if record is None:
            # 已被取消/已被领取/退避未到（理论上不会，双保险）
            return
        logger.info(
            "领取任务 task_id=%s type=%s attempt=%d/%d priority=%s scope=%s",
            record.id, record.task_type, record.attempts, record.max_attempts,
            record.priority.value, record.scope,
        )
        handler = self._registry.get(record.task_type)
        token = CancellationToken()
        self._tokens[task_id] = token

        if handler is None:
            # 未注册处理器视为不可重试失败，避免任务无限重试占资源
            await self._store.mark_failed(
                task_id,
                error_code="handler_not_found",
                error_message=f"未注册的 task_type: {record.task_type}",
                retryable=False,
                backoff_seconds=0.0,
            )
            return

        payload = dict(record.payload)
        payload["_attempt"] = record.attempts
        try:
            result = await asyncio.wait_for(
                handler(payload, token), timeout=record.timeout_seconds
            )
        except asyncio.CancelledError:
            fresh = await self._store.get_task(task_id)
            if fresh is not None and fresh.cancel_requested:
                settled = await self._store.mark_cancelled_if_running(
                    task_id, "运行中响应取消请求"
                )
                logger.info("任务 %s 取消落库: %s", task_id,
                            settled.status if settled else "竞争失败")
            else:
                # 停止期被中断而非用户取消：保持 running，交由下次启动恢复
                logger.warning("任务 %s 随停止中断，保持 running 等待恢复", task_id)
            return
        except TimeoutError:
            backoff = self._backoff_for(record.attempts)
            settled = await self._store.mark_timed_out(task_id, backoff)
            logger.warning("任务 %s 执行超时，退避 %.3fs 后状态=%s",
                           task_id, backoff, settled.status if settled else "?")
            return
        except FatalTaskError as exc:
            settled = await self._store.mark_failed(
                task_id, "fatal", str(exc), retryable=False, backoff_seconds=0.0
            )
            logger.warning("任务 %s 不可重试失败: %s", task_id, exc)
            if settled is None:
                logger.warning("任务 %s 终态竞争：fatal 落库失败", task_id)
            return
        except RetryableTaskError as exc:
            backoff = self._backoff_for(record.attempts)
            settled = await self._store.mark_failed(
                task_id, "retryable", str(exc), retryable=True,
                backoff_seconds=backoff,
            )
            logger.warning(
                "任务 %s 可重试失败（第 %d 次），退避 %.3fs 后状态=%s",
                task_id, record.attempts, backoff,
                settled.status if settled else "?",
            )
            return
        except Exception as exc:  # noqa: BLE001 - 未预期错误按可重试处理
            backoff = self._backoff_for(record.attempts)
            settled = await self._store.mark_failed(
                task_id, "handler_error", f"{type(exc).__name__}: {exc}",
                retryable=True, backoff_seconds=backoff,
            )
            logger.exception("任务 %s 处理器异常", task_id)
            if settled is not None and settled.status is TaskStatus.FAILED:
                logger.warning("任务 %s 重试次数已耗尽，进入 failed", task_id)
            return

        # 成功路径仍需与取消请求竞争：cancel_requested=1 时成功不得落库
        settled = await self._store.mark_succeeded(task_id, result)
        if settled is None:
            cancelled = await self._store.mark_cancelled_if_running(
                task_id, "完成与取消竞争：取消先生效"
            )
            logger.info(
                "任务 %s 完成时发现取消已生效 -> %s",
                task_id, cancelled.status if cancelled else "终态竞争失败",
            )
        else:
            logger.info("任务 %s 成功: %s", task_id, result)
