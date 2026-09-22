"""执行器：并发控制 + 老化优先级调度 + 租约与终态 CAS + 退避重试。

并发模型：
- 全局信号量 + 每作用域信号量双重门控，取任务时两者都必须有空位；
- 每个作用域独立 ReadyQueue，调度循环在作用域间做公平轮转，
  作用域内由 ReadyQueue 保证"高优先级先取 + 老化防饥饿"；
- 执行前用 lease_for_run(version) 原子占用 RUNNING 租约，
  未占用成功（已被取消/已重试/重启恢复）则不执行、不计数；
- 执行结果经 finish(version, cancel_sensitive) 落终态；
  若取消请求先到，则由 force_cancel_running 仲裁为 CANCELLED，
  保证取消及时生效，且终态只产生一次。
超时：asyncio 硬超时；超时是确定终态 TIMED_OUT（不重试，避免长期占用）。
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid

from .errors import NonRetryableTaskError, RetryableTaskError
from .handlers import ExecutionContext, HandlerRegistry
from .models import SchedulerConfig, TaskRecord, TaskStatus
from .ready_queue import ReadyQueue
from .repository import TaskRepository


class Executor:
    def __init__(
        self,
        repo: TaskRepository,
        registry: HandlerRegistry,
        config: SchedulerConfig,
    ):
        self.repo = repo
        self.registry = registry
        self.config = config
        self.owner_id = f"exec-{uuid.uuid4().hex[:12]}"
        self._global_slots = asyncio.Semaphore(config.global_concurrency)
        self._scope_slots: dict[str, asyncio.Semaphore] = {}
        self._scope_limits: dict[str, int] = {}
        self._queues: dict[str, ReadyQueue] = {}
        self._cancel_events: dict[str, asyncio.Event] = {}
        self._timers: set[asyncio.Task] = set()
        self._inflight: set[str] = set()
        self._dispatch_task: asyncio.Task | None = None
        self._stopping = False
        self._idle_cv = asyncio.Condition()

    # ---- 生命周期 -------------------------------------------------------

    async def start(self) -> None:
        active = await self.repo.recover_stale_leases()
        now = time.time()
        for rec in active:
            await self.admit(rec, now)
        self._stopping = False
        self._dispatch_task = asyncio.create_task(self._dispatch_loop())

    async def stop(self) -> None:
        """停止调度并等待在执行任务收敛（终态/取消均已持久化）。"""
        self._stopping = True
        for q in self._queues.values():
            q.wake()
        if self._dispatch_task is not None:
            self._dispatch_task.cancel()
        for timer in list(self._timers):
            timer.cancel()
        # 等待所有在执行任务完成并落库
        while self._inflight:
            await asyncio.sleep(0.01)
        await self.repo.close()

    # ---- 资源与队列 -----------------------------------------------------

    def _scope_limit(self, scope: str) -> int:
        if scope not in self._scope_limits:
            self._scope_limits[scope] = self.config.scope_concurrency.get(
                scope, self.config.default_scope_concurrency
            )
        return self._scope_limits[scope]

    def _scope_slot(self, scope: str) -> asyncio.Semaphore:
        if scope not in self._scope_slots:
            self._scope_slots[scope] = asyncio.Semaphore(self._scope_limit(scope))
        return self._scope_slots[scope]

    def _queue(self, scope: str) -> ReadyQueue:
        if scope not in self._queues:
            self._queues[scope] = ReadyQueue(self.config.aging_rate)
        return self._queues[scope]

    def _cancel_event(self, task_id: str) -> asyncio.Event:
        ev = self._cancel_events.get(task_id)
        if ev is None:
            ev = asyncio.Event()
            self._cancel_events[task_id] = ev
        return ev

    async def admit(self, record: TaskRecord, now: float | None = None) -> None:
        """任务进入对应作用域队列（新提交或重启恢复）。"""
        if record.status in (TaskStatus.SUCCEEDED, TaskStatus.FAILED,
                             TaskStatus.CANCELLED, TaskStatus.TIMED_OUT):
            return
        now = now if now is not None else time.time()
        self._queue(record.scope).add(record, now)

    # ---- 调度循环 -------------------------------------------------------

    async def _dispatch_loop(self) -> None:
        rr_index = 0
        scopes = list(self._queues.keys())
        while not self._stopping:
            now = time.time()
            # 公平轮转：作用域顺序每轮偏移，避免固定作用域饥饿
            scopes = list(self._queues.keys())
            launched_any = False
            if scopes:
                for offset in range(len(scopes)):
                    scope = scopes[(rr_index + offset) % len(scopes)]
                    if await self._try_launch(scope, now):
                        launched_any = True
                rr_index = (rr_index + 1) % max(1, len(scopes))

            if launched_any:
                await asyncio.sleep(0)
                continue

            # 无可启动任务：计算最近的唤醒时刻（退避到期或信号量释放靠 admit/释放）
            wake_at: float | None = None
            for q in self._queues.values():
                nxt = q.next_wakeup(time.time())
                if nxt is not None and (wake_at is None or nxt < wake_at):
                    wake_at = nxt
            timeout = None
            if wake_at is not None:
                timeout = max(0.0, min(0.5, wake_at - time.time() + 0.001))
            # 监听任意队列的新工作唤醒
            waits = [
                asyncio.ensure_future(q.wait_for_work(timeout))
                for q in self._queues.values()
            ]
            if waits:
                done, pending = await asyncio.wait(
                    waits, return_when=asyncio.FIRST_COMPLETED
                )
                for t in pending:
                    t.cancel()
            else:
                await asyncio.sleep(self.config.poll_interval)

    async def _try_launch(self, scope: str, now: float) -> bool:
        """尝试在某作用域启动一个任务；受全局+作用域并发上限约束。"""
        queue = self._queue(scope)
        scope_slot = self._scope_slot(scope)
        # 非阻塞探测两个信号量，绝不突破并发上限
        if self._global_slots.locked() or scope_slot.locked():
            return False
        candidate = queue.pop_due(now)
        if candidate is None:
            return False
        if candidate.id in self._inflight:
            # 安全兜底：同任务不重复执行
            queue.add(candidate, now)
            return False
        # 原子占用两个名额（asyncio 单线程，locked 检查之间无 await 抢占）
        await self._global_slots.acquire()
        await scope_slot.acquire()
        self._inflight.add(candidate.id)
        asyncio.create_task(self._guarded_run(candidate, scope))
        return True

    # ---- 执行与收敛 -----------------------------------------------------

    async def _guarded_run(self, record: TaskRecord, scope: str) -> None:
        """持信号量执行；任何路径都必须释放名额并落定终态。"""
        try:
            await self._run_once(record)
        except asyncio.CancelledError:
            # 停机取消：记录仍由租约保护，重启恢复不会丢失
            pass
        except Exception as exc:  # 防御：处理器/落库的未预期异常
            fresh = await self.repo.get(record.id)
            if fresh is not None and fresh.status is TaskStatus.RUNNING:
                await self.repo.finish(
                    fresh.id, fresh.version, TaskStatus.FAILED,
                    attempts=fresh.attempts, error=f"executor bug: {exc!r}",
                )
        finally:
            self._inflight.discard(record.id)
            self._global_slots.release()
            self._scope_slot(scope).release()
            self._cancel_events.pop(record.id, None)
            queue = self._queues.get(scope)
            if queue is not None:
                queue.wake()
            async with self._idle_cv:
                self._idle_cv.notify_all()

    async def _run_once(self, record: TaskRecord) -> None:
        leased = await self.repo.lease_for_run(record.id, record.version, self.owner_id)
        if leased is None:
            # 版本失效（等待中被取消 / 状态已变）：不执行
            return
        rec = leased
        handler = self.registry.get(rec.task_type)
        if handler is None:
            await self._settle_failure(
                rec, NonRetryableTaskError(f"unknown task type: {rec.task_type}")
            )
            return

        timed_out = asyncio.Event()
        ctx = ExecutionContext(
            rec,
            timed_out,
            self._cancel_event(rec.id),
            cancel_check=lambda: self._db_cancel_requested(rec.id),
        )

        try:
            result = await asyncio.wait_for(
                handler(rec_payload(rec), ctx),
                timeout=rec.timeout_seconds,
            )
        except asyncio.TimeoutError:
            timed_out.set()
            await self._settle_timeout(rec)
            return
        except asyncio.CancelledError:
            # 取消事件触发（handler 内 sleep 主动抛 CancelledError）或停机
            if self._stopping:
                raise
            await self._settle_cancelled(rec, "cancelled while running")
            return
        except RetryableTaskError as exc:
            await self._settle_retryable_failure(rec, exc)
            return
        except NonRetryableTaskError as exc:
            await self._settle_failure(rec, exc)
            return
        except Exception as exc:  # 未声明的异常视为不可重试，避免无限重试
            await self._settle_failure(rec, NonRetryableTaskError(repr(exc)))
            return

        fresh = await self.repo.finish(
            rec.id, rec.version, TaskStatus.SUCCEEDED,
            attempts=rec.attempts + 1,
            result_json=json.dumps(result, ensure_ascii=False, sort_keys=True),
        )
        if fresh is None:
            await self._settle_cancelled(rec, "cancelled before completion")

    def _retry_delay(self, attempts: int) -> float:
        """确定性指数退避：base * 2**(attempts-1)，截断到 cap（无抖动，便于测试）。"""
        delay = self.config.retry_base_delay * (2 ** max(0, attempts - 1))
        return min(delay, self.config.retry_cap_delay)

    async def _settle_retryable_failure(
        self, rec: TaskRecord, exc: RetryableTaskError
    ) -> None:
        attempts = rec.attempts + 1
        if attempts <= rec.max_retries:
            not_before = time.time() + self._retry_delay(attempts)
            updated = await self.repo.schedule_retry(
                rec.id, rec.version, attempts, not_before, exc.message
            )
            if updated is None:
                # 取消竞速：重试落位失败，转取消仲裁
                fresh = await self.repo.get(rec.id)
                if fresh is not None and fresh.status is TaskStatus.RUNNING:
                    await self._settle_cancelled(fresh, "cancelled before retry")
                return
            await self._schedule_wakeup(updated, not_before)
        else:
            # 重试次数耗尽：确定终态 FAILED
            fresh = await self.repo.finish(
                rec.id, rec.version, TaskStatus.FAILED,
                attempts=attempts,
                error=f"retries exhausted ({attempts} attempts): {exc.message}",
            )
            if fresh is None:
                await self._settle_cancelled(rec, "cancelled before failure")

    async def _settle_failure(self, rec: TaskRecord, exc: Exception) -> None:
        message = exc.message if hasattr(exc, "message") else repr(exc)
        fresh = await self.repo.finish(
            rec.id, rec.version, TaskStatus.FAILED,
            attempts=rec.attempts + 1, error=message,
        )
        if fresh is None:
            await self._settle_cancelled(rec, "cancelled before failure")

    async def _settle_timeout(self, rec: TaskRecord) -> None:
        fresh = await self.repo.finish(
            rec.id, rec.version, TaskStatus.TIMED_OUT,
            attempts=rec.attempts + 1, error="execution timed out",
        )
        if fresh is None:
            await self._settle_cancelled(rec, "cancelled before timeout")

    async def _settle_cancelled(self, rec: TaskRecord, reason: str) -> None:
        """取消仲裁：先读最新版本，仍为 RUNNING 则强制落 CANCELLED。"""
        fresh = await self.repo.get(rec.id)
        if fresh is None:
            return
        if fresh.status in (
            TaskStatus.SUCCEEDED, TaskStatus.FAILED,
            TaskStatus.CANCELLED, TaskStatus.TIMED_OUT,
        ):
            return  # 终态已由竞争方产生，不重复转换
        if fresh.status is TaskStatus.RUNNING:
            await self.repo.force_cancel_running(fresh.id, fresh.version, reason)

    async def _schedule_wakeup(self, record: TaskRecord, not_before: float) -> None:
        """为 RETRY_WAIT 安排到期重新入队（仅内存调度；持久状态在 DB）。"""
        delay = max(0.0, not_before - time.time())

        async def _on_due() -> None:
            await asyncio.sleep(delay)
            fresh = await self.repo.get(record.id)
            if fresh is not None and fresh.status is TaskStatus.RETRY_WAIT:
                # 到期的 retry_wait 在入队前转 READY（CAS by version+status）
                ready = await self._promote_retry_wait(fresh)
                if ready is not None:
                    await self.admit(ready)

        timer = asyncio.create_task(_on_due())
        self._timers.add(timer)
        timer.add_done_callback(self._timers.discard)

    async def _promote_retry_wait(self, rec: TaskRecord) -> TaskRecord | None:
        """RETRY_WAIT -> READY（到期且未取消）。"""
        return await self.repo.promote_retry_wait(rec.id, rec.version)

    async def _db_cancel_requested(self, task_id: str) -> bool:
        fresh = await self.repo.get(task_id)
        return fresh is not None and fresh.cancel_requested

    # ---- 外部控制与观测 -------------------------------------------------

    async def request_cancel(self, task_id: str) -> None:
        self._cancel_event(task_id).set()

    def running_count(self) -> int:
        return len(self._inflight)

    def queued_count(self) -> int:
        return sum(len(q) for q in self._queues.values())

    async def wait_idle(self, timeout: float = 5.0) -> None:
        """测试辅助：等待队列排空、无在执行任务。"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.running_count() == 0 and self.queued_count() == 0:
                return
            await asyncio.sleep(0.01)
        raise TimeoutError("executor did not become idle")


def rec_payload(rec: TaskRecord) -> dict:
    return json.loads(rec.payload_json)
