"""任务处理器：执行上下文、注册表与内置处理器。

内置处理器（payload 约定）：
- echo    : {"message"?: ...} 原样回显，始终成功；
- flaky   : {"fail_times": N} 前 N 次尝试抛 RetryableTaskError，之后成功，
             用于确定性验证重试/退避；
- fail    : {"retryable": bool} 按类型失败（默认不可重试）；
- sleep   : {"seconds": float} 睡眠，供取消/超时测试，响应取消与超时。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from .errors import NonRetryableTaskError, RetryableTaskError
from .models import TaskRecord

Handler = Callable[[dict, "ExecutionContext"], Awaitable[dict]]


class ExecutionContext:
    """传给处理器的运行上下文：超时截止、协作式取消与可中断 sleep。"""

    def __init__(
        self,
        record: TaskRecord,
        timed_out: asyncio.Event,
        cancel_event: asyncio.Event,
        cancel_check: Callable[[], bool],
    ):
        self.record = record
        self.attempt = record.attempts
        self._timed_out = timed_out
        self._cancel_event = cancel_event
        self._cancel_check = cancel_check

    @property
    def cancel_requested(self) -> bool:
        """取消请求是否已到达（运行中及时生效）。"""
        return self._cancel_check() or self._cancel_event.is_set()

    async def sleep(self, seconds: float) -> None:
        """可被取消/超时打断的 sleep；触发时抛 asyncio.CancelledError。"""
        if seconds <= 0:
            await asyncio.sleep(0)
            return
        timeout_task = asyncio.ensure_future(self._timed_out.wait())
        cancel_task = asyncio.ensure_future(self._cancel_event.wait())
        sleep_task = asyncio.ensure_future(asyncio.sleep(seconds))
        try:
            done, pending = await asyncio.wait(
                {sleep_task, timeout_task, cancel_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for t in pending:
                t.cancel()
            if sleep_task not in done:
                raise asyncio.CancelledError()
        finally:
            for t in (timeout_task, cancel_task, sleep_task):
                if not t.done():
                    t.cancel()


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, Handler] = {}

    def register(self, name: str, handler: Handler) -> None:
        self._handlers[name] = handler

    def get(self, name: str) -> Handler | None:
        return self._handlers.get(name)

    def known(self, name: str) -> bool:
        return name in self._handlers


# ---- 内置处理器 -----------------------------------------------------------


async def echo_handler(payload: dict, ctx: ExecutionContext) -> dict:
    return {"echo": payload.get("message", payload)}


async def flaky_handler(payload: dict, ctx: ExecutionContext) -> dict:
    fail_times = int(payload.get("fail_times", 1))
    # ctx.attempt 为当前已完成尝试数（0 起）；第 1..fail_times 次执行失败
    if ctx.attempt < fail_times:
        raise RetryableTaskError(
            f"flaky failure {ctx.attempt + 1}/{fail_times}"
        )
    return {"ok": True, "attempt": ctx.attempt + 1}


async def fail_handler(payload: dict, ctx: ExecutionContext) -> dict:
    message = str(payload.get("message", "forced failure"))
    if bool(payload.get("retryable", False)):
        raise RetryableTaskError(message)
    raise NonRetryableTaskError(message)


async def sleep_handler(payload: dict, ctx: ExecutionContext) -> dict:
    seconds = float(payload.get("seconds", 1.0))
    # 分小段睡，保证取消信号在 ~0.02s 内被观察到
    step = 0.02
    remaining = seconds
    while remaining > 0:
        await ctx.sleep(min(step, remaining))
        remaining -= step
    return {"slept": seconds}


def default_registry() -> HandlerRegistry:
    registry = HandlerRegistry()
    registry.register("echo", echo_handler)
    registry.register("flaky", flaky_handler)
    registry.register("fail", fail_handler)
    registry.register("sleep", sleep_handler)
    return registry
