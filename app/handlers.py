"""任务处理器：按 task_type 注册的可取消异步执行单元。

处理器接收 payload 与一个 CancellationToken；长时间运行的处理器应周期性
检查 token（wait_cancellable / raise_if_cancelled），以便取消请求在
任务运行中及时生效，而不是等超时或自然结束。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any


class RetryableTaskError(Exception):
    """处理器抛出：失败可重试，将按指数退避重新调度。"""


class FatalTaskError(Exception):
    """处理器抛出：不可重试，任务直接进入 failed 终态。"""


@dataclass
class CancellationToken:
    """协作式取消令牌；由执行器在 cancel_requested 置位后 set。"""

    event: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def cancelled(self) -> bool:
        return self.event.is_set()

    def raise_if_cancelled(self) -> None:
        """检查点：取消已触发则抛出 CancelledError，由执行器转为终态。"""
        if self.event.is_set():
            raise asyncio.CancelledError("收到取消请求")

    async def wait_cancellable(self, delay: float) -> None:
        """可被取消打断的等待：取消先到则立即抛出 CancelledError。"""
        try:
            await asyncio.wait_for(self.event.wait(), timeout=delay)
        except TimeoutError:
            return
        raise asyncio.CancelledError("等待期间收到取消请求")


TaskHandler = Callable[[dict[str, Any], CancellationToken], Awaitable[dict[str, Any]]]


class HandlerRegistry:
    """task_type -> 处理函数的注册表。"""

    def __init__(self) -> None:
        self._handlers: dict[str, TaskHandler] = {}

    def register(self, task_type: str, handler: TaskHandler) -> None:
        if not task_type:
            raise ValueError("task_type 不能为空")
        self._handlers[task_type] = handler

    def get(self, task_type: str) -> TaskHandler | None:
        return self._handlers.get(task_type)

    def contains(self, task_type: str) -> bool:
        return task_type in self._handlers


# ---------------------------------------------------------------------------
# 内置处理器
# ---------------------------------------------------------------------------


async def echo_handler(
    payload: dict[str, Any], token: CancellationToken
) -> dict[str, Any]:
    """回显处理器：可通过 sleep 模拟耗时，用于取消与并发测试。

    payload: {"value": 任意可序列化值, "sleep": 可选秒数}
    """
    token.raise_if_cancelled()
    delay = float(payload.get("sleep", 0) or 0)
    if delay > 0:
        await token.wait_cancellable(delay)
    return {"echo": payload.get("value")}


async def flaky_handler(
    payload: dict[str, Any], token: CancellationToken
) -> dict[str, Any]:
    """前 fail_times 次调用抛可重试错误，之后成功。执行器通过 attempts 注入。

    为避免依赖进程内计数器（重启即失效，无法验证重试持久化），失败次数由
    payload.fail_times 声明，执行次数由执行器通过 payload 注入 _attempt。
    """
    token.raise_if_cancelled()
    fail_times = int(payload.get("fail_times", 1))
    attempt = int(payload.get("_attempt", 1))
    if attempt <= fail_times:
        raise RetryableTaskError(f"模拟瞬时失败（第 {attempt} 次，共需失败 {fail_times} 次）")
    return {"ok": True, "attempt": attempt}


async def always_fail_handler(
    payload: dict[str, Any], token: CancellationToken
) -> dict[str, Any]:
    """总是抛出不可重试错误，用于验证直接终态。"""
    token.raise_if_cancelled()
    raise FatalTaskError("模拟不可恢复失败")


async def slow_handler(
    payload: dict[str, Any], token: CancellationToken
) -> dict[str, Any]:
    """慢速处理器：按 duration 秒分段睡眠，期间每 0.02s 检查一次取消。"""
    duration = float(payload.get("duration", 1.0))
    remaining = duration
    while remaining > 0:
        token.raise_if_cancelled()
        step = min(0.02, remaining)
        await token.wait_cancellable(step)
        remaining -= step
    return {"slept": duration}


def register_builtin_handlers(registry: HandlerRegistry) -> None:
    """注册内置示例处理器：echo / flaky / always_fail / slow。"""
    registry.register("echo", echo_handler)
    registry.register("flaky", flaky_handler)
    registry.register("always_fail", always_fail_handler)
    registry.register("slow", slow_handler)
