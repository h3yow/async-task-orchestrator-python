"""带老化(aging)的就绪队列：防止低优先级任务在持续高优先级流量下饥饿。

排序键 effective = priority + aging_rate * 等待秒数（now - enqueued_at）。
同分按入队序号 FIFO。pop_due 仅放行 not_before <= now 的任务；
队首任务未到期时，即使后面有到期任务也按"到期+有效分"一起重算
（实现中遍历堆选出到期且有效分最高者，规模为本地单机可接受）。
"""

from __future__ import annotations

import asyncio
import heapq
import itertools

from .models import TaskRecord


class ReadyQueue:
    def __init__(self, aging_rate: float = 2.0):
        self.aging_rate = aging_rate
        # 堆元素: (-effective_seq_bucket?) —— 有效分随时间变化，
        # 因此堆中只存 (enqueued_at, seq, record)，pop 时线性选取。
        self._items: list[tuple[float, int, TaskRecord]] = []
        self._counter = itertools.count()
        self._wakeup = asyncio.Event()

    def _score(self, enqueued_at: float, record: TaskRecord, now: float) -> float:
        return record.priority + self.aging_rate * max(0.0, now - enqueued_at)

    def add(self, record: TaskRecord, now: float) -> None:
        self._items.append((now, next(self._counter), record))
        self._wakeup.set()

    def pop_due(self, now: float) -> TaskRecord | None:
        """取出已到期且当前有效分最高的任务；无到期任务返回 None。"""
        best_idx = -1
        best_score: float | None = None
        best_seq: int | None = None
        for i, (enqueued_at, seq, rec) in enumerate(self._items):
            if rec.not_before > now:
                continue
            score = self._score(enqueued_at, rec, now)
            if (
                best_score is None
                or score > best_score
                or (score == best_score and (best_seq is None or seq < best_seq))
            ):
                best_score, best_seq, best_idx = score, seq, i
        if best_idx < 0:
            return None
        _, _, record = self._items.pop(best_idx)
        return record

    def remove(self, task_id: str) -> TaskRecord | None:
        for i, (_, _, rec) in enumerate(self._items):
            if rec.id == task_id:
                _, _, record = self._items.pop(i)
                return record
        return None

    def next_wakeup(self, now: float) -> float | None:
        """最近一个未到期任务的到期时刻（调度循环据此 sleep）。"""
        future = [rec.not_before for _, _, rec in self._items if rec.not_before > now]
        return min(future) if future else None

    def __len__(self) -> int:
        return len(self._items)

    def wake(self) -> None:
        self._wakeup.set()

    async def wait_for_work(self, timeout: float | None = None) -> bool:
        """等待唤醒信号；返回是否在 timeout 内被唤醒。"""
        try:
            await asyncio.wait_for(self._wakeup.wait(), timeout=timeout)
            return True
        except TimeoutError:
            return False
        finally:
            self._wakeup.clear()
