"""崩溃恢复测试的工作进程脚本：

用法: python crash_worker.py <db_path> <marker_path> <progress_path>
任务执行到一半时若 marker 不存在则 os._exit(0) 模拟硬崩溃。
"""

import asyncio
import os
import sys

sys.path.insert(0, os.getcwd())

from task_orchestrator.models import SchedulerConfig, SubmitRequest
from task_orchestrator.orchestrator import TaskOrchestrator
from task_orchestrator.repository import TaskRepository


async def main(db_path: str, marker: str, progress: str) -> None:
    async def crash_once(payload, ctx):
        with open(progress, "a") as f:
            f.write(f"start:{ctx.record.idempotency_key}\n")
        await ctx.sleep(0.05)
        if not os.path.exists(marker):
            open(marker, "w").write("1")
            os._exit(0)
        return {"survived": True}

    config = SchedulerConfig(
        global_concurrency=2, default_scope_concurrency=2,
        aging_rate=2.0, retry_base_delay=0.02, retry_cap_delay=0.2,
    )
    orch = TaskOrchestrator(TaskRepository(db_path), config=config)
    orch.registry.register("crash_once", crash_once)
    await orch.start()
    await orch.submit(SubmitRequest(
        task_type="crash_once", payload={}, idempotency_key="crash-k",
        timeout_seconds=5.0,
    ))
    for _ in range(300):
        await asyncio.sleep(0.01)
        if os.path.exists(marker):
            break


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1], sys.argv[2], sys.argv[3]))
