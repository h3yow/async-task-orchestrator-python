"""批量依赖编排：一次提交多个带依赖关系的任务。

本模块只在既有单任务能力之上增加一层编排，不改变单任务提交/调度/恢复语义：
- 批次自身有业务幂等键，重复（含并发）提交返回同一组任务与同一套依赖；
- 依赖引用按业务幂等键表达，可指向同批任务或历史已提交任务；
- 非法批次（空批次、未知类型、循环依赖、引用缺失、跨作用域依赖等）在提交时
  整体拒绝，错误原因通过 error_code 区分；
- 依赖未全部成功的任务以 blocked 状态等待，不进入调度；任一依赖终态为
  failed/cancelled/skipped，任务级联进入 skipped 终态（error_code 区分原因），
  与真正执行失败的 failed 终态区分开。

校验分两阶段，任何一阶段失败都在写入前整体拒绝：
1. 语法/自洽校验（不访问存储）：批次键、任务条目、键唯一、类型、参数、
   重复声明依赖、批内循环依赖；
2. 引用校验（读存储）：依赖键必须存在于同批或历史任务，且与被依赖任务同
   作用域。跨批循环不可能出现：已有任务的依赖边在其提交时已固定，新批只能
   追加“新任务 -> 旧任务”的边，旧任务不可能反向依赖尚未创建的新任务。
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field

from .handlers import HandlerRegistry
from .models import (
    BatchConflict,
    BatchNotFound,
    BatchRecord,
    PayloadConflict,
    Priority,
    TaskRecord,
    TaskStatus,
    ValidationFailure,
    utcnow,
)
from .service import canonical_payload_hash
from .store import BatchEntry, BatchPersistenceResult, TaskStore


@dataclass
class BatchTaskSpec:
    """批量提交中单个任务的声明（依赖以业务幂等键表达）。"""

    idempotency_key: str
    task_type: str
    payload: dict
    depends_on: tuple[str, ...] | list[str] = field(default_factory=tuple)
    priority: str = "normal"
    scope: str = "default"
    max_attempts: int = 3
    timeout_seconds: float = 10.0


@dataclass
class BatchSubmitResult:
    """批次提交结果：批次记录 + 有序任务视图（created 标记每个任务是否新建）。"""

    batch: BatchRecord
    created: bool
    tasks: list[tuple[TaskRecord, bool]] = field(default_factory=list)


def _invalid(code: str, message: str) -> ValidationFailure:
    return ValidationFailure(f"{code}:{message}")


class BatchService:
    """批量依赖提交用例服务。"""

    def __init__(self, store: TaskStore, registry: HandlerRegistry) -> None:
        self._store = store
        self._registry = registry

    async def list_dependencies(self, task_id: str) -> list[str]:
        """查询单个任务声明的依赖业务键（按声明位置有序）。"""
        return await self._store.list_dependencies(task_id)

    # -- 校验 ---------------------------------------------------------------

    def _validate_syntax(
        self, batch_key: str, specs: list
    ) -> dict[str, object]:
        if not isinstance(batch_key, str) or not batch_key.strip():
            raise _invalid("invalid_batch_key", "批次幂等键不能为空")
        if not isinstance(specs, list) or not specs:
            raise _invalid("empty_batch", "批次至少要包含一个任务")

        keys: list[str] = []
        key_set: set[str] = set()
        normalized: dict[str, dict] = {}
        deps_map: dict[str, list[str]] = {}

        for index, spec in enumerate(specs):
            where = f"第 {index + 1} 个任务"
            key = getattr(spec, "idempotency_key", None)
            if not isinstance(key, str) or not key.strip():
                raise _invalid(
                    "invalid_idempotency_key", f"{where} 的幂等键不能为空"
                )
            if key in key_set:
                raise _invalid(
                    "duplicate_task_in_batch",
                    f"批次内业务唯一键 {key!r} 重复声明",
                )
            task_type = getattr(spec, "task_type", None)
            if not isinstance(task_type, str) or not task_type.strip():
                raise _invalid(
                    "invalid_task_type", f"{where}({key}) 的 task_type 不能为空"
                )
            if not self._registry.contains(task_type):
                raise _invalid(
                    "unknown_task_type",
                    f"{where}({key}) 的任务类型 {task_type!r} 未注册",
                )
            payload = getattr(spec, "payload", None)
            if not isinstance(payload, dict):
                raise _invalid(
                    "invalid_payload", f"{where}({key}) 的 payload 必须是 JSON 对象"
                )
            try:
                priority = Priority(getattr(spec, "priority", "normal"))
            except ValueError:
                raise _invalid(
                    "invalid_priority",
                    f"{where}({key}) 的优先级 {getattr(spec, 'priority', None)!r} 非法",
                )
            scope = getattr(spec, "scope", "default")
            if not isinstance(scope, str) or not scope.strip():
                raise _invalid("invalid_scope", f"{where}({key}) 的 scope 不能为空")
            max_attempts = getattr(spec, "max_attempts", 3)
            if (
                not isinstance(max_attempts, int)
                or isinstance(max_attempts, bool)
                or max_attempts < 1
            ):
                raise _invalid(
                    "invalid_max_attempts",
                    f"{where}({key}) 的 max_attempts 必须为 >=1 的整数",
                )
            timeout_seconds = getattr(spec, "timeout_seconds", 10.0)
            if (
                not isinstance(timeout_seconds, (int, float))
                or isinstance(timeout_seconds, bool)
                or timeout_seconds <= 0
            ):
                raise _invalid(
                    "invalid_timeout",
                    f"{where}({key}) 的 timeout_seconds 必须为 >0 的数值",
                )
            raw_deps = getattr(spec, "depends_on", ()) or ()
            if not isinstance(raw_deps, (list, tuple)):
                raise _invalid(
                    "invalid_dependency",
                    f"{where}({key}) 的 depends_on 必须是业务唯一键数组",
                )
            deps: list[str] = []
            dep_seen: set[str] = set()
            for dep in raw_deps:
                if not isinstance(dep, str) or not dep.strip():
                    raise _invalid(
                        "invalid_dependency",
                        f"{where}({key}) 的依赖键必须是非空字符串",
                    )
                if dep == key:
                    raise _invalid(
                        "cycle_dependency", f"任务 {key!r} 依赖自身，构成循环"
                    )
                if dep in dep_seen:
                    raise _invalid(
                        "duplicate_dependency",
                        f"任务 {key!r} 重复声明依赖 {dep!r}",
                    )
                dep_seen.add(dep)
                deps.append(dep)

            keys.append(key)
            key_set.add(key)
            deps_map[key] = deps
            normalized[key] = {
                "idempotency_key": key,
                "task_type": task_type,
                "payload_hash": canonical_payload_hash(payload),
                "priority": priority.value,
                "scope": scope,
                "max_attempts": max_attempts,
                "timeout_seconds": float(timeout_seconds),
                "depends_on": sorted(deps),
            }

        # 批内循环依赖检测：Kahn 拓扑排序（只统计批内边，缺失引用稍后处理）
        indegree = {k: 0 for k in keys}
        adjacency: dict[str, list[str]] = {k: [] for k in keys}
        for key in keys:
            for dep in deps_map[key]:
                if dep in key_set:
                    indegree[key] += 1
                    adjacency[dep].append(key)
        queue = [k for k in keys if indegree[k] == 0]
        visited = 0
        while queue:
            node = queue.pop()
            visited += 1
            for nxt in adjacency[node]:
                indegree[nxt] -= 1
                if indegree[nxt] == 0:
                    queue.append(nxt)
        if visited != len(keys):
            raise _invalid(
                "cycle_dependency",
                "批次内存在循环依赖，无法确定执行先后关系",
            )

        return {
            "keys": keys,
            "normalized": normalized,
            "deps_map": deps_map,
        }

    async def _validate_references(
        self,
        specs: list,
        syntax: dict[str, object],
    ) -> tuple[dict[str, dict], dict[str, list[str]], dict[str, TaskRecord]]:
        keys: list[str] = syntax["keys"]  # type: ignore[assignment]
        normalized: dict[str, dict] = syntax["normalized"]  # type: ignore[assignment]
        deps_map: dict[str, list[str]] = syntax["deps_map"]  # type: ignore[assignment]
        key_set = set(keys)

        all_dep_keys = {dep for deps in deps_map.values() for dep in deps}
        external_keys = [k for k in all_dep_keys if k not in key_set]
        existing = await self._store.get_many_by_idempotency_keys(external_keys)

        for key in keys:
            scope = normalized[key]["scope"]
            for dep in deps_map[key]:
                if dep in key_set:
                    dep_scope = normalized[dep]["scope"]
                else:
                    dep_record = existing.get(dep)
                    if dep_record is None:
                        raise _invalid(
                            "dependency_not_found",
                            f"任务 {key!r} 依赖的业务唯一键 {dep!r} "
                            "在同批与历史任务中均不存在",
                        )
                    dep_scope = dep_record.scope
                if dep_scope != scope:
                    raise _invalid(
                        "cross_scope_dependency",
                        f"任务 {key!r}(scope={scope}) 不允许依赖其他作用域 "
                        f"{dep!r}(scope={dep_scope})",
                    )
        return normalized, deps_map, existing

    @staticmethod
    def _content_hash(normalized: dict[str, dict], keys: list[str]) -> str:
        """批次内容指纹：按提交位置排列的规范化任务声明（含依赖集合）。

        提交顺序属于批次内容的一部分：同集合不同顺序视为不同批次；
        任务内依赖以排序后的集合参与哈希，依赖声明顺序不影响指纹。
        """
        return canonical_payload_hash({"tasks": [normalized[k] for k in keys]})

    # -- 用例 ---------------------------------------------------------------

    async def submit_batch(
        self, batch_key: str, specs: list
    ):
        """提交带依赖的任务批次，返回 BatchSubmitResult。

        非法批次在任何写入之前整体拒绝（ValidationFailure，error_code 可区分）；
        批次键命中但内容不一致抛 BatchConflict；批次内单键与历史任务冲突时
        同样以 409 拒绝（payload_conflict）。
        """
        syntax = self._validate_syntax(batch_key, specs)
        normalized, deps_map, _existing = await self._validate_references(
            specs, syntax
        )
        keys: list[str] = syntax["keys"]  # type: ignore[assignment]
        content_hash = self._content_hash(normalized, keys)

        now = utcnow()
        entries: list[BatchEntry] = []
        for spec in specs:
            key = spec.idempotency_key
            info = normalized[key]
            # 有依赖的新任务一律以 blocked 落库，随后在同一事务内传播：
            # 全部前置已成功 -> pending；任一前置坏终态 -> skipped；其余保持 blocked
            initial_status = (
                TaskStatus.BLOCKED if deps_map[key] else TaskStatus.PENDING
            )
            record = TaskRecord(
                id=uuid.uuid4().hex,
                idempotency_key=key,
                task_type=spec.task_type,
                payload=spec.payload,
                payload_hash=info["payload_hash"],
                priority=Priority(info["priority"]),
                scope=info["scope"],
                max_attempts=info["max_attempts"],
                timeout_seconds=info["timeout_seconds"],
                status=initial_status,
                attempts=0,
                created_at=now,
                updated_at=now,
                scheduled_at=now,
            )
            entries.append(
                BatchEntry(record=record, depends_on=list(deps_map[key]))
            )

        result: BatchPersistenceResult = await self._store.commit_batch(
            batch_id=uuid.uuid4().hex,
            batch_key=batch_key,
            content_hash=content_hash,
            entries=entries,
        )
        if result.outcome == "batch_conflict":
            raise BatchConflict(
                "batch_conflict:相同批次幂等键的批次内容与已存在批次不一致"
            )
        if result.outcome == "task_conflict":
            raise PayloadConflict(
                "payload_conflict:批次内业务唯一键 "
                f"{result.conflict_key!r} 已被占用且任务参数或依赖不一致"
            )

        return BatchSubmitResult(
            batch=result.batch,  # type: ignore[arg-type]
            created=result.outcome == "created",
            tasks=result.tasks,
        )

    async def get_batch(self, batch_key: str):
        """按批次幂等键查询批次及其任务（按提交位置有序）。"""
        batch = await self._store.get_batch_by_key(batch_key)
        if batch is None:
            raise BatchNotFound(
                f"batch_not_found:批次幂等键 {batch_key!r} 不存在"
            )
        task_ids = await self._store.list_batch_task_ids(batch.id)
        tasks: list[tuple[TaskRecord, bool]] = []
        for task_id in task_ids:
            record = await self._store.get_task(task_id)
            if record is not None:
                tasks.append((record, False))
        return BatchSubmitResult(batch=batch, created=False, tasks=tasks)
