"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

实时指标计算链路需要“结果版本 + 数据新鲜度 + 窗口覆盖率 + 下游消费”在同一个事务里
提交，因此这里提供最小可用的写事务：

- 事务在表的深拷贝上执行，提交时一次性替换当前快照，其它读者在提交前只能看到旧版本；
- 任何异常都会丢弃工作副本，结果表、版本表、事件表不会出现“写了一半”的状态；
- ``fail_next_commit`` 用于模拟数据库提交失败，验证残缺版本不会暴露给接口。
"""
from __future__ import annotations

import copy
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterator

from app.seed import SEED_ROWS
from app.seed_realtime import (
    DASHBOARD_CONFIGS,
    DATASOURCE_STREAMS,
    REALTIME_METRICS,
    THRESHOLD_RULES,
    build_seed_events,
)


class TransactionFailed(RuntimeError):
    """提交阶段失败：工作副本已丢弃，调用方应把本次写入视为未发生。"""


# 只在运营概览中隐藏的技术表，避免把计算中间表当成业务模块计数。
INTERNAL_TABLES = {
    "datasource_stream",
    "metric_event",
    "metric_event_ingest",
    "metric_window_result",
    "metric_window_version",
    "metric_compute_batch",
    "dashboard_window_snapshot",
    "threshold_evaluation",
}


def system_now() -> datetime:
    """统一使用带时区的 UTC 时间，避免窗口比较时混用本地时间。"""
    return datetime.now(timezone.utc)


class Store:
    def __init__(self, clock: Callable[[], datetime] | None = None) -> None:
        self._clock = clock or system_now
        self._lock = threading.RLock()
        self._tables: dict[str, list[dict[str, Any]]] = {}
        self._next_ids: dict[str, int] = {}
        self._tx_tables: dict[str, list[dict[str, Any]]] | None = None
        self._tx_next_ids: dict[str, int] | None = None
        self._tx_depth = 0
        self._fail_commit_reason: str | None = None
        self.realtime_bootstrapped = False
        self.reset()

    # ------------------------------------------------------------------
    # 基础读写
    # ------------------------------------------------------------------
    def reset(self, now: datetime | None = None) -> None:
        """重建示例数据；测试可传入固定时间以获得确定性的窗口与新鲜度。"""
        if now is not None:
            fixed = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
            self._clock = lambda: fixed
        tables: dict[str, list[dict[str, Any]]] = {
            name: [copy.deepcopy(row) for row in rows]
            for name, rows in SEED_ROWS.items()
        }
        # 实时指标、监控大盘、阈值管理使用可计算的样例定义覆盖 seed.py 中的占位数据。
        tables["realtimemetric"] = copy.deepcopy(REALTIME_METRICS)
        tables["dashboard"] = copy.deepcopy(DASHBOARD_CONFIGS)
        tables["threshold"] = copy.deepcopy(THRESHOLD_RULES)
        tables["datasource_stream"] = copy.deepcopy(DATASOURCE_STREAMS)
        tables["metric_event"] = build_seed_events(self.now())
        tables["metric_event_ingest"] = []
        tables["metric_window_result"] = []
        tables["metric_window_version"] = []
        tables["metric_compute_batch"] = []
        tables["dashboard_window_snapshot"] = []
        tables["threshold_evaluation"] = []
        self._tables = tables
        self._next_ids = {
            name: max((int(row.get("id", 0)) for row in rows), default=0)
            for name, rows in tables.items()
        }
        self.realtime_bootstrapped = False

    def now(self) -> datetime:
        return self._clock()

    def set_now(self, value: datetime) -> None:
        """测试辅助：把逻辑时钟固定在某一时刻。"""
        fixed = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        self._clock = lambda: fixed

    def advance(self, seconds: int) -> None:
        """测试辅助：推进逻辑时钟，观察新鲜度是否随之更新。"""
        base = self.now()
        self._clock = lambda: base + timedelta(seconds=seconds)

    def module_names(self) -> list[str]:
        return sorted(name for name in self._tables if name not in INTERNAL_TABLES)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._active_tables().setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def peek_id(self, module: str) -> int:
        """查看下一个可用编号，不占用编号。"""
        ids = self._active_ids()
        return ids.get(module, 0) + 1

    def insert(self, module: str, row: dict[str, Any]) -> dict[str, Any]:
        ids = self._active_ids()
        next_id = ids.get(module, 0) + 1
        ids[module] = next_id
        stored = {"id": next_id, **row}
        self.rows(module).append(stored)
        return stored

    # ------------------------------------------------------------------
    # 事务
    # ------------------------------------------------------------------
    def _active_tables(self) -> dict[str, list[dict[str, Any]]]:
        return self._tx_tables if self._tx_tables is not None else self._tables

    def _active_ids(self) -> dict[str, int]:
        return self._tx_next_ids if self._tx_next_ids is not None else self._next_ids

    def fail_next_commit(self, reason: str = "模拟数据库事务提交失败") -> None:
        """模拟提交失败：下一次事务即使业务逻辑跑完也不会替换正式快照。"""
        self._fail_commit_reason = reason

    @contextmanager
    def transaction(self) -> Iterator["Store"]:
        """在隔离副本上执行写入，成功时原子替换，失败时整体丢弃。"""
        with self._lock:
            if self._tx_tables is not None:
                # 嵌套事务沿用外层工作副本；内层异常交由外层统一回滚。
                self._tx_depth += 1
                try:
                    yield self
                finally:
                    self._tx_depth -= 1
                return

            working = copy.deepcopy(self._tables)
            working_ids = dict(self._next_ids)
            self._tx_tables = working
            self._tx_next_ids = working_ids
            self._tx_depth = 1
            try:
                yield self
            except BaseException:
                self._tx_tables = None
                self._tx_next_ids = None
                self._tx_depth = 0
                raise
            else:
                reason = self._fail_commit_reason
                self._fail_commit_reason = None
                self._tx_tables = None
                self._tx_next_ids = None
                self._tx_depth = 0
                if reason:
                    raise TransactionFailed(reason)
                self._tables = working
                self._next_ids = working_ids

    # ------------------------------------------------------------------
    # 概览
    # ------------------------------------------------------------------
    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()
