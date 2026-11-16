"""监控大盘业务规则：配置维护 + 按窗口结果数据集联动刷新。

大盘只从实时指标计算结果数据集读取「可消费窗口」，保证：
- 迟到事件重放后，大盘快照随窗口版本一起更新，不会残留旧结果；
- 覆盖率不足 / 新鲜度滞留的窗口不进入大盘，宁可保留上一份完整结果；
- 每个快照都能回溯到结果版本、结果指纹与计算批次。
"""
from __future__ import annotations

import importlib
from typing import Any

from app.services import realtimecompute as compute
from app.store import store

MODULE = "dashboard"
REQUIRED_FIELDS = ["大盘编号", "大盘名称", "包含指标组"]
STATUS_ORDER = ["草稿", "大盘评审中", "已发布", "已下线"]
ACTION_RULES = {"提交大盘评审": "大盘评审中", "发布大盘": "已发布", "下线大盘": "已下线"}
NEGATIVE_ACTIONS = ["下线大盘"]


def _groups(row: dict[str, Any]) -> list[str]:
    return [
        part.strip()
        for part in str(row.get("包含指标组") or "").split(",")
        if part.strip()
    ]


class DashboardService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            needle = keyword.strip().lower()
            rows = [
                row for row in rows
                if needle in " ".join(
                    str(row.get(field) or "")
                    for field in ("大盘编号", "大盘名称", "包含指标组", "负责人")
                ).lower()
            ]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        compute.ensure_bootstrapped()
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        item = dict(entry)
        item["指标卡"] = self.refresh(entry)
        item["刷新口径"] = "只消费已到齐且新鲜度完整的窗口结果，滞后窗口保留上一份完整结果"
        return item

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        with store.transaction():
            entry = store.insert(MODULE, {
                **{field: values.get(field) for field in REQUIRED_FIELDS},
                "status": STATUS_ORDER[0],
                "pending": True,
                "abnormal": False,
            })
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"大盘配置 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于监控大盘可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        with store.transaction():
            current = store.find(MODULE, entry_id)
            assert current is not None
            current["status"] = target
            current["pending"] = target != STATUS_ORDER[-1]
            current["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"大盘配置已{action}"

    # ------------------------------------------------------------------
    # 窗口结果消费
    # ------------------------------------------------------------------
    def refresh(self, dashboard: dict[str, Any]) -> list[dict[str, Any]]:
        """按指标组组装大盘指标卡，优先读已同步快照，缺快照时回退最新可消费窗口。

        每张卡都带「最新尝试窗口」与「滞后窗口数」：如果计算链路已经产出更新的窗口
        但尚不可消费，大盘保留上一份完整结果并明确标注滞后，而不是把旧结果当成当前值。
        """
        compute.ensure_bootstrapped()
        now = store.now()
        cards: list[dict[str, Any]] = []
        for metric_number in _groups(dashboard):
            metric_row = compute._metric_by_number(metric_number)  # 与窗口计算共用取数口径
            config, errors = (
                compute.metric_config(metric_row, compute.stream_index())
                if metric_row else (None, ["指标未定义"])
            )
            metric_results = sorted(
                (
                    row for row in store.rows(compute.RESULT_MODULE)
                    if row.get("指标编号") == metric_number
                ),
                key=lambda row: str(row.get("窗口开始") or ""),
                reverse=True,
            )
            latest_attempt = metric_results[0] if metric_results else None
            pipeline = compute.metric_pipeline(metric_number, config, now)
            snapshots = [
                row for row in store.rows(compute.DASHBOARD_SNAPSHOT_MODULE)
                if row.get("大盘编号") == dashboard.get("大盘编号")
                and row.get("指标编号") == metric_number
            ]
            snapshots.sort(key=lambda row: str(row.get("窗口开始") or ""), reverse=True)
            if snapshots:
                latest = dict(snapshots[0])
                window_end = compute.parse_time(latest.get("窗口结束"))
                latest["卡片数据时延秒"] = (
                    max(0, int((now - window_end).total_seconds())) if window_end else None
                )
                lag = 0
                if latest_attempt is not None:
                    newest_start = compute.parse_time(latest_attempt.get("窗口开始"))
                    current_start = compute.parse_time(latest.get("窗口开始"))
                    if newest_start and current_start and (config or {}).get("计算窗口秒"):
                        lag = max(
                            0,
                            int(
                                (newest_start - current_start).total_seconds()
                                // config["计算窗口秒"]
                            ),
                        )
                latest["滞后窗口数"] = lag
                latest["最新尝试窗口"] = (latest_attempt or {}).get("窗口开始")
                latest["最新窗口状态"] = (latest_attempt or {}).get("窗口状态")
                latest["链路新鲜度"] = pipeline["链路新鲜度"]
                latest["来源"] = "窗口结果数据集（已同步）"
                latest["是否可展示"] = True
                if lag > 0:
                    latest["说明"] = (
                        f"更新的窗口 {latest['最新尝试窗口']} 状态为"
                        f"「{latest['最新窗口状态'] or '未知'}」，大盘保留上一份完整结果并标注滞后"
                    )
                else:
                    latest["说明"] = "当前展示最新可消费窗口结果"
                cards.append(latest)
                continue
            # 尚未产生可消费窗口：给出占位卡，明确说明不是旧结果残留。
            cards.append({
                "大盘编号": dashboard.get("大盘编号"),
                "大盘名称": dashboard.get("大盘名称"),
                "指标编号": metric_number,
                "指标名称": (config or {}).get("指标名称") or (metric_row or {}).get("指标名称"),
                "数据源流": (config or {}).get("数据源流"),
                "聚合函数": (config or {}).get("聚合函数"),
                "维度字段组": (config or {}).get("维度字段组"),
                "窗口开始": None,
                "窗口结束": None,
                "指标值": None,
                "覆盖率": None,
                "数据新鲜度秒": None,
                "新鲜度状态": "等待可消费窗口",
                "窗口状态": (latest_attempt or {}).get("窗口状态") or "覆盖率不足",
                "结果版本": None,
                "结果指纹": None,
                "计算批次": None,
                "刷新时间": compute.iso(now),
                "是否可展示": False,
                "最新尝试窗口": (latest_attempt or {}).get("窗口开始"),
                "最新窗口状态": (latest_attempt or {}).get("窗口状态"),
                "滞后窗口数": None,
                "链路新鲜度": pipeline["链路新鲜度"],
                "来源": "窗口结果数据集",
                "说明": "；".join(errors) if errors else "该指标还没有已到齐且新鲜度完整的窗口结果，暂不展示旧值",
            })
        return cards

    def refresh_all(self) -> dict[str, Any]:
        """批量刷新全部已发布大盘，返回每个大盘的指标卡与刷新结果。"""
        compute.ensure_bootstrapped()
        # 刷新前先按现行取数链路把阈值评估推进到最新窗口，保证同一次刷新里
        # 大盘与阈值读到的是同一批窗口结果版本。
        threshold_module = importlib.import_module("app.services.threshold")
        threshold_sync = threshold_module.ThresholdService().refresh()
        refreshed: list[dict[str, Any]] = []
        total_cards = 0
        for dashboard in store.rows(MODULE):
            if dashboard.get("status") != "已发布":
                continue
            cards = self.refresh(dashboard)
            total_cards += len(cards)
            refreshed.append({
                "大盘编号": dashboard.get("大盘编号"),
                "大盘名称": dashboard.get("大盘名称"),
                "指标组": _groups(dashboard),
                "指标卡": cards,
            })
        return {
            "refreshed": refreshed,
            "dashboard_count": len(refreshed),
            "card_count": total_cards,
            "threshold_sync": threshold_sync,
            "refreshed_at": compute.iso(store.now()),
            "口径": "大盘结果随窗口重放联动刷新；不可消费窗口不展示旧结果",
        }

    def snapshots(self, dashboard_number: str | None = None) -> list[dict[str, Any]]:
        compute.ensure_bootstrapped()
        rows = [dict(row) for row in store.rows(compute.DASHBOARD_SNAPSHOT_MODULE)]
        if dashboard_number:
            rows = [row for row in rows if row.get("大盘编号") == dashboard_number]
        return rows
