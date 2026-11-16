"""阈值管理业务规则：规则维护 + 按现行取数链路消费窗口结果。

阈值评估不直接读源事件，而是消费实时指标计算结果数据集里的窗口结果：
- 只有「已到齐且新鲜度完整」的窗口才参与判定；
- 窗口重放产生新版本时，同窗口的评估记录随之更新，不保留旧结论；
- 不可消费窗口写一条“数据不可用”留痕，避免拿半成品误报。
"""
from __future__ import annotations

from typing import Any

from app.services import realtimecompute as compute
from app.store import store

MODULE = "threshold"
REQUIRED_FIELDS = ["规则编号", "关联指标", "阈值下限"]
STATUS_ORDER = ["草稿", "规则评审中", "已生效", "已关闭"]
ACTION_RULES = {"提交规则评审": "规则评审中", "生效规则": "已生效", "关闭规则": "已关闭"}
NEGATIVE_ACTIONS = []
MAX_EVALUATION_WINDOWS = 8


class ThresholdService:
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
                    for field in ("规则编号", "关联指标", "触发条件", "告警等级")
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
        item["最近评估"] = [
            dict(row) for row in reversed(store.rows(compute.THRESHOLD_EVALUATION_MODULE))
            if row.get("规则编号") == entry.get("规则编号")
        ][:10]
        return item

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        metric_number = str(values.get("关联指标") or "").strip()
        metric_row = compute._metric_by_number(metric_number)
        if metric_row is None:
            return None, [f"关联指标「{metric_number}」未在实时指标定义中登记"]
        with store.transaction():
            entry = store.insert(MODULE, {
                "规则编号": values.get("规则编号"),
                "关联指标": metric_number,
                "阈值下限": values.get("阈值下限"),
                "阈值上限": values.get("阈值上限"),
                "触发条件": values.get("触发条件") or "低于下限或高于上限",
                "持续周期": values.get("持续周期") or "1 个窗口",
                "告警等级": values.get("告警等级") or "P3",
                "生效时间范围": values.get("生效时间范围") or "长期有效",
                "status": STATUS_ORDER[0],
                "pending": True,
                "abnormal": False,
            })
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"阈值规则 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于阈值管理可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        with store.transaction():
            current = store.find(MODULE, entry_id)
            assert current is not None
            current["status"] = target
            current["pending"] = target != STATUS_ORDER[-1]
            current["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"阈值规则已{action}"

    # ------------------------------------------------------------------
    # 窗口结果消费
    # ------------------------------------------------------------------
    def evaluations(
        self,
        *,
        rule_number: str | None = None,
        metric_number: str | None = None,
        verdict: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> dict[str, Any]:
        compute.ensure_bootstrapped()
        rows = [dict(row) for row in store.rows(compute.THRESHOLD_EVALUATION_MODULE)]
        if rule_number:
            rows = [row for row in rows if row.get("规则编号") == rule_number]
        if metric_number:
            rows = [row for row in rows if row.get("指标编号") == metric_number]
        if verdict:
            rows = [row for row in rows if row.get("判定结果") == verdict]
        rows.sort(
            key=lambda row: (str(row.get("窗口开始") or ""), str(row.get("规则编号") or "")),
            reverse=True,
        )
        total = len(rows)
        start = (max(page, 1) - 1) * size
        summary = {
            "评估总数": total,
            "正常": sum(1 for row in rows if row.get("判定结果") == "正常"),
            "低于下限": sum(1 for row in rows if row.get("判定结果") == "低于下限"),
            "高于上限": sum(1 for row in rows if row.get("判定结果") == "高于上限"),
            "数据不可用": sum(1 for row in rows if row.get("判定结果") == "数据不可用"),
        }
        return {
            "items": rows[start:start + size],
            "total": total,
            "page": page,
            "size": size,
            "summary": summary,
            "取数链路": "阈值规则 → 实时指标计算结果数据集（只消费可消费窗口）→ 评估记录",
        }

    def refresh(self) -> dict[str, Any]:
        """按现行取数链路刷新阈值评估：从最新可消费窗口结果重算判定。"""
        compute.ensure_bootstrapped()
        updated = 0
        skipped = 0
        for rule in store.rows(MODULE):
            if rule.get("status") != "已生效":
                skipped += 1
                continue
            metric_number = str(rule.get("关联指标") or "").strip()
            metric_row = compute._metric_by_number(metric_number)
            if metric_row is None:
                skipped += 1
                continue
            windows = [
                row for row in store.rows(compute.RESULT_MODULE)
                if row.get("指标编号") == metric_number
            ]
            windows.sort(key=lambda row: str(row.get("窗口开始") or ""), reverse=True)
            config, _ = compute.metric_config(metric_row, compute.stream_index())
            for row in windows[:MAX_EVALUATION_WINDOWS]:
                if config is None:
                    break
                with store.transaction():
                    compute._refresh_threshold_evaluation(
                        config=config,
                        result=row,
                        now=store.now(),
                        batch_number=str(row.get("计算批次") or "阈值同步"),
                    )
                updated += 1
        return {
            "updated": updated,
            "skipped_rules": skipped,
            "evaluations": len(store.rows(compute.THRESHOLD_EVALUATION_MODULE)),
            "取数链路": "阈值规则 → 实时指标计算结果数据集 → 评估记录",
        }
