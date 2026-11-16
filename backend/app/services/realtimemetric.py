"""实时指标业务规则：定义维护、组合定位与窗口计算编排都收在这里。"""
from __future__ import annotations

from typing import Any

from app.services import realtimecompute as compute
from app.store import store

MODULE = "realtimemetric"
REQUIRED_FIELDS = ["指标编号", "指标名称", "计算窗口秒"]
STATUS_ORDER = ["草稿", "定义评审中", "已上线", "已下线"]
ACTION_RULES = {"提交定义评审": "定义评审中", "上线指标": "已上线", "下线指标": "已下线"}
NEGATIVE_ACTIONS = ["下线指标"]

LIST_FIELDS = [
    "指标编号", "指标名称", "计算窗口秒", "聚合函数",
    "数据源流", "维度字段组", "延迟容忍毫秒", "精度位数",
]
LOCATE_FIELDS = ["数据源流", "计算窗口秒", "聚合函数", "维度字段组"]
SORT_FIELDS = {
    "metric": "指标编号",
    "name": "指标名称",
    "window": "计算窗口秒",
    "datasource": "数据源流",
    "aggregate": "聚合函数",
    "status": "status",
}
STATUS_RANK = {status: index for index, status in enumerate(STATUS_ORDER)}


class RealtimemetricService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        datasource_stream: str | None = None,
        window_seconds: str | int | None = None,
        aggregate: str | None = None,
        dimension_group: str | None = None,
        sort: str = "metric",
        order: str = "asc",
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int, dict[str, Any], dict[str, Any]]:
        """按数据源流、窗口范围、聚合方式和维度字段组组合定位指标定义。

        关键字检索同时覆盖指标编号、指标名称、数据源流与维度字段组；返回未分页的
        汇总信息与可选口径（facets），避免把当前页条数误当成全部窗口数。
        """
        compute.ensure_bootstrapped()
        now = store.now()
        streams = compute.stream_index()
        configs: dict[str, dict[str, Any]] = {}
        for metric_row in store.rows(MODULE):
            config, _ = compute.metric_config(metric_row, streams)
            if config:
                configs[str(metric_row.get("指标编号"))] = config
        windows = store.rows(compute.RESULT_MODULE)
        window_stats: dict[str, dict[str, Any]] = {}
        for row in windows:
            number = str(row.get("指标编号"))
            bucket = window_stats.setdefault(number, {
                "窗口数": 0, "已到齐": 0, "可容忍迟到": 0, "覆盖率不足": 0,
                "可消费": 0, "最新窗口": None,
            })
            bucket["窗口数"] += 1
            # 统计口径与窗口视图一致：按当前时钟重算状态，避免封窗后仍残留“可容忍迟到”。
            status_name = compute.window_status_now(row, configs.get(number), now)
            if status_name in bucket:
                bucket[status_name] += 1
            if compute.consumable(row, configs.get(number), now):
                bucket["可消费"] += 1
            if bucket["最新窗口"] is None or str(row.get("窗口结束")) > str(bucket["最新窗口"]):
                bucket["最新窗口"] = row.get("窗口结束")

        rows = [dict(row) for row in store.rows(MODULE)]
        if keyword:
            needle = keyword.strip().lower()
            rows = [
                row for row in rows
                if needle in " ".join(
                    str(row.get(field) or "")
                    for field in ("指标编号", "指标名称", "数据源流", "维度字段组", "聚合函数")
                ).lower()
            ]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        if datasource_stream:
            rows = [row for row in rows if row.get("数据源流") == datasource_stream]
        if window_seconds not in (None, "", "all"):
            rows = [row for row in rows if str(row.get("计算窗口秒")) == str(window_seconds)]
        if aggregate:
            rows = [row for row in rows if row.get("聚合函数") == aggregate]
        if dimension_group:
            rows = [row for row in rows if row.get("维度字段组") == dimension_group]

        sort_key = SORT_FIELDS.get(sort or "metric", "指标编号")
        reverse = str(order or "asc").lower() == "desc"
        if sort_key == "status":
            rows.sort(key=lambda row: STATUS_RANK.get(str(row.get("status")), 9), reverse=reverse)
        elif sort_key == "计算窗口秒":
            rows.sort(key=lambda row: int(str(row.get(sort_key) or 0) or 0), reverse=reverse)
        else:
            rows.sort(key=lambda row: str(row.get(sort_key) or ""), reverse=reverse)

        total = len(rows)
        page = max(1, int(page or 1))
        size = max(1, min(int(size or 20), 200))
        start = (page - 1) * size
        items = []
        for row in rows[start:start + size]:
            item = dict(row)
            item["窗口统计"] = window_stats.get(str(row.get("指标编号")), {
                "窗口数": 0, "已到齐": 0, "可容忍迟到": 0, "覆盖率不足": 0,
                "可消费": 0, "最新窗口": None,
            })
            items.append(item)

        all_rows = [dict(row) for row in store.rows(MODULE)]
        summary = {
            "指标定义总数": len(all_rows),
            "已上线指标": sum(1 for row in all_rows if row.get("status") == "已上线"),
            "评审中指标": sum(1 for row in all_rows if row.get("status") == "定义评审中"),
            "当前筛选结果数": total,
            "当前页条数": len(items),
            "窗口结果总数": len(windows),
            "可消费窗口": sum(
                bucket["可消费"] for bucket in window_stats.values()
            ),
            "覆盖率不足窗口": sum(
                bucket["覆盖率不足"] for bucket in window_stats.values()
            ),
        }
        facets = {
            "数据源流": sorted({str(row.get("数据源流")) for row in all_rows if row.get("数据源流")}),
            "计算窗口秒": sorted({int(str(row.get("计算窗口秒") or 0) or 0) for row in all_rows}),
            "聚合函数": sorted({str(row.get("聚合函数")) for row in all_rows if row.get("聚合函数")}),
            "维度字段组": sorted({str(row.get("维度字段组")) for row in all_rows if row.get("维度字段组")}),
            "状态": list(STATUS_ORDER),
            "定位口径": LIST_FIELDS,
            "可组合定位字段": LOCATE_FIELDS,
        }
        return items, total, summary, facets

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        compute.ensure_bootstrapped()
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        item = dict(entry)
        config, errors = compute.metric_config(entry, compute.stream_index())
        item["校验结果"] = {"通过": config is not None, "问题": errors}
        item["数据源流记录"] = config["流记录"] if config else None
        item["窗口结果"] = [
            dict(row) for row in reversed(store.rows(compute.RESULT_MODULE))
            if row.get("指标编号") == entry.get("指标编号")
        ][:20]
        return item

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        payload = {field: values.get(field) for field in REQUIRED_FIELDS}
        for extra in LIST_FIELDS:
            if extra in values:
                payload[extra] = values.get(extra)
        payload.setdefault("延迟容忍毫秒", values.get("延迟容忍毫秒") or "60000")
        payload.setdefault("精度位数", values.get("精度位数") or "2")
        payload.setdefault("新鲜度阈值秒", values.get("新鲜度阈值秒") or values.get("计算窗口秒"))
        payload.setdefault("覆盖率下限", values.get("覆盖率下限") or "0.8")
        payload["status"] = STATUS_ORDER[0]
        payload["pending"] = True
        payload["abnormal"] = False
        with store.transaction():
            entry = store.insert(MODULE, payload)
        return entry, []

    def update_entry(self, entry_id: int, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, [f"指标定义 {entry_id} 不存在或已归档"]
        editable = list(LIST_FIELDS)
        changed: dict[str, Any] = {}
        for field in editable:
            if field in values and values.get(field) is not None:
                changed[field] = values.get(field)
        if not changed:
            return entry, []
        candidate = {**entry, **changed}
        _, errors = compute.metric_config(candidate, compute.stream_index())
        if errors:
            return None, errors
        with store.transaction():
            current = store.find(MODULE, entry_id)
            assert current is not None
            current.update(changed)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"指标定义 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于实时指标可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        if action == "上线指标":
            _, errors = compute.metric_config(entry, compute.stream_index())
            if errors:
                return None, f"指标定义未通过校验，不能上线：{'；'.join(errors)}"
        with store.transaction():
            current = store.find(MODULE, entry_id)
            assert current is not None
            current["status"] = target
            current["pending"] = target != STATUS_ORDER[-1]
            current["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"指标定义已{action}"

    # ------------------------------------------------------------------
    # 计算链路口径
    # ------------------------------------------------------------------
    def validate_definitions(self) -> dict[str, Any]:
        compute.ensure_bootstrapped()
        configs, notes = compute.configs_for([int(row["id"]) for row in store.rows(MODULE)])
        return {
            "通过": sum(1 for note in notes if note["级别"] != "阻断"),
            "阻断": [note for note in notes if note["级别"] == "阻断"],
            "提示": [note for note in notes if note["级别"] == "提示"],
            "可计算指标": [config["指标编号"] for config in configs],
            "数据源流记录": [dict(row) for row in store.rows(compute.STREAM_MODULE)],
            "校验口径": "指标编号唯一、窗口为正整数、聚合在允许集合内、数据源流已登记、维度字段属于该流",
        }

    def recompute(self, payload: dict[str, Any]) -> dict[str, Any]:
        compute.ensure_bootstrapped()
        metric_ids = payload.get("metric_ids")
        if isinstance(metric_ids, str):
            metric_ids = [part.strip() for part in metric_ids.split(",") if part.strip()]
        parsed_ids: list[int] | None = None
        if metric_ids:
            parsed_ids = []
            for value in metric_ids:
                try:
                    parsed_ids.append(int(value))
                except (TypeError, ValueError):
                    return {"ok": False, "message": f"指标编号「{value}」不是有效整数", "batch": None, "校验信息": []}
        return compute.compute_windows(
            metric_ids=parsed_ids,
            lookback=int(payload.get("lookback") or compute.DEFAULT_LOOKBACK_WINDOWS),
            window_from=payload.get("window_from"),
            window_to=payload.get("window_to"),
            trigger=str(payload.get("trigger") or "手工校验计算"),
            requested_by=payload.get("requested_by"),
            simulate_failure=bool(payload.get("simulate_failure")),
        )

    def submit_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        compute.ensure_bootstrapped()
        return compute.submit_event(
            payload,
            requested_by=payload.get("requested_by"),
            simulate_failure=bool(payload.get("simulate_failure")),
        )

    def windows(self, params: dict[str, Any]) -> dict[str, Any]:
        return compute.list_windows(**params)

    def dataset(self, params: dict[str, Any]) -> dict[str, Any]:
        return compute.dataset(**params)
