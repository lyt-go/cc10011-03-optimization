"""实时指标窗口计算引擎。

本模块把“迟到事件重放 → 窗口结果版本 → 下游消费”收敛到同一条链路：

1. **幂等摄取**：以「数据源流 + 事件编号」为唯一键，重复提交只记流水、不改结果；
2. **事件时间窗口**：窗口按指标的计算窗口秒对齐，事件按事件时间归窗；
3. **水位与重放**：窗口结束 + 延迟容忍毫秒是重放截止时间（水位）。截止前允许新事件
   触发窗口整体重算并生成新版本；截止后到达的事件只记流水、不再进入该窗口；
4. **原子提交**：窗口结果、版本流水、大盘快照、阈值评估在同一个事务里写入，
   事务失败时一起回滚，残缺版本不会暴露；
5. **下游只读完整结果**：大盘与阈值只消费「已到齐 + 新鲜度未滞留 + 覆盖率达标」
   的窗口，宁可保留上一份完整结果，也不读半成品。
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from app.store import TransactionFailed, store

MODULE = "realtimemetric"
STREAM_MODULE = "datasource_stream"
EVENT_MODULE = "metric_event"
INGEST_MODULE = "metric_event_ingest"
RESULT_MODULE = "metric_window_result"
VERSION_MODULE = "metric_window_version"
BATCH_MODULE = "metric_compute_batch"
DASHBOARD_SNAPSHOT_MODULE = "dashboard_window_snapshot"
THRESHOLD_EVALUATION_MODULE = "threshold_evaluation"

AGGREGATES = ("COUNT", "SUM", "AVG", "MAX", "MIN")

STATUS_COMPLETE = "已到齐"
STATUS_TOLERABLE = "可容忍迟到"
STATUS_INSUFFICIENT = "覆盖率不足"
WINDOW_STATUSES = (STATUS_COMPLETE, STATUS_TOLERABLE, STATUS_INSUFFICIENT)
STATUS_RANK = {STATUS_COMPLETE: 0, STATUS_TOLERABLE: 1, STATUS_INSUFFICIENT: 2}

FRESHNESS_FRESH = "新鲜"
FRESHNESS_STALE = "滞留"
FRESHNESS_SEALED = "已封窗"
FRESHNESS_WAITING = "等待数据"

DEFAULT_LOOKBACK_WINDOWS = 8
MAX_LOOKBACK_WINDOWS = 120
MAX_RANGE_SECONDS = 7 * 24 * 3600


# ---------------------------------------------------------------------------
# 时间与序列化辅助
# ---------------------------------------------------------------------------
def parse_time(value: Any) -> datetime | None:
    """把 ISO 时间字符串解析成带时区的 UTC 时间；无法解析时返回 None。"""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def iso(moment: datetime | None) -> str | None:
    if moment is None:
        return None
    return moment.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def align_window(moment: datetime, window_seconds: int) -> tuple[datetime, datetime]:
    """把时刻对齐到窗口左闭右开区间。"""
    epoch = int(moment.timestamp())
    start_epoch = epoch - epoch % window_seconds
    start = datetime.fromtimestamp(start_epoch, tz=timezone.utc)
    return start, start + timedelta(seconds=window_seconds)


def _round_value(value: float | int | None, precision: int) -> float | int | None:
    if value is None:
        return None
    rounded = round(float(value), precision)
    if precision == 0 and float(rounded).is_integer():
        return int(rounded)
    return rounded


def _digest(*parts: Any) -> str:
    payload = json.dumps(parts, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# 配置解析与校验
# ---------------------------------------------------------------------------
def _as_int(value: Any, default: int | None = None) -> int | None:
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float | None = None) -> float | None:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return default


def stream_index() -> dict[str, dict[str, Any]]:
    return {str(row.get("数据源流")): row for row in store.rows(STREAM_MODULE)}


def metric_config(row: dict[str, Any], streams: dict[str, dict[str, Any]]) -> tuple[dict[str, Any] | None, list[str]]:
    """把指标定义解析成可执行配置，并返回字段级校验问题。"""
    errors: list[str] = []
    window_seconds = _as_int(row.get("计算窗口秒"))
    if window_seconds is None or window_seconds <= 0:
        errors.append("计算窗口秒必须是正整数")
    elif window_seconds > 86400:
        errors.append("计算窗口秒不能超过 86400")
    aggregate = str(row.get("聚合函数") or "").strip().upper()
    if aggregate not in AGGREGATES:
        errors.append(f"聚合函数必须是 {'、'.join(AGGREGATES)} 之一")
    stream_name = str(row.get("数据源流") or "").strip()
    stream_row = streams.get(stream_name)
    if not stream_row:
        errors.append(f"数据源流「{stream_name or '未填写'}」没有登记在数据源流记录里")
    dimension_text = str(row.get("维度字段组") or "").strip()
    dimensions = [part.strip() for part in dimension_text.split(",") if part.strip()]
    if not dimensions:
        errors.append("维度字段组不能为空")
    tolerance_ms = _as_int(row.get("延迟容忍毫秒"), 0)
    if tolerance_ms is None or tolerance_ms < 0:
        errors.append("延迟容忍毫秒不能为负数")
    freshness_threshold = _as_int(row.get("新鲜度阈值秒"), window_seconds or 60)
    if freshness_threshold is None or freshness_threshold <= 0:
        errors.append("新鲜度阈值秒必须是正整数")
    coverage_floor = _as_float(row.get("覆盖率下限"), 0.8)
    if coverage_floor is None or not 0 <= coverage_floor <= 1:
        errors.append("覆盖率下限必须在 0 到 1 之间")
    precision = _as_int(row.get("精度位数"), 2)
    if precision is None or not 0 <= precision <= 6:
        errors.append("精度位数必须在 0 到 6 之间")
    if stream_row and dimensions:
        stream_dimensions = {
            part.strip()
            for part in str(stream_row.get("维度字段", "")).split(",")
            if part.strip()
        }
        unknown = [name for name in dimensions if name not in stream_dimensions]
        if unknown:
            errors.append(f"维度字段{'、'.join(unknown)}不在数据源流「{stream_name}」的维度字段里")
    if errors:
        return None, errors
    assert window_seconds is not None and stream_row is not None
    return {
        "id": int(row["id"]),
        "指标编号": str(row.get("指标编号") or ""),
        "指标名称": str(row.get("指标名称") or ""),
        "计算窗口秒": window_seconds,
        "聚合函数": aggregate,
        "数据源流": stream_name,
        "维度字段组": ",".join(dimensions),
        "维度字段": dimensions,
        "延迟容忍毫秒": tolerance_ms or 0,
        "新鲜度阈值秒": freshness_threshold or window_seconds,
        "覆盖率下限": coverage_floor,
        "精度位数": precision,
        "status": str(row.get("status") or ""),
        "流记录": stream_row,
    }, []


def configs_for(metric_ids: Iterable[int] | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """返回可执行配置与会话级校验结果。"""
    streams = stream_index()
    configs: list[dict[str, Any]] = []
    notes: list[dict[str, Any]] = []
    wanted = set(metric_ids or [])
    for row in store.rows(MODULE):
        if wanted and int(row.get("id", 0)) not in wanted:
            continue
        if not wanted and row.get("status") != "已上线":
            continue
        config, errors = metric_config(row, streams)
        if config is None:
            notes.append({
                "指标编号": row.get("指标编号"),
                "指标名称": row.get("指标名称"),
                "级别": "阻断",
                "问题": "；".join(errors),
            })
            continue
        if config["status"] != "已上线":
            notes.append({
                "指标编号": config["指标编号"],
                "指标名称": config["指标名称"],
                "级别": "提示",
                "问题": f"指标状态为「{config['status']}」，只校验不参与正式窗口计算",
            })
            continue
        configs.append(config)
    return configs, notes


# ---------------------------------------------------------------------------
# 事件收集与聚合
# ---------------------------------------------------------------------------
def expected_events(stream_row: dict[str, Any], window_seconds: int) -> int:
    base = max(int(stream_row.get("每窗口预期事件数", 1)), 1)
    base_window = max(int(stream_row.get("基准窗口秒", 60)), 1)
    return max(1, int(round(base * window_seconds / base_window)))


def collect_events(stream_name: str, window_start: datetime, window_end: datetime, deadline: datetime) -> list[dict[str, Any]]:
    """读取窗口内、且在水位前已到达的事件，按事件编号去重。

    水位过滤保证：窗口封窗后即使源事件表里后来补进了迟到事件，重算也不会把
    封窗结果改掉，重算结果对同一水位是可复现的。
    """
    seen: dict[str, dict[str, Any]] = {}
    for row in store.rows(EVENT_MODULE):
        if row.get("数据源流") != stream_name:
            continue
        event_time = parse_time(row.get("事件时间"))
        received_time = parse_time(row.get("收到时间"))
        if event_time is None or received_time is None:
            continue
        if not (window_start <= event_time < window_end):
            continue
        if received_time > deadline:
            continue
        event_id = str(row.get("事件编号") or "")
        if not event_id:
            continue
        existing = seen.get(event_id)
        # 同一事件编号只保留最早到达的一条，重复提交不会重复累计。
        if existing is None or received_time < (parse_time(existing.get("收到时间")) or received_time):
            seen[event_id] = row
    return sorted(seen.values(), key=lambda item: str(item.get("事件编号")))


def aggregate_values(aggregate: str, events: list[dict[str, Any]]) -> tuple[float | int | None, list[float]]:
    values: list[float] = []
    for row in events:
        value = _as_float(row.get("度量值"))
        if value is not None:
            values.append(value)
    if aggregate == "COUNT":
        return len(events), values
    if not values:
        return None, values
    if aggregate == "SUM":
        return sum(values), values
    if aggregate == "AVG":
        return sum(values) / len(values), values
    if aggregate == "MAX":
        return max(values), values
    if aggregate == "MIN":
        return min(values), values
    return None, values


def dimension_breakdown(
    events: list[dict[str, Any]],
    dimensions: list[str],
    aggregate: str,
    precision: int,
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for row in events:
        raw = row.get("维度") if isinstance(row.get("维度"), dict) else {}
        key = tuple(str(raw.get(name, "")) for name in dimensions)
        groups.setdefault(key, []).append(row)
    breakdown: list[dict[str, Any]] = []
    for key, rows in sorted(groups.items()):
        value, _ = aggregate_values(aggregate, rows)
        breakdown.append({
            "维度": {name: key[index] for index, name in enumerate(dimensions)},
            "指标值": _round_value(value, precision),
            "事件数": len(rows),
        })
    return breakdown


def classify_window(
    *,
    coverage: float,
    coverage_floor: float,
    event_count: int,
    expected: int,
    sealed: bool,
) -> str:
    """区分已到齐 / 可容忍迟到 / 覆盖率不足。

    - 覆盖率低于指标下限：无论是否封窗都是「覆盖率不足」；
    - 覆盖率达标但仍在重放窗口内、事件数未到预期：属于「可容忍迟到」，允许继续重放；
    - 其余达标窗口（已封窗或事件已到预期）视为「已到齐」。
    """
    if coverage < coverage_floor:
        return STATUS_INSUFFICIENT
    if not sealed and event_count < expected:
        return STATUS_TOLERABLE
    return STATUS_COMPLETE


def freshness_of(
    result: dict[str, Any],
    metric: dict[str, Any] | None,
    now: datetime,
) -> dict[str, Any]:
    """数据新鲜度：未封窗窗口按“距最新数据到达”判定，封窗窗口标记为最终结果。

    封窗窗口不会再有事件进入，结果是最终态，不能因为时间推移被误判成滞留；
    未封窗窗口如果最新数据已超过新鲜度阈值，说明重放没跟上，属于滞留。
    """
    latest_received = parse_time(result.get("数据最新收到时间"))
    deadline = parse_time(result.get("重放截止时间"))
    threshold = int(
        (metric or {}).get("新鲜度阈值秒")
        or result.get("新鲜度阈值秒")
        or 60
    )
    sealed = deadline is not None and now >= deadline
    if latest_received is None:
        return {
            "数据新鲜度秒": None,
            "新鲜度状态": FRESHNESS_STALE if sealed else FRESHNESS_WAITING,
            "新鲜度阈值秒": threshold,
            "新鲜度口径": "窗口内没有收到任何数据，无法判定新鲜度"
            if sealed else "当前窗口尚未收到数据，正在等待首条事件",
        }
    age = max(0, int((now - latest_received).total_seconds()))
    if sealed:
        return {
            "数据新鲜度秒": age,
            "新鲜度状态": FRESHNESS_SEALED,
            "新鲜度阈值秒": threshold,
            "新鲜度口径": "窗口已封窗，结果为最终版本，新鲜度仅作追溯",
        }
    return {
        "数据新鲜度秒": age,
        "新鲜度状态": FRESHNESS_FRESH if age <= threshold else FRESHNESS_STALE,
        "新鲜度阈值秒": threshold,
        "新鲜度口径": "距窗口内最新数据到达的秒数，超过阈值视为滞留",
    }


def window_status_now(
    result: dict[str, Any],
    metric: dict[str, Any] | None,
    now: datetime,
) -> str:
    """窗口状态随时间推移会从“可容忍迟到”转为“已到齐”，读时按当前时钟重算。"""
    coverage_floor = float(
        (metric or {}).get("覆盖率下限")
        or result.get("覆盖率下限")
        or 0.0
    )
    deadline = parse_time(result.get("重放截止时间"))
    sealed = deadline is not None and now >= deadline
    return classify_window(
        coverage=float(result.get("覆盖率") or 0.0),
        coverage_floor=coverage_floor,
        event_count=int(result.get("去重后事件数") or 0),
        expected=int(result.get("预期事件数") or 0),
        sealed=sealed,
    )


def consumable(result: dict[str, Any], metric: dict[str, Any] | None, now: datetime) -> bool:
    """下游消费准入：窗口已到齐、覆盖率达标、数据新鲜度未滞留。

    封窗窗口的新鲜度是最终态（已封窗），只要「已到齐」即可消费；未封窗窗口必须是
    「新鲜」状态，避免下游读到重放没跟上的半成品。
    """
    status = window_status_now(result, metric, now)
    freshness = freshness_of(result, metric, now)
    return status == STATUS_COMPLETE and freshness["新鲜度状态"] in (FRESHNESS_FRESH, FRESHNESS_SEALED)


# ---------------------------------------------------------------------------
# 版本与下游同步
# ---------------------------------------------------------------------------
def _next_batch_number(now: datetime) -> str:
    rows = store.rows(BATCH_MODULE)
    prefix = now.strftime("BATCH-%Y%m%d%H%M%S")
    return f"{prefix}-{len(rows) + 1:03d}"


def _metric_by_number(number: str) -> dict[str, Any] | None:
    for row in store.rows(MODULE):
        if str(row.get("指标编号")) == number:
            return row
    return None


def _upsert_window_result(
    *,
    config: dict[str, Any],
    window_start: datetime,
    window_end: datetime,
    deadline: datetime,
    events: list[dict[str, Any]],
    value: Any,
    coverage: float,
    status: str,
    latest_received: datetime | None,
    now: datetime,
    batch_number: str,
    trigger: str,
    version: int,
) -> dict[str, Any]:
    expected = expected_events(config["流记录"], config["计算窗口秒"])
    result_rows = store.rows(RESULT_MODULE)
    existing = next(
        (
            row
            for row in result_rows
            if row.get("指标编号") == config["指标编号"]
            and row.get("窗口开始") == iso(window_start)
        ),
        None,
    )
    event_ids = sorted(str(row.get("事件编号")) for row in events)
    digest = _digest(config["指标编号"], iso(window_start), config["聚合函数"], value, len(event_ids), event_ids)
    freshness_seconds = None
    if latest_received is not None:
        freshness_seconds = max(0, int((now - latest_received).total_seconds()))
    payload = {
        "指标编号": config["指标编号"],
        "指标名称": config["指标名称"],
        "指标定义编号": config["id"],
        "数据源流": config["数据源流"],
        "计算窗口秒": config["计算窗口秒"],
        "聚合函数": config["聚合函数"],
        "维度字段组": config["维度字段组"],
        "窗口开始": iso(window_start),
        "窗口结束": iso(window_end),
        "窗口标识": f"{config['指标编号']}@{iso(window_start)}",
        "重放截止时间": iso(deadline),
        "指标值": _round_value(value, config["精度位数"]),
        "事件数": len(events),
        "去重后事件数": len(events),
        "预期事件数": expected,
        "覆盖率": coverage,
        "覆盖率下限": config["覆盖率下限"],
        "数据最新收到时间": iso(latest_received),
        "最后计算时间": iso(now),
        "新鲜度阈值秒": config["新鲜度阈值秒"],
        "窗口状态": status,
        "结果版本": version,
        "计算批次": batch_number,
        "触发方式": trigger,
        "结果指纹": digest,
        "维度明细": dimension_breakdown(events, config["维度字段"], config["聚合函数"], config["精度位数"]),
    }
    if existing is None:
        result = store.insert(RESULT_MODULE, payload)
    else:
        existing.update(payload)
        result = existing
    # 版本流水：每次校验并计算都留痕，旧版本标记为非当前。
    previous_versions = [
        row for row in store.rows(VERSION_MODULE)
        if row.get("指标编号") == config["指标编号"] and row.get("窗口开始") == iso(window_start)
    ]
    for row in previous_versions:
        row["是否当前版本"] = False
    store.insert(VERSION_MODULE, {
        "版本号": version,
        "指标编号": config["指标编号"],
        "指标名称": config["指标名称"],
        "数据源流": config["数据源流"],
        "窗口开始": iso(window_start),
        "窗口结束": iso(window_end),
        "窗口标识": result["窗口标识"],
        "聚合函数": config["聚合函数"],
        "指标值": payload["指标值"],
        "去重后事件数": len(events),
        "预期事件数": expected,
        "覆盖率": coverage,
        "覆盖率下限": config["覆盖率下限"],
        "数据新鲜度秒": freshness_seconds,
        "新鲜度阈值秒": config["新鲜度阈值秒"],
        "窗口状态": status,
        "重放截止时间": iso(deadline),
        "计算批次": batch_number,
        "触发方式": trigger,
        "计算出时间": iso(now),
        "结果指纹": digest,
        "是否当前版本": True,
        "校验结论": {
            "窗口字段完整": True,
            "覆盖率达标": coverage >= config["覆盖率下限"],
            "新鲜度可判定": latest_received is not None,
            "可被下游消费": status == STATUS_COMPLETE
            and freshness_seconds is not None
            and freshness_seconds <= config["新鲜度阈值秒"],
        },
        "版本说明": "首次计算" if version == 1 else f"第 {version} 次计算（迟到事件重放或人工重算）",
    })
    return result


def _refresh_dashboard_snapshot(
    *,
    config: dict[str, Any],
    result: dict[str, Any],
    now: datetime,
    batch_number: str,
    trigger: str,
) -> int:
    """把窗口结果同步到包含该指标的大盘；只同步可消费窗口。"""
    if not consumable(result, config, now):
        return 0
    freshness = freshness_of(result, config, now)
    touched = 0
    for dashboard in store.rows("dashboard"):
        groups = {
            part.strip()
            for part in str(dashboard.get("包含指标组") or "").split(",")
            if part.strip()
        }
        if config["指标编号"] not in groups:
            continue
        rows = store.rows(DASHBOARD_SNAPSHOT_MODULE)
        existing = next(
            (
                row for row in rows
                if row.get("大盘编号") == dashboard.get("大盘编号")
                and row.get("指标编号") == config["指标编号"]
                and row.get("窗口开始") == result["窗口开始"]
            ),
            None,
        )
        payload = {
            "大盘编号": dashboard.get("大盘编号"),
            "大盘名称": dashboard.get("大盘名称"),
            "指标编号": config["指标编号"],
            "指标名称": config["指标名称"],
            "数据源流": config["数据源流"],
            "聚合函数": config["聚合函数"],
            "维度字段组": config["维度字段组"],
            "窗口开始": result["窗口开始"],
            "窗口结束": result["窗口结束"],
            "指标值": result.get("指标值"),
            "覆盖率": result.get("覆盖率"),
            "数据新鲜度秒": freshness["数据新鲜度秒"],
            "新鲜度状态": freshness["新鲜度状态"],
            "窗口状态": result.get("窗口状态"),
            "结果版本": result.get("结果版本"),
            "结果指纹": result.get("结果指纹"),
            "计算批次": batch_number,
            "触发方式": trigger,
            "刷新时间": iso(now),
            "是否可展示": True,
        }
        if existing is None:
            store.insert(DASHBOARD_SNAPSHOT_MODULE, payload)
        else:
            existing.update(payload)
        touched += 1
    return touched


def _refresh_threshold_evaluation(
    *,
    config: dict[str, Any],
    result: dict[str, Any],
    now: datetime,
    batch_number: str,
) -> int:
    """阈值管理按现行取数链路读取窗口结果，写入评估记录。

    只有可消费窗口才参与阈值判定；不可消费窗口写一条“数据不可用”的评估留痕，
    避免阈值规则拿半成品数据误报。
    """
    freshness = freshness_of(result, config, now)
    allowed = consumable(result, config, now)
    value = result.get("指标值")
    touched = 0
    for rule in store.rows("threshold"):
        if str(rule.get("关联指标") or "").strip() != config["指标编号"]:
            continue
        lower = _as_float(rule.get("阈值下限"))
        upper = _as_float(rule.get("阈值上限"))
        numeric = _as_float(value)
        if not allowed:
            verdict = "数据不可用"
        elif numeric is None:
            verdict = "数据不可用"
        elif lower is not None and numeric < lower:
            verdict = "低于下限"
        elif upper is not None and numeric > upper:
            verdict = "高于上限"
        else:
            verdict = "正常"
        rows = store.rows(THRESHOLD_EVALUATION_MODULE)
        existing = next(
            (
                row for row in rows
                if row.get("规则编号") == rule.get("规则编号")
                and row.get("指标编号") == config["指标编号"]
                and row.get("窗口开始") == result["窗口开始"]
            ),
            None,
        )
        payload = {
            "规则编号": rule.get("规则编号"),
            "关联指标": config["指标编号"],
            "指标编号": config["指标编号"],
            "数据源流": config["数据源流"],
            "窗口开始": result["窗口开始"],
            "窗口结束": result["窗口结束"],
            "指标值": value,
            "阈值下限": rule.get("阈值下限"),
            "阈值上限": rule.get("阈值上限"),
            "判定结果": verdict,
            "是否已消费": allowed and verdict != "数据不可用",
            "窗口状态": result.get("窗口状态"),
            "覆盖率": result.get("覆盖率"),
            "数据新鲜度秒": freshness["数据新鲜度秒"],
            "结果版本": result.get("结果版本"),
            "结果指纹": result.get("结果指纹"),
            "计算批次": batch_number,
            "评估时间": iso(now),
            "评估次数": int((existing or {}).get("评估次数", 0)) + 1,
        }
        if existing is None:
            store.insert(THRESHOLD_EVALUATION_MODULE, payload)
        else:
            existing.update(payload)
        touched += 1
    return touched


# ---------------------------------------------------------------------------
# 计算入口
# ---------------------------------------------------------------------------
def _compute_config_windows(
    *,
    config: dict[str, Any],
    windows: list[datetime],
    trigger: str,
    now: datetime,
    batch_number: str,
) -> dict[str, Any]:
    stats = {
        "计算窗口数": 0,
        "已到齐": 0,
        "可容忍迟到": 0,
        "覆盖率不足": 0,
        "下游刷新数": 0,
    }
    for window_start in windows:
        window_end = window_start + timedelta(seconds=config["计算窗口秒"])
        deadline = window_end + timedelta(milliseconds=config["延迟容忍毫秒"])
        events = collect_events(config["数据源流"], window_start, window_end, deadline)
        expected = expected_events(config["流记录"], config["计算窗口秒"])
        coverage = round(min(1.0, len(events) / expected), 4) if expected else 0.0
        value, _ = aggregate_values(config["聚合函数"], events)
        status = classify_window(
            coverage=coverage,
            coverage_floor=config["覆盖率下限"],
            event_count=len(events),
            expected=expected,
            sealed=now >= deadline,
        )
        latest_received = max(
            (parse_time(row.get("收到时间")) for row in events),
            default=None,
        )
        existing = next(
            (
                row for row in store.rows(RESULT_MODULE)
                if row.get("指标编号") == config["指标编号"]
                and row.get("窗口开始") == iso(window_start)
            ),
            None,
        )
        version = int((existing or {}).get("结果版本", 0)) + 1
        result = _upsert_window_result(
            config=config,
            window_start=window_start,
            window_end=window_end,
            deadline=deadline,
            events=events,
            value=value,
            coverage=coverage,
            status=status,
            latest_received=latest_received,
            now=now,
            batch_number=batch_number,
            trigger=trigger,
            version=version,
        )
        stats["计算窗口数"] += 1
        stats[status] = stats.get(status, 0) + 1
        stats["下游刷新数"] += _refresh_dashboard_snapshot(
            config=config,
            result=result,
            now=now,
            batch_number=batch_number,
            trigger=trigger,
        )
        _refresh_threshold_evaluation(
            config=config,
            result=result,
            now=now,
            batch_number=batch_number,
        )
    return stats


def _windows_for_metric(config: dict[str, Any], now: datetime, lookback: int) -> list[datetime]:
    """返回最近 lookback 个“已经结束”的窗口。

    尚在进行中的窗口不进入结果集：它的事件还在陆续到达，提前落一个覆盖率不足的
    版本只会制造噪音；真正需要观察的迟到重放发生在已结束但未封窗的窗口上。
    """
    _, current_end = align_window(now, config["计算窗口秒"])
    last_completed_start = current_end - timedelta(seconds=config["计算窗口秒"] * 2)
    first_start = last_completed_start - timedelta(seconds=config["计算窗口秒"] * (lookback - 1))
    return [
        first_start + timedelta(seconds=config["计算窗口秒"] * index)
        for index in range(lookback)
    ]


def _record_failed_batch(
    *,
    trigger: str,
    message: str,
    metric_numbers: list[str],
    window_from: str | None,
    window_to: str | None,
    requested_by: str | None,
) -> dict[str, Any]:
    """提交失败后单独落一条失败批次，不携带任何窗口结果，避免暴露残缺版本。"""
    with store.transaction():
        now = store.now()
        batch = store.insert(BATCH_MODULE, {
            "批次编号": _next_batch_number(now),
            "触发方式": trigger,
            "发起人": requested_by or "值班管理员",
            "请求指标": metric_numbers,
            "窗口范围": [window_from, window_to],
            "开始时间": iso(now),
            "结束时间": iso(now),
            "状态": "失败",
            "计算窗口数": 0,
            "已到齐": 0,
            "可容忍迟到": 0,
            "覆盖率不足": 0,
            "下游刷新数": 0,
            "校验信息": [],
            "失败原因": message,
            "结果版本条目": [],
            "窗口标识样例": [],
        })
        return batch


def compute_windows(
    *,
    metric_ids: Iterable[int] | None = None,
    lookback: int = DEFAULT_LOOKBACK_WINDOWS,
    window_from: str | None = None,
    window_to: str | None = None,
    trigger: str = "手工校验计算",
    requested_by: str | None = None,
    simulate_failure: bool = False,
) -> dict[str, Any]:
    """校验并计算实时指标窗口结果；版本、新鲜度、覆盖率随批次一起原子落库。"""
    now = store.now()
    configs, notes = configs_for(metric_ids)
    if not configs:
        return {
            "ok": False,
            "message": "没有可参与计算的已上线指标，请先完成指标定义校验",
            "batch": None,
            "校验信息": notes,
        }
    lookback = max(1, min(int(lookback or DEFAULT_LOOKBACK_WINDOWS), MAX_LOOKBACK_WINDOWS))
    parsed_from = parse_time(window_from)
    parsed_to = parse_time(window_to)
    if (window_from and parsed_from is None) or (window_to and parsed_to is None):
        return {"ok": False, "message": "窗口范围时间格式无法解析，请使用 ISO 时间", "batch": None, "校验信息": notes}
    if parsed_from and parsed_to:
        if parsed_from >= parsed_to:
            return {"ok": False, "message": "窗口范围起点必须早于终点", "batch": None, "校验信息": notes}
        if (parsed_to - parsed_from).total_seconds() > MAX_RANGE_SECONDS:
            return {"ok": False, "message": "单次计算窗口范围不能超过 7 天", "batch": None, "校验信息": notes}
    if simulate_failure:
        store.fail_next_commit("故障演练：模拟数据库事务提交失败")

    batch_number = _next_batch_number(now)
    try:
        with store.transaction():
            stats = {
                "计算窗口数": 0,
                "已到齐": 0,
                "可容忍迟到": 0,
                "覆盖率不足": 0,
                "下游刷新数": 0,
            }
            version_numbers: list[int] = []
            window_labels: list[str] = []
            range_starts: list[str] = []
            range_ends: list[str] = []
            for config in configs:
                if parsed_from and parsed_to:
                    _, last_end = align_window(parsed_to, config["计算窗口秒"])
                    cursor = align_window(parsed_from, config["计算窗口秒"])[1]
                    windows = []
                    while cursor <= last_end:
                        windows.append(cursor - timedelta(seconds=config["计算窗口秒"]))
                        cursor += timedelta(seconds=config["计算窗口秒"])
                else:
                    windows = _windows_for_metric(config, parsed_to or now, lookback)
                if windows:
                    range_starts.append(iso(windows[0]))
                    range_ends.append(
                        iso(windows[-1] + timedelta(seconds=config["计算窗口秒"]))
                    )
                partial = _compute_config_windows(
                    config=config,
                    windows=windows,
                    trigger=trigger,
                    now=now,
                    batch_number=batch_number,
                )
                for key, value in partial.items():
                    stats[key] = stats.get(key, 0) + value
                for row in store.rows(VERSION_MODULE):
                    if row.get("计算批次") == batch_number:
                        version_numbers.append(int(row.get("id", 0)))
                        window_labels.append(str(row.get("窗口标识")))
            finished = store.now()
            batch = store.insert(BATCH_MODULE, {
                "批次编号": batch_number,
                "触发方式": trigger,
                "发起人": requested_by or "值班管理员",
                "请求指标": [config["指标编号"] for config in configs],
                "窗口范围": [
                    min(range_starts) if range_starts else None,
                    max(range_ends) if range_ends else None,
                ],
                "窗口标识样例": sorted(set(window_labels))[:5],
                "开始时间": iso(now),
                "结束时间": iso(finished),
                "状态": "成功",
                "计算窗口数": stats["计算窗口数"],
                "已到齐": stats["已到齐"],
                "可容忍迟到": stats["可容忍迟到"],
                "覆盖率不足": stats["覆盖率不足"],
                "下游刷新数": stats["下游刷新数"],
                "校验信息": notes,
                "失败原因": None,
                "结果版本条目": version_numbers,
                "结果新鲜度": [
                    {
                        "指标编号": config["指标编号"],
                        "新鲜度阈值秒": config["新鲜度阈值秒"],
                        "覆盖率下限": config["覆盖率下限"],
                    }
                    for config in configs
                ],
            })
            return {
                "ok": True,
                "message": (
                    f"完成 {stats['计算窗口数']} 个窗口的校验计算，"
                    f"新增 {len(version_numbers)} 条结果版本，下游刷新 {stats['下游刷新数']} 处"
                ),
                "batch": batch,
                "校验信息": notes,
            }
    except TransactionFailed as exc:
        batch = _record_failed_batch(
            trigger=trigger,
            message=str(exc),
            metric_numbers=[config["指标编号"] for config in configs],
            window_from=window_from,
            window_to=window_to,
            requested_by=requested_by,
        )
        return {
            "ok": False,
            "message": "数据库事务提交失败，本次窗口结果与来源记录已整体回滚，未暴露残缺版本",
            "batch": batch,
            "校验信息": notes,
        }


# ---------------------------------------------------------------------------
# 迟到事件摄取
# ---------------------------------------------------------------------------
def _event_payload_errors(payload: dict[str, Any], streams: dict[str, dict[str, Any]]) -> tuple[dict[str, Any] | None, list[str]]:
    errors: list[str] = []
    stream_name = str(payload.get("数据源流") or "").strip()
    stream_row = streams.get(stream_name)
    if not stream_row:
        errors.append(f"数据源流「{stream_name or '未填写'}」没有登记")
    event_id = str(payload.get("事件编号") or "").strip()
    if not event_id:
        errors.append("事件编号不能为空")
    elif len(event_id) > 96:
        errors.append("事件编号长度不能超过 96")
    event_time = parse_time(payload.get("事件时间"))
    if event_time is None:
        errors.append("事件时间无法解析，请使用 ISO 时间")
    measure = payload.get("度量值")
    if measure is None or _as_float(measure) is None:
        errors.append("度量值必须是数字")
    dimensions = payload.get("维度") or {}
    if not isinstance(dimensions, dict):
        errors.append("维度必须是对象")
        dimensions = {}
    if stream_row:
        allowed = {
            part.strip()
            for part in str(stream_row.get("维度字段") or "").split(",")
            if part.strip()
        }
        unknown = [name for name in dimensions if name not in allowed]
        if unknown:
            errors.append(f"维度字段{'、'.join(unknown)}不属于数据源流「{stream_name}」")
    if errors:
        return None, errors
    assert event_time is not None and stream_row is not None
    return {
        "数据源流": stream_name,
        "事件编号": event_id,
        "事件时间": event_time,
        "度量值": float(_as_float(measure) or 0),
        "维度": {str(key): str(value) for key, value in dimensions.items()},
        "流记录": stream_row,
    }, []


def _existing_event(stream_name: str, event_id: str) -> dict[str, Any] | None:
    for row in store.rows(EVENT_MODULE):
        if row.get("数据源流") == stream_name and row.get("事件编号") == event_id:
            return row
    return None


def submit_event(
    payload: dict[str, Any],
    *,
    requested_by: str | None = None,
    simulate_failure: bool = False,
) -> dict[str, Any]:
    """提交一条实时事件；重复提交只留流水，迟到事件在重放窗口内触发窗口重算。"""
    now = store.now()
    streams = stream_index()
    parsed, errors = _event_payload_errors(payload, streams)
    affected_configs, _ = configs_for()
    if parsed is None:
        with store.transaction():
            stream_name = str(payload.get("数据源流") or "").strip()
            event_id = str(payload.get("事件编号") or "").strip()
            store.insert(INGEST_MODULE, {
                "数据源流": stream_name,
                "事件编号": event_id,
                "提交时间": iso(now),
                "提交结果": "校验失败",
                "说明": "；".join(errors),
                "窗口开始": None,
                "窗口结束": None,
                "事件时间": iso(parse_time(payload.get("事件时间"))),
                "影响指标": [],
                "是否触发重放": False,
                "计算批次": None,
                "重复次数": 1,
                "提交人": requested_by or "值班管理员",
            })
        return {"ok": False, "message": f"事件未通过校验：{'；'.join(errors)}", "ingest": None, "replayed": []}

    assert parsed is not None
    stream_name = parsed["数据源流"]
    event_id = parsed["事件编号"]
    if _existing_event(stream_name, event_id):
        with store.transaction():
            store.insert(INGEST_MODULE, {
                "数据源流": stream_name,
                "事件编号": event_id,
                "提交时间": iso(now),
                "提交结果": "重复提交忽略",
                "说明": "同一事件编号已计入，本次不重复累计、不刷新窗口",
                "窗口开始": None,
                "窗口结束": None,
                "事件时间": iso(parsed["事件时间"]),
                "影响指标": [],
                "是否触发重放": False,
                "计算批次": None,
                "重复次数": 2,
                "提交人": requested_by or "值班管理员",
            })
        return {
            "ok": True,
            "message": "重复事件已忽略：同一窗口只累计一次，结果版本与新鲜度保持不变",
            "ingest": None,
            "replayed": [],
            "duplicate": True,
        }

    candidates: list[dict[str, Any]] = []
    for config in affected_configs:
        if config["数据源流"] != stream_name:
            continue
        window_start, window_end = align_window(parsed["事件时间"], config["计算窗口秒"])
        deadline = window_end + timedelta(milliseconds=config["延迟容忍毫秒"])
        candidates.append({
            "config": config,
            "窗口开始": window_start,
            "窗口结束": window_end,
            "重放截止时间": deadline,
            "可重放": now < deadline,
        })
    if not candidates:
        with store.transaction():
            store.insert(INGEST_MODULE, {
                "数据源流": stream_name,
                "事件编号": event_id,
                "提交时间": iso(now),
                "提交结果": "无可用指标",
                "说明": "数据源流下没有已上线的实时指标，事件未计入",
                "窗口开始": None,
                "窗口结束": None,
                "事件时间": iso(parsed["事件时间"]),
                "影响指标": [],
                "是否触发重放": False,
                "计算批次": None,
                "重复次数": 1,
                "提交人": requested_by or "值班管理员",
            })
        return {"ok": False, "message": "数据源流下没有已上线的实时指标，事件未计入", "ingest": None, "replayed": []}

    replayable = [item for item in candidates if item["可重放"]]
    if not replayable:
        with store.transaction():
            deadline = min(item["重放截止时间"] for item in candidates)
            store.insert(INGEST_MODULE, {
                "数据源流": stream_name,
                "事件编号": event_id,
                "提交时间": iso(now),
                "提交结果": "超出重放窗口拒绝",
                "说明": f"事件所属窗口已于 {iso(deadline)} 封窗，迟到事件不再计入",
                "窗口开始": iso(candidates[0]["窗口开始"]),
                "窗口结束": iso(candidates[0]["窗口结束"]),
                "事件时间": iso(parsed["事件时间"]),
                "影响指标": sorted(item["config"]["指标编号"] for item in candidates),
                "是否触发重放": False,
                "计算批次": None,
                "重复次数": 1,
                "提交人": requested_by or "值班管理员",
            })
        return {
            "ok": False,
            "message": "事件超出全部指标的重放窗口，已拒绝且未计入任何窗口",
            "ingest": None,
            "replayed": [],
        }

    batch_number = _next_batch_number(now)
    if simulate_failure:
        store.fail_next_commit("故障演练：模拟数据库事务提交失败")
    try:
        with store.transaction():
            store.insert(EVENT_MODULE, {
                "数据源流": stream_name,
                "事件编号": event_id,
                "事件时间": iso(parsed["事件时间"]),
                "收到时间": iso(now),
                "度量值": parsed["度量值"],
                "维度": parsed["维度"],
                "来源": "迟到事件提交",
                "状态": "已计入",
                "窗口开始": iso(replayable[0]["窗口开始"]),
                "窗口结束": iso(replayable[0]["窗口结束"]),
                "提交人": requested_by or "值班管理员",
            })
            ingest = store.insert(INGEST_MODULE, {
                "数据源流": stream_name,
                "事件编号": event_id,
                "提交时间": iso(now),
                "提交结果": "已计入",
                "说明": f"迟到 {(now - parsed['事件时间']).total_seconds():.0f} 秒到达，触发 {len(replayable)} 个窗口重放",
                "窗口开始": iso(replayable[0]["窗口开始"]),
                "窗口结束": iso(replayable[0]["窗口结束"]),
                "事件时间": iso(parsed["事件时间"]),
                "影响指标": sorted(item["config"]["指标编号"] for item in candidates),
                "是否触发重放": True,
                "计算批次": batch_number,
                "重复次数": 1,
                "提交人": requested_by or "值班管理员",
            })
            replayed: list[dict[str, Any]] = []
            for item in replayable:
                config = item["config"]
                before = next(
                    (
                        row for row in store.rows(RESULT_MODULE)
                        if row.get("指标编号") == config["指标编号"]
                        and row.get("窗口开始") == iso(item["窗口开始"])
                    ),
                    None,
                )
                partial = _compute_config_windows(
                    config=config,
                    windows=[item["窗口开始"]],
                    trigger="迟到事件重放",
                    now=now,
                    batch_number=batch_number,
                )
                after = next(
                    (
                        row for row in store.rows(RESULT_MODULE)
                        if row.get("指标编号") == config["指标编号"]
                        and row.get("窗口开始") == iso(item["窗口开始"])
                    ),
                    None,
                )
                replayed.append({
                    "指标编号": config["指标编号"],
                    "指标名称": config["指标名称"],
                    "窗口开始": iso(item["窗口开始"]),
                    "窗口结束": iso(item["窗口结束"]),
                    "窗口状态": (after or {}).get("窗口状态"),
                    "覆盖率": (after or {}).get("覆盖率"),
                    "指标值": (after or {}).get("指标值"),
                    "结果版本": (after or {}).get("结果版本"),
                    "旧版本指标值": (before or {}).get("指标值"),
                    "下游刷新数": partial["下游刷新数"],
                })
            batch = store.insert(BATCH_MODULE, {
                "批次编号": batch_number,
                "触发方式": "迟到事件重放",
                "发起人": requested_by or "值班管理员",
                "请求指标": sorted(item["config"]["指标编号"] for item in replayable),
                "窗口范围": [iso(min(item["窗口开始"] for item in replayable)), iso(max(item["窗口结束"] for item in replayable))],
                "窗口标识样例": sorted({iso(item["窗口开始"]) for item in replayable})[:5],
                "开始时间": iso(now),
                "结束时间": iso(store.now()),
                "状态": "成功",
                "计算窗口数": len(replayable),
                "已到齐": sum(1 for item in replayed if item["窗口状态"] == STATUS_COMPLETE),
                "可容忍迟到": sum(1 for item in replayed if item["窗口状态"] == STATUS_TOLERABLE),
                "覆盖率不足": sum(1 for item in replayed if item["窗口状态"] == STATUS_INSUFFICIENT),
                "下游刷新数": sum(item["下游刷新数"] for item in replayed),
                "校验信息": [],
                "失败原因": None,
                "结果版本条目": [],
                "结果新鲜度": [],
            })
            return {
                "ok": True,
                "message": f"事件已计入，{len(replayable)} 个窗口完成重放，重复提交不会再次累计",
                "ingest": ingest,
                "replayed": replayed,
                "batch": batch,
                "duplicate": False,
            }
    except TransactionFailed as exc:
        batch = _record_failed_batch(
            trigger="迟到事件重放",
            message=str(exc),
            metric_numbers=sorted(item["config"]["指标编号"] for item in replayable),
            window_from=iso(min(item["窗口开始"] for item in replayable)),
            window_to=iso(max(item["窗口结束"] for item in replayable)),
            requested_by=requested_by,
        )
        return {
            "ok": False,
            "message": "数据库事务提交失败，事件、窗口结果与下游刷新已整体回滚，未暴露残缺版本",
            "ingest": None,
            "replayed": [],
            "batch": batch,
        }


# ---------------------------------------------------------------------------
# 查询与读时新鲜度
# ---------------------------------------------------------------------------
SORT_FIELDS = {
    "window_start": "窗口开始",
    "window_end": "窗口结束",
    "metric": "指标编号",
    "datasource": "数据源流",
    "coverage": "覆盖率",
    "freshness": "数据新鲜度秒",
    "value": "指标值",
    "status": "窗口状态",
    "version": "结果版本",
}


def metric_pipeline(
    metric_number: str,
    config: dict[str, Any] | None,
    now: datetime,
) -> dict[str, Any]:
    """指标级新鲜度：最新结果窗口是否跟上了时间推进。

    单个窗口封窗后结果即最终态，但整条链路可能停止计算——这时最新窗口会停在过去，
    大盘看起来“有值”其实是旧结果。这里用「应到最新窗口 - 实到最新窗口」的窗口数
    衡量链路滞后，和窗口级新鲜度一起暴露给页面与下游。
    """
    windows = [
        row for row in store.rows(RESULT_MODULE)
        if row.get("指标编号") == metric_number
    ]
    if not windows:
        return {"最新结果窗口": None, "应有最新窗口": None, "链路滞后窗口数": None, "链路新鲜度": "无结果"}
    latest = max(windows, key=lambda row: str(row.get("窗口开始") or ""))
    latest_start = parse_time(latest.get("窗口开始"))
    if config is None or latest_start is None:
        return {
            "最新结果窗口": latest.get("窗口开始"),
            "应有最新窗口": None,
            "链路滞后窗口数": None,
            "链路新鲜度": "无法判定",
            "最新计算时间": latest.get("最后计算时间"),
        }
    window_seconds = config["计算窗口秒"]
    _, current_end = align_window(now, window_seconds)
    expected_start = current_end - timedelta(seconds=window_seconds * 2)
    lag_windows = int((expected_start - latest_start).total_seconds() // window_seconds)
    lag_windows = max(0, lag_windows)
    last_compute = parse_time(latest.get("最后计算时间"))
    compute_lag = max(0, int((now - last_compute).total_seconds())) if last_compute else None
    stalled = lag_windows > 0 or (
        compute_lag is not None and compute_lag > config["新鲜度阈值秒"] + window_seconds * 2
    )
    return {
        "最新结果窗口": latest.get("窗口开始"),
        "应有最新窗口": iso(expected_start),
        "链路滞后窗口数": lag_windows,
        "链路滞后秒": compute_lag,
        "链路新鲜度": "滞留" if stalled else "正常",
        "最新计算时间": latest.get("最后计算时间"),
    }


def _decorate(result: dict[str, Any], metric: dict[str, Any] | None, now: datetime) -> dict[str, Any]:
    status = window_status_now(result, metric, now)
    freshness = freshness_of(result, metric, now)
    allowed = consumable(result, metric, now)
    item = dict(result)
    item.update(freshness)
    item["窗口状态"] = status
    item["消费准入"] = allowed
    item["覆盖率达标"] = float(result.get("覆盖率") or 0) >= float(result.get("覆盖率下限") or 0)
    item["定位信息"] = {
        "数据源流": result.get("数据源流"),
        "窗口范围": [result.get("窗口开始"), result.get("窗口结束")],
        "聚合方式": result.get("聚合函数"),
        "维度字段组": result.get("维度字段组"),
        "新鲜度区间": [freshness["数据新鲜度秒"], freshness["新鲜度阈值秒"]],
    }
    return item


def _matches_common(
    item: dict[str, Any],
    *,
    keyword: str | None,
    datasource_stream: str | None,
    window_seconds: str | int | None,
    aggregate: str | None,
    dimension_group: str | None,
) -> bool:
    if keyword:
        haystack = " ".join(
            str(item.get(field) or "")
            for field in ("指标编号", "指标名称", "数据源流", "维度字段组", "聚合函数", "窗口标识")
        )
        if keyword.lower() not in haystack.lower():
            return False
    if datasource_stream and item.get("数据源流") != datasource_stream:
        return False
    if window_seconds not in (None, "", "all"):
        if str(item.get("计算窗口秒")) != str(window_seconds):
            return False
    if aggregate and str(item.get("聚合函数")) != aggregate:
        return False
    if dimension_group:
        if str(item.get("维度字段组")) != dimension_group:
            return False
    return True


def _sort_rows(rows: list[dict[str, Any]], sort: str, order: str) -> list[dict[str, Any]]:
    sort = sort if sort in SORT_FIELDS else "window_end"
    reverse = str(order or "desc").lower() != "asc"
    field = SORT_FIELDS[sort]
    if sort == "status":
        return sorted(rows, key=lambda item: (STATUS_RANK.get(str(item.get(field)), 9), str(item.get("窗口开始"))), reverse=reverse)
    if sort == "freshness":
        # 新鲜度滞留（数值大）在前，缺值放最后。
        return sorted(
            rows,
            key=lambda item: (
                item.get(field) is None,
                -(item.get(field) or 0) if reverse else (item.get(field) or 0),
            ),
        )
    return sorted(
        rows,
        key=lambda item: (item.get(field) is None, item.get(field)),
        reverse=reverse,
    )


def _collect_windows(
    *,
    keyword: str | None,
    datasource_stream: str | None,
    window_seconds: str | int | None,
    aggregate: str | None,
    dimension_group: str | None,
    window_status: str | None,
    freshness_state: str | None,
    freshness_min: int | None,
    freshness_max: int | None,
    coverage_min: float | None,
    coverage_max: float | None,
    window_from: str | None,
    window_to: str | None,
    metric_id: int | None,
    metric_number: str | None,
    now: datetime,
    configs: dict[str, dict[str, Any]],
) -> list[dict[str, Any]] | None:
    """按定位条件收集全部匹配窗口（不分页）。指标不存在时返回 None。"""
    parsed_from = parse_time(window_from)
    parsed_to = parse_time(window_to)
    if metric_id is not None:
        target = store.find(MODULE, metric_id)
        if target is None:
            return None
        keyword = str(target.get("指标编号") or keyword or "")
    if metric_number:
        keyword = metric_number
    decorated: list[dict[str, Any]] = []
    for row in store.rows(RESULT_MODULE):
        config = configs.get(str(row.get("指标编号")))
        item = _decorate(row, config, now)
        if metric_number and item.get("指标编号") != metric_number:
            continue
        if not _matches_common(
            item,
            keyword=keyword,
            datasource_stream=datasource_stream,
            window_seconds=window_seconds,
            aggregate=aggregate,
            dimension_group=dimension_group,
        ):
            continue
        if window_status and item["窗口状态"] != window_status:
            continue
        if freshness_state and item["新鲜度状态"] != freshness_state:
            continue
        age = item.get("数据新鲜度秒")
        if freshness_min is not None and (age is None or age < freshness_min):
            continue
        if freshness_max is not None and (age is None or age > freshness_max):
            continue
        if coverage_min is not None and float(item.get("覆盖率") or 0) < coverage_min:
            continue
        if coverage_max is not None and float(item.get("覆盖率") or 0) > coverage_max:
            continue
        start = parse_time(item.get("窗口开始"))
        if parsed_from and (start is None or start < parsed_from):
            continue
        if parsed_to and (start is None or start >= parsed_to):
            continue
        decorated.append(item)
    return decorated


def _metric_configs() -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    metric_rows = {str(row.get("指标编号")): row for row in store.rows(MODULE)}
    streams = stream_index()
    configs: dict[str, dict[str, Any]] = {}
    for number, row in metric_rows.items():
        config, _ = metric_config(row, streams)
        if config:
            configs[number] = config
    return metric_rows, configs


def list_windows(
    *,
    keyword: str | None = None,
    datasource_stream: str | None = None,
    window_seconds: str | int | None = None,
    aggregate: str | None = None,
    dimension_group: str | None = None,
    window_status: str | None = None,
    freshness_state: str | None = None,
    freshness_min: int | None = None,
    freshness_max: int | None = None,
    coverage_min: float | None = None,
    coverage_max: float | None = None,
    window_from: str | None = None,
    window_to: str | None = None,
    metric_id: int | None = None,
    metric_number: str | None = None,
    sort: str = "window_end",
    order: str = "desc",
    page: int = 1,
    size: int = 20,
) -> dict[str, Any]:
    """窗口新鲜度视图：组合定位、状态区分、新鲜度区间过滤、排序与翻页。"""
    ensure_bootstrapped()
    now = store.now()
    metric_rows, configs = _metric_configs()
    all_items = _collect_windows(
        keyword=keyword,
        datasource_stream=datasource_stream,
        window_seconds=window_seconds,
        aggregate=aggregate,
        dimension_group=dimension_group,
        window_status=window_status,
        freshness_state=freshness_state,
        freshness_min=freshness_min,
        freshness_max=freshness_max,
        coverage_min=coverage_min,
        coverage_max=coverage_max,
        window_from=window_from,
        window_to=window_to,
        metric_id=metric_id,
        metric_number=metric_number,
        now=now,
        configs=configs,
    )
    if all_items is None:
        return {
            "items": [], "total": 0, "page": page, "size": size, "pages": 0,
            "summary": {}, "facets": {}, "applied_filters": {},
            "定位口径": "指标不存在，未匹配到任何窗口",
        }
    decorated = _sort_rows(all_items, sort, order)
    total = len(decorated)
    page = max(1, int(page or 1))
    size = max(1, min(int(size or 20), 200))
    start = (page - 1) * size
    items = decorated[start:start + size]

    summary = {
        "窗口总数": total,
        STATUS_COMPLETE: sum(1 for item in all_items if item["窗口状态"] == STATUS_COMPLETE),
        STATUS_TOLERABLE: sum(1 for item in all_items if item["窗口状态"] == STATUS_TOLERABLE),
        STATUS_INSUFFICIENT: sum(1 for item in all_items if item["窗口状态"] == STATUS_INSUFFICIENT),
        "可消费窗口": sum(1 for item in all_items if item["消费准入"]),
        "新鲜度滞留窗口": sum(1 for item in all_items if item["新鲜度状态"] == FRESHNESS_STALE),
        "平均覆盖率": round(
            sum(float(item.get("覆盖率") or 0) for item in all_items) / total, 4
        ) if total else 0,
    }
    pipeline = [
        {"指标编号": number, "指标名称": (metric_rows.get(number) or {}).get("指标名称"),
         **metric_pipeline(number, configs.get(number), now)}
        for number in sorted({str(item.get("指标编号")) for item in all_items})
    ]
    facets = {
        "数据源流": sorted({str(item.get("数据源流")) for item in all_items}),
        "计算窗口秒": sorted({int(item.get("计算窗口秒") or 0) for item in all_items}),
        "聚合方式": sorted({str(item.get("聚合函数")) for item in all_items}),
        "维度字段组": sorted({str(item.get("维度字段组")) for item in all_items}),
        "窗口状态": list(WINDOW_STATUSES),
        "新鲜度区间": {
            "最小": min((item.get("数据新鲜度秒") for item in all_items if item.get("数据新鲜度秒") is not None), default=None),
            "最大": max((item.get("数据新鲜度秒") for item in all_items if item.get("数据新鲜度秒") is not None), default=None),
        },
        "指标新鲜度": pipeline,
    }
    return {
        "items": items,
        "total": total,
        "page": page,
        "size": size,
        "pages": math.ceil(total / size) if total else 0,
        "summary": summary,
        "facets": facets,
        "applied_filters": {
            "keyword": keyword,
            "datasource_stream": datasource_stream,
            "window_seconds": window_seconds,
            "aggregate": aggregate,
            "dimension_group": dimension_group,
            "window_status": window_status,
            "freshness_state": freshness_state,
            "freshness_min": freshness_min,
            "freshness_max": freshness_max,
            "coverage_min": coverage_min,
            "coverage_max": coverage_max,
            "window_from": window_from,
            "window_to": window_to,
            "metric_id": metric_id,
            "metric_number": metric_number,
            "sort": sort,
            "order": order,
        },
        "定位口径": "窗口结果按 数据源流 × 窗口范围 × 聚合方式 × 维度字段组 组合定位",
    }


def get_window(result_id: int) -> dict[str, Any] | None:
    ensure_bootstrapped()
    row = store.find(RESULT_MODULE, result_id)
    if row is None:
        return None
    now = store.now()
    metric_row = _metric_by_number(str(row.get("指标编号")))
    config = metric_config(metric_row, stream_index())[0] if metric_row else None
    item = _decorate(row, config, now)
    versions = sorted(
        (
            dict(version) for version in store.rows(VERSION_MODULE)
            if version.get("指标编号") == row.get("指标编号")
            and version.get("窗口开始") == row.get("窗口开始")
        ),
        key=lambda version: int(version.get("版本号") or 0),
        reverse=True,
    )
    item["版本流水"] = versions
    item["数据源流记录"] = config["流记录"] if config else None
    item["大盘联动"] = [
        dict(snapshot)
        for snapshot in store.rows(DASHBOARD_SNAPSHOT_MODULE)
        if snapshot.get("指标编号") == row.get("指标编号")
        and snapshot.get("窗口开始") == row.get("窗口开始")
    ]
    item["阈值联动"] = [
        dict(evaluation)
        for evaluation in store.rows(THRESHOLD_EVALUATION_MODULE)
        if evaluation.get("指标编号") == row.get("指标编号")
        and evaluation.get("窗口开始") == row.get("窗口开始")
    ]
    return item


def dataset(
    *,
    keyword: str | None = None,
    datasource_stream: str | None = None,
    window_seconds: str | int | None = None,
    aggregate: str | None = None,
    dimension_group: str | None = None,
    window_from: str | None = None,
    window_to: str | None = None,
    include_provisional: bool = False,
    sort: str = "window_end",
    order: str = "desc",
    page: int = 1,
    size: int = 20,
) -> dict[str, Any]:
    """实时指标计算结果数据集：默认只输出可被下游消费的完整窗口。"""
    ensure_bootstrapped()
    now = store.now()
    _, configs = _metric_configs()
    all_rows = _collect_windows(
        keyword=keyword,
        datasource_stream=datasource_stream,
        window_seconds=window_seconds,
        aggregate=aggregate,
        dimension_group=dimension_group,
        window_status=None,
        freshness_state=None,
        freshness_min=None,
        freshness_max=None,
        coverage_min=None,
        coverage_max=None,
        window_from=window_from,
        window_to=window_to,
        metric_id=None,
        metric_number=None,
        now=now,
        configs=configs,
    )
    all_rows = all_rows or []
    if not include_provisional:
        all_rows = [item for item in all_rows if item["消费准入"]]
    rows = _sort_rows(all_rows, sort, order)
    total = len(rows)
    page = max(1, int(page or 1))
    size = max(1, min(int(size or 20), 200))
    start = (page - 1) * size
    items = rows[start:start + size]
    dataset_digest = _digest(
        sorted(
            (item.get("窗口标识"), item.get("结果版本"), item.get("结果指纹"))
            for item in rows
        )
    )
    return {
        "dataset": "realtime_metric_window_result",
        "dataset_version": f"RMW-{dataset_digest}",
        "generated_at": iso(store.now()),
        "total": total,
        "page": page,
        "size": size,
        "pages": math.ceil(total / size) if total else 0,
        "include_provisional": include_provisional,
        "items": items,
        "traceability": {
            "数据集": "realtime_metric_window_result",
            "数据集版本": f"RMW-{dataset_digest}",
            "每个结果都带": ["结果版本", "数据新鲜度秒", "覆盖率", "计算批次", "结果指纹"],
            "消费门槛": "窗口状态=已到齐 且 数据新鲜度未滞留（封窗结果视为最终版本）",
            "窗口覆盖率口径": "去重后事件数 / 数据源流预期事件数，低于指标覆盖率下限即为覆盖率不足",
            "可回溯关系": "窗口结果 → 结果版本流水 → 计算批次；窗口结果 → 大盘快照 / 阈值评估",
        },
        "summary": {
            "数据集窗口数": total,
            "覆盖率不足窗口": sum(1 for item in rows if item["窗口状态"] == STATUS_INSUFFICIENT),
            "新鲜度滞留窗口": sum(1 for item in rows if item["新鲜度状态"] == FRESHNESS_STALE),
        },
    }


def list_batches(*, status: str | None = None, page: int = 1, size: int = 20) -> dict[str, Any]:
    rows = sorted(
        (dict(row) for row in store.rows(BATCH_MODULE)),
        key=lambda row: str(row.get("开始时间") or ""),
        reverse=True,
    )
    if status:
        rows = [row for row in rows if row.get("状态") == status]
    total = len(rows)
    start = (max(page, 1) - 1) * size
    return {"items": rows[start:start + size], "total": total, "page": page, "size": size}


def ensure_bootstrapped() -> None:
    """首次访问时把最近窗口算出来，保证页面打开即可看到新鲜度视图。"""
    if store.realtime_bootstrapped:
        return
    # 先标记再计算：即使启动计算异常，也不要在每个请求里反复重试启动批次。
    store.realtime_bootstrapped = True
    compute_windows(lookback=DEFAULT_LOOKBACK_WINDOWS, trigger="启动刷新")
