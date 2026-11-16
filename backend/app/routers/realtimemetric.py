"""实时指标接口：定义维护、窗口新鲜度视图、计算结果数据集与校验计算链路。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, MetricWindowResult, PageResult
from app.services import realtimecompute as compute
from app.services.realtimemetric import RealtimemetricService

router = APIRouter(prefix="/api/realtimemetric", tags=["实时指标"])

service = RealtimemetricService()

LIST_FIELDS = [
    "指标编号", "指标名称", "计算窗口秒", "聚合函数",
    "数据源流", "维度字段组", "延迟容忍毫秒", "精度位数",
]
STATUSES = ["草稿", "定义评审中", "已上线", "已下线"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按指标编号、名称、数据源流、维度字段组检索"),
    status: str | None = Query(default=None, description="草稿、定义评审中、已上线、已下线"),
    datasource_stream: str | None = Query(default=None, description="按数据源流筛选"),
    window_seconds: str | None = Query(default=None, description="按计算窗口秒筛选"),
    aggregate: str | None = Query(default=None, description="按聚合方式筛选"),
    dimension_group: str | None = Query(default=None, description="按维度字段组筛选"),
    sort: str = Query(default="metric", description="metric、name、window、datasource、aggregate、status"),
    order: str = Query(default="asc"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按数据源流、窗口范围、聚合方式和维度字段组组合定位实时指标。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total, summary, facets = service.list_entries(
        keyword=keyword,
        status=status,
        datasource_stream=datasource_stream,
        window_seconds=window_seconds,
        aggregate=aggregate,
        dimension_group=dimension_group,
        sort=sort,
        order=order,
        page=page,
        size=size,
    )
    return PageResult(
        items=items,
        total=total,
        page=page,
        size=size,
        pages=(total + size - 1) // size if size else 0,
        summary=summary,
        facets=facets,
        applied_filters={
            "keyword": keyword,
            "status": status,
            "datasource_stream": datasource_stream,
            "window_seconds": window_seconds,
            "aggregate": aggregate,
            "dimension_group": dimension_group,
            "sort": sort,
            "order": order,
            "可组合定位字段": ["数据源流", "计算窗口秒", "聚合函数", "维度字段组"],
        },
        note=f"当前页 {len(items)} 条 / 筛选后 {total} 条 / 指标定义 {summary['指标定义总数']} 条",
    )


@router.get("/windows", response_model=PageResult[MetricWindowResult])
def list_windows(
    keyword: str | None = Query(default=None, description="指标编号、名称、数据源流、维度字段组检索"),
    datasource_stream: str | None = Query(default=None, description="按数据源流定位"),
    window_seconds: str | None = Query(default=None, description="按窗口范围（秒）定位"),
    aggregate: str | None = Query(default=None, description="按聚合方式定位"),
    dimension_group: str | None = Query(default=None, description="按维度字段组定位"),
    window_status: str | None = Query(default=None, description="已到齐、可容忍迟到、覆盖率不足"),
    freshness_state: str | None = Query(default=None, description="新鲜、滞留、已封窗、等待数据"),
    freshness_min: int | None = Query(default=None, description="新鲜度区间下限（秒）"),
    freshness_max: int | None = Query(default=None, description="新鲜度区间上限（秒）"),
    coverage_min: float | None = Query(default=None, description="覆盖率下限"),
    coverage_max: float | None = Query(default=None, description="覆盖率上限"),
    window_from: str | None = Query(default=None, description="窗口开始时间下界 ISO"),
    window_to: str | None = Query(default=None, description="窗口开始时间上界 ISO"),
    metric_id: int | None = Query(default=None, description="直接定位到具体指标"),
    metric_number: str | None = Query(default=None, description="直接按指标编号定位"),
    sort: str = Query(default="window_end", description="window_start、window_end、metric、datasource、coverage、freshness、value、status、version"),
    order: str = Query(default="desc"),
    page: int = 1,
    size: int = 20,
) -> PageResult[MetricWindowResult]:
    """窗口新鲜度视图：组合定位、状态区分、新鲜度区间与翻页。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    compute.ensure_bootstrapped()
    payload = service.windows({
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
        "page": page,
        "size": size,
    })
    return PageResult(
        items=payload["items"],
        total=payload["total"],
        page=payload["page"],
        size=payload["size"],
        pages=payload.get("pages", 0),
        summary=payload.get("summary"),
        facets=payload.get("facets"),
        applied_filters=payload.get("applied_filters"),
        note=payload.get("定位口径"),
    )


@router.get("/windows/{result_id}", response_model=dict)
def get_window(result_id: int) -> dict:
    """单个窗口结果：版本流水、大盘联动、阈值联动与新鲜度全链路可追溯。"""
    item = compute.get_window(result_id)
    if item is None:
        raise HTTPException(status_code=404, detail=f"窗口结果 {result_id} 不存在或已归档")
    return item


@router.get("/dataset", response_model=dict)
def window_dataset(
    keyword: str | None = Query(default=None),
    datasource_stream: str | None = Query(default=None),
    window_seconds: str | None = Query(default=None),
    aggregate: str | None = Query(default=None),
    dimension_group: str | None = Query(default=None),
    window_from: str | None = Query(default=None),
    window_to: str | None = Query(default=None),
    include_provisional: bool = Query(default=False, description="是否包含可容忍迟到 / 覆盖率不足窗口"),
    sort: str = Query(default="window_end"),
    order: str = Query(default="desc"),
    page: int = 1,
    size: int = 20,
) -> dict:
    """实时指标计算结果数据集：默认只输出下游可消费的完整窗口，并带版本与新鲜度溯源。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    compute.ensure_bootstrapped()
    return compute.dataset(
        keyword=keyword,
        datasource_stream=datasource_stream,
        window_seconds=window_seconds,
        aggregate=aggregate,
        dimension_group=dimension_group,
        window_from=window_from,
        window_to=window_to,
        include_provisional=include_provisional,
        sort=sort,
        order=order,
        page=page,
        size=size,
    )


@router.get("/batches", response_model=PageResult[dict])
def list_batches(
    status: str | None = Query(default=None, description="成功、失败"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """计算批次流水：每次校验计算都落一条，失败批次同样可追溯。"""
    compute.ensure_bootstrapped()
    payload = compute.list_batches(status=status, page=page, size=size)
    return PageResult(
        items=payload["items"],
        total=payload["total"],
        page=payload["page"],
        size=payload["size"],
        pages=(payload["total"] + size - 1) // size if size else 0,
    )


@router.get("/streams", response_model=list[dict])
def list_streams() -> list[dict]:
    """数据源流记录：窗口覆盖率的预期事件数来源。"""
    compute.ensure_bootstrapped()
    return [dict(row) for row in compute.store.rows(compute.STREAM_MODULE)]


@router.get("/validation", response_model=dict)
def validate_definitions() -> dict:
    """校验指标定义与数据源流记录是否满足窗口计算前置条件。"""
    return service.validate_definitions()


@router.post("/compute", response_model=ActionResult)
def compute_windows(payload: EntryPayload) -> ActionResult:
    """校验并计算实时指标窗口结果：版本、新鲜度、覆盖率与下游刷新同事务落库。"""
    result = service.recompute(payload.values)
    entry = {"batch": result.get("batch"), "replayed": result.get("replayed")}
    if result.get("校验信息"):
        entry["校验信息"] = result["校验信息"]
    return ActionResult(ok=bool(result.get("ok")), message=str(result.get("message")), entry=entry)


@router.post("/events", response_model=ActionResult)
def submit_event(payload: EntryPayload) -> ActionResult:
    """提交实时事件：重复事件只记流水，迟到事件在重放窗口内触发窗口重算。"""
    result = service.submit_event(payload.values)
    entry = {
        "ingest": result.get("ingest"),
        "replayed": result.get("replayed"),
        "batch": result.get("batch"),
        "duplicate": result.get("duplicate", False),
    }
    return ActionResult(ok=bool(result.get("ok")), message=str(result.get("message")), entry=entry)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出实时指标清单：返回当前过滤条件下的全量数据。"""
    items, total, summary, facets = service.list_entries(page=1, size=10000)
    return {
        "module": "realtimemetric",
        "total": total,
        "items": items,
        "summary": summary,
        "facets": facets,
    }


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条指标定义明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"指标定义 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条指标定义，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="指标定义已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条指标定义执行提交定义评审、上线指标、下线指标；未通过校验的定义不能上线。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
