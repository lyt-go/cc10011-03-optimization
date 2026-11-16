"""阈值管理接口：维护阈值规则，并按现行取数链路消费实时指标窗口结果。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.threshold import ThresholdService

router = APIRouter(prefix="/api/threshold", tags=["阈值管理"])

service = ThresholdService()

LIST_FIELDS = ["规则编号", "关联指标", "阈值下限", "阈值上限", "触发条件", "持续周期", "告警等级", "生效时间范围"]
STATUSES = ["草稿", "规则评审中", "已生效", "已关闭"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按规则编号、关联指标、告警等级检索"),
    status: str | None = Query(default=None, description="草稿、规则评审中、已生效、已关闭"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按关键字与状态过滤阈值规则列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/evaluations", response_model=dict)
def list_evaluations(
    rule_number: str | None = Query(default=None, description="按规则编号过滤"),
    metric_number: str | None = Query(default=None, description="按指标编号过滤"),
    verdict: str | None = Query(default=None, description="正常、低于下限、高于上限、数据不可用"),
    page: int = 1,
    size: int = 20,
) -> dict[str, Any]:
    """阈值评估记录：每条都指向具体窗口结果版本，可追溯到覆盖率与新鲜度。"""
    return service.evaluations(
        rule_number=rule_number,
        metric_number=metric_number,
        verdict=verdict,
        page=page,
        size=size,
    )


@router.post("/refresh", response_model=dict)
def refresh_evaluations() -> dict[str, Any]:
    """按现行取数链路刷新阈值评估：只消费可消费窗口，窗口重放后同步更新结论。"""
    return service.refresh()


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出阈值管理清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "threshold", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条阈值规则明细，并带最近评估记录。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"阈值规则 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条阈值规则；关联指标必须已登记，缺字段时说明原因。"""
    entry, errors = service.create_entry(payload.values)
    if errors:
        if entry is None and any("未在实时指标定义" in error for error in errors):
            return ActionResult(ok=False, message="；".join(errors))
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(errors)}")
    return ActionResult(ok=True, message="阈值规则已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条阈值规则执行提交规则评审、生效规则、关闭规则；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
