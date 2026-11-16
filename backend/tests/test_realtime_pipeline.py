"""实时指标窗口计算链路的行为测试。

覆盖需求中的关键约束：
- 组合定位（数据源流 / 窗口 / 聚合方式 / 维度字段组）与搜索、筛选、排序、翻页；
- 已到齐 / 可容忍迟到 / 覆盖率不足三态区分；
- 迟到事件重复提交幂等，重放只累计一次；
- 每次计算落库结果版本、新鲜度、覆盖率，并能追溯到批次；
- 事务失败时结果与来源记录整体回滚，不暴露残缺版本；
- 大盘与阈值按现行取数链路消费结果数据集。
"""
from __future__ import annotations

from datetime import datetime

import pytest
from fastapi.testclient import TestClient

import app.services.realtimecompute as compute
from app.store import store


NOW = "2026-11-16T12:00:00+00:00"


@pytest.fixture()
def client():
    # reset 时把逻辑时钟固定下来，示例事件与窗口计算才会落在同一时间轴上。
    store.reset(datetime.fromisoformat(NOW))
    compute.ensure_bootstrapped()
    from app.main import app

    return TestClient(app)


@pytest.fixture()
def computed(client):
    client.post("/api/realtimemetric/compute", json={"values": {"lookback": 8}})
    return client


def _windows(client, **params):
    response = client.get("/api/realtimemetric/windows", params={"size": 200, **params})
    assert response.status_code == 200
    return response.json()


def test_metric_list_combines_stream_window_aggregate_and_dimensions(client):
    payload = client.get(
        "/api/realtimemetric",
        params={
            "datasource_stream": "stream.payment",
            "window_seconds": "60",
            "aggregate": "COUNT",
            "dimension_group": "channel,region",
        },
    ).json()
    assert payload["total"] == 1
    assert payload["items"][0]["指标编号"] == "REAL-0001"
    assert payload["summary"]["指标定义总数"] == 7
    assert payload["summary"]["当前页条数"] == 1
    assert payload["facets"]["可组合定位字段"] == ["数据源流", "计算窗口秒", "聚合函数", "维度字段组"]


def test_metric_search_sort_and_pager(client):
    by_name = client.get("/api/realtimemetric", params={"keyword": "支付"}).json()
    assert {item["指标编号"] for item in by_name["items"]} >= {"REAL-0001", "REAL-0002"}

    by_dimension = client.get("/api/realtimemetric", params={"keyword": "category"}).json()
    assert [item["指标编号"] for item in by_dimension["items"]] == ["REAL-0003"]

    by_dimension_group = client.get(
        "/api/realtimemetric", params={"dimension_group": "channel", "size": 50}
    ).json()
    assert {item["指标编号"] for item in by_dimension_group["items"]} == {"REAL-0006", "REAL-0007"}

    sorted_rows = client.get(
        "/api/realtimemetric", params={"sort": "window", "order": "desc", "size": 3}
    ).json()
    windows = [int(item["计算窗口秒"]) for item in sorted_rows["items"]]
    assert windows == sorted(windows, reverse=True)
    assert sorted_rows["pages"] == 3

    empty = client.get("/api/realtimemetric", params={"page": 99}).json()
    assert empty["items"] == [] and empty["total"] == 7


def test_window_statuses_are_distinguished(computed):
    payload = _windows(computed)
    summary = payload["summary"]
    assert summary["窗口总数"] == 40
    assert summary["已到齐"] > 0
    assert summary["可容忍迟到"] > 0
    assert summary["覆盖率不足"] > 0
    assert summary["已到齐"] + summary["可容忍迟到"] + summary["覆盖率不足"] == summary["窗口总数"]

    insufficient = _windows(computed, window_status="覆盖率不足")
    assert insufficient["total"] == summary["覆盖率不足"]
    for item in insufficient["items"]:
        assert item["覆盖率"] < item["覆盖率下限"]
        assert item["消费准入"] is False

    tolerable = _windows(computed, window_status="可容忍迟到")
    for item in tolerable["items"]:
        assert item["覆盖率"] >= item["覆盖率下限"]
        assert item["消费准入"] is False


def test_window_filters_sorting_and_pagination(computed):
    filtered = _windows(
        computed,
        datasource_stream="stream.payment",
        window_seconds="60",
        aggregate="COUNT",
        dimension_group="channel,region",
    )
    assert filtered["total"] == 8
    assert {item["指标编号"] for item in filtered["items"]} == {"REAL-0001"}
    assert filtered["facets"]["数据源流"] == ["stream.payment"]

    stale = _windows(computed, freshness_state="滞留")
    for item in stale["items"]:
        assert item["新鲜度状态"] == "滞留"

    coverage = _windows(computed, coverage_min=0.9)
    assert all(item["覆盖率"] >= 0.9 for item in coverage["items"])

    page_one = _windows(computed, sort="window_end", order="desc", size=5, page=1)
    page_two = _windows(computed, sort="window_end", order="desc", size=5, page=2)
    assert page_one["pages"] == 8
    assert {item["id"] for item in page_one["items"]}.isdisjoint(
        {item["id"] for item in page_two["items"]}
    )
    assert page_one["items"][0]["窗口结束"] >= page_one["items"][-1]["窗口结束"]


def test_duplicate_event_is_counted_once(computed):
    seed_event = next(
        row for row in store.rows("metric_event")
        if row["数据源流"] == "stream.payment"
    )
    payload = {
        "values": {
            "数据源流": seed_event["数据源流"],
            "事件编号": seed_event["事件编号"],
            "事件时间": seed_event["事件时间"],
            "度量值": 999,
            "维度": seed_event["维度"],
        }
    }
    results_before = {
        row["窗口标识"]: (row["结果版本"], row["指标值"], row["结果指纹"])
        for row in store.rows("metric_window_result")
    }
    response = computed.post("/api/realtimemetric/events", json=payload).json()
    assert response["ok"] is True
    assert response["entry"]["duplicate"] is True
    results_after = {
        row["窗口标识"]: (row["结果版本"], row["指标值"], row["结果指纹"])
        for row in store.rows("metric_window_result")
    }
    assert results_before == results_after
    assert sum(
        1 for row in store.rows("metric_event_ingest") if row["提交结果"] == "重复提交忽略"
    ) == 1


def test_late_event_replays_only_once_and_updates_downstream(computed):
    open_window = _windows(computed, sort="window_end", order="desc", size=1)["items"][0]
    metric_number = open_window["指标编号"]
    event = {
        "values": {
            "数据源流": open_window["数据源流"],
            "事件编号": "LATE-TEST-0001",
            "事件时间": open_window["窗口开始"].replace("Z", "+00:00"),
            "度量值": 66.5,
            "维度": {"channel": "wechat", "region": "east"},
        }
    }
    first = computed.post("/api/realtimemetric/events", json=event).json()
    assert first["ok"] is True
    replayed = {item["指标编号"]: item for item in first["entry"]["replayed"]}
    assert metric_number in replayed
    assert replayed[metric_number]["结果版本"] >= 2

    versions = [
        row for row in store.rows("metric_window_version")
        if row["指标编号"] == metric_number and row["窗口开始"] == open_window["窗口开始"]
    ]
    assert len(versions) == replayed[metric_number]["结果版本"]
    assert sum(1 for row in versions if row["是否当前版本"]) == 1
    current = next(row for row in versions if row["是否当前版本"])
    assert current["数据新鲜度秒"] is not None
    assert current["覆盖率"] == replayed[metric_number]["覆盖率"]

    snapshots = [
        row for row in store.rows("dashboard_window_snapshot")
        if row["指标编号"] == metric_number and row["窗口开始"] == open_window["窗口开始"]
    ]
    assert snapshots and snapshots[0]["结果版本"] == replayed[metric_number]["结果版本"]

    second = computed.post("/api/realtimemetric/events", json=event).json()
    assert second["ok"] is True and second["entry"]["duplicate"] is True
    assert second["entry"]["replayed"] == []
    versions_after = [
        row for row in store.rows("metric_window_version")
        if row["指标编号"] == metric_number and row["窗口开始"] == open_window["窗口开始"]
    ]
    assert len(versions_after) == len(versions)


def test_late_event_after_seal_is_rejected(computed):
    sealed = _windows(
        computed, datasource_stream="stream.payment", sort="window_end", order="asc", size=1
    )["items"][0]
    response = computed.post(
        "/api/realtimemetric/events",
        json={
            "values": {
                "数据源流": sealed["数据源流"],
                "事件编号": "LATE-AFTER-SEAL",
                "事件时间": sealed["窗口开始"].replace("Z", "+00:00"),
                "度量值": 12,
                "维度": {"channel": "wechat", "region": "east"},
            }
        },
    ).json()
    assert response["ok"] is False
    assert "重放窗口" in response["message"]
    assert response["entry"]["ingest"] is None
    assert not any(row["事件编号"] == "LATE-AFTER-SEAL" for row in store.rows("metric_event"))
    history = [row for row in store.rows("metric_event_ingest") if row["事件编号"] == "LATE-AFTER-SEAL"]
    assert history and history[-1]["提交结果"] == "超出重放窗口拒绝"


def test_transaction_failure_rolls_back_and_keeps_versions_clean(computed):
    results_before = {
        row["窗口标识"]: (row["结果版本"], row["指标值"], row["结果指纹"])
        for row in store.rows("metric_window_result")
    }
    versions_before = len(store.rows("metric_window_version"))
    response = computed.post(
        "/api/realtimemetric/compute",
        json={"values": {"lookback": 4, "simulate_failure": True}},
    ).json()
    assert response["ok"] is False
    assert response["entry"]["batch"]["状态"] == "失败"
    results_after = {
        row["窗口标识"]: (row["结果版本"], row["指标值"], row["结果指纹"])
        for row in store.rows("metric_window_result")
    }
    assert results_before == results_after
    assert len(store.rows("metric_window_version")) == versions_before

    dataset = computed.get("/api/realtimemetric/dataset", params={"size": 200}).json()
    consumable = computed.get(
        "/api/realtimemetric/windows", params={"size": 200, "window_status": "已到齐"}
    ).json()
    assert dataset["total"] == sum(1 for item in consumable["items"] if item["消费准入"])


def test_event_transaction_failure_does_not_leak(computed):
    open_window = _windows(computed, sort="window_end", order="desc", size=1)["items"][0]
    events_before = len(store.rows("metric_event"))
    response = computed.post(
        "/api/realtimemetric/events",
        json={
            "values": {
                "数据源流": open_window["数据源流"],
                "事件编号": "FAIL-EVENT-1",
                "事件时间": open_window["窗口开始"].replace("Z", "+00:00"),
                "度量值": 5,
                "维度": {"channel": "wechat", "region": "east"},
                "simulate_failure": True,
            }
        },
    ).json()
    assert response["ok"] is False
    assert "整体回滚" in response["message"]
    assert len(store.rows("metric_event")) == events_before


def test_dataset_only_exposes_consumable_windows(computed):
    dataset = computed.get("/api/realtimemetric/dataset", params={"size": 200}).json()
    windows = _windows(computed)
    assert dataset["dataset"] == "realtime_metric_window_result"
    assert dataset["total"] == windows["summary"]["可消费窗口"]
    for item in dataset["items"]:
        assert item["窗口状态"] == "已到齐"
        assert item["消费准入"] is True
        assert item["结果版本"] >= 1
        assert item["数据新鲜度秒"] is not None
        assert item["覆盖率"] is not None
        assert item["计算批次"]
        assert item["结果指纹"]

    with_provisional = computed.get(
        "/api/realtimemetric/dataset",
        params={"size": 200, "include_provisional": True},
    ).json()
    assert with_provisional["total"] > dataset["total"]


def test_threshold_and_dashboard_consume_result_dataset(computed):
    refresh = computed.get("/api/dashboard/refresh").json()
    assert refresh["dashboard_count"] >= 1
    for dashboard in refresh["refreshed"]:
        for card in dashboard["指标卡"]:
            if card["是否可展示"]:
                assert card["结果版本"] >= 1
                assert card["窗口状态"] == "已到齐"

    thresholds = computed.post("/api/threshold/refresh").json()
    assert thresholds["evaluations"] > 0
    evaluations = computed.get("/api/threshold/evaluations", params={"size": 200}).json()
    assert evaluations["total"] == thresholds["evaluations"]
    for item in evaluations["items"]:
        assert evaluations["取数链路"]
        assert item["窗口状态"] in ("已到齐", "可容忍迟到", "覆盖率不足")
        if item["判定结果"] == "数据不可用":
            assert item["是否已消费"] is False
        else:
            assert item["结果版本"] >= 1


def test_freshness_ages_with_clock_but_sealed_results_stay_final(computed):
    open_window = _windows(computed, window_status="已到齐", sort="window_end", order="desc", size=1)["items"][0]
    assert open_window["新鲜度状态"] in ("新鲜", "已封窗")
    store.advance(600)
    aged = _windows(computed, sort="window_end", order="desc", size=200)
    sealed = [item for item in aged["items"] if item["新鲜度状态"] == "已封窗"]
    assert sealed, "封窗窗口必须标记为最终结果而不是滞留"
    assert not any(item["新鲜度状态"] == "滞留" for item in sealed)
    for item in sealed:
        assert item["消费准入"] is (item["窗口状态"] == "已到齐")


def test_validation_reports_blocking_issues(computed):
    validation = computed.get("/api/realtimemetric/validation").json()
    assert "REAL-0006" in validation["提示"][0]["指标编号"] or any(
        note["指标编号"] == "REAL-0006" for note in validation["提示"]
    )
    assert validation["可计算指标"]
    assert validation["数据源流记录"]


def test_window_detail_traces_versions_and_downstream(computed):
    windows = _windows(computed, window_status="已到齐", size=1)
    item = windows["items"][0]
    detail = computed.get(f"/api/realtimemetric/windows/{item['id']}").json()
    assert detail["版本流水"]
    assert detail["版本流水"][0]["是否当前版本"] is True
    assert "校验结论" in detail["版本流水"][0]
    assert detail["数据源流记录"]["数据源流"] == detail["数据源流"]
    assert any(
        snapshot["结果版本"] == detail["结果版本"] for snapshot in detail["大盘联动"]
    ) or detail["消费准入"] is False
