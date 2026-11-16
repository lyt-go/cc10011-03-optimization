"""实时指标计算链路的示例数据。

`seed.py` 里其它业务模块继续沿用原有样例；实时指标、监控大盘、阈值管理三个与
计算链路相关的模块在这里换成可计算、可追溯的定义，并准备数据源流记录与事件流。

设计口径：

- 指标定义中的窗口、聚合方式、数据源流、维度字段组、迟到容忍、新鲜度阈值和覆盖率
  下限都是可执行配置，计算时会逐项校验。
- 数据源流记录给出“每 60 秒预期事件数”，窗口预期事件数按窗口秒数等比换算，
  覆盖率 = 去重后的窗口内事件数 / 预期事件数。
- 启动画像生成最近 60 分钟的源事件，故意让部分窗口少收事件，便于观察
  “已到齐 / 可容忍迟到 / 覆盖率不足”三种窗口状态。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

# ---------------------------------------------------------------------------
# 数据源流记录
# ---------------------------------------------------------------------------
DATASOURCE_STREAMS: list[dict[str, Any]] = [
    {
        "id": 1,
        "流编号": "DS-0001",
        "数据源流": "stream.payment",
        "流名称": "支付事件流",
        "分区数": 4,
        "每窗口预期事件数": 6,
        "基准窗口秒": 60,
        "维度字段": "channel,region",
        "延迟容忍毫秒": 600000,
        "消费位点": "kafka:payment:482011",
        "状态": "运行中",
        "负责人": "支付数据组",
        "说明": "网关支付成功事件，按事件编号幂等消费",
    },
    {
        "id": 2,
        "流编号": "DS-0002",
        "数据源流": "stream.order",
        "流名称": "订单事件流",
        "分区数": 6,
        "每窗口预期事件数": 4,
        "基准窗口秒": 60,
        "维度字段": "channel,category",
        "延迟容忍毫秒": 600000,
        "消费位点": "kafka:order:391017",
        "状态": "运行中",
        "负责人": "交易数据组",
        "说明": "订单创建事件，按事件编号幂等消费",
    },
    {
        "id": 3,
        "流编号": "DS-0003",
        "数据源流": "stream.apm",
        "流名称": "应用性能流",
        "分区数": 8,
        "每窗口预期事件数": 10,
        "基准窗口秒": 60,
        "维度字段": "service,region",
        "延迟容忍毫秒": 300000,
        "消费位点": "kafka:apm:918204",
        "状态": "运行中",
        "负责人": "稳定性数据组",
        "说明": "服务端响应耗时采样，用于峰值时延类指标",
    },
    {
        "id": 4,
        "流编号": "DS-0004",
        "数据源流": "stream.risk",
        "流名称": "风控事件流",
        "分区数": 2,
        "每窗口预期事件数": 3,
        "基准窗口秒": 60,
        "维度字段": "rule,channel",
        "延迟容忍毫秒": 900000,
        "消费位点": "kafka:risk:120338",
        "状态": "运行中",
        "负责人": "风控数据组",
        "说明": "风控规则命中评分，用于均值类监控",
    },
]

# ---------------------------------------------------------------------------
# 实时指标定义（真实可计算字段 + 新鲜度 / 覆盖率门槛）
# ---------------------------------------------------------------------------
REALTIME_METRICS: list[dict[str, Any]] = [
    {
        "id": 1,
        "status": "已上线",
        "pending": False,
        "abnormal": False,
        "指标编号": "REAL-0001",
        "指标名称": "支付成功笔数",
        "计算窗口秒": "60",
        "聚合函数": "COUNT",
        "数据源流": "stream.payment",
        "维度字段组": "channel,region",
        "延迟容忍毫秒": "120000",
        "精度位数": "0",
        "新鲜度阈值秒": "90",
        "覆盖率下限": "0.8",
        "指标说明": "按支付事件流统计窗口内支付成功笔数，同一事件编号只计一次",
    },
    {
        "id": 2,
        "status": "已上线",
        "pending": False,
        "abnormal": False,
        "指标编号": "REAL-0002",
        "指标名称": "支付金额",
        "计算窗口秒": "300",
        "聚合函数": "SUM",
        "数据源流": "stream.payment",
        "维度字段组": "channel,region",
        "延迟容忍毫秒": "600000",
        "精度位数": "2",
        "新鲜度阈值秒": "330",
        "覆盖率下限": "0.9",
        "指标说明": "支付事件金额求和，迟到事件在窗口封窗前可重放",
    },
    {
        "id": 3,
        "status": "已上线",
        "pending": False,
        "abnormal": False,
        "指标编号": "REAL-0003",
        "指标名称": "订单均价",
        "计算窗口秒": "60",
        "聚合函数": "AVG",
        "数据源流": "stream.order",
        "维度字段组": "channel,category",
        "延迟容忍毫秒": "180000",
        "精度位数": "2",
        "新鲜度阈值秒": "120",
        "覆盖率下限": "0.75",
        "指标说明": "订单事件金额均值，覆盖率低于下限时下游不可消费",
    },
    {
        "id": 4,
        "status": "已上线",
        "pending": False,
        "abnormal": False,
        "指标编号": "REAL-0004",
        "指标名称": "峰值响应时延",
        "计算窗口秒": "60",
        "聚合函数": "MAX",
        "数据源流": "stream.apm",
        "维度字段组": "service,region",
        "延迟容忍毫秒": "60000",
        "精度位数": "0",
        "新鲜度阈值秒": "90",
        "覆盖率下限": "0.7",
        "指标说明": "应用性能流窗口内最大响应耗时",
    },
    {
        "id": 5,
        "status": "已上线",
        "pending": False,
        "abnormal": False,
        "指标编号": "REAL-0005",
        "指标名称": "风控命中均值",
        "计算窗口秒": "120",
        "聚合函数": "AVG",
        "数据源流": "stream.risk",
        "维度字段组": "rule,channel",
        "延迟容忍毫秒": "240000",
        "精度位数": "3",
        "新鲜度阈值秒": "180",
        "覆盖率下限": "0.8",
        "指标说明": "风控规则命中评分均值，窗口封窗后不再重放",
    },
    {
        "id": 6,
        "status": "定义评审中",
        "pending": True,
        "abnormal": False,
        "指标编号": "REAL-0006",
        "指标名称": "支付失败笔数",
        "计算窗口秒": "60",
        "聚合函数": "COUNT",
        "数据源流": "stream.payment",
        "维度字段组": "channel",
        "延迟容忍毫秒": "120000",
        "精度位数": "0",
        "新鲜度阈值秒": "90",
        "覆盖率下限": "0.8",
        "指标说明": "仍在定义评审中，不允许参与正式窗口计算",
    },
    {
        "id": 7,
        "status": "已下线",
        "pending": False,
        "abnormal": True,
        "指标编号": "REAL-0007",
        "指标名称": "订单取消率",
        "计算窗口秒": "300",
        "聚合函数": "AVG",
        "数据源流": "stream.order",
        "维度字段组": "channel",
        "延迟容忍毫秒": "600000",
        "精度位数": "4",
        "新鲜度阈值秒": "330",
        "覆盖率下限": "0.8",
        "指标说明": "已下线指标，历史窗口保留只读",
    },
]

# ---------------------------------------------------------------------------
# 监控大盘：包含指标组直接引用指标编号，用于联动刷新
# ---------------------------------------------------------------------------
DASHBOARD_CONFIGS: list[dict[str, Any]] = [
    {
        "id": 1,
        "status": "已发布",
        "pending": False,
        "abnormal": False,
        "大盘编号": "DASH-0001",
        "大盘名称": "支付实时大盘",
        "包含指标组": "REAL-0001,REAL-0002",
        "布局模板": "双列指标卡",
        "刷新频率秒": "60",
        "对比周期": "上一窗口",
        "分享范围": "支付业务组",
        "负责人": "值班管理员",
    },
    {
        "id": 2,
        "status": "已发布",
        "pending": False,
        "abnormal": False,
        "大盘编号": "DASH-0002",
        "大盘名称": "交易体验大盘",
        "包含指标组": "REAL-0003,REAL-0004",
        "布局模板": "指标卡 + 趋势线",
        "刷新频率秒": "60",
        "对比周期": "上一小时同期",
        "分享范围": "交易与稳定性组",
        "负责人": "值班管理员",
    },
    {
        "id": 3,
        "status": "已发布",
        "pending": False,
        "abnormal": False,
        "大盘编号": "DASH-0003",
        "大盘名称": "风控全景大盘",
        "包含指标组": "REAL-0005",
        "布局模板": "单列指标卡",
        "刷新频率秒": "120",
        "对比周期": "上一窗口",
        "分享范围": "风控组",
        "负责人": "风控值班",
    },
    {
        "id": 4,
        "status": "草稿",
        "pending": True,
        "abnormal": False,
        "大盘编号": "DASH-0004",
        "大盘名称": "支付失败专项大盘",
        "包含指标组": "REAL-0006",
        "布局模板": "双列指标卡",
        "刷新频率秒": "60",
        "对比周期": "上一窗口",
        "分享范围": "支付业务组",
        "负责人": "支付值班",
    },
]

# ---------------------------------------------------------------------------
# 阈值规则：关联指标写法与指标编号一致，计算完成后自动消费最新窗口结果
# ---------------------------------------------------------------------------
THRESHOLD_RULES: list[dict[str, Any]] = [
    {
        "id": 1,
        "status": "已生效",
        "pending": False,
        "abnormal": False,
        "规则编号": "THRE-0001",
        "关联指标": "REAL-0001",
        "阈值下限": "3",
        "阈值上限": "12",
        "触发条件": "低于下限或高于上限",
        "持续周期": "1 个窗口",
        "告警等级": "P2",
        "生效时间范围": "2026-01-01 至今",
    },
    {
        "id": 2,
        "status": "已生效",
        "pending": False,
        "abnormal": False,
        "规则编号": "THRE-0002",
        "关联指标": "REAL-0002",
        "阈值下限": "500",
        "阈值上限": "5000",
        "触发条件": "高于上限",
        "持续周期": "2 个窗口",
        "告警等级": "P1",
        "生效时间范围": "2026-01-01 至今",
    },
    {
        "id": 3,
        "status": "已生效",
        "pending": False,
        "abnormal": False,
        "规则编号": "THRE-0003",
        "关联指标": "REAL-0003",
        "阈值下限": "20",
        "阈值上限": "180",
        "触发条件": "低于下限",
        "持续周期": "1 个窗口",
        "告警等级": "P3",
        "生效时间范围": "2026-01-01 至今",
    },
    {
        "id": 4,
        "status": "已生效",
        "pending": False,
        "abnormal": False,
        "规则编号": "THRE-0004",
        "关联指标": "REAL-0004",
        "阈值下限": "0",
        "阈值上限": "800",
        "触发条件": "高于上限",
        "持续周期": "3 个窗口",
        "告警等级": "P1",
        "生效时间范围": "2026-01-01 至今",
    },
    {
        "id": 5,
        "status": "已生效",
        "pending": False,
        "abnormal": False,
        "规则编号": "THRE-0005",
        "关联指标": "REAL-0005",
        "阈值下限": "0.2",
        "阈值上限": "0.85",
        "触发条件": "高于上限",
        "持续周期": "1 个窗口",
        "告警等级": "P2",
        "生效时间范围": "2026-01-01 至今",
    },
]

# 每个数据源流故意短缺的 60 秒切片（1 = 最近一个已结束的切片），
# 数值表示保留的事件比例，用来制造“覆盖率不足”和“可容忍迟到”的窗口。
# APM 最近切片保留 70%（恰好压在下限上）用于展示可容忍迟到；更早的短缺切片
# 落在迟到容忍期之外，用于展示覆盖率不足。
SHORT_SLICES: dict[str, dict[int, float]] = {
    "stream.payment": {2: 0.5, 17: 0.5, 41: 0.4},
    "stream.order": {5: 0.5, 23: 0.4},
    "stream.apm": {1: 0.7, 3: 0.4, 31: 0.5},
    "stream.risk": {7: 0.5, 19: 0.4},
}

_DIMENSION_VALUES: dict[str, dict[str, list[str]]] = {
    "stream.payment": {
        "channel": ["wechat", "alipay", "unionpay"],
        "region": ["east", "north", "south"],
    },
    "stream.order": {
        "channel": ["app", "web", "mini"],
        "category": ["digital", "grocery", "fashion"],
    },
    "stream.apm": {
        "service": ["gateway", "order-svc", "pay-svc"],
        "region": ["east", "north"],
    },
    "stream.risk": {
        "rule": ["velocity", "geo", "device"],
        "channel": ["wechat", "alipay"],
    },
}

_MEASURE_RANGES: dict[str, tuple[float, float]] = {
    "stream.payment": (12.0, 480.0),
    "stream.order": (8.0, 220.0),
    "stream.apm": (25.0, 880.0),
    "stream.risk": (0.05, 0.98),
}

SEED_SLICE_COUNT = 60


def _floor_to_seconds(moment: datetime, seconds: int) -> datetime:
    epoch = int(moment.timestamp())
    return datetime.fromtimestamp(epoch - epoch % seconds, tz=timezone.utc)


def build_seed_events(now: datetime) -> list[dict[str, Any]]:
    """按确定性规则生成最近 60 分钟的源事件。

    事件时间落在按 60 秒对齐的窗口内，``收到时间`` 略晚于事件时间，模拟正常到达；
    每个事件都有稳定的“事件编号”，供幂等去重使用。
    """
    now = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    current_slice_start = _floor_to_seconds(now, 60)
    rows: list[dict[str, Any]] = []
    next_id = 1

    for stream in DATASOURCE_STREAMS:
        stream_name = str(stream["数据源流"])
        base = int(stream["每窗口预期事件数"])
        dimensions = _DIMENSION_VALUES[stream_name]
        low, high = _MEASURE_RANGES[stream_name]
        # slice_index = 0 表示当前尚未结束的切片，只生成已经到达的那部分事件；
        # 1 及以后是已经结束的完整切片，用于演示重放与封窗。
        for slice_index in range(0, SEED_SLICE_COUNT + 1):
            slice_start = current_slice_start - timedelta(seconds=60 * slice_index)
            if slice_index == 0:
                ratio = 0.5
            else:
                ratio = SHORT_SLICES.get(stream_name, {}).get(slice_index, 1.0)
            count = max(1, int(round(base * ratio)))
            for position in range(count):
                event_time = slice_start + timedelta(
                    seconds=(position * 60.0 / count) + (slice_index % 3) * 0.25
                )
                received_at = event_time + timedelta(seconds=1 + (position % 4) * 0.5)
                if event_time > now or received_at > now:
                    # 当前切片里还没发生的事件不能提前出现在源事件表里。
                    continue
                event_id = f"EVT-{stream_name.split('.')[-1].upper()}-{int(slice_start.timestamp())}-{position:03d}"
                measure = round(low + (high - low) * (((slice_index * 37 + position * 11) % 100) / 100.0), 4)
                dimension_values = {
                    name: values[(slice_index + position) % len(values)]
                    for name, values in dimensions.items()
                }
                rows.append({
                    "id": next_id,
                    "数据源流": stream_name,
                    "事件编号": event_id,
                    "事件时间": event_time.isoformat(),
                    "收到时间": received_at.isoformat(),
                    "度量值": measure,
                    "维度": dimension_values,
                    "来源": "启动画像",
                    "状态": "已计入",
                })
                next_id += 1
    return rows
