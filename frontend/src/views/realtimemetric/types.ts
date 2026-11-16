/** 窗口新鲜度视图用到的类型定义，与后端 /api/realtimemetric/windows 保持一致。 */
export type WindowRow = {
  id: number
  指标编号: string
  指标名称: string
  数据源流: string
  计算窗口秒: number
  聚合函数: string
  维度字段组: string
  窗口开始: string
  窗口结束: string
  窗口标识: string
  重放截止时间: string
  指标值: number | null
  去重后事件数: number
  预期事件数: number
  覆盖率: number
  覆盖率下限: number
  覆盖率达标: boolean
  数据最新收到时间: string | null
  数据新鲜度秒: number | null
  新鲜度阈值秒: number
  新鲜度状态: string
  新鲜度口径: string
  窗口状态: string
  消费准入: boolean
  结果版本: number
  结果指纹: string
  计算批次: string
  触发方式: string
  最后计算时间: string
  维度明细: { 维度: Record<string, string>; 指标值: number | null; 事件数: number }[]
  定位信息: Record<string, unknown>
}

export type WindowSummary = {
  窗口总数: number
  已到齐: number
  可容忍迟到: number
  覆盖率不足: number
  可消费窗口: number
  新鲜度滞留窗口: number
  平均覆盖率: number
}

export type MetricPipeline = {
  指标编号: string
  指标名称: string | null
  最新结果窗口: string | null
  应有最新窗口: string | null
  链路滞后窗口数: number | null
  链路滞后秒: number | null
  链路新鲜度: string
}

export type WindowPage = {
  items: WindowRow[]
  total: number
  page: number
  size: number
  pages: number
  summary: WindowSummary
  facets: {
    数据源流: string[]
    计算窗口秒: number[]
    聚合方式: string[]
    维度字段组: string[]
    窗口状态: string[]
    新鲜度区间: { 最小: number | null; 最大: number | null }
    指标新鲜度: MetricPipeline[]
  }
  applied_filters: Record<string, unknown>
  note: string
}

export type WindowDetail = WindowRow & {
  版本流水: {
    id: number
    版本号: number
    指标值: number | null
    覆盖率: number
    数据新鲜度秒: number | null
    窗口状态: string
    计算批次: string
    触发方式: string
    计算出时间: string
    是否当前版本: boolean
    版本说明: string
    校验结论: Record<string, boolean>
  }[]
  数据源流记录: Record<string, unknown> | null
  大盘联动: Record<string, unknown>[]
  阈值联动: Record<string, unknown>[]
}

export type MetricListItem = {
  id: number
  指标编号: string
  指标名称: string
  计算窗口秒: string
  聚合函数: string
  数据源流: string
  维度字段组: string
  延迟容忍毫秒: string
  精度位数: string
  新鲜度阈值秒?: string
  覆盖率下限?: string
  status: string
  窗口统计: {
    窗口数: number
    已到齐: number
    可容忍迟到: number
    覆盖率不足: number
    可消费: number
    最新窗口: string | null
  }
}

export type MetricPage = {
  items: MetricListItem[]
  total: number
  page: number
  size: number
  pages: number
  summary: Record<string, number>
  facets: Record<string, unknown>
  note: string
}

export type BatchRow = {
  id: number
  批次编号: string
  触发方式: string
  发起人: string
  请求指标: string[]
  开始时间: string
  结束时间: string
  状态: string
  计算窗口数: number
  已到齐: number
  可容忍迟到: number
  覆盖率不足: number
  下游刷新数: number
  失败原因: string | null
}
