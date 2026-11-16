import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

import { formatTime } from './format'
import type { MetricPage, MetricListItem } from './types'

const ENDPOINT = '/api/realtimemetric'
const columns = [
  '指标编号',
  '指标名称',
  '计算窗口秒',
  '聚合函数',
  '数据源流',
  '维度字段组',
  '延迟容忍毫秒',
  '精度位数',
]
const actions = ['提交定义评审', '上线指标', '下线指标']
const statuses = ['草稿', '定义评审中', '已上线', '已下线']
const SORT_OPTIONS = [
  { value: 'metric', label: '指标编号' },
  { value: 'name', label: '指标名称' },
  { value: 'window', label: '计算窗口秒' },
  { value: 'datasource', label: '数据源流' },
  { value: 'aggregate', label: '聚合函数' },
  { value: 'status', label: '状态' },
]

type Props = {
  reloadToken: number
  onLocateWindows: (metricNumber: string) => void
  onChanged: () => void
}

type Filters = {
  keyword: string
  status: string
  datasource_stream: string
  window_seconds: string
  aggregate: string
  dimension_group: string
  sort: string
  order: string
}

const EMPTY_FILTERS: Filters = {
  keyword: '',
  status: '',
  datasource_stream: '',
  window_seconds: '',
  aggregate: '',
  dimension_group: '',
  sort: 'metric',
  order: 'asc',
}

function buildQuery(filters: Filters, page: number, size: number): string {
  const params = new URLSearchParams()
  Object.entries(filters).forEach(([key, value]) => {
    if (value) params.set(key, value)
  })
  params.set('page', String(page))
  params.set('size', String(size))
  return params.toString()
}

export default function MetricDefinitionPanel({ reloadToken, onLocateWindows, onChanged }: Props) {
  const [filters, setFilters] = useState<Filters>(EMPTY_FILTERS)
  const [draft, setDraft] = useState<Filters>(EMPTY_FILTERS)
  const [page, setPage] = useState(1)
  const [size, setSize] = useState(10)
  const [data, setData] = useState<MetricPage | null>(null)
  const [errorMessage, setErrorMessage] = useState('')
  const [notice, setNotice] = useState('')

  const reload = useMemo(
    () => async () => {
      try {
        const response = await request(`${ENDPOINT}?${buildQuery(filters, page, size)}`)
        if (!response.ok) throw new Error('指标定义列表读取失败')
        setData((await response.json()) as MetricPage)
      } catch (error) {
        setErrorMessage(error instanceof Error ? error.message : '指标定义列表读取失败')
      }
    },
    [filters, page, size],
  )

  useEffect(() => {
    void reload()
  }, [reload, reloadToken])

  const facets = data?.facets as
    | { 数据源流?: string[]; 计算窗口秒?: number[]; 聚合函数?: string[]; 维度字段组?: string[] }
    | undefined
  const summary = data?.summary ?? {}

  const apply = useCallback(() => {
    setFilters(draft)
    setPage(1)
  }, [draft])

  async function runAction(action: string, row: MetricListItem) {
    setErrorMessage('')
    setNotice('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ values: { action } }),
      })
      const payload = await response.json()
      if (!payload.ok) throw new Error(payload.message ?? '动作未生效')
      setNotice(payload.message)
      await reload()
      onChanged()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '实时指标操作失败')
    }
  }

  const totalPages = data?.pages ?? 0

  return (
    <div>
      <div className="stat-row">
        <article className="stat-card">
          <span className="stat-label">指标定义总数</span>
          <strong className="stat-value">{summary['指标定义总数'] ?? 0}</strong>
          <span className="stat-hint">当前页 {summary['当前页条数'] ?? 0} 条</span>
        </article>
        <article className="stat-card">
          <span className="stat-label">已上线指标</span>
          <strong className="stat-value">{summary['已上线指标'] ?? 0}</strong>
          <span className="stat-hint">参与正式窗口计算</span>
        </article>
        <article className="stat-card">
          <span className="stat-label">评审中指标</span>
          <strong className="stat-value">{summary['评审中指标'] ?? 0}</strong>
          <span className="stat-hint">只校验不计算</span>
        </article>
        <article className="stat-card">
          <span className="stat-label">窗口结果总数</span>
          <strong className="stat-value">{summary['窗口结果总数'] ?? 0}</strong>
          <span className="stat-hint">覆盖全部已上线指标</span>
        </article>
        <article className="stat-card">
          <span className="stat-label">可消费窗口</span>
          <strong className="stat-value">{summary['可消费窗口'] ?? 0}</strong>
          <span className="stat-hint">覆盖率不足 {summary['覆盖率不足窗口'] ?? 0} 个</span>
        </article>
      </div>

      <form
        className="filter-bar"
        onSubmit={(event) => {
          event.preventDefault()
          apply()
        }}
      >
        <label className="filter-item">
          <span>搜索</span>
          <input
            value={draft.keyword}
            placeholder="指标编号 / 名称 / 数据源流"
            onChange={(event) => setDraft((prev) => ({ ...prev, keyword: event.target.value }))}
          />
        </label>
        <label className="filter-item">
          <span>数据源流</span>
          <select
            value={draft.datasource_stream}
            onChange={(event) => setDraft((prev) => ({ ...prev, datasource_stream: event.target.value }))}
          >
            <option value="">全部</option>
            {(facets?.数据源流 ?? []).map((item) => <option key={item} value={item}>{item}</option>)}
          </select>
        </label>
        <label className="filter-item">
          <span>窗口范围</span>
          <select
            value={draft.window_seconds}
            onChange={(event) => setDraft((prev) => ({ ...prev, window_seconds: event.target.value }))}
          >
            <option value="">全部</option>
            {(facets?.计算窗口秒 ?? []).map((item) => <option key={item} value={item}>{item} 秒</option>)}
          </select>
        </label>
        <label className="filter-item">
          <span>聚合方式</span>
          <select
            value={draft.aggregate}
            onChange={(event) => setDraft((prev) => ({ ...prev, aggregate: event.target.value }))}
          >
            <option value="">全部</option>
            {(facets?.聚合函数 ?? []).map((item) => <option key={item} value={item}>{item}</option>)}
          </select>
        </label>
        <label className="filter-item">
          <span>维度字段组</span>
          <select
            value={draft.dimension_group}
            onChange={(event) => setDraft((prev) => ({ ...prev, dimension_group: event.target.value }))}
          >
            <option value="">全部</option>
            {(facets?.维度字段组 ?? []).map((item) => <option key={item} value={item}>{item}</option>)}
          </select>
        </label>
        <label className="filter-item">
          <span>状态</span>
          <select value={draft.status} onChange={(event) => setDraft((prev) => ({ ...prev, status: event.target.value }))}>
            <option value="">全部</option>
            {statuses.map((item) => <option key={item} value={item}>{item}</option>)}
          </select>
        </label>
        <label className="filter-item">
          <span>排序</span>
          <select value={draft.sort} onChange={(event) => setDraft((prev) => ({ ...prev, sort: event.target.value }))}>
            {SORT_OPTIONS.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}
          </select>
        </label>
        <label className="filter-item">
          <span>方向</span>
          <select value={draft.order} onChange={(event) => setDraft((prev) => ({ ...prev, order: event.target.value }))}>
            <option value="asc">升序</option>
            <option value="desc">降序</option>
          </select>
        </label>
        <button className="btn primary" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => { setDraft(EMPTY_FILTERS); setFilters(EMPTY_FILTERS); setPage(1) }}>
          重置条件
        </button>
      </form>

      {notice && <div className="notice ok">{notice}</div>}
      {errorMessage && <div className="notice danger">{errorMessage}</div>}
      {data?.note && <p className="hint-text">{data.note}</p>}

      <table className="data-table">
        <thead>
          <tr>
            <th>指标编号</th>
            <th>指标名称</th>
            <th>计算窗口秒</th>
            <th>聚合函数</th>
            <th>数据源流</th>
            <th>维度字段组</th>
            <th>延迟容忍毫秒</th>
            <th>精度位数</th>
            <th>状态</th>
            <th>窗口覆盖</th>
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {data?.items.map((row) => (
            <tr key={row.id}>
              <td>
                <button className="link" type="button" onClick={() => onLocateWindows(row.指标编号)}>
                  {row.指标编号}
                </button>
              </td>
              <td>{row.指标名称}</td>
              <td>{row.计算窗口秒}</td>
              <td>{row.聚合函数}</td>
              <td>{row.数据源流}</td>
              <td>{row.维度字段组}</td>
              <td>{row.延迟容忍毫秒}</td>
              <td>{row.精度位数}</td>
              <td><span className={`badge ${row.status === '已上线' ? 'ok' : row.status === '已下线' ? 'muted' : 'info'}`}>{row.status}</span></td>
              <td>
                <div>共 {row.窗口统计?.窗口数 ?? 0} 个窗口 · 可消费 {row.窗口统计?.可消费 ?? 0}</div>
                <div className="hint-text">
                  到齐 {row.窗口统计?.已到齐 ?? 0} · 迟到 {row.窗口统计?.可容忍迟到 ?? 0} · 不足 {row.窗口统计?.覆盖率不足 ?? 0}
                </div>
                <div className="hint-text">最新 {formatTime(row.窗口统计?.最新窗口)}</div>
              </td>
              <td className="row-actions">
                {actions.map((action) => (
                  <button key={action} className="link" type="button" onClick={() => void runAction(action, row)}>
                    {action}
                  </button>
                ))}
              </td>
            </tr>
          ))}
          {data && data.items.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={11}>没有匹配的指标定义，请放宽组合定位条件</td>
            </tr>
          )}
        </tbody>
      </table>

      <div className="pager">
        <span className="hint-text">共 {data?.total ?? 0} 条 · 第 {data?.page ?? 1} / {Math.max(totalPages, 1)} 页</span>
        <select value={size} onChange={(event) => { setSize(Number(event.target.value)); setPage(1) }}>
          {[10, 20, 50].map((value) => <option key={value} value={value}>{value} 条/页</option>)}
        </select>
        <button className="btn small" type="button" disabled={page <= 1} onClick={() => setPage((prev) => Math.max(1, prev - 1))}>上一页</button>
        <button className="btn small" type="button" disabled={page >= totalPages} onClick={() => setPage((prev) => prev + 1)}>下一页</button>
      </div>
    </div>
  )
}
