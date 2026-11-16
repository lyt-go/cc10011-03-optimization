import { useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

import {
  FRESHNESS_STATES,
  WINDOW_STATUSES,
  coverageTone,
  formatPercent,
  formatSeconds,
  formatTime,
  formatValue,
  formatWindow,
  freshnessTone,
  windowStatusTone,
} from './format'
import type { WindowDetail, WindowPage, WindowRow } from './types'

const ENDPOINT = '/api/realtimemetric/windows'
const SORT_OPTIONS = [
  { value: 'window_end', label: '窗口结束时间' },
  { value: 'window_start', label: '窗口开始时间' },
  { value: 'freshness', label: '数据新鲜度' },
  { value: 'coverage', label: '覆盖率' },
  { value: 'value', label: '指标值' },
  { value: 'version', label: '结果版本' },
  { value: 'status', label: '窗口状态' },
  { value: 'metric', label: '指标编号' },
]

const EMPTY_FILTERS = {
  keyword: '',
  datasource_stream: '',
  window_seconds: '',
  aggregate: '',
  dimension_group: '',
  window_status: '',
  freshness_state: '',
  freshness_min: '',
  freshness_max: '',
  coverage_min: '',
  window_from: '',
  window_to: '',
  sort: 'window_end',
  order: 'desc',
}

type Filters = typeof EMPTY_FILTERS

type Props = {
  reloadToken: number
  focusMetric?: string
}

/** 把筛选条件里空字符串去掉，避免后端把空值当成有效过滤。 */
function buildQuery(filters: Filters, page: number, size: number): string {
  const params = new URLSearchParams()
  Object.entries(filters).forEach(([key, value]) => {
    if (value !== '' && value !== null && value !== undefined) params.set(key, String(value))
  })
  params.set('page', String(page))
  params.set('size', String(size))
  return params.toString()
}

export default function WindowFreshnessPanel({ reloadToken, focusMetric = '' }: Props) {
  const [filters, setFilters] = useState<Filters>(EMPTY_FILTERS)
  const [draft, setDraft] = useState<Filters>(EMPTY_FILTERS)
  const [page, setPage] = useState(1)
  const [size, setSize] = useState(20)
  const [data, setData] = useState<WindowPage | null>(null)
  const [errorMessage, setErrorMessage] = useState('')
  const [loading, setLoading] = useState(false)
  const [selected, setSelected] = useState<WindowDetail | null>(null)
  const [detailError, setDetailError] = useState('')

  const reload = useMemo(
    () => async () => {
      setLoading(true)
      setErrorMessage('')
      try {
        const response = await request(`${ENDPOINT}?${buildQuery(filters, page, size)}`)
        if (!response.ok) throw new Error('窗口新鲜度视图读取失败')
        const payload = (await response.json()) as WindowPage
        setData(payload)
        if (payload.items.length === 0 && page > 1) {
          setPage(1)
        }
      } catch (error) {
        setErrorMessage(error instanceof Error ? error.message : '窗口新鲜度视图读取失败')
      } finally {
        setLoading(false)
      }
    },
    [filters, page, size],
  )

  useEffect(() => {
    void reload()
  }, [reload, reloadToken])

  useEffect(() => {
    if (!focusMetric) return
    const next = { ...EMPTY_FILTERS, keyword: focusMetric }
    setDraft(next)
    setFilters(next)
    setPage(1)
  }, [focusMetric])

  useEffect(() => {
    if (!selected) return
    let cancelled = false
    request(`${ENDPOINT}/${selected.id}`)
      .then(async (response) => {
        if (!response.ok) throw new Error('窗口结果详情读取失败')
        return (await response.json()) as WindowDetail
      })
      .then((payload) => {
        if (!cancelled) setSelected(payload)
      })
      .catch((error: unknown) => {
        if (!cancelled) setDetailError(error instanceof Error ? error.message : '窗口结果详情读取失败')
      })
    return () => {
      cancelled = true
    }
  }, [selected?.id])

  const facets = data?.facets
  const summary = data?.summary

  function applyFilters() {
    setFilters(draft)
    setPage(1)
  }

  function resetFilters() {
    setDraft(EMPTY_FILTERS)
    setFilters(EMPTY_FILTERS)
    setPage(1)
  }

  function quickStatus(status: string) {
    const next = { ...draft, window_status: draft.window_status === status ? '' : status }
    setDraft(next)
    setFilters(next)
    setPage(1)
  }

  function locateMetric(metricNumber: string) {
    const next = { ...EMPTY_FILTERS, keyword: metricNumber }
    setDraft(next)
    setFilters(next)
    setPage(1)
  }

  async function openDetail(row: WindowRow) {
    setDetailError('')
    setSelected(row as WindowDetail)
  }

  const totalPages = data?.pages ?? 0

  return (
    <div>
      <div className="stat-row">
        <article className="stat-card">
          <span className="stat-label">窗口总数（筛选后）</span>
          <strong className="stat-value">{summary?.窗口总数 ?? 0}</strong>
          <span className="stat-hint">当前页 {data?.items.length ?? 0} 条，共 {totalPages} 页</span>
        </article>
        <article className="stat-card">
          <span className="stat-label">已到齐窗口</span>
          <strong className="stat-value">{summary?.已到齐 ?? 0}</strong>
          <span className="stat-hint">可进入大盘与阈值判定</span>
        </article>
        <article className="stat-card">
          <span className="stat-label">可容忍迟到窗口</span>
          <strong className="stat-value">{summary?.可容忍迟到 ?? 0}</strong>
          <span className="stat-hint">仍在重放窗口内，等待补齐</span>
        </article>
        <article className="stat-card">
          <span className="stat-label">覆盖率不足窗口</span>
          <strong className="stat-value">{summary?.覆盖率不足 ?? 0}</strong>
          <span className="stat-hint">下游不消费，保留上一份完整结果</span>
        </article>
        <article className="stat-card">
          <span className="stat-label">新鲜度滞留窗口</span>
          <strong className="stat-value">{summary?.新鲜度滞留窗口 ?? 0}</strong>
          <span className="stat-hint">平均覆盖率 {formatPercent(summary?.平均覆盖率)}</span>
        </article>
      </div>

      {facets?.指标新鲜度?.length ? (
        <div className="legend">
          <strong>指标链路新鲜度：</strong>
          {facets.指标新鲜度.map((item) => (
            <button
              key={item.指标编号}
              type="button"
              className="link"
              onClick={() => locateMetric(item.指标编号)}
              title={`最新结果窗口 ${item.最新结果窗口 ?? '—'} / 应有 ${item.应有最新窗口 ?? '—'}`}
            >
              {item.指标编号}
              <span className={`badge ${item.链路新鲜度 === '正常' ? 'ok' : 'danger'}`} style={{ marginLeft: 4 }}>
                {item.链路新鲜度}
                {item.链路滞后窗口数 ? ` · 滞后${item.链路滞后窗口数}窗` : ''}
              </span>
            </button>
          ))}
        </div>
      ) : null}

      <form
        className="filter-bar"
        onSubmit={(event) => {
          event.preventDefault()
          applyFilters()
        }}
      >
        <label className="filter-item">
          <span>搜索</span>
          <input
            value={draft.keyword}
            placeholder="指标编号 / 名称 / 数据源流 / 维度"
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
            {(facets?.聚合方式 ?? []).map((item) => <option key={item} value={item}>{item}</option>)}
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
          <span>窗口状态</span>
          <select
            value={draft.window_status}
            onChange={(event) => setDraft((prev) => ({ ...prev, window_status: event.target.value }))}
          >
            <option value="">全部</option>
            {WINDOW_STATUSES.map((item) => <option key={item} value={item}>{item}</option>)}
          </select>
        </label>
        <label className="filter-item">
          <span>新鲜度状态</span>
          <select
            value={draft.freshness_state}
            onChange={(event) => setDraft((prev) => ({ ...prev, freshness_state: event.target.value }))}
          >
            <option value="">全部</option>
            {FRESHNESS_STATES.map((item) => <option key={item} value={item}>{item}</option>)}
          </select>
        </label>
        <label className="filter-item">
          <span>新鲜度区间（秒）</span>
          <div style={{ display: 'flex', gap: 4 }}>
            <input
              style={{ minWidth: 70, width: 70 }}
              value={draft.freshness_min}
              placeholder={`${facets?.新鲜度区间?.最小 ?? '最小'}`}
              onChange={(event) => setDraft((prev) => ({ ...prev, freshness_min: event.target.value }))}
            />
            <input
              style={{ minWidth: 70, width: 70 }}
              value={draft.freshness_max}
              placeholder={`${facets?.新鲜度区间?.最大 ?? '最大'}`}
              onChange={(event) => setDraft((prev) => ({ ...prev, freshness_max: event.target.value }))}
            />
          </div>
        </label>
        <label className="filter-item">
          <span>覆盖率下限</span>
          <input
            style={{ minWidth: 80, width: 80 }}
            value={draft.coverage_min}
            placeholder="0.8"
            onChange={(event) => setDraft((prev) => ({ ...prev, coverage_min: event.target.value }))}
          />
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
            <option value="desc">降序</option>
            <option value="asc">升序</option>
          </select>
        </label>
        <button className="btn primary" type="submit">{loading ? '查询中…' : '查询'}</button>
        <button className="btn ghost" type="button" onClick={resetFilters}>重置条件</button>
      </form>

      <div className="action-row">
        <span className="hint-text">快速筛选：</span>
        {WINDOW_STATUSES.map((status) => (
          <button
            key={status}
            type="button"
            className={`btn small ${draft.window_status === status ? 'primary' : ''}`}
            onClick={() => quickStatus(status)}
          >
            {status}
          </button>
        ))}
        <span className="hint-text">{data?.note}</span>
      </div>

      {errorMessage && <div className="notice danger">{errorMessage}</div>}

      <table className="data-table">
        <thead>
          <tr>
            <th>指标</th>
            <th>数据源流</th>
            <th>窗口范围</th>
            <th>聚合 / 维度</th>
            <th>指标值</th>
            <th>覆盖率</th>
            <th>数据新鲜度</th>
            <th>窗口状态</th>
            <th>版本 / 批次</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          {data?.items.map((row) => (
            <tr key={row.id} className={selected?.id === row.id ? 'selected' : ''}>
              <td>
                <button className="link" type="button" onClick={() => locateMetric(row.指标编号)}>
                  {row.指标编号}
                </button>
                <div className="hint-text">{row.指标名称}</div>
              </td>
              <td>{row.数据源流}</td>
              <td>
                <div>{formatWindow(row.窗口开始, row.窗口结束)}</div>
                <div className="hint-text">{row.计算窗口秒}s · 封窗 {formatTime(row.重放截止时间)}</div>
              </td>
              <td>
                <div>{row.聚合函数}</div>
                <div className="hint-text">{row.维度字段组}</div>
              </td>
              <td>{formatValue(row.指标值)}</td>
              <td>
                <div className="coverage-cell">
                  <span>{formatPercent(row.覆盖率)} / 下限 {formatPercent(row.覆盖率下限)}</span>
                  <div className={`coverage-bar ${coverageTone(row.覆盖率, row.覆盖率下限)}`}>
                    <i style={{ width: `${Math.min(100, Math.round(row.覆盖率 * 100))}%` }} />
                  </div>
                  <span className="hint-text">{row.去重后事件数}/{row.预期事件数} 事件</span>
                </div>
              </td>
              <td>
                <div>{formatSeconds(row.数据新鲜度秒)} / 阈值 {formatSeconds(row.新鲜度阈值秒)}</div>
                <span className={`badge ${freshnessTone(row.新鲜度状态)}`}>{row.新鲜度状态}</span>
              </td>
              <td>
                <span className={`badge ${windowStatusTone(row.窗口状态)}`}>{row.窗口状态}</span>
                <div className="hint-text">{row.消费准入 ? '下游可消费' : '下游不消费'}</div>
              </td>
              <td>
                <div>v{row.结果版本}</div>
                <div className="hint-text">{row.计算批次}</div>
              </td>
              <td>
                <button className="link" type="button" onClick={() => void openDetail(row)}>查看溯源</button>
              </td>
            </tr>
          ))}
          {data && data.items.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={10}>没有匹配的窗口结果，请放宽定位条件</td>
            </tr>
          )}
        </tbody>
      </table>

      <div className="pager">
        <span className="hint-text">
          共 {data?.total ?? 0} 条 · 第 {data?.page ?? 1} / {Math.max(totalPages, 1)} 页
        </span>
        <select value={size} onChange={(event) => { setSize(Number(event.target.value)); setPage(1) }}>
          {[10, 20, 50, 100].map((value) => <option key={value} value={value}>{value} 条/页</option>)}
        </select>
        <button className="btn small" type="button" disabled={page <= 1} onClick={() => setPage((prev) => Math.max(1, prev - 1))}>上一页</button>
        <button className="btn small" type="button" disabled={page >= totalPages} onClick={() => setPage((prev) => prev + 1)}>下一页</button>
      </div>

      {selected && (
        <section className="panel">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <h3>窗口结果溯源 · {selected.窗口标识}</h3>
            <button className="btn small" type="button" onClick={() => setSelected(null)}>收起</button>
          </div>
          {detailError && <div className="notice danger">{detailError}</div>}
          <div className="panel-grid">
            <div className="kv"><span>窗口状态</span><strong><span className={`badge ${windowStatusTone(selected.窗口状态)}`}>{selected.窗口状态}</span></strong></div>
            <div className="kv"><span>数据新鲜度</span><strong>{formatSeconds(selected.数据新鲜度秒)} / 阈值 {formatSeconds(selected.新鲜度阈值秒)} · {selected.新鲜度状态}</strong></div>
            <div className="kv"><span>覆盖率</span><strong>{formatPercent(selected.覆盖率)} / 下限 {formatPercent(selected.覆盖率下限)}（{selected.去重后事件数}/{selected.预期事件数}）</strong></div>
            <div className="kv"><span>数据源流</span><strong>{selected.数据源流}</strong></div>
            <div className="kv"><span>窗口范围</span><strong>{formatWindow(selected.窗口开始, selected.窗口结束)}</strong></div>
            <div className="kv"><span>聚合方式 / 维度字段组</span><strong>{selected.聚合函数} · {selected.维度字段组}</strong></div>
            <div className="kv"><span>重放截止时间</span><strong>{formatTime(selected.重放截止时间)}</strong></div>
            <div className="kv"><span>结果版本 / 指纹</span><strong>v{selected.结果版本} · {selected.结果指纹}</strong></div>
            <div className="kv"><span>计算批次 / 触发方式</span><strong>{selected.计算批次} · {selected.触发方式}</strong></div>
            <div className="kv"><span>最后计算时间</span><strong>{formatTime(selected.最后计算时间)}</strong></div>
            <div className="kv"><span>最新数据到达</span><strong>{formatTime(selected.数据最新收到时间)}</strong></div>
            <div className="kv"><span>下游消费</span><strong>{selected.消费准入 ? '允许' : '不允许（保留上一份完整结果）'}</strong></div>
          </div>
          <p className="hint-text">新鲜度口径：{selected.新鲜度口径}</p>

          {selected.数据源流记录 && (
            <>
              <h3 style={{ marginTop: 12 }}>数据源流记录</h3>
              <div className="panel-grid">
                {Object.entries(selected.数据源流记录)
                  .filter(([key]) => key !== 'id')
                  .map(([key, value]) => (
                    <div key={key} className="kv"><span>{key}</span><strong>{String(value)}</strong></div>
                  ))}
              </div>
            </>
          )}

          <h3 style={{ marginTop: 12 }}>版本流水（每次校验计算都留痕）</h3>
          <table className="sub-table">
            <thead>
              <tr>
                <th>版本</th><th>指标值</th><th>覆盖率</th><th>新鲜度</th><th>窗口状态</th><th>触发方式</th><th>计算批次</th><th>计算出时间</th><th>说明</th>
              </tr>
            </thead>
            <tbody>
              {selected.版本流水?.map((version) => (
                <tr key={version.id}>
                  <td>v{version.版本号}{version.是否当前版本 ? '（当前）' : ''}</td>
                  <td>{formatValue(version.指标值)}</td>
                  <td>{formatPercent(version.覆盖率)}</td>
                  <td>{formatSeconds(version.数据新鲜度秒)}</td>
                  <td><span className={`badge ${windowStatusTone(version.窗口状态)}`}>{version.窗口状态}</span></td>
                  <td>{version.触发方式}</td>
                  <td>{version.计算批次}</td>
                  <td>{formatTime(version.计算出时间)}</td>
                  <td>{version.版本说明}</td>
                </tr>
              ))}
              {!selected.版本流水?.length && (
                <tr><td colSpan={9} className="empty-state">暂无版本流水</td></tr>
              )}
            </tbody>
          </table>

          <h3 style={{ marginTop: 12 }}>大盘同步结果</h3>
          <table className="sub-table">
            <thead><tr><th>大盘</th><th>指标值</th><th>覆盖率</th><th>结果版本</th><th>刷新时间</th></tr></thead>
            <tbody>
              {selected.大盘联动?.map((row, index) => (
                <tr key={`${String(row.大盘编号)}-${index}`}>
                  <td>{String(row.大盘名称 ?? row.大盘编号)}</td>
                  <td>{formatValue(row.指标值 as number | null)}</td>
                  <td>{formatPercent(row.覆盖率 as number | null)}</td>
                  <td>v{String(row.结果版本 ?? '—')}</td>
                  <td>{formatTime(row.刷新时间 as string | null)}</td>
                </tr>
              ))}
              {!selected.大盘联动?.length && (
                <tr><td colSpan={5} className="empty-state">该窗口未被下游大盘消费（窗口未到齐或覆盖率不足）</td></tr>
              )}
            </tbody>
          </table>

          <h3 style={{ marginTop: 12 }}>阈值评估</h3>
          <table className="sub-table">
            <thead><tr><th>规则</th><th>指标值</th><th>阈值区间</th><th>判定结果</th><th>是否已消费</th><th>评估时间</th></tr></thead>
            <tbody>
              {selected.阈值联动?.map((row, index) => (
                <tr key={`${String(row.规则编号)}-${index}`}>
                  <td>{String(row.规则编号)}</td>
                  <td>{formatValue(row.指标值 as number | null)}</td>
                  <td>{String(row.阈值下限)} ~ {String(row.阈值上限)}</td>
                  <td>{String(row.判定结果)}</td>
                  <td>{row.是否已消费 ? '是' : '否'}</td>
                  <td>{formatTime(row.评估时间 as string | null)}</td>
                </tr>
              ))}
              {!selected.阈值联动?.length && (
                <tr><td colSpan={6} className="empty-state">暂无阈值评估记录，可在阈值管理中刷新</td></tr>
              )}
            </tbody>
          </table>
        </section>
      )}
    </div>
  )
}
