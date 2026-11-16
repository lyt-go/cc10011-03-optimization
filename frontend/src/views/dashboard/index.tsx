import { useCallback, useEffect, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/dashboard'
const columns = [
  '大盘编号',
  '大盘名称',
  '包含指标组',
  '布局模板',
  '刷新频率秒',
  '对比周期',
  '分享范围',
  '负责人',
]
const actions = ['提交大盘评审', '发布大盘', '下线大盘']
const statuses = ['草稿', '大盘评审中', '已发布', '已下线']

type Row = Record<string, string | number | null>
type Card = Record<string, unknown>
type RefreshPayload = {
  refreshed: { 大盘编号: string; 大盘名称: string; 指标组: string[]; 指标卡: Card[] }[]
  dashboard_count: number
  card_count: number
  refreshed_at: string
  口径: string
}

function badgeFor(value: unknown) {
  if (value === '已到齐') return 'ok'
  if (value === '可容忍迟到') return 'warn'
  if (value === '覆盖率不足') return 'danger'
  return 'muted'
}

export default function DashboardView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [notice, setNotice] = useState('')
  const [keyword, setKeyword] = useState('')
  const [status, setStatus] = useState('')
  const [refresh, setRefresh] = useState<RefreshPayload | null>(null)

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams()
    if (keyword) query.set('keyword', keyword)
    if (status) query.set('status', status)
    try {
      const response = await request(`${ENDPOINT}?${query.toString()}`)
      if (!response.ok) throw new Error('大盘配置列表读取失败')
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '监控大盘列表读取失败')
    }
  }, [keyword, status])

  const reloadCards = useCallback(async () => {
    try {
      const response = await request(`${ENDPOINT}/refresh`)
      if (!response.ok) throw new Error('大盘联动刷新失败')
      setRefresh((await response.json()) as RefreshPayload)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '大盘联动刷新失败')
    }
  }, [])

  useEffect(() => {
    void reload()
    void reloadCards()
  }, [reload, reloadCards])

  async function runAction(action: string, row: Row) {
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
      await reloadCards()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '监控大盘操作失败')
    }
  }

  return (
    <section className="page" data-module="dashboard">
      <header className="page-head">
        <div>
          <h2>监控大盘管理</h2>
          <p className="page-desc">
            大盘结果只从实时指标计算结果数据集读取可消费窗口；迟到事件重放后，大盘指标卡随窗口版本联动刷新，
            覆盖率不足或新鲜度滞留的窗口保留上一份完整结果并标注滞后。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => void reloadCards()}>
            联动刷新大盘
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/snapshots`, '_blank')}>
            查看窗口快照
          </button>
        </div>
      </header>

      <div className="stat-row">
        <article className="stat-card">
          <span className="stat-label">大盘配置总数</span>
          <strong className="stat-value">{total}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">已发布大盘</span>
          <strong className="stat-value">{rows.filter((row) => row.status === '已发布').length}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">联动指标卡</span>
          <strong className="stat-value">{refresh?.card_count ?? 0}</strong>
          <span className="stat-hint">刷新于 {refresh?.refreshed_at ?? '—'}</span>
        </article>
      </div>

      {notice && <div className="notice ok">{notice}</div>}
      {errorMessage && <div className="notice danger">{errorMessage}</div>}

      {refresh?.refreshed.map((dashboard) => (
        <div key={dashboard.大盘编号} className="panel">
          <h3>{dashboard.大盘名称} · {dashboard.大盘编号}</h3>
          <p className="hint-text">包含指标组：{dashboard.指标组.join('、')}</p>
          <table className="sub-table">
            <thead>
              <tr>
                <th>指标</th><th>数据源流</th><th>窗口</th><th>指标值</th><th>覆盖率</th>
                <th>窗口状态</th><th>版本</th><th>滞后</th><th>说明</th>
              </tr>
            </thead>
            <tbody>
              {dashboard.指标卡.map((card) => (
                <tr key={String(card.指标编号)}>
                  <td>{String(card.指标编号)}<div className="hint-text">{String(card.指标名称 ?? '')}</div></td>
                  <td>{String(card.数据源流 ?? '—')}</td>
                  <td>{String(card.窗口开始 ?? '—')}</td>
                  <td>{card.指标值 === null || card.指标值 === undefined ? '—' : String(card.指标值)}</td>
                  <td>{card.覆盖率 === null || card.覆盖率 === undefined ? '—' : `${(Number(card.覆盖率) * 100).toFixed(1)}%`}</td>
                  <td><span className={`badge ${badgeFor(card.窗口状态)}`}>{String(card.窗口状态 ?? '—')}</span></td>
                  <td>{card.结果版本 ? `v${String(card.结果版本)}` : '—'}</td>
                  <td>{card.滞后窗口数 === null || card.滞后窗口数 === undefined ? '—' : String(card.滞后窗口数)}</td>
                  <td>{String(card.说明 ?? '')}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ))}
      {refresh && <p className="hint-text">{refresh.口径}</p>}

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }} style={{ marginTop: 16 }}>
        <label className="filter-item">
          <span>搜索</span>
          <input value={keyword} placeholder="大盘编号 / 名称 / 指标组" onChange={(event) => setKeyword(event.target.value)} />
        </label>
        <label className="filter-item">
          <span>状态</span>
          <select value={status} onChange={(event) => setStatus(event.target.value)}>
            <option value="">全部</option>
            {statuses.map((item) => <option key={item} value={item}>{item}</option>)}
          </select>
        </label>
        <button className="btn" type="submit">查询</button>
      </form>

      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
              <td className="row-actions">
                {actions.map((action) => (
                  <button key={action} className="link" type="button" onClick={() => void runAction(action, row)}>
                    {action}
                  </button>
                ))}
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 1}>暂无监控大盘数据，可先登记大盘配置</td>
            </tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>共 {total} 条监控大盘记录</span>
      </footer>
    </section>
  )
}
