import { useCallback, useEffect, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/threshold'
const columns = [
  '规则编号',
  '关联指标',
  '阈值下限',
  '阈值上限',
  '触发条件',
  '持续周期',
  '告警等级',
  '生效时间范围',
]
const actions = ['提交规则评审', '生效规则', '关闭规则']
const statuses = ['草稿', '规则评审中', '已生效', '已关闭']
const verdicts = ['正常', '低于下限', '高于上限', '数据不可用']

type Row = Record<string, string | number | null>
type Evaluation = Record<string, unknown>
type EvaluationPayload = {
  items: Evaluation[]
  total: number
  summary: Record<string, number>
  取数链路: string
}

export default function ThresholdView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [notice, setNotice] = useState('')
  const [keyword, setKeyword] = useState('')
  const [status, setStatus] = useState('')
  const [verdict, setVerdict] = useState('')
  const [evaluations, setEvaluations] = useState<EvaluationPayload | null>(null)
  const [page, setPage] = useState(1)

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams()
    if (keyword) query.set('keyword', keyword)
    if (status) query.set('status', status)
    try {
      const response = await request(`${ENDPOINT}?${query.toString()}`)
      if (!response.ok) throw new Error('阈值规则列表读取失败')
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '阈值管理列表读取失败')
    }
  }, [keyword, status])

  const reloadEvaluations = useCallback(async () => {
    try {
      const query = new URLSearchParams({ page: String(page), size: '20' })
      if (verdict) query.set('verdict', verdict)
      const response = await request(`${ENDPOINT}/evaluations?${query.toString()}`)
      if (!response.ok) throw new Error('阈值评估记录读取失败')
      setEvaluations((await response.json()) as EvaluationPayload)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '阈值评估记录读取失败')
    }
  }, [page, verdict])

  useEffect(() => {
    void reload()
    void reloadEvaluations()
  }, [reload, reloadEvaluations])

  async function refreshEvaluations() {
    setNotice('')
    try {
      const response = await request(`${ENDPOINT}/refresh`, { method: 'POST' })
      const payload = await response.json()
      setNotice(
        `按现行取数链路刷新 ${payload.updated} 条评估（跳过 ${payload.skipped_rules} 条未生效规则），共 ${payload.evaluations} 条评估记录`,
      )
      await reloadEvaluations()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '阈值评估刷新失败')
    }
  }

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
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '阈值管理操作失败')
    }
  }

  return (
    <section className="page" data-module="threshold">
      <header className="page-head">
        <div>
          <h2>阈值管理</h2>
          <p className="page-desc">
            阈值评估按现行取数链路消费实时指标计算结果数据集：只对已到齐且新鲜度完整的窗口判定，
            窗口重放产生新版本后同窗口结论随之更新，不拿半成品数据误报。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => void refreshEvaluations()}>
            按窗口结果刷新评估
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出阈值规则清单
          </button>
        </div>
      </header>

      <div className="stat-row">
        <article className="stat-card">
          <span className="stat-label">阈值规则总数</span>
          <strong className="stat-value">{total}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">评估记录</span>
          <strong className="stat-value">{evaluations?.summary?.['评估总数'] ?? 0}</strong>
          <span className="stat-hint">正常 {evaluations?.summary?.['正常'] ?? 0} 条</span>
        </article>
        <article className="stat-card">
          <span className="stat-label">触发下限 / 上限</span>
          <strong className="stat-value">
            {evaluations?.summary?.['低于下限'] ?? 0} / {evaluations?.summary?.['高于上限'] ?? 0}
          </strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">数据不可用</span>
          <strong className="stat-value">{evaluations?.summary?.['数据不可用'] ?? 0}</strong>
          <span className="stat-hint">窗口未到齐或覆盖率不足</span>
        </article>
      </div>

      {notice && <div className="notice ok">{notice}</div>}
      {errorMessage && <div className="notice danger">{errorMessage}</div>}

      <div className="panel">
        <div className="action-row">
          <h3 style={{ margin: 0 }}>窗口评估记录</h3>
          <label className="filter-item">
            <span>判定结果</span>
            <select
              value={verdict}
              onChange={(event) => { setVerdict(event.target.value); setPage(1) }}
            >
              <option value="">全部</option>
              {verdicts.map((item) => <option key={item} value={item}>{item}</option>)}
            </select>
          </label>
          <span className="hint-text">{evaluations?.['取数链路']}</span>
        </div>
        <table className="sub-table">
          <thead>
            <tr>
              <th>规则</th><th>指标</th><th>数据源流</th><th>窗口开始</th><th>指标值</th>
              <th>阈值区间</th><th>判定结果</th><th>窗口状态</th><th>覆盖率</th><th>版本</th><th>是否已消费</th>
            </tr>
          </thead>
          <tbody>
            {evaluations?.items.map((row) => (
              <tr key={`${String(row.规则编号)}-${String(row.窗口开始)}`}>
                <td>{String(row.规则编号)}</td>
                <td>{String(row.指标编号)}</td>
                <td>{String(row.数据源流 ?? '—')}</td>
                <td>{String(row.窗口开始)}</td>
                <td>{row.指标值 === null || row.指标值 === undefined ? '—' : String(row.指标值)}</td>
                <td>{String(row.阈值下限)} ~ {String(row.阈值上限)}</td>
                <td>
                  <span className={`badge ${row.判定结果 === '正常' ? 'ok' : row.判定结果 === '数据不可用' ? 'muted' : 'danger'}`}>
                    {String(row.判定结果)}
                  </span>
                </td>
                <td>{String(row.窗口状态 ?? '—')}</td>
                <td>{row.覆盖率 === null || row.覆盖率 === undefined ? '—' : `${(Number(row.覆盖率) * 100).toFixed(1)}%`}</td>
                <td>v{String(row.结果版本 ?? '—')}</td>
                <td>{row.是否已消费 ? '是' : '否'}</td>
              </tr>
            ))}
            {evaluations && evaluations.items.length === 0 && (
              <tr><td className="empty-state" colSpan={11}>暂无评估记录，可点击“按窗口结果刷新评估”</td></tr>
            )}
          </tbody>
        </table>
        <div className="pager">
          <span className="hint-text">共 {evaluations?.total ?? 0} 条</span>
          <button className="btn small" type="button" disabled={page <= 1} onClick={() => setPage((prev) => Math.max(1, prev - 1))}>上一页</button>
          <button className="btn small" type="button" disabled={(evaluations?.total ?? 0) <= page * 20} onClick={() => setPage((prev) => prev + 1)}>下一页</button>
        </div>
      </div>

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }} style={{ marginTop: 12 }}>
        <label className="filter-item">
          <span>搜索</span>
          <input value={keyword} placeholder="规则编号 / 关联指标 / 告警等级" onChange={(event) => setKeyword(event.target.value)} />
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
              <td className="empty-state" colSpan={columns.length + 1}>暂无阈值管理数据，可先登记阈值规则</td>
            </tr>
          )}
        </tbody>
      </table>
    </section>
  )
}
