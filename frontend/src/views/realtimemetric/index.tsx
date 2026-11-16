import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

import WindowFreshnessPanel from './WindowFreshnessPanel'
import MetricDefinitionPanel from './MetricDefinitionPanel'
import type { BatchRow } from './types'
import { formatTime } from './format'

const ENDPOINT = '/api/realtimemetric'
const TABS = [
  { value: 'windows', label: '窗口新鲜度视图' },
  { value: 'definitions', label: '指标定义' },
  { value: 'batches', label: '计算批次与数据集' },
]

type BatchPage = { items: BatchRow[]; total: number }
type DataSetPayload = {
  dataset: string
  dataset_version: string
  generated_at: string
  total: number
  page: number
  pages: number
  items: Record<string, unknown>[]
  traceability: Record<string, unknown>
  summary: Record<string, number>
}

const EMPTY_EVENT = {
  datasource_stream: 'stream.payment',
  事件编号: '',
  事件时间: '',
  度量值: '',
  dimensions: {} as Record<string, string>,
}

export default function RealtimemetricView() {
  const [activeTab, setActiveTab] = useState('windows')
  const [reloadToken, setReloadToken] = useState(0)
  const [windowFocus, setWindowFocus] = useState('')
  const [notice, setNotice] = useState('')
  const [errorMessage, setErrorMessage] = useState('')
  const [busy, setBusy] = useState(false)
  const [lookback, setLookback] = useState(8)
  const [simulateFailure, setSimulateFailure] = useState(false)
  const [batches, setBatches] = useState<BatchRow[]>([])
  const [dataset, setDataset] = useState<DataSetPayload | null>(null)
  const [datasetPage, setDatasetPage] = useState(1)
  const [includeProvisional, setIncludeProvisional] = useState(false)
  const [eventForm, setEventForm] = useState(EMPTY_EVENT)
  const [streams, setStreams] = useState<Record<string, unknown>[]>([])

  const activeDimensions = useMemo(() => {
    const stream = streams.find((row) => row['数据源流'] === eventForm.datasource_stream)
    const names = String(stream?.['维度字段'] ?? '')
      .split(',')
      .map((name) => name.trim())
      .filter(Boolean)
    if (names.length) return names
    return eventForm.datasource_stream === 'stream.payment' ? ['channel', 'region'] : []
  }, [streams, eventForm.datasource_stream])

  const previewStream = useCallback((streamName: string) => {
    const stream = streams.find((row) => row['数据源流'] === streamName)
    const names = String(stream?.['维度字段'] ?? '')
      .split(',')
      .map((name) => name.trim())
      .filter(Boolean)
    const dimensions: Record<string, string> = {}
    names.forEach((name) => {
      dimensions[name] = eventForm.dimensions[name] ?? ''
    })
    setEventForm((prev) => ({ ...prev, datasource_stream: streamName, dimensions }))
  }, [streams, eventForm.dimensions])

  const refreshDerived = useCallback(async () => {
    try {
      const batchResponse = await request(`${ENDPOINT}/batches?size=8`)
      if (batchResponse.ok) setBatches(((await batchResponse.json()) as BatchPage).items ?? [])
      const datasetResponse = await request(
        `${ENDPOINT}/dataset?size=10&page=${datasetPage}&include_provisional=${includeProvisional}`,
      )
      if (datasetResponse.ok) setDataset((await datasetResponse.json()) as DataSetPayload)
    } catch {
      // 批次与数据集属于辅助视图，失败时由主操作提示兜底。
    }
  }, [datasetPage, includeProvisional])

  useEffect(() => {
    void refreshDerived()
  }, [refreshDerived, reloadToken])

  useEffect(() => {
    request(`${ENDPOINT}/streams`)
      .then(async (response) => (response.ok ? ((await response.json()) as Record<string, unknown>[]) : []))
      .then(setStreams)
      .catch(() => setStreams([]))
  }, [])

  async function runCompute() {
    setBusy(true)
    setErrorMessage('')
    setNotice('')
    try {
      const response = await request(`${ENDPOINT}/compute`, {
        method: 'POST',
        body: JSON.stringify({
          values: { lookback, simulate_failure: simulateFailure, trigger: '手工校验计算' },
        }),
      })
      const payload = await response.json()
      setNotice(payload.message)
      if (!payload.ok) setErrorMessage(payload.message)
      setReloadToken((prev) => prev + 1)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '校验计算失败')
    } finally {
      setBusy(false)
    }
  }

  async function submitEvent(repeat: boolean) {
    setBusy(true)
    setErrorMessage('')
    setNotice('')
    try {
      const eventId = eventForm.事件编号 || `EVT-MANUAL-${Date.now()}`
      const body = {
        values: {
          数据源流: eventForm.datasource_stream,
          事件编号: eventId,
          事件时间: eventForm.事件时间 || new Date().toISOString(),
          度量值: Number(eventForm.度量值 || 0),
          维度: eventForm.dimensions,
          requested_by: '值班管理员',
        },
      }
      let lastPayload: { ok: boolean; message: string } | null = null
      const times = repeat ? 2 : 1
      for (let index = 0; index < times; index += 1) {
        const response = await request(`${ENDPOINT}/events`, {
          method: 'POST',
          body: JSON.stringify(body),
        })
        lastPayload = await response.json()
      }
      if (lastPayload) {
        setNotice(`${lastPayload.message}${repeat ? '（已连续提交两次以验证幂等）' : ''}`)
        if (!lastPayload.ok) setErrorMessage(lastPayload.message)
      }
      setEventForm((prev) => ({ ...prev, 事件编号: eventId }))
      setReloadToken((prev) => prev + 1)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '事件提交失败')
    } finally {
      setBusy(false)
    }
  }

  return (
    <section className="page" data-module="realtimemetric">
      <header className="page-head">
        <div>
          <h2>实时指标管理</h2>
          <p className="page-desc">
            按数据源流、窗口范围、聚合方式和维度字段组组合定位指标；窗口结果区分已到齐、可容忍迟到与覆盖率不足，
            每次校验计算都落库结果版本、数据新鲜度和窗口覆盖率。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" disabled={busy} onClick={() => void runCompute()}>
            {busy ? '计算中…' : `校验并计算最近 ${lookback} 个窗口`}
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出实时指标清单
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/dataset?size=200`, '_blank')}>
            导出计算结果数据集
          </button>
        </div>
      </header>

      <div className="tabs">
        {TABS.map((tab) => (
          <button
            key={tab.value}
            type="button"
            className={`tab ${activeTab === tab.value ? 'active' : ''}`}
            onClick={() => setActiveTab(tab.value)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {notice && <div className="notice ok">{notice}</div>}
      {errorMessage && <div className="notice danger">{errorMessage}</div>}

      {activeTab === 'windows' && (
        <>
          <div className="panel">
            <div className="action-row" style={{ marginBottom: 0 }}>
              <label className="checkbox-line">
                重算窗口数
                <input
                  style={{ width: 64, padding: '3px 6px', border: '1px solid var(--border)', borderRadius: 6 }}
                  type="number"
                  min={1}
                  max={120}
                  value={lookback}
                  onChange={(event) => setLookback(Number(event.target.value))}
                />
              </label>
              <label className="checkbox-line">
                <input
                  type="checkbox"
                  checked={simulateFailure}
                  onChange={(event) => setSimulateFailure(event.target.checked)}
                />
                故障演练（模拟提交失败，验证不暴露残缺版本）
              </label>
              <span className="hint-text">
                {windowFocus ? `已定位指标 ${windowFocus}` : '当前展示全部指标窗口'}
              </span>
              {windowFocus && (
                <button className="btn small" type="button" onClick={() => setWindowFocus('')}>取消定位</button>
              )}
            </div>
          </div>
          <WindowFreshnessPanel reloadToken={reloadToken} focusMetric={windowFocus} />
        </>
      )}

      {activeTab === 'definitions' && (
        <MetricDefinitionPanel
          reloadToken={reloadToken}
          onLocateWindows={(metricNumber) => {
            setWindowFocus(metricNumber)
            setActiveTab('windows')
          }}
          onChanged={() => setReloadToken((prev) => prev + 1)}
        />
      )}

      {activeTab === 'batches' && (
        <div>
          <div className="panel">
            <h3>事件提交（验证重复提交与迟到重放）</h3>
            <p className="hint-text">
              重复提交同一事件编号只留摄取流水，不重复累计；迟到事件在窗口重放截止时间前会触发窗口整体重算。
            </p>
            <div className="inline-form">
              <label>
                <span>数据源流</span>
                <select
                  value={eventForm.datasource_stream}
                  onChange={(event) => previewStream(event.target.value)}
                >
                  {(streams.length ? streams.map((row) => String(row['数据源流'])) : ['stream.payment']).map((item) => (
                    <option key={item} value={item}>{item}</option>
                  ))}
                </select>
              </label>
              <label>
                <span>事件编号（留空自动生成）</span>
                <input
                  value={eventForm.事件编号}
                  placeholder="EVT-..."
                  onChange={(event) => setEventForm((prev) => ({ ...prev, 事件编号: event.target.value }))}
                />
              </label>
              <label>
                <span>事件时间（ISO，留空取当前时间）</span>
                <input
                  value={eventForm.事件时间}
                  placeholder="2026-11-16T11:58:30Z"
                  onChange={(event) => setEventForm((prev) => ({ ...prev, 事件时间: event.target.value }))}
                />
              </label>
              <label>
                <span>度量值</span>
                <input
                  value={eventForm.度量值}
                  placeholder="100"
                  onChange={(event) => setEventForm((prev) => ({ ...prev, 度量值: event.target.value }))}
                />
              </label>
              {activeDimensions.map((name) => (
                <label key={name}>
                  <span>{name}</span>
                  <input
                    value={eventForm.dimensions[name] ?? ''}
                    placeholder={`${name} 取值`}
                    onChange={(event) => setEventForm((prev) => ({
                      ...prev,
                      dimensions: { ...prev.dimensions, [name]: event.target.value },
                    }))}
                  />
                </label>
              ))}
            </div>
            {activeDimensions.length === 0 && (
              <p className="hint-text">该数据源流没有登记维度字段，提交时可以不带维度。</p>
            )}
            <div className="action-row" style={{ marginTop: 10, marginBottom: 0 }}>
              <button className="btn primary" type="button" disabled={busy} onClick={() => void submitEvent(false)}>
                提交事件
              </button>
              <button className="btn" type="button" disabled={busy} onClick={() => void submitEvent(true)}>
                连续提交两次（验证幂等）
              </button>
            </div>
          </div>

          <div className="panel">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <h3>计算结果数据集 · {dataset?.dataset_version ?? '—'}</h3>
              <label className="checkbox-line">
                <input
                  type="checkbox"
                  checked={includeProvisional}
                  onChange={(event) => { setIncludeProvisional(event.target.checked); setDatasetPage(1) }}
                />
                包含可容忍迟到 / 覆盖率不足窗口
              </label>
            </div>
            <p className="hint-text">
              默认只输出可被下游消费的窗口（已到齐且新鲜度完整）。生成时间 {formatTime(dataset?.generated_at)}，
              共 {dataset?.total ?? 0} 条，第 {dataset?.page ?? 1}/{Math.max(dataset?.pages ?? 0, 1)} 页。
            </p>
            <table className="data-table">
              <thead>
                <tr><th>窗口标识</th><th>数据源流</th><th>窗口状态</th><th>指标值</th><th>覆盖率</th><th>新鲜度</th><th>版本</th><th>批次</th></tr>
              </thead>
              <tbody>
                {dataset?.items.map((row) => (
                  <tr key={String(row['窗口标识'])}>
                    <td>{String(row['窗口标识'])}</td>
                    <td>{String(row['数据源流'])}</td>
                    <td><span className="badge info">{String(row['窗口状态'])}</span></td>
                    <td>{String(row['指标值'] ?? '—')}</td>
                    <td>{String(row['覆盖率'] ?? '—')}</td>
                    <td>{String(row['数据新鲜度秒'] ?? '—')}s</td>
                    <td>v{String(row['结果版本'] ?? '—')}</td>
                    <td>{String(row['计算批次'] ?? '—')}</td>
                  </tr>
                ))}
                {dataset && dataset.items.length === 0 && (
                  <tr><td className="empty-state" colSpan={8}>数据集中没有可消费窗口，请先执行校验计算</td></tr>
                )}
              </tbody>
            </table>
            <div className="pager">
              <button className="btn small" type="button" disabled={datasetPage <= 1} onClick={() => setDatasetPage((prev) => Math.max(1, prev - 1))}>上一页</button>
              <button className="btn small" type="button" disabled={datasetPage >= (dataset?.pages ?? 1)} onClick={() => setDatasetPage((prev) => prev + 1)}>下一页</button>
            </div>
            <h3 style={{ marginTop: 14 }}>可追溯性说明</h3>
            <div className="panel-grid">
              {dataset?.traceability && Object.entries(dataset.traceability).map(([key, value]) => (
                <div key={key} className="kv">
                  <span>{key}</span>
                  <strong>{Array.isArray(value) ? value.join('、') : String(value)}</strong>
                </div>
              ))}
            </div>
          </div>

          <div className="panel">
            <h3>计算批次流水</h3>
            <table className="data-table">
              <thead>
                <tr>
                  <th>批次编号</th><th>触发方式</th><th>状态</th><th>计算窗口数</th>
                  <th>已到齐</th><th>可容忍迟到</th><th>覆盖率不足</th><th>下游刷新</th><th>结束时间</th><th>失败原因</th>
                </tr>
              </thead>
              <tbody>
                {batches.map((row) => (
                  <tr key={row.id}>
                    <td>{row.批次编号}</td>
                    <td>{row.触发方式}</td>
                    <td><span className={`badge ${row.状态 === '成功' ? 'ok' : 'danger'}`}>{row.状态}</span></td>
                    <td>{row.计算窗口数}</td>
                    <td>{row.已到齐}</td>
                    <td>{row.可容忍迟到}</td>
                    <td>{row.覆盖率不足}</td>
                    <td>{row.下游刷新数}</td>
                    <td>{formatTime(row.结束时间)}</td>
                    <td>{row.失败原因 ?? '—'}</td>
                  </tr>
                ))}
                {batches.length === 0 && (
                  <tr><td className="empty-state" colSpan={10}>暂无计算批次</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </section>
  )
}
