/** 窗口状态与新鲜度的展示口径，集中在这里保证列表、徽标和详情用同一套颜色。 */
export const WINDOW_STATUSES = ['已到齐', '可容忍迟到', '覆盖率不足'] as const
export const FRESHNESS_STATES = ['新鲜', '滞留', '已封窗', '等待数据'] as const

export type StatusTone = 'ok' | 'warn' | 'danger' | 'info' | 'muted'

export function windowStatusTone(status: string): StatusTone {
  if (status === '已到齐') return 'ok'
  if (status === '可容忍迟到') return 'warn'
  if (status === '覆盖率不足') return 'danger'
  return 'muted'
}

export function freshnessTone(state: string): StatusTone {
  if (state === '新鲜') return 'ok'
  if (state === '已封窗') return 'info'
  if (state === '滞留') return 'danger'
  return 'muted'
}

export function coverageTone(ratio: number, floor: number): string {
  if (ratio < floor) return 'low'
  if (ratio < Math.min(1, floor + 0.1)) return 'mid'
  return ''
}

export function formatSeconds(value: number | null | undefined): string {
  if (value === null || value === undefined) return '—'
  if (value < 60) return `${value}s`
  if (value < 3600) return `${Math.floor(value / 60)}m${value % 60 ? `${value % 60}s` : ''}`
  const hours = Math.floor(value / 3600)
  const minutes = Math.floor((value % 3600) / 60)
  return `${hours}h${minutes ? `${minutes}m` : ''}`
}

export function formatPercent(value: number | null | undefined): string {
  if (value === null || value === undefined) return '—'
  return `${(value * 100).toFixed(1)}%`
}

export function formatTime(value: string | null | undefined): string {
  if (!value) return '—'
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) return value
  return parsed.toLocaleString('zh-CN', { hour12: false })
}

export function formatWindow(start: string, end: string): string {
  const short = (value: string) => {
    const parsed = new Date(value)
    if (Number.isNaN(parsed.getTime())) return value
    return parsed.toLocaleTimeString('zh-CN', { hour12: false })
  }
  return `${short(start)} ~ ${short(end)}`
}

export function formatValue(value: number | null | undefined): string {
  if (value === null || value === undefined) return '—'
  return typeof value === 'number' ? value.toLocaleString('zh-CN', { maximumFractionDigits: 4 }) : String(value)
}
