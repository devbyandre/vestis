import { Loader2, AlertCircle, X, ChevronUp, ChevronDown, ChevronsUpDown, Download } from 'lucide-react'
import { useState, useMemo } from 'react'
import { clsx } from 'clsx'

// ── Loading states ────────────────────────────────────────────────────────────

export function Spinner({ size = 16, className = '' }) {
  return <Loader2 size={size} className={clsx('animate-spin text-accent', className)} />
}

export function LoadingOverlay({ label = 'Loading...' }) {
  return (
    <div className="flex flex-col items-center justify-center gap-3 py-16 text-gray-500">
      <Spinner size={24} />
      <span className="text-sm">{label}</span>
    </div>
  )
}

export function InlineLoader() {
  return (
    <div className="flex items-center gap-2 text-gray-500 text-sm py-4">
      <Spinner size={14} />
      <span>Loading…</span>
    </div>
  )
}

// ── Error ─────────────────────────────────────────────────────────────────────

export function ErrorMsg({ error, label = 'Failed to load data' }) {
  const msg = error?.response?.data?.detail || error?.message || label
  return (
    <div className="flex items-center gap-2 text-danger text-sm bg-danger/5 border border-danger/20 rounded-lg p-3">
      <AlertCircle size={14} className="shrink-0" />
      <span>{msg}</span>
    </div>
  )
}

// ── Modal ─────────────────────────────────────────────────────────────────────

export function Modal({ open, onClose, title, children, wide = false }) {
  if (!open) return null
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4"
      style={{ background: 'rgba(0,0,0,0.7)' }}
      onClick={(e) => e.target === e.currentTarget && onClose()}
    >
      <div className={clsx('bg-surface-1 border border-surface-3 rounded-xl shadow-2xl w-full', wide ? 'max-w-2xl' : 'max-w-md')}>
        <div className="flex items-center justify-between px-5 py-4 border-b border-surface-3">
          <h3 className="text-sm font-semibold text-gray-100">{title}</h3>
          <button onClick={onClose} className="text-gray-500 hover:text-gray-300 transition-colors">
            <X size={16} />
          </button>
        </div>
        <div className="p-5">{children}</div>
      </div>
    </div>
  )
}

// ── Confirm dialog ────────────────────────────────────────────────────────────

export function ConfirmModal({ open, onClose, onConfirm, title, message, danger = false }) {
  return (
    <Modal open={open} onClose={onClose} title={title}>
      <p className="text-sm text-gray-400 mb-5">{message}</p>
      <div className="flex gap-2 justify-end">
        <button className="btn-ghost" onClick={onClose}>Cancel</button>
        <button
          className={danger ? 'btn-danger' : 'btn-primary'}
          onClick={() => { onConfirm(); onClose() }}
        >
          Confirm
        </button>
      </div>
    </Modal>
  )
}

// ── Section header ────────────────────────────────────────────────────────────

export function SectionHeader({ title, subtitle, action }) {
  return (
    <div className="flex items-start justify-between mb-4">
      <div>
        <h2 className="text-base font-semibold text-gray-100">{title}</h2>
        {subtitle && <p className="text-xs text-gray-500 mt-0.5">{subtitle}</p>}
      </div>
      {action}
    </div>
  )
}

// ── Metric card ───────────────────────────────────────────────────────────────

export function MetricCard({ label, value, sub, color = '', icon }) {
  return (
    <div className="metric-card">
      <div className="flex items-center justify-between">
        <span className="metric-label">{label}</span>
        {icon && <span className="text-gray-600">{icon}</span>}
      </div>
      <div className={clsx('metric-value', color)}>{value ?? '—'}</div>
      {sub && <div className="text-xs text-gray-600">{sub}</div>}
    </div>
  )
}

// ── Sortable table ────────────────────────────────────────────────────────────

export function SortableTable({
  columns, data, defaultSort, className = '',
  searchable = false, searchKeys = null, pageSize = 0,
  exportable = false, exportName = 'data',
}) {
  const [sort, setSort] = useState(defaultSort || { key: columns[0]?.key, asc: true })
  const [query, setQuery] = useState('')
  const [page, setPage] = useState(1)

  // Search filter
  const filtered = useMemo(() => {
    if (!searchable || !query.trim()) return data
    const q = query.toLowerCase()
    const keys = searchKeys || columns.map(c => c.key)
    return data.filter(row =>
      keys.some(k => String(row[k] ?? '').toLowerCase().includes(q))
    )
  }, [data, query, searchable, searchKeys, columns])

  // Sort
  const sorted = useMemo(() => {
    return [...filtered].sort((a, b) => {
      const va = a[sort.key] ?? (sort.asc ? -Infinity : Infinity)
      const vb = b[sort.key] ?? (sort.asc ? -Infinity : Infinity)
      if (typeof va === 'string' || typeof vb === 'string') {
        return sort.asc
          ? String(va).localeCompare(String(vb))
          : String(vb).localeCompare(String(va))
      }
      return sort.asc ? va - vb : vb - va
    })
  }, [filtered, sort])

  // Pagination
  const pages = pageSize > 0 ? Math.max(1, Math.ceil(sorted.length / pageSize)) : 1
  const pageData = pageSize > 0
    ? sorted.slice((page - 1) * pageSize, page * pageSize)
    : sorted

  const toggleSort = (key) => {
    setSort(s => s.key === key ? { key, asc: !s.asc } : { key, asc: true })
  }

  const exportCsv = () => {
    const header = columns.map(c => `"${c.label}"`).join(',')
    const rows = sorted.map(row =>
      columns.map(c => {
        const raw = c.exportValue ? c.exportValue(row[c.key], row) : row[c.key]
        return `"${String(raw ?? '').replace(/"/g, '""')}"`
      }).join(',')
    )
    const csv = [header, ...rows].join('\n')
    const blob = new Blob([csv], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `${exportName}.csv`
    a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <div className={clsx('', className)}>
      {(searchable || exportable) && (
        <div className="flex items-center justify-between gap-2 mb-3">
          {searchable ? (
            <input
              className="input max-w-xs text-sm"
              placeholder="Search…"
              value={query}
              onChange={e => { setQuery(e.target.value); setPage(1) }}
            />
          ) : <div />}
          {exportable && (
            <button className="btn-ghost text-xs" onClick={exportCsv}>
              <Download size={12} /> CSV
            </button>
          )}
        </div>
      )}
      <div className="overflow-x-auto">
        <table className="w-full text-left">
          <thead>
            <tr className="border-b border-surface-3">
              {columns.map(col => (
                <th
                  key={col.key}
                  className={clsx('th cursor-pointer select-none hover:text-gray-300 transition-colors', col.align === 'right' ? 'text-right' : '')}
                  onClick={() => toggleSort(col.key)}
                >
                  <span className="inline-flex items-center gap-1">
                    {col.label}
                    {sort.key === col.key
                      ? (sort.asc ? <ChevronUp size={10} /> : <ChevronDown size={10} />)
                      : <ChevronsUpDown size={10} className="opacity-30" />}
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {pageData.map((row, i) => (
              <tr key={i} className="table-row">
                {columns.map(col => (
                  <td key={col.key} className={clsx('td', col.align === 'right' ? 'text-right' : '')}>
                    {col.render ? col.render(row[col.key], row) : (row[col.key] ?? '—')}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
        {!pageData.length && (
          <div className="py-8 text-center text-sm text-gray-600">No data</div>
        )}
      </div>
      {pageSize > 0 && pages > 1 && (
        <div className="flex items-center gap-2 mt-3 text-sm text-gray-500">
          <button className="btn-ghost px-2 py-1 text-xs disabled:opacity-30"
            disabled={page <= 1} onClick={() => setPage(p => p - 1)}>‹ Prev</button>
          <span className="text-xs">{page} / {pages}</span>
          <button className="btn-ghost px-2 py-1 text-xs disabled:opacity-30"
            disabled={page >= pages} onClick={() => setPage(p => p + 1)}>Next ›</button>
          <span className="ml-2 text-xs text-gray-600">{sorted.length} rows</span>
        </div>
      )}
    </div>
  )
}

// ── Pagination ────────────────────────────────────────────────────────────────

export function Pagination({ page, total, pageSize, onChange }) {
  const pages = Math.max(1, Math.ceil(total / pageSize))
  if (pages <= 1) return null
  return (
    <div className="flex items-center gap-2 mt-3 text-sm text-gray-500">
      <button
        className="btn-ghost px-2 py-1 text-xs disabled:opacity-30"
        disabled={page <= 1}
        onClick={() => onChange(page - 1)}
      >
        ‹ Prev
      </button>
      <span className="text-xs">{page} / {pages}</span>
      <button
        className="btn-ghost px-2 py-1 text-xs disabled:opacity-30"
        disabled={page >= pages}
        onClick={() => onChange(page + 1)}
      >
        Next ›
      </button>
      <span className="ml-2 text-xs text-gray-600">{total} total</span>
    </div>
  )
}

// ── Expandable section ────────────────────────────────────────────────────────

export function Expander({ title, children, defaultOpen = false }) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <div className="border border-surface-3 rounded-xl overflow-hidden">
      <button
        className="w-full flex items-center justify-between px-4 py-3 text-sm font-medium text-gray-300 hover:bg-surface-2 transition-colors"
        onClick={() => setOpen(o => !o)}
      >
        {title}
        {open ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
      </button>
      {open && <div className="px-4 pb-4 pt-1">{children}</div>}
    </div>
  )
}

// ── Badge helpers ─────────────────────────────────────────────────────────────

export function PnlBadge({ value, suffix = '%', multiplier = 100 }) {
  if (value == null || isNaN(value)) return <span className="text-gray-600">—</span>
  const v = value * multiplier
  return (
    <span className={clsx('badge', v >= 0 ? 'badge-green' : 'badge-red')}>
      {v >= 0 ? '+' : ''}{v.toFixed(1)}{suffix}
    </span>
  )
}

// ── Select input ──────────────────────────────────────────────────────────────

export function Select({ label: lbl, value, onChange, options, className = '' }) {
  return (
    <div className={className}>
      {lbl && <label className="label">{lbl}</label>}
      <select className="select" value={value} onChange={e => onChange(e.target.value)}>
        {options.map(o => (
          <option key={o.value ?? o} value={o.value ?? o}>
            {o.label ?? o}
          </option>
        ))}
      </select>
    </div>
  )
}

// ── Text input ────────────────────────────────────────────────────────────────

export function Input({ label: lbl, value, onChange, type = 'text', placeholder, className = '', min, max, step }) {
  return (
    <div className={className}>
      {lbl && <label className="label">{lbl}</label>}
      <input
        className="input"
        type={type}
        value={value ?? ''}
        onChange={e => onChange(e.target.value)}
        placeholder={placeholder}
        min={min}
        max={max}
        step={step}
      />
    </div>
  )
}

// ── Tabs ──────────────────────────────────────────────────────────────────────

export function TabBar({ tabs, active, onChange }) {
  return (
    <div className="flex gap-1 overflow-x-auto pb-1">
      {tabs.map(t => (
        <button
          key={t.id}
          onClick={() => onChange(t.id)}
          className={clsx(
            'flex items-center gap-2 px-3 py-2 rounded-lg text-sm font-medium whitespace-nowrap transition-all',
            active === t.id
              ? 'bg-accent/15 text-accent-bright border border-accent/30'
              : 'text-gray-500 hover:text-gray-300 hover:bg-surface-2'
          )}
        >
          {t.icon && <span>{t.icon}</span>}
          {t.label}
        </button>
      ))}
    </div>
  )
}
