import { useState, useMemo } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Plus, Pencil, Trash2 } from 'lucide-react'
import toast from 'react-hot-toast'
import { alertsApi, securitiesApi } from '../lib/api'
import { qk } from '../lib/queryClient'
import { fmt } from '../lib/utils'
import { LoadingOverlay, ErrorMsg, SectionHeader, Modal, ConfirmModal, Pagination, Input, Select } from '../components/ui'

const PAGE_SIZE = 15

const ALERT_TYPES = ['price', 'rsi', 'ma_crossover', '52w', 'volume_spike', 'pct_change', 'earnings_soon', 'mos']

function AlertForm({ initial, securities, onSubmit, onClose }) {
  const [form, setForm] = useState(initial || {
    security_id: '', alert_type: 'price', params: '{}',
    note: '', notify_mode: 'immediate', cooldown_seconds: 14400, active: true,
  })
  const set = k => v => setForm(f => ({ ...f, [k]: v }))

  const handleSubmit = (e) => {
    e.preventDefault()
    let params = {}
    try { params = JSON.parse(form.params || '{}') } catch { toast.error('Invalid JSON in params'); return }
    onSubmit({ ...form, params, security_id: Number(form.security_id), cooldown_seconds: Number(form.cooldown_seconds) })
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-3">
      <div>
        <label className="label">Security</label>
        <select className="select" value={form.security_id} onChange={e => set('security_id')(e.target.value)} required>
          <option value="">Select…</option>
          {securities.map(s => <option key={s.id} value={s.id}>{s.symbol || s.yahoo_ticker} {s.name ? `— ${s.name}` : ''}</option>)}
        </select>
      </div>
      <Select label="Alert Type" value={form.alert_type} onChange={set('alert_type')} options={ALERT_TYPES} />
      <div>
        <label className="label">Parameters (JSON)</label>
        <textarea className="input font-mono text-xs" rows={3} value={form.params} onChange={e => set('params')(e.target.value)} />
        <p className="text-xs text-gray-600 mt-0.5">e.g. {`{"threshold": 150, "direction": "below", "mode": "absolute"}`}</p>
      </div>
      <div className="grid grid-cols-2 gap-3">
        <Input label="Note" value={form.note} onChange={set('note')} />
        <Select label="Notify Mode" value={form.notify_mode} onChange={set('notify_mode')}
          options={['immediate', 'digest_daily', 'digest_weekly']} />
      </div>
      <Input label="Cooldown (seconds)" type="number" value={form.cooldown_seconds} onChange={set('cooldown_seconds')} min="0" />
      <div className="flex gap-2 justify-end">
        <button type="button" className="btn-ghost" onClick={onClose}>Cancel</button>
        <button type="submit" className="btn-primary">Save</button>
      </div>
    </form>
  )
}

export default function TabAlerts() {
  const qc = useQueryClient()
  const [page, setPage] = useState(1)
  const [statusFilter, setStatusFilter] = useState('active')
  const [typeFilter, setTypeFilter] = useState('')
  const [showAdd, setShowAdd] = useState(false)
  const [editAlert, setEditAlert] = useState(null)
  const [deleteAlert, setDeleteAlert] = useState(null)

  const { data: securities = [] } = useQuery({ queryKey: qk.securities(), queryFn: securitiesApi.list })
  const { data: alerts = [], isLoading, error } = useQuery({ queryKey: qk.alerts(), queryFn: () => alertsApi.list(false) })

  const filtered = useMemo(() => {
    let rows = alerts.filter(a => a.alert_type !== 'split_pending')
    if (statusFilter === 'active') rows = rows.filter(r => r.active === 1 || r.active === true)
    if (statusFilter === 'inactive') rows = rows.filter(r => !r.active)
    if (typeFilter) rows = rows.filter(r => r.alert_type === typeFilter)
    return rows
  }, [alerts, statusFilter, typeFilter])

  const paged = filtered.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE)

  const createMut = useMutation({
    mutationFn: alertsApi.create,
    onSuccess: () => { qc.invalidateQueries({ queryKey: qk.alerts() }); setShowAdd(false); toast.success('Alert created') },
    onError: e => toast.error(e.message),
  })
  const editMut = useMutation({
    mutationFn: ({ id, data }) => alertsApi.edit(id, data),
    onSuccess: () => { qc.invalidateQueries({ queryKey: qk.alerts() }); setEditAlert(null); toast.success('Updated') },
    onError: e => toast.error(e.message),
  })
  const deleteMut = useMutation({
    mutationFn: alertsApi.delete,
    onSuccess: () => { qc.invalidateQueries({ queryKey: qk.alerts() }); toast.success('Deleted') },
    onError: e => toast.error(e.message),
  })
  const toggleMut = useMutation({
    mutationFn: ({ id, active }) => alertsApi.edit(id, { active }),
    onSuccess: () => qc.invalidateQueries({ queryKey: qk.alerts() }),
  })

  const describeAlert = (type, params) => {
    try {
      const p = typeof params === 'string' ? JSON.parse(params || '{}') : (params || {})
      if (type === 'price') return `Price ${p.direction || ''} ${p.threshold != null ? fmt.currency(p.threshold, 2) : ''}`
      if (type === 'pct_change') return `>${p.pct || 5}% ${p.direction || 'down'} in ${p.days || 1}d`
      if (type === 'rsi') return `RSI >${p.overbought || 70} / <${p.underbought || 30}`
      if (type === 'ma_crossover') return `${p.crossover_type || 'golden'} cross ${p.short}/${p.long}`
      if (type === '52w') return `52w ${p.type || 'high'}`
      if (type === 'earnings_soon') return `Earnings ≤${p.days || 3} days`
      return type
    } catch { return type }
  }

  const uniqueTypes = [...new Set(alerts.map(a => a.alert_type).filter(t => t !== 'split_pending'))].sort()

  return (
    <div className="space-y-4">
      <SectionHeader title="Alerts Manager" action={<button className="btn-primary" onClick={() => setShowAdd(true)}><Plus size={14} /> New Alert</button>} />

      {/* Filters */}
      <div className="card flex flex-wrap gap-3 items-center">
        <div className="flex gap-1">
          {[['active', 'Active'], ['inactive', 'Inactive'], ['', 'All']].map(([v, l]) => (
            <button key={v} className={`badge cursor-pointer ${statusFilter === v ? 'badge-blue' : 'bg-surface-3 text-gray-400'}`} onClick={() => setStatusFilter(v)}>{l}</button>
          ))}
        </div>
        <select className="select text-xs max-w-xs" value={typeFilter} onChange={e => setTypeFilter(e.target.value)}>
          <option value="">All types</option>
          {uniqueTypes.map(t => <option key={t} value={t}>{t}</option>)}
        </select>
      </div>

      {isLoading ? <LoadingOverlay /> : error ? <ErrorMsg error={error} /> : (
        <div className="card overflow-hidden p-0">
          <div className="px-4 py-2 border-b border-surface-3 text-xs text-gray-500">{filtered.length} alerts</div>
          <table className="w-full text-left">
            <thead><tr className="border-b border-surface-3">
              {['Security', 'Type', 'Description', 'Note', 'Mode', 'Last Triggered', 'Status', ''].map((h, i) => <th key={i} className="th">{h}</th>)}
            </tr></thead>
            <tbody>
              {paged.map((r, i) => (
                <tr key={r.id ?? i} className="table-row">
                  <td className="td font-medium text-xs">{r.symbol || '—'}</td>
                  <td className="td"><span className="badge bg-surface-3 text-gray-400 text-xs">{r.alert_type}</span></td>
                  <td className="td text-xs text-gray-400">{describeAlert(r.alert_type, r.params)}</td>
                  <td className="td text-xs text-gray-500">{r.note || '—'}</td>
                  <td className="td text-xs text-gray-500">{r.notify_mode}</td>
                  <td className="td text-xs text-gray-600">{r.last_triggered ? fmt.date(r.last_triggered) : '—'}</td>
                  <td className="td">
                    <button onClick={() => toggleMut.mutate({ id: r.id, active: !r.active })}
                      className={`badge cursor-pointer text-xs ${r.active ? 'badge-green' : 'bg-surface-3 text-gray-600'}`}>
                      {r.active ? 'Active' : 'Inactive'}
                    </button>
                  </td>
                  <td className="td">
                    <div className="flex gap-2">
                      <button className="text-gray-600 hover:text-accent transition-colors" onClick={() => setEditAlert(r)}><Pencil size={12} /></button>
                      <button className="text-gray-600 hover:text-danger transition-colors" onClick={() => setDeleteAlert(r)}><Trash2 size={12} /></button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <div className="px-4 py-2 border-t border-surface-3">
            <Pagination page={page} total={filtered.length} pageSize={PAGE_SIZE} onChange={setPage} />
          </div>
        </div>
      )}

      <Modal open={showAdd} onClose={() => setShowAdd(false)} title="Create Alert" wide>
        <AlertForm securities={securities} onSubmit={data => createMut.mutate(data)} onClose={() => setShowAdd(false)} />
      </Modal>

      {editAlert && (
        <Modal open={!!editAlert} onClose={() => setEditAlert(null)} title="Edit Alert" wide>
          <AlertForm initial={{ ...editAlert, params: typeof editAlert.params === 'object' ? JSON.stringify(editAlert.params, null, 2) : editAlert.params }}
            securities={securities}
            onSubmit={data => editMut.mutate({ id: editAlert.id, data })}
            onClose={() => setEditAlert(null)} />
        </Modal>
      )}

      <ConfirmModal open={!!deleteAlert} onClose={() => setDeleteAlert(null)}
        onConfirm={() => deleteMut.mutate(deleteAlert.id)} danger
        title="Delete alert" message={`Delete ${deleteAlert?.alert_type} alert for ${deleteAlert?.symbol}?`} />
    </div>
  )
}
