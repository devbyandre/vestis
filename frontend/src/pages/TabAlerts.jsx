import { useState, useMemo } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Plus, Pencil, Trash2 } from 'lucide-react'
import toast from 'react-hot-toast'
import { alertsApi, securitiesApi } from '../lib/api'
import { qk } from '../lib/queryClient'
import { fmt } from '../lib/utils'
import { useUrlParam, setUrlParams } from '../lib/urlState'
import { ALERT_SCHEMAS, ALERT_TYPES, toFormValues, toParams, validateForm, describeAlert, isEditable } from '../lib/alertSchema'
import { LoadingOverlay, ErrorMsg, SectionHeader, Modal, ConfirmModal, Pagination, Input, Select, Expander } from '../components/ui'

const PAGE_SIZE = 15

const errMsg = e => e.response?.data?.detail || e.message

function AlertForm({ initial, securities, onSubmit, onClose }) {
  const [form, setForm] = useState(() => {
    const type = initial?.alert_type || 'price'
    return {
      security_id: initial?.security_id ?? '', alert_type: type,
      note: initial?.note || '', notify_mode: initial?.notify_mode || 'immediate',
      cooldown_seconds: initial?.cooldown_seconds ?? 14400, active: initial?.active ?? true,
      values: toFormValues(type, initial?.params),
    }
  })
  const base = initial?.alert_type === form.alert_type ? initial?.params : {}
  const set = k => v => setForm(f => ({ ...f, [k]: v }))
  const setValue = (k, v) => setForm(f => {
    const values = { ...f.values, [k]: v }
    // switching RSI zone swaps the default level (70 <-> 30) unless the user typed their own
    if (f.alert_type === 'rsi' && k === 'direction') {
      const untouched = f.values.threshold === (f.values.direction === 'above' ? '70' : '30')
      if (untouched) values.threshold = v === 'above' ? '70' : '30'
    }
    return { ...f, values }
  })
  const changeType = type => setForm(f => ({ ...f, alert_type: type, values: toFormValues(type, {}) }))

  const schema = ALERT_SCHEMAS[form.alert_type]
  const preview = describeAlert(form.alert_type, toParams(form.alert_type, form.values))

  const handleSubmit = (e) => {
    e.preventDefault()
    const err = validateForm(form.alert_type, form.values)
    if (err) { toast.error(err); return }
    onSubmit({
      security_id: Number(form.security_id), alert_type: form.alert_type,
      params: toParams(form.alert_type, form.values, base),
      note: form.note, notify_mode: form.notify_mode,
      cooldown_seconds: Number(form.cooldown_seconds), active: form.active,
    })
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
      <Select label="Alert type" value={form.alert_type} onChange={changeType}
        options={ALERT_TYPES.map(t => ({ value: t, label: ALERT_SCHEMAS[t].label }))} />
      <p className="text-xs text-gray-500 -mt-1">{schema.help}</p>
      <div className="grid grid-cols-2 gap-3">
        {schema.fields.map(f => f.type === 'select'
          ? <Select key={f.key} label={f.label} value={form.values[f.key]} onChange={v => setValue(f.key, v)}
              options={f.options.map(([value, label]) => ({ value, label }))} />
          : <Input key={f.key} label={f.label} type="number" value={form.values[f.key]} onChange={v => setValue(f.key, v)}
              min={f.min} max={f.max} step={f.step || '1'} />)}
      </div>
      <div className="text-xs text-accent-bright bg-surface-2 rounded-lg px-3 py-2">
        You will be notified when: <span className="font-medium">{preview}</span>
      </div>
      <div className="grid grid-cols-2 gap-3">
        <Input label="Note" value={form.note} onChange={set('note')} />
        <Select label="Notify mode" value={form.notify_mode} onChange={set('notify_mode')}
          options={[
            { value: 'immediate', label: 'Immediately' },
            { value: 'digest_daily', label: 'Daily digest' },
            { value: 'digest_weekly', label: 'Weekly digest' },
          ]} />
      </div>
      <Input label="Cooldown (seconds)" type="number" value={form.cooldown_seconds} onChange={set('cooldown_seconds')} min="0" />
      <div className="flex gap-2 justify-end">
        <button type="button" className="btn-ghost" onClick={onClose}>Cancel</button>
        <button type="submit" className="btn-primary">Save</button>
      </div>
    </form>
  )
}

// Open a security in the Technical tab (pushes history so Back returns here)
const openTechnical = symbol => setUrlParams({ tab: 'technical', symbol, mode: null }, { push: true })

function AlertHistory() {
  const [page, setPage] = useState(1)
  const { data: rows = [], isLoading, error } = useQuery({
    queryKey: qk.alertHistory(), queryFn: () => alertsApi.history(500),
  })
  if (isLoading) return <LoadingOverlay />
  if (error) return <ErrorMsg error={error} />
  const paged = rows.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE)
  return (
    <div className="card overflow-hidden p-0">
      <div className="px-4 py-2 border-b border-surface-3 text-xs text-gray-500">
        {rows.length ? `Last ${rows.length} triggers — what fired, when, and with which values` : 'No alerts have fired yet.'}
      </div>
      {rows.length > 0 && (
        <table className="w-full text-left">
          <thead><tr className="border-b border-surface-3">
            {['When', 'Security', 'Alert', 'Values', 'Note', 'Delivered'].map(h => <th key={h} className="th">{h}</th>)}
          </tr></thead>
          <tbody>
            {paged.map(r => (
              <tr key={r.id} className="table-row">
                <td className="td text-xs text-gray-400 whitespace-nowrap">{fmt.dateTime(r.triggered_at)}</td>
                <td className="td text-xs">
                  {r.symbol
                    ? <button className="font-medium text-accent-bright hover:underline" title={r.security_name || ''}
                        onClick={() => openTechnical(r.symbol)}>{r.symbol}</button>
                    : '—'}
                </td>
                <td className="td text-xs text-gray-300">{describeAlert(r.alert_type, r.params)}</td>
                <td className="td text-xs text-gray-400">{r.detail || '—'}</td>
                <td className="td text-xs text-gray-500">{r.note || '—'}</td>
                <td className="td"><span className={`badge text-xs ${r.delivery === 'immediate' ? 'badge-blue' : 'bg-surface-3 text-gray-400'}`}
                  title={r.delivery === 'held' ? 'Triggered during quiet hours; delivered afterwards in one summary' : undefined}>
                  {r.delivery === 'held' ? 'quiet hours' : r.delivery}</span></td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <div className="px-4 py-2 border-t border-surface-3">
        <Pagination page={page} total={rows.length} pageSize={PAGE_SIZE} onChange={setPage} />
      </div>
    </div>
  )
}

export default function TabAlerts() {
  const [view, setView] = useUrlParam('mode', 'manage')
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
    onError: e => toast.error(errMsg(e)),
  })
  const editMut = useMutation({
    mutationFn: ({ id, data }) => alertsApi.edit(id, data),
    onSuccess: () => { qc.invalidateQueries({ queryKey: qk.alerts() }); setEditAlert(null); toast.success('Updated') },
    onError: e => toast.error(errMsg(e)),
  })
  const deleteMut = useMutation({
    mutationFn: alertsApi.delete,
    onSuccess: () => { qc.invalidateQueries({ queryKey: qk.alerts() }); toast.success('Deleted') },
    onError: e => toast.error(errMsg(e)),
  })
  const toggleMut = useMutation({
    mutationFn: ({ id, active }) => alertsApi.edit(id, { active }),
    onSuccess: () => qc.invalidateQueries({ queryKey: qk.alerts() }),
  })

  const uniqueTypes = [...new Set(alerts.map(a => a.alert_type).filter(t => t !== 'split_pending'))].sort()

  return (
    <div className="space-y-4">
      <SectionHeader title="Alerts Manager" action={
        <div className="flex gap-2 items-center">
          <div className="flex gap-1">
            {[['manage', 'Alerts'], ['history', 'History']].map(([v, l]) => (
              <button key={v} className={`btn text-xs px-3 py-1.5 ${view === v ? 'btn-primary' : 'btn-ghost'}`}
                onClick={() => setView(v)}>{l}</button>
            ))}
          </div>
          <button className="btn-primary" onClick={() => setShowAdd(true)}><Plus size={14} /> New Alert</button>
        </div>
      } />

      {view === 'history' ? <AlertHistory /> : <>

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

      <Expander title="ℹ️ Alert Type Descriptions">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-1 text-xs text-gray-400">
          {ALERT_TYPES.map(t => (
            <div key={t}><span className="text-gray-200 font-medium">{ALERT_SCHEMAS[t].label}</span> — {ALERT_SCHEMAS[t].help}</div>
          ))}
        </div>
      </Expander>

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
                  <td className="td font-medium text-xs">
                    {r.symbol ? <button className="hover:text-accent-bright hover:underline" onClick={() => openTechnical(r.symbol)}>{r.symbol}</button> : '—'}
                  </td>
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
                      {isEditable(r) && <button className="text-gray-600 hover:text-accent transition-colors" onClick={() => setEditAlert(r)}><Pencil size={12} /></button>}
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
      </>}

      <Modal open={showAdd} onClose={() => setShowAdd(false)} title="Create Alert" wide>
        <AlertForm securities={securities} onSubmit={data => createMut.mutate(data)} onClose={() => setShowAdd(false)} />
      </Modal>

      {editAlert && (
        <Modal open={!!editAlert} onClose={() => setEditAlert(null)} title="Edit Alert" wide>
          <AlertForm initial={editAlert}
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
