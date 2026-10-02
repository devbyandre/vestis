import { useState, useMemo, lazy, Suspense } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Plus, Pencil, Trash2, GitMerge } from 'lucide-react'
import toast from 'react-hot-toast'
import { transactionsApi, portfolioApi, securitiesApi } from '../lib/api'
import { qk } from '../lib/queryClient'
import { fmt, plotlyConfig } from '../lib/utils'
import { LoadingOverlay, ErrorMsg, Modal, ConfirmModal, SortableTable, SectionHeader, Input, Select, Expander, MetricCard } from '../components/ui'

function PortfolioManager({ portfolios }) {
  const qc = useQueryClient()
  const [showAdd, setShowAdd] = useState(false)
  const [newName, setNewName] = useState('')
  const [renamePf, setRenamePf] = useState(null)
  const [renameValue, setRenameValue] = useState('')
  const [deletePf, setDeletePf] = useState(null)
  const [reassignTo, setReassignTo] = useState('')

  const invalidate = () => qc.invalidateQueries({ queryKey: qk.portfolios() })

  const createMut = useMutation({
    mutationFn: (name) => portfolioApi.create(name),
    onSuccess: () => { invalidate(); setShowAdd(false); setNewName(''); toast.success('Portfolio created') },
    onError: (e) => toast.error(e.message),
  })
  const renameMut = useMutation({
    mutationFn: ({ old_name, new_name }) => portfolioApi.rename(old_name, new_name),
    onSuccess: () => { invalidate(); setRenamePf(null); toast.success('Portfolio renamed') },
    onError: (e) => toast.error(e.message),
  })
  const deleteMut = useMutation({
    mutationFn: ({ name, reassign_to }) => portfolioApi.delete(name, reassign_to),
    onSuccess: () => {
      invalidate()
      qc.invalidateQueries({ queryKey: ['transactions'] })
      qc.invalidateQueries({ queryKey: ['holdings'] })
      setDeletePf(null)
      toast.success('Portfolio deleted')
    },
    onError: (e) => toast.error(e.message),
  })

  const openDelete = (p) => {
    setDeletePf(p)
    setReassignTo(portfolios.find(x => x.id !== p.id)?.name || '')
  }

  return (
    <>
      <div className="flex items-center justify-between mb-3">
        <p className="text-xs text-gray-500">Create, rename, or delete portfolios. Deleting reassigns its transactions to another portfolio.</p>
        <button className="btn-ghost text-xs shrink-0" onClick={() => setShowAdd(true)}><Plus size={12} /> New Portfolio</button>
      </div>
      <div className="space-y-1.5">
        {portfolios.map(p => (
          <div key={p.id} className="flex items-center justify-between px-3 py-2 rounded-lg bg-surface-2 text-sm">
            <span className="text-gray-200">{p.name}</span>
            <div className="flex gap-2">
              <button className="text-gray-500 hover:text-accent transition-colors"
                onClick={() => { setRenamePf(p); setRenameValue(p.name) }}>
                <Pencil size={12} />
              </button>
              <button
                className={deleteBtnClass(portfolios.length <= 1)}
                disabled={portfolios.length <= 1}
                title={portfolios.length <= 1 ? 'Cannot delete the only portfolio' : 'Delete'}
                onClick={() => openDelete(p)}
              >
                <Trash2 size={12} />
              </button>
            </div>
          </div>
        ))}
      </div>

      <Modal open={showAdd} onClose={() => setShowAdd(false)} title="New Portfolio">
        <form onSubmit={(e) => { e.preventDefault(); createMut.mutate(newName) }} className="space-y-3">
          <Input label="Name" value={newName} onChange={setNewName} placeholder="e.g. Satellite" />
          <div className="flex gap-2 justify-end pt-2">
            <button type="button" className="btn-ghost" onClick={() => setShowAdd(false)}>Cancel</button>
            <button type="submit" className="btn-primary" disabled={!newName.trim()}>Create</button>
          </div>
        </form>
      </Modal>

      <Modal open={!!renamePf} onClose={() => setRenamePf(null)} title={`Rename "${renamePf?.name}"`}>
        <form onSubmit={(e) => { e.preventDefault(); renameMut.mutate({ old_name: renamePf.name, new_name: renameValue }) }} className="space-y-3">
          <Input label="New name" value={renameValue} onChange={setRenameValue} />
          <div className="flex gap-2 justify-end pt-2">
            <button type="button" className="btn-ghost" onClick={() => setRenamePf(null)}>Cancel</button>
            <button type="submit" className="btn-primary" disabled={!renameValue.trim()}>Save</button>
          </div>
        </form>
      </Modal>

      <Modal open={!!deletePf} onClose={() => setDeletePf(null)} title={`Delete "${deletePf?.name}"`}>
        <div className="space-y-3">
          <p className="text-sm text-gray-400">
            Its transactions will be reassigned to the portfolio below. This cannot be undone.
          </p>
          <Select label="Reassign transactions to" value={reassignTo} onChange={setReassignTo}
            options={portfolios.filter(p => p.id !== deletePf?.id).map(p => ({ value: p.name, label: p.name }))} />
          <div className="flex gap-2 justify-end pt-2">
            <button className="btn-ghost" onClick={() => setDeletePf(null)}>Cancel</button>
            <button className="btn-danger" disabled={!reassignTo}
              onClick={() => deleteMut.mutate({ name: deletePf.name, reassign_to: reassignTo })}>
              Delete
            </button>
          </div>
        </div>
      </Modal>
    </>
  )
}

function deleteBtnClass(disabled) {
  return disabled
    ? 'text-gray-700 cursor-not-allowed'
    : 'text-gray-500 hover:text-danger transition-colors'
}

const Plot = lazy(() =>
  // Vite's dev-mode CJS interop for this package can double-wrap the
  // default export ({ default: { default: Component } }) depending on the
  // bundler version — unwrap defensively so it works either way.
  import('react-plotly.js').then(m => ({ default: m.default?.default ?? m.default }))
)
const BASE = {
  paper_bgcolor: 'transparent', plot_bgcolor: 'transparent',
  font: { color: '#9ca3af', size: 11 }, margin: { l: 50, r: 20, t: 36, b: 40 },
  xaxis: { gridcolor: '#1a1a24', linecolor: '#2a2a3a' },
  yaxis: { gridcolor: '#1a1a24', linecolor: '#2a2a3a' },
  hovermode: 'x unified',
  hoverlabel: { bgcolor: '#1a1a24', bordercolor: '#2a2a3a', font: { color: '#e5e7eb', size: 11 } },
}
function LazyPlot(props) {
  return <Suspense fallback={<div className="text-gray-500 text-xs py-4">Loading chart…</div>}><Plot {...props} /></Suspense>
}

function TxForm({ initial, portfolios, securities, onSubmit, onClose }) {
  const [form, setForm] = useState(initial || {
    portfolio_id: portfolios[0]?.id || '', symbol: '',
    tx_date: new Date().toISOString().slice(0, 10),
    tx_type: 'buy', quantity: '', price: '', fees: '0',
  })
  const isSplit = form.tx_type === 'split'
  const set = (k) => (v) => setForm(f => ({ ...f, [k]: v }))
  const handleSubmit = (e) => { e.preventDefault(); onSubmit(form) }

  return (
    <form onSubmit={handleSubmit} className="space-y-3">
      <div className="grid grid-cols-2 gap-3">
        <Select label="Portfolio" value={form.portfolio_id} onChange={set('portfolio_id')}
          options={portfolios.map(p => ({ value: p.id, label: p.name }))} />
        <div>
          <label className="label">Security</label>
          <input className="input" list="sec-list" value={form.symbol}
            onChange={e => set('symbol')(e.target.value)} placeholder="e.g. AAPL" required />
          <datalist id="sec-list">
            {securities.map(s => <option key={s.symbol || s.yahoo_ticker} value={s.symbol || s.yahoo_ticker} />)}
          </datalist>
        </div>
      </div>
      <div className="grid grid-cols-2 gap-3">
        <Input label="Date" type="date" value={form.tx_date} onChange={set('tx_date')} />
        <Select label="Type" value={form.tx_type} onChange={set('tx_type')}
          options={['buy', 'sell', 'split'].map(v => ({ value: v, label: v.toUpperCase() }))} />
      </div>
      {isSplit ? (
        <div>
          <Input label="Split ratio (new/old, e.g. 3 for 3-for-1)" type="number"
            value={form.quantity} onChange={set('quantity')} min="0.01" step="0.01" />
          <p className="text-xs text-gray-500 mt-1">Adjusts all historical qty/price automatically. No tax event.</p>
        </div>
      ) : (
        <div className="grid grid-cols-3 gap-3">
          <Input label="Quantity" type="number" value={form.quantity} onChange={set('quantity')} min="0" step="any" />
          <Input label="Price (€)" type="number" value={form.price} onChange={set('price')} min="0" step="any" />
          <Input label="Fees (€)" type="number" value={form.fees} onChange={set('fees')} min="0" step="any" />
        </div>
      )}
      <div className="flex gap-2 justify-end pt-2">
        <button type="button" className="btn-ghost" onClick={onClose}>Cancel</button>
        <button type="submit" className="btn-primary">Save</button>
      </div>
    </form>
  )
}

export default function TabTransactions() {
  const qc = useQueryClient()
  const [portfolioFilter, setPortfolioFilter] = useState('')
  const [securityFilter, setSecurityFilter] = useState([])
  const [typeFilter, setTypeFilter] = useState([])
  const [editTx, setEditTx] = useState(null)
  const [deleteTx, setDeleteTx] = useState(null)
  const [showAdd, setShowAdd] = useState(false)
  const [showSplit, setShowSplit] = useState(false)

  const pfIds = portfolioFilter ? [Number(portfolioFilter)] : null

  const { data: portfolios = [] } = useQuery({ queryKey: qk.portfolios(), queryFn: portfolioApi.list })
  const { data: securities = [] } = useQuery({ queryKey: qk.securities(), queryFn: securitiesApi.list })
  const { data: transactions = [], isLoading, error } = useQuery({
    queryKey: qk.transactions(pfIds),
    queryFn: () => transactionsApi.list(pfIds),
  })
  const { data: summary } = useQuery({
    queryKey: qk.txSummary(pfIds),
    queryFn: () => transactionsApi.summary(pfIds),
  })

  const addMut = useMutation({
    mutationFn: (form) => {
      if (form.tx_type === 'split') {
        return transactionsApi.addSplit({ symbol: form.symbol, split_date: form.tx_date, ratio: Number(form.quantity), portfolio_id: Number(form.portfolio_id) })
      }
      return transactionsApi.add({ ...form, portfolio_id: Number(form.portfolio_id), quantity: Number(form.quantity), price: Number(form.price), fees: Number(form.fees) })
    },
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['transactions'] }); qc.invalidateQueries({ queryKey: ['holdings'] }); setShowAdd(false); setShowSplit(false); toast.success('Transaction added') },
    onError: (e) => toast.error(e.message),
  })
  const editMut = useMutation({
    mutationFn: ({ id, form }) => transactionsApi.edit(id, { ...form, portfolio_id: Number(form.portfolio_id), quantity: Number(form.quantity), price: Number(form.price), fees: Number(form.fees) }),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['transactions'] }); qc.invalidateQueries({ queryKey: ['holdings'] }); setEditTx(null); toast.success('Transaction updated') },
    onError: (e) => toast.error(e.message),
  })
  const deleteMut = useMutation({
    mutationFn: (id) => transactionsApi.delete(id),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['transactions'] }); qc.invalidateQueries({ queryKey: ['holdings'] }); toast.success('Deleted') },
    onError: (e) => toast.error(e.message),
  })

  const securityOptions = useMemo(() =>
    [...new Set(transactions.map(t => t.security_label || t.symbol).filter(Boolean))].sort(),
    [transactions])
  const typeOptions = useMemo(() =>
    [...new Set(transactions.map(t => t.tx_type).filter(Boolean))].sort(),
    [transactions])

  const filteredTx = useMemo(() => {
    let rows = transactions
    if (securityFilter.length) rows = rows.filter(t => securityFilter.includes(t.security_label || t.symbol))
    if (typeFilter.length) rows = rows.filter(t => typeFilter.includes(t.tx_type))
    return rows
  }, [transactions, securityFilter, typeFilter])

  const toggleFilter = (setter) => (val) => setter(prev => prev.includes(val) ? prev.filter(x => x !== val) : [...prev, val])

  // Trends charts data
  const trends = useMemo(() => {
    if (!filteredTx.length) return null
    const sorted = [...filteredTx]
      .filter(t => t.tx_type !== 'split')
      .sort((a, b) => (a.tx_date || a.date) < (b.tx_date || b.date) ? -1 : 1)
    let cumQty = 0, cumFees = 0, cumBuys = 0, cumSells = 0
    const dates = [], qtyArr = [], feesArr = [], buysArr = [], sellsArr = []
    sorted.forEach(t => {
      cumQty += (t.quantity || 0)
      cumFees += (t.tx_cost || 0)
      if (t.tx_type === 'buy') cumBuys += (t.quantity || 0)
      if (t.tx_type === 'sell') cumSells += (t.quantity || 0)
      dates.push(t.tx_date || t.date)
      qtyArr.push(cumQty); feesArr.push(cumFees); buysArr.push(cumBuys); sellsArr.push(cumSells)
    })
    return { dates, qtyArr, feesArr, buysArr, sellsArr }
  }, [filteredTx])

  const typeBadge = (t, qty) => {
    if (t === 'buy') return <span className="badge badge-green">↑ BUY</span>
    if (t === 'sell') return <span className="badge badge-red">↓ SELL</span>
    if (t === 'split') return <span className="badge badge-yellow">🔀 ×{Number(qty).toFixed(2)}</span>
    return <span className="badge">{t}</span>
  }

  const COLS = [
    { key: 'tx_date', label: 'Date', render: (v, r) => fmt.date(v || r.date) },
    { key: 'security_label', label: 'Security', render: (v, r) => v || r.symbol || r.yahoo_ticker },
    { key: 'portfolio', label: 'Portfolio', render: (v, r) => v || r.portfolio_name || '—' },
    { key: 'tx_type', label: 'Type', render: (v, r) => typeBadge(v, r.quantity) },
    { key: 'quantity', label: 'Qty', align: 'right', render: (v, r) => r.tx_type === 'split' ? `×${Number(v).toFixed(4)}` : fmt.num(v, 4) },
    { key: 'price', label: 'Price', align: 'right', render: (v, r) => r.tx_type === 'split' || !v ? '—' : fmt.currency(v, 2) },
    { key: 'tx_cost', label: 'Fees', align: 'right', render: (v, r) => r.tx_type === 'split' || !v ? '—' : fmt.currency(v, 2) },
    { key: 'total_cost', label: 'Total', align: 'right', render: (v, r) => r.tx_type === 'split' || !v ? '—' : fmt.currency(v, 2) },
    { key: '_edit', label: '', render: (_, r) => (
      <button className="text-gray-600 hover:text-accent transition-colors" onClick={() => setEditTx(r)}><Pencil size={12} /></button>
    ) },
    { key: '_del', label: '', render: (_, r) => (
      <button className="text-gray-600 hover:text-danger transition-colors" onClick={() => setDeleteTx(r)}><Trash2 size={12} /></button>
    ) },
  ]

  return (
    <div className="space-y-4">
      <SectionHeader title="Transactions" action={
        <div className="flex gap-2">
          <button className="btn-ghost" onClick={() => setShowSplit(true)}><GitMerge size={14} /> Record Split</button>
          <button className="btn-primary" onClick={() => setShowAdd(true)}><Plus size={14} /> Add</button>
        </div>
      } />

      {/* 7 summary metrics */}
      {summary && (
        <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-3">
          <MetricCard label="Transactions" value={summary.count} />
          <MetricCard label="Total Qty" value={fmt.num(summary.total_quantity, 2)} />
          <MetricCard label="Total Buys" value={fmt.currency(summary.total_buys)} color="text-green-400" />
          <MetricCard label="# Buys" value={summary.num_buys} />
          <MetricCard label="Total Sales" value={fmt.currency(summary.total_sells)} color="text-red-400" />
          <MetricCard label="# Sells" value={summary.num_sells} />
          <MetricCard label="Total Fees" value={fmt.currency(summary.total_fees)} />
        </div>
      )}

      {/* Portfolio filter */}
      <div className="flex gap-2 items-center">
        <select className="select max-w-xs" value={portfolioFilter} onChange={e => setPortfolioFilter(e.target.value)}>
          <option value="">All portfolios</option>
          {portfolios.map(p => <option key={p.id} value={p.id}>{p.name}</option>)}
        </select>
      </div>

      {/* Security / type filters */}
      <Expander title="Filters">
        <div className="space-y-3">
          <div>
            <span className="text-xs text-gray-500 mr-1">Security:</span>
            <div className="flex flex-wrap gap-1.5 mt-1">
              {securityOptions.map(s => (
                <button key={s}
                  className={`badge cursor-pointer ${securityFilter.includes(s) ? 'badge-blue' : 'bg-surface-3 text-gray-400'}`}
                  onClick={() => toggleFilter(setSecurityFilter)(s)}>
                  {s}
                </button>
              ))}
            </div>
          </div>
          <div>
            <span className="text-xs text-gray-500 mr-1">Type:</span>
            <div className="flex flex-wrap gap-1.5 mt-1">
              {typeOptions.map(t => (
                <button key={t}
                  className={`badge cursor-pointer ${typeFilter.includes(t) ? 'badge-blue' : 'bg-surface-3 text-gray-400'}`}
                  onClick={() => toggleFilter(setTypeFilter)(t)}>
                  {t.toUpperCase()}
                </button>
              ))}
            </div>
          </div>
        </div>
      </Expander>

      {/* Portfolio management */}
      <Expander title="Manage Portfolios">
        <PortfolioManager portfolios={portfolios} />
      </Expander>

      {/* Trends */}
      {trends && (
        <Expander title="Transaction Trends & Fee Analysis">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <LazyPlot
              data={[
                { x: trends.dates, y: trends.qtyArr, name: 'Cumulative Qty', line: { color: '#6366f1' }, type: 'scatter' },
                { x: trends.dates, y: trends.feesArr, name: 'Cumulative Fees (€)', line: { color: '#f59e0b' }, yaxis: 'y2', type: 'scatter' },
              ]}
              layout={{ ...BASE, title: { text: 'Cumulative Quantity & Fees', font: { color: '#d1d5db', size: 13 } },
                yaxis2: { overlaying: 'y', side: 'right', gridcolor: 'transparent', tickprefix: '€' }, height: 280 }}
              config={plotlyConfig} style={{ width: '100%' }} useResizeHandler />
            <LazyPlot
              data={[
                { x: trends.dates, y: trends.buysArr, name: 'Cumulative Buys', line: { color: '#22c55e' }, type: 'scatter' },
                { x: trends.dates, y: trends.sellsArr, name: 'Cumulative Sells', line: { color: '#ef4444' }, type: 'scatter' },
              ]}
              layout={{ ...BASE, title: { text: 'Cumulative Buys vs Sells (shares)', font: { color: '#d1d5db', size: 13 } }, height: 280 }}
              config={plotlyConfig} style={{ width: '100%' }} useResizeHandler />
          </div>
        </Expander>
      )}

      {/* Table */}
      <div className="card overflow-hidden">
        {isLoading ? <LoadingOverlay /> : error ? <ErrorMsg error={error} /> : (
          <SortableTable
            columns={COLS}
            data={filteredTx}
            defaultSort={{ key: 'tx_date', asc: false }}
            searchable searchKeys={['security_label', 'symbol', 'portfolio', 'tx_type']}
            pageSize={20}
            exportable exportName="transactions"
          />
        )}
      </div>

      <Modal open={showAdd} onClose={() => setShowAdd(false)} title="Add Transaction">
        <TxForm portfolios={portfolios} securities={securities} onSubmit={(f) => addMut.mutate(f)} onClose={() => setShowAdd(false)} />
      </Modal>
      <Modal open={showSplit} onClose={() => setShowSplit(false)} title="Record Stock Split">
        <TxForm initial={{ tx_type: 'split', tx_date: new Date().toISOString().slice(0, 10), portfolio_id: portfolios[0]?.id, quantity: '2', symbol: '' }}
          portfolios={portfolios} securities={securities} onSubmit={(f) => addMut.mutate(f)} onClose={() => setShowSplit(false)} />
      </Modal>
      {editTx && (
        <Modal open={!!editTx} onClose={() => setEditTx(null)} title={`Edit Transaction #${editTx.id}`}>
          <TxForm initial={{ portfolio_id: editTx.portfolio_id, symbol: editTx.symbol || editTx.yahoo_ticker, tx_date: (editTx.tx_date || editTx.date)?.slice(0, 10), tx_type: editTx.tx_type, quantity: editTx.quantity, price: editTx.price, fees: editTx.tx_cost || 0 }}
            portfolios={portfolios} securities={securities}
            onSubmit={(f) => editMut.mutate({ id: editTx.id, form: f })} onClose={() => setEditTx(null)} />
        </Modal>
      )}
      <ConfirmModal open={!!deleteTx} onClose={() => setDeleteTx(null)}
        onConfirm={() => deleteMut.mutate(deleteTx.id)} danger
        title="Delete transaction"
        message={`Remove ${deleteTx?.tx_type?.toUpperCase()} of ${deleteTx?.quantity} ${deleteTx?.symbol}? This cannot be undone.`} />
    </div>
  )
}
