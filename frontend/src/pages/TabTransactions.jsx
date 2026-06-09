import { useState, useMemo } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Plus, Pencil, Trash2, GitMerge } from 'lucide-react'
import toast from 'react-hot-toast'
import { transactionsApi, portfolioApi, securitiesApi } from '../lib/api'
import { qk } from '../lib/queryClient'
import { fmt } from '../lib/utils'
import { LoadingOverlay, ErrorMsg, Modal, ConfirmModal, Pagination, SectionHeader, Input, Select } from '../components/ui'

const PAGE_SIZE = 20

function TxForm({ initial, portfolios, securities, onSubmit, onClose }) {
  const [form, setForm] = useState(initial || {
    portfolio_id: portfolios[0]?.id || '',
    symbol: '',
    tx_date: new Date().toISOString().slice(0, 10),
    tx_type: 'buy',
    quantity: '',
    price: '',
    fees: '0',
  })
  const isSplit = form.tx_type === 'split'

  const set = (k) => (v) => setForm(f => ({ ...f, [k]: v }))

  const handleSubmit = (e) => {
    e.preventDefault()
    onSubmit(form)
  }

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
  const [page, setPage] = useState(1)
  const [search, setSearch] = useState('')
  const [portfolioFilter, setPortfolioFilter] = useState('')
  const [editTx, setEditTx] = useState(null)
  const [deleteTx, setDeleteTx] = useState(null)
  const [showAdd, setShowAdd] = useState(false)
  const [showSplit, setShowSplit] = useState(false)

  const { data: portfolios = [] } = useQuery({ queryKey: qk.portfolios(), queryFn: portfolioApi.list })
  const { data: securities = [] } = useQuery({ queryKey: qk.securities(), queryFn: securitiesApi.list })
  const { data: transactions = [], isLoading, error } = useQuery({
    queryKey: qk.transactions(portfolioFilter || null),
    queryFn: () => transactionsApi.list(portfolioFilter ? [Number(portfolioFilter)] : null),
  })

  const filtered = useMemo(() => {
    let rows = transactions
    if (search) {
      const q = search.toLowerCase()
      rows = rows.filter(r => (r.symbol || r.yahoo_ticker || '').toLowerCase().includes(q) ||
        (r.security_name || '').toLowerCase().includes(q))
    }
    return rows.sort((a, b) => b.date > a.date ? 1 : -1)
  }, [transactions, search])

  const paged = filtered.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE)

  const addMut = useMutation({
    mutationFn: (form) => {
      if (form.tx_type === 'split') {
        return transactionsApi.addSplit({ symbol: form.symbol, split_date: form.tx_date, ratio: Number(form.quantity), portfolio_id: Number(form.portfolio_id) })
      }
      return transactionsApi.add({ ...form, portfolio_id: Number(form.portfolio_id), quantity: Number(form.quantity), price: Number(form.price), fees: Number(form.fees) })
    },
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['transactions'] }); qc.invalidateQueries({ queryKey: ['holdings'] }); setShowAdd(false); toast.success('Transaction added') },
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

  const typeLabel = (t, qty) => {
    if (t === 'buy') return <span className="badge badge-green">↑ BUY</span>
    if (t === 'sell') return <span className="badge badge-red">↓ SELL</span>
    if (t === 'split') return <span className="badge badge-yellow">🔀 SPLIT ×{Number(qty).toFixed(2)}</span>
    return <span className="badge">{t}</span>
  }

  // Summary metrics
  const totalBuys = transactions.filter(r => r.tx_type === 'buy').reduce((s, r) => s + (r.total_cost || 0), 0)
  const totalSells = transactions.filter(r => r.tx_type === 'sell').reduce((s, r) => s + (r.total_cost || 0), 0)
  const totalFees = transactions.reduce((s, r) => s + (r.tx_cost || 0), 0)

  return (
    <div className="space-y-4">
      <SectionHeader
        title="Transactions"
        action={
          <div className="flex gap-2">
            <button className="btn-ghost" onClick={() => setShowSplit(true)}>
              <GitMerge size={14} /> Record Split
            </button>
            <button className="btn-primary" onClick={() => setShowAdd(true)}>
              <Plus size={14} /> Add
            </button>
          </div>
        }
      />

      {/* Summary metrics */}
      <div className="grid grid-cols-3 gap-3">
        <div className="metric-card"><div className="metric-label">Total Buys</div><div className="metric-value text-green-400">{fmt.currency(totalBuys)}</div></div>
        <div className="metric-card"><div className="metric-label">Total Sells</div><div className="metric-value text-red-400">{fmt.currency(totalSells)}</div></div>
        <div className="metric-card"><div className="metric-label">Total Fees</div><div className="metric-value">{fmt.currency(totalFees)}</div></div>
      </div>

      {/* Filters */}
      <div className="flex gap-2 items-center">
        <input className="input max-w-xs" placeholder="Search symbol or name…" value={search}
          onChange={e => { setSearch(e.target.value); setPage(1) }} />
        <select className="select max-w-xs" value={portfolioFilter} onChange={e => setPortfolioFilter(e.target.value)}>
          <option value="">All portfolios</option>
          {portfolios.map(p => <option key={p.id} value={p.id}>{p.name}</option>)}
        </select>
      </div>

      {/* Table */}
      <div className="card overflow-hidden p-0">
        {isLoading ? <div className="p-4"><LoadingOverlay /></div> : error ? <div className="p-4"><ErrorMsg error={error} /></div> : (
          <>
            <table className="w-full text-left">
              <thead>
                <tr className="border-b border-surface-3">
                  {['Date','Security','Portfolio','Type','Qty','Price','Fees','Total','',''].map((h, i) => (
                    <th key={i} className="th">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {paged.map((row, i) => {
                  const isSplit = row.tx_type === 'split'
                  return (
                    <tr key={row.id ?? i} className="table-row">
                      <td className="td text-gray-400">{fmt.date(row.date)}</td>
                      <td className="td font-medium">{row.security_label || row.symbol || row.yahoo_ticker}</td>
                      <td className="td text-gray-500">{row.portfolio_name || row.portfolio}</td>
                      <td className="td">{typeLabel(row.tx_type, row.quantity)}</td>
                      <td className="td text-right font-mono">{isSplit ? `×${Number(row.quantity).toFixed(4)}` : fmt.num(row.quantity, 4)}</td>
                      <td className="td text-right font-mono">{isSplit || !row.price ? '—' : fmt.currency(row.price, 2)}</td>
                      <td className="td text-right font-mono">{isSplit || !row.tx_cost ? '—' : fmt.currency(row.tx_cost, 2)}</td>
                      <td className="td text-right font-mono">{isSplit || !row.total_cost ? '—' : fmt.currency(row.total_cost, 2)}</td>
                      <td className="td">
                        <button className="text-gray-600 hover:text-accent transition-colors" onClick={() => setEditTx(row)}>
                          <Pencil size={12} />
                        </button>
                      </td>
                      <td className="td">
                        <button className="text-gray-600 hover:text-danger transition-colors" onClick={() => setDeleteTx(row)}>
                          <Trash2 size={12} />
                        </button>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
            <div className="px-4 py-2 border-t border-surface-3">
              <Pagination page={page} total={filtered.length} pageSize={PAGE_SIZE} onChange={setPage} />
            </div>
          </>
        )}
      </div>

      {/* Add modal */}
      <Modal open={showAdd} onClose={() => setShowAdd(false)} title="Add Transaction">
        <TxForm portfolios={portfolios} securities={securities}
          onSubmit={(form) => addMut.mutate(form)} onClose={() => setShowAdd(false)} />
      </Modal>

      {/* Split modal */}
      <Modal open={showSplit} onClose={() => setShowSplit(false)} title="Record Stock Split">
        <TxForm initial={{ tx_type: 'split', tx_date: new Date().toISOString().slice(0, 10), portfolio_id: portfolios[0]?.id, quantity: '2', symbol: '' }}
          portfolios={portfolios} securities={securities}
          onSubmit={(form) => addMut.mutate(form)} onClose={() => setShowSplit(false)} />
      </Modal>

      {/* Edit modal */}
      {editTx && (
        <Modal open={!!editTx} onClose={() => setEditTx(null)} title={`Edit Transaction #${editTx.id}`}>
          <TxForm initial={{ portfolio_id: editTx.portfolio_id, symbol: editTx.symbol || editTx.yahoo_ticker, tx_date: editTx.date?.slice(0, 10), tx_type: editTx.tx_type, quantity: editTx.quantity, price: editTx.price, fees: editTx.tx_cost || 0 }}
            portfolios={portfolios} securities={securities}
            onSubmit={(form) => editMut.mutate({ id: editTx.id, form })}
            onClose={() => setEditTx(null)} />
        </Modal>
      )}

      {/* Delete confirm */}
      <ConfirmModal open={!!deleteTx} onClose={() => setDeleteTx(null)}
        onConfirm={() => deleteMut.mutate(deleteTx.id)} danger
        title="Delete transaction"
        message={`Remove ${deleteTx?.tx_type?.toUpperCase()} of ${deleteTx?.quantity} ${deleteTx?.symbol}? This cannot be undone.`} />
    </div>
  )
}
