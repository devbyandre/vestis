// TabWatchlist.jsx
import { useState, useMemo } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Plus, Trash2 } from 'lucide-react'
import toast from 'react-hot-toast'
import { watchlistApi } from '../lib/api'
import { qk } from '../lib/queryClient'
import { fmt, pnlColor } from '../lib/utils'
import { LoadingOverlay, ErrorMsg, SectionHeader, ConfirmModal, Modal, Input, SortableTable } from '../components/ui'

export function TabWatchlist() {
  const qc = useQueryClient()
  const [showAdd, setShowAdd] = useState(false)
  const [deleteItem, setDeleteItem] = useState(null)
  const [form, setForm] = useState({ symbol: '', name: '', isin: '' })
  const [sectorFilter, setSectorFilter] = useState('')

  const { data: watchlist = [], isLoading, error } = useQuery({ queryKey: qk.watchlist(), queryFn: watchlistApi.list })

  const addMut = useMutation({
    mutationFn: () => watchlistApi.add(form.symbol, form.name, form.isin),
    onSuccess: () => { qc.invalidateQueries({ queryKey: qk.watchlist() }); setShowAdd(false); setForm({ symbol: '', name: '', isin: '' }); toast.success('Added to watchlist') },
    onError: e => toast.error(e.message),
  })

  const delMut = useMutation({
    mutationFn: (symbol) => watchlistApi.remove(symbol),
    onSuccess: () => { qc.invalidateQueries({ queryKey: qk.watchlist() }); toast.success('Removed') },
    onError: e => toast.error(e.message),
  })

  const sectors = useMemo(() =>
    [...new Set(watchlist.map(r => r.sector).filter(Boolean))].sort(), [watchlist])

  const filtered = useMemo(() =>
    sectorFilter ? watchlist.filter(r => r.sector === sectorFilter) : watchlist,
    [watchlist, sectorFilter])

  const WATCH_COLS = [
    { key: 'security_label', label: 'Security', render: (v, r) => v || r.name || r.symbol || r.yahoo_ticker },
    { key: 'regularMarketPrice', label: 'Price', align: 'right', render: (v, r) => { const p = v ?? r.current_price; return p != null ? fmt.currency(p, 2) : '—' }, exportValue: (v, r) => v ?? r.current_price },
    { key: 'rsi', label: 'RSI', align: 'right', render: v => v != null ? Number(v).toFixed(1) : '—' },
    { key: 'trailingPE', label: 'P/E', align: 'right', render: v => v != null ? Number(v).toFixed(1) : '—' },
    { key: 'sector', label: 'Sector', render: v => v || '—' },
    { key: 'Temperature', label: 'Temp', render: (v, r) => v || r.temperature || '—' },
    { key: '_del', label: '', render: (_, r) => (
      <button className="text-gray-600 hover:text-danger transition-colors" onClick={() => setDeleteItem(r)}><Trash2 size={12} /></button>
    ) },
  ]

  return (
    <div className="space-y-4">
      <SectionHeader title="Watchlist" action={<button className="btn-primary" onClick={() => setShowAdd(true)}><Plus size={14} /> Add</button>} />
      <div className="card flex flex-wrap gap-2 items-center">
        <span className="text-xs text-gray-500 mr-1">Sector:</span>
        <button className={`badge cursor-pointer ${!sectorFilter ? 'badge-blue' : 'bg-surface-3 text-gray-400'}`} onClick={() => setSectorFilter('')}>All</button>
        {sectors.map(s => (
          <button key={s} className={`badge cursor-pointer ${sectorFilter === s ? 'badge-blue' : 'bg-surface-3 text-gray-400'}`}
            onClick={() => setSectorFilter(s)}>{s}</button>
        ))}
      </div>

      {isLoading ? <LoadingOverlay /> : error ? <ErrorMsg error={error} /> : (
        <div className="card overflow-hidden">
          <SortableTable
            columns={WATCH_COLS}
            data={filtered}
            defaultSort={{ key: 'security_label', asc: true }}
            searchable searchKeys={['security_label', 'symbol', 'sector']}
            exportable exportName="watchlist"
          />
        </div>
      )}
      <Modal open={showAdd} onClose={() => setShowAdd(false)} title="Add to Watchlist">
        <div className="space-y-3">
          <Input label="Yahoo Symbol *" value={form.symbol} onChange={v => setForm(f => ({ ...f, symbol: v }))} placeholder="e.g. AAPL" />
          <Input label="Name (optional)" value={form.name} onChange={v => setForm(f => ({ ...f, name: v }))} />
          <Input label="ISIN (optional)" value={form.isin} onChange={v => setForm(f => ({ ...f, isin: v }))} />
          <div className="flex gap-2 justify-end">
            <button className="btn-ghost" onClick={() => setShowAdd(false)}>Cancel</button>
            <button className="btn-primary" onClick={() => addMut.mutate()} disabled={!form.symbol}>Add</button>
          </div>
        </div>
      </Modal>
      <ConfirmModal open={!!deleteItem} onClose={() => setDeleteItem(null)} onConfirm={() => delMut.mutate(deleteItem?.symbol)} danger
        title="Remove from watchlist" message={`Remove ${deleteItem?.symbol} from your watchlist?`} />
    </div>
  )
}
export default TabWatchlist
