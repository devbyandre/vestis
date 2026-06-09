// TabWatchlist.jsx
import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Plus, Trash2 } from 'lucide-react'
import toast from 'react-hot-toast'
import { watchlistApi } from '../lib/api'
import { qk } from '../lib/queryClient'
import { fmt, pnlColor } from '../lib/utils'
import { LoadingOverlay, ErrorMsg, SectionHeader, ConfirmModal, Modal, Input } from '../components/ui'

export function TabWatchlist() {
  const qc = useQueryClient()
  const [showAdd, setShowAdd] = useState(false)
  const [deleteItem, setDeleteItem] = useState(null)
  const [form, setForm] = useState({ symbol: '', name: '', isin: '' })

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

  return (
    <div className="space-y-4">
      <SectionHeader title="Watchlist" action={<button className="btn-primary" onClick={() => setShowAdd(true)}><Plus size={14} /> Add</button>} />
      {isLoading ? <LoadingOverlay /> : error ? <ErrorMsg error={error} /> : (
        <div className="card overflow-hidden p-0">
          <table className="w-full text-left">
            <thead><tr className="border-b border-surface-3">
              {['Security', 'Price', 'RSI', 'P/E', 'Sector', 'Temperature', ''].map((h, i) => <th key={i} className="th">{h}</th>)}
            </tr></thead>
            <tbody>
              {watchlist.map((r, i) => (
                <tr key={i} className="table-row">
                  <td className="td font-medium">{r.security_label || r.symbol}</td>
                  <td className="td text-right font-mono">{r.current_price != null ? fmt.currency(r.current_price, 2) : '—'}</td>
                  <td className="td text-right">{r.rsi != null ? r.rsi.toFixed(1) : '—'}</td>
                  <td className="td text-right">{r.trailingPE != null ? Number(r.trailingPE).toFixed(1) : '—'}</td>
                  <td className="td text-gray-500">{r.sector || '—'}</td>
                  <td className="td">{r.temperature || '—'}</td>
                  <td className="td">
                    <button className="text-gray-600 hover:text-danger transition-colors" onClick={() => setDeleteItem(r)}>
                      <Trash2 size={12} />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {!watchlist.length && <div className="py-8 text-center text-gray-500 text-sm">Watchlist is empty</div>}
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
