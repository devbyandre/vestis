// TabWatchlist.jsx
import { useState, useMemo } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Plus, Trash2 } from 'lucide-react'
import { clsx } from 'clsx'
import toast from 'react-hot-toast'
import { watchlistApi } from '../lib/api'
import { qk } from '../lib/queryClient'
import { fmt, kpiColor, temperatureBadgeClass } from '../lib/utils'
import { LoadingOverlay, ErrorMsg, SectionHeader, ConfirmModal, Modal, Input, Expander, SortableTable } from '../components/ui'

function FilterGroup({ label, options, selected, onToggle }) {
  if (!options.length) return null
  return (
    <div>
      <span className="text-xs text-gray-500 mr-1">{label}:</span>
      <div className="flex flex-wrap gap-1.5 mt-1">
        {options.map(o => (
          <button key={o}
            className={clsx('badge cursor-pointer', selected.includes(o) ? 'badge-blue' : 'bg-surface-3 text-gray-400')}
            onClick={() => onToggle(o)}>
            {o}
          </button>
        ))}
      </div>
    </div>
  )
}

export function TabWatchlist() {
  const qc = useQueryClient()
  const [showAdd, setShowAdd] = useState(false)
  const [deleteItem, setDeleteItem] = useState(null)
  const [form, setForm] = useState({ symbol: '', name: '', isin: '' })

  const [countryFilter, setCountryFilter] = useState([])
  const [sectorFilter, setSectorFilter] = useState([])
  const [industryFilter, setIndustryFilter] = useState([])
  const [typeFilter, setTypeFilter] = useState([])
  const [useBeta, setUseBeta] = useState(false)
  const [betaThreshold, setBetaThreshold] = useState('1.0')

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

  const opts = (key) => [...new Set(watchlist.map(r => r[key] || 'Unknown'))].sort()
  const countries = useMemo(() => opts('country'), [watchlist])
  const sectors = useMemo(() => opts('sector'), [watchlist])
  const industries = useMemo(() => opts('industry'), [watchlist])
  const types = useMemo(() => opts('security_type'), [watchlist])

  const toggle = (setter) => (val) => setter(prev => prev.includes(val) ? prev.filter(x => x !== val) : [...prev, val])

  const filtered = useMemo(() => {
    let rows = watchlist
    if (countryFilter.length) rows = rows.filter(r => countryFilter.includes(r.country || 'Unknown'))
    if (sectorFilter.length) rows = rows.filter(r => sectorFilter.includes(r.sector || 'Unknown'))
    if (industryFilter.length) rows = rows.filter(r => industryFilter.includes(r.industry || 'Unknown'))
    if (typeFilter.length) rows = rows.filter(r => typeFilter.includes(r.security_type || 'Unknown'))
    if (useBeta) {
      const max = Number(betaThreshold)
      rows = rows.filter(r => (r.beta ?? 9999) <= max)
    }
    return rows
  }, [watchlist, countryFilter, sectorFilter, industryFilter, typeFilter, useBeta, betaThreshold])

  const WATCH_COLS = [
    { key: 'security_label', label: 'Security', render: (v, r) => v || r.security_name || r.name || r.symbol || r.yahoo_ticker },
    { key: 'regularMarketPrice', label: 'Price', align: 'right', render: (v, r) => fmt.currency(v ?? r.current_price, 2), exportValue: (v, r) => v ?? r.current_price },
    { key: 'fiftyTwoWeekLow', label: '52w Low', align: 'right', render: v => fmt.currency(v, 2) },
    { key: 'fiftyTwoWeekHigh', label: '52w High', align: 'right', render: v => fmt.currency(v, 2) },
    { key: 'beta', label: 'Beta', align: 'right', render: v => <span className={kpiColor.beta(v)}>{v != null ? Number(v).toFixed(2) : '—'}</span> },
    { key: 'trailingPE', label: 'P/E', align: 'right', render: v => <span className={kpiColor.pe(v)}>{v != null ? Number(v).toFixed(1) : '—'}</span> },
    { key: 'pb_ratio', label: 'P/B', align: 'right', render: v => <span className={kpiColor.pb(v)}>{v != null ? Number(v).toFixed(2) : '—'}</span> },
    { key: 'dividendYield', label: 'Div Yield', align: 'right', render: v => <span className={kpiColor.divYield(v)}>{v != null ? fmt.pct(v, 2) : '—'}</span> },
    { key: 'profitMargins', label: 'Profit Margin', align: 'right', render: v => <span className={kpiColor.profitMargin(v)}>{v != null ? fmt.pct(v, 1) : '—'}</span> },
    { key: 'sector', label: 'Sector', render: v => v || '—' },
    { key: 'Temperature', label: 'Temp', render: (v, r) => {
      const t = v || r.temperature
      return <span className={clsx('badge', temperatureBadgeClass(t))}>{t || '—'}</span>
    } },
    { key: '_del', label: '', render: (_, r) => (
      <button className="text-gray-600 hover:text-danger transition-colors" onClick={() => setDeleteItem(r)}><Trash2 size={12} /></button>
    ) },
  ]

  return (
    <div className="space-y-4">
      <SectionHeader title="Watchlist" action={<button className="btn-primary" onClick={() => setShowAdd(true)}><Plus size={14} /> Add</button>} />

      <Expander title="Watchlist Filters">
        <div className="space-y-3">
          <FilterGroup label="Country" options={countries} selected={countryFilter} onToggle={toggle(setCountryFilter)} />
          <FilterGroup label="Sector" options={sectors} selected={sectorFilter} onToggle={toggle(setSectorFilter)} />
          <FilterGroup label="Industry" options={industries} selected={industryFilter} onToggle={toggle(setIndustryFilter)} />
          <FilterGroup label="Security Type" options={types} selected={typeFilter} onToggle={toggle(setTypeFilter)} />
          <div className="flex items-center gap-3 pt-1">
            <label className="flex items-center gap-1.5 text-xs text-gray-400 cursor-pointer">
              <input type="checkbox" checked={useBeta} onChange={e => setUseBeta(e.target.checked)} />
              Enable Beta filter
            </label>
            <input className="input max-w-[100px] text-sm" type="number" step="0.1" disabled={!useBeta}
              value={betaThreshold} onChange={e => setBetaThreshold(e.target.value)} placeholder="Max Beta" />
          </div>
        </div>
      </Expander>

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
