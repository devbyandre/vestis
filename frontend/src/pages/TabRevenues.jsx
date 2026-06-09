import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { analyticsApi, portfolioApi } from '../lib/api'
import { qk } from '../lib/queryClient'
import { fmt, pnlColor } from '../lib/utils'
import { LoadingOverlay, ErrorMsg, SortableTable, SectionHeader, MetricCard } from '../components/ui'

const currentYear = new Date().getFullYear()
const YEARS = [null, ...Array.from({ length: 7 }, (_, i) => currentYear - i)]

const CG_COLS = [
  { key: 'symbol', label: 'Security' },
  { key: 'buy_date', label: 'Buy Date', render: v => fmt.date(v) },
  { key: 'sell_date', label: 'Sell Date', render: v => fmt.date(v) },
  { key: 'quantity', label: 'Qty', align: 'right', render: v => fmt.num(v, 4) },
  { key: 'buy_price', label: 'Buy Price', align: 'right', render: v => fmt.currency(v, 2) },
  { key: 'sell_price', label: 'Sell Price', align: 'right', render: v => fmt.currency(v, 2) },
  { key: 'gain', label: 'Gain/Loss', align: 'right', render: v => <span className={pnlColor(v)}>{fmt.currency(v)}</span> },
]

const DIV_COLS = [
  { key: 'symbol', label: 'Security' },
  { key: 'date', label: 'Date', render: v => fmt.date(v) },
  { key: 'dividend_per_share', label: 'Per Share', align: 'right', render: v => fmt.currency(v, 4) },
  { key: 'shares', label: 'Shares', align: 'right', render: v => fmt.num(v, 4) },
  { key: 'total', label: 'Total', align: 'right', render: v => fmt.currency(v) },
  { key: 'year', label: 'Year', align: 'right' },
]

export default function TabRevenues() {
  const [portfolioIds, setPortfolioIds] = useState([])
  const [year, setYear] = useState(currentYear)

  const { data: portfolios = [] } = useQuery({ queryKey: qk.portfolios(), queryFn: portfolioApi.list })

  const ids = portfolioIds.length ? portfolioIds : null

  const { data: cg = [], isLoading: cgLoading, error: cgError } = useQuery({
    queryKey: qk.capitalGains(ids, year),
    queryFn: () => analyticsApi.capitalGains(ids, year),
  })

  const { data: divs = [], isLoading: divLoading, error: divError } = useQuery({
    queryKey: qk.dividends(ids, year),
    queryFn: () => analyticsApi.dividends(ids, year),
  })

  const totalGain = cg.reduce((s, r) => s + (r.gain || 0), 0)
  const taxRate = 0.25 // from config; could be fetched
  const estimatedTax = Math.max(0, totalGain * taxRate)
  const totalDivs = divs.reduce((s, r) => s + (r.total || 0), 0)
  const divTax = totalDivs * taxRate

  return (
    <div className="space-y-5">
      <SectionHeader title="Revenues & Taxes" />

      {/* Filters */}
      <div className="card flex flex-wrap gap-4 items-center">
        <div>
          <label className="label">Year</label>
          <select className="select" value={year ?? ''} onChange={e => setYear(e.target.value ? Number(e.target.value) : null)}>
            {YEARS.map(y => <option key={y ?? 'all'} value={y ?? ''}>{y ?? 'All years'}</option>)}
          </select>
        </div>
        <div>
          <label className="label">Portfolio</label>
          <div className="flex flex-wrap gap-1">
            <button className={`badge cursor-pointer ${!portfolioIds.length ? 'badge-blue' : 'bg-surface-3 text-gray-400'}`} onClick={() => setPortfolioIds([])}>All</button>
            {portfolios.map(p => (
              <button key={p.id} className={`badge cursor-pointer ${portfolioIds.includes(p.id) ? 'badge-blue' : 'bg-surface-3 text-gray-400'}`}
                onClick={() => setPortfolioIds(ids => ids.includes(p.id) ? ids.filter(i => i !== p.id) : [...ids, p.id])}>
                {p.name}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Summary */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <MetricCard label="Total Gains/Losses" value={fmt.currency(totalGain)} color={pnlColor(totalGain)} />
        <MetricCard label="Est. Tax (25%)" value={fmt.currency(estimatedTax)} color="text-warning" />
        <MetricCard label="Total Dividends" value={fmt.currency(totalDivs)} color="text-success" />
        <MetricCard label="Div. Tax (25%)" value={fmt.currency(divTax)} color="text-warning" />
      </div>

      {/* Capital gains */}
      <div className="card overflow-hidden p-0">
        <div className="px-4 py-3 border-b border-surface-3">
          <h3 className="text-sm font-semibold">Capital Gains (FIFO)</h3>
        </div>
        {cgLoading ? <div className="p-4"><LoadingOverlay /></div> :
         cgError ? <div className="p-4"><ErrorMsg error={cgError} /></div> :
         <SortableTable columns={CG_COLS} data={cg} defaultSort={{ key: 'sell_date', asc: false }} />}
      </div>

      {/* Dividends */}
      <div className="card overflow-hidden p-0">
        <div className="px-4 py-3 border-b border-surface-3">
          <h3 className="text-sm font-semibold">Dividends</h3>
        </div>
        {divLoading ? <div className="p-4"><LoadingOverlay /></div> :
         divError ? <div className="p-4"><ErrorMsg error={divError} /></div> :
         <SortableTable columns={DIV_COLS} data={divs} defaultSort={{ key: 'date', asc: false }} />}
      </div>
    </div>
  )
}
