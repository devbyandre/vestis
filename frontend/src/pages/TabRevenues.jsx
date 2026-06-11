import { useState, useMemo, lazy, Suspense } from 'react'
import { useQuery } from '@tanstack/react-query'
import { analyticsApi, portfolioApi } from '../lib/api'
import { qk } from '../lib/queryClient'
import { fmt, pnlColor, plotlyConfig } from '../lib/utils'
import { LoadingOverlay, ErrorMsg, SortableTable, SectionHeader, MetricCard, Expander } from '../components/ui'

const Plot = lazy(() => import('react-plotly.js'))
const BASE = {
  paper_bgcolor: 'transparent', plot_bgcolor: 'transparent',
  font: { color: '#9ca3af', size: 11 }, margin: { l: 60, r: 20, t: 40, b: 60 },
  xaxis: { gridcolor: '#1a1a24', linecolor: '#2a2a3a' },
  yaxis: { gridcolor: '#1a1a24', linecolor: '#2a2a3a' },
  hoverlabel: { bgcolor: '#1a1a24', bordercolor: '#2a2a3a', font: { color: '#e5e7eb', size: 11 } },
  barmode: 'group', legend: { font: { size: 10 } },
}
function LazyPlot(props) {
  return <Suspense fallback={<div className="text-gray-500 text-xs py-4">Loading chart…</div>}><Plot {...props} /></Suspense>
}

const currentYear = new Date().getFullYear()
const YEARS = [null, ...Array.from({ length: 7 }, (_, i) => currentYear - i)]

export default function TabRevenues() {
  const [portfolioIds, setPortfolioIds] = useState([])
  const [year, setYear] = useState(null)
  const [symbolFilter, setSymbolFilter] = useState('')

  const { data: portfolios = [] } = useQuery({ queryKey: qk.portfolios(), queryFn: portfolioApi.list })
  const ids = portfolioIds.length ? portfolioIds : null

  const { data: summary, isLoading, error } = useQuery({
    queryKey: qk.revenuesSummary(ids, year),
    queryFn: () => analyticsApi.revenuesSummary(ids, year),
  })

  const cg = summary?.capital_gains || []
  const divs = summary?.dividends || []

  // Symbol filter applied to tables
  const filteredCg = useMemo(() =>
    symbolFilter ? cg.filter(r => r.symbol === symbolFilter) : cg, [cg, symbolFilter])
  const filteredDivs = useMemo(() =>
    symbolFilter ? divs.filter(r => r.symbol === symbolFilter) : divs, [divs, symbolFilter])

  const symbols = useMemo(() =>
    [...new Set([...cg.map(r => r.symbol), ...divs.map(r => r.symbol)].filter(Boolean))].sort(),
    [cg, divs])

  const CG_COLS = [
    { key: 'symbol', label: 'Security' },
    { key: 'sell_date', label: 'Sell Date', render: v => fmt.date(v) },
    { key: 'quantity', label: 'Qty', align: 'right', render: v => fmt.num(v, 4) },
    { key: 'proceeds', label: 'Proceeds', align: 'right', render: v => fmt.currency(v) },
    { key: 'cost_basis', label: 'Cost Basis', align: 'right', render: v => fmt.currency(v) },
    { key: 'profit', label: 'Profit/Loss', align: 'right', render: v => <span className={pnlColor(v)}>{fmt.currency(v)}</span>, exportValue: v => v },
    { key: 'year', label: 'Year', align: 'right' },
  ]
  const DIV_COLS = [
    { key: 'symbol', label: 'Security' },
    { key: 'date', label: 'Date', render: v => fmt.date(v) },
    { key: 'dividend_per_share', label: 'Per Share', align: 'right', render: v => fmt.currency(v, 4) },
    { key: 'shares', label: 'Shares', align: 'right', render: v => fmt.num(v, 4) },
    { key: 'total', label: 'Total', align: 'right', render: v => fmt.currency(v), exportValue: v => v },
    { key: 'year', label: 'Year', align: 'right' },
  ]
  const TAXLOSS_COLS = [
    { key: 'symbol', label: 'Security' },
    { key: 'loss', label: 'Realised Loss', align: 'right', render: v => <span className="text-red-400">{fmt.currency(v)}</span>, exportValue: v => v },
  ]

  if (isLoading) return <LoadingOverlay label="Loading revenues…" />
  if (error) return <ErrorMsg error={error} />

  const byYear = summary?.by_year || []
  const bySec = (summary?.by_security || []).slice(0, 20)
  const taxLoss = summary?.tax_loss_candidates || []

  return (
    <div className="space-y-5">
      <SectionHeader title="Revenues & Taxes" />

      {/* Filters */}
      <div className="card flex flex-wrap gap-4 items-end">
        <div>
          <label className="label">Year</label>
          <select className="select" value={year ?? ''} onChange={e => setYear(e.target.value ? Number(e.target.value) : null)}>
            {YEARS.map(y => <option key={y ?? 'all'} value={y ?? ''}>{y ?? 'All years'}</option>)}
          </select>
        </div>
        <div>
          <label className="label">Security</label>
          <select className="select" value={symbolFilter} onChange={e => setSymbolFilter(e.target.value)}>
            <option value="">All securities</option>
            {symbols.map(s => <option key={s} value={s}>{s}</option>)}
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
        <MetricCard label="Capital Gains" value={fmt.currency(summary?.total_gains || 0)} color={pnlColor(summary?.total_gains || 0)} />
        <MetricCard label="Total Dividends" value={fmt.currency(summary?.total_dividends || 0)} color="text-success" />
        <MetricCard label={`Est. Tax (${((summary?.tax_rate || 0.26375) * 100).toFixed(2)}%)`} value={fmt.currency(summary?.estimated_tax || 0)} color="text-warning" />
        <MetricCard label="Net After Tax" value={fmt.currency((summary?.total_gains || 0) + (summary?.total_dividends || 0) - (summary?.estimated_tax || 0))} />
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="card">
          <p className="text-xs text-gray-500 mb-2">By Year</p>
          <LazyPlot
            data={[
              { x: byYear.map(r => r.year), y: byYear.map(r => r.gains), name: 'Capital Gains', type: 'bar', marker: { color: '#636EFA' } },
              { x: byYear.map(r => r.year), y: byYear.map(r => r.dividends), name: 'Dividends', type: 'bar', marker: { color: '#00CC96' } },
            ]}
            layout={{ ...BASE, height: 300, yaxis: { ...BASE.yaxis, tickprefix: '€' }, xaxis: { ...BASE.xaxis, type: 'category' } }}
            config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
          />
        </div>
        <div className="card">
          <p className="text-xs text-gray-500 mb-2">By Portfolio</p>
          <LazyPlot
            data={[{
              type: 'bar',
              x: portfolios.map(p => p.name),
              y: portfolios.map(p => {
                const pgains = cg.filter(r => r.portfolio_id === p.id).reduce((s, r) => s + (r.profit || 0), 0)
                const pdivs = divs.filter(r => r.portfolio_id === p.id).reduce((s, r) => s + (r.total || 0), 0)
                return pgains + pdivs
              }),
              marker: { color: '#a78bfa' },
            }]}
            layout={{ ...BASE, height: 300, yaxis: { ...BASE.yaxis, tickprefix: '€' } }}
            config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
          />
        </div>
      </div>

      <div className="card">
        <p className="text-xs text-gray-500 mb-2">By Security (top 20, gains + dividends)</p>
        <LazyPlot
          data={[
            { y: bySec.map(r => r.symbol), x: bySec.map(r => r.gains), name: 'Capital Gains', type: 'bar', orientation: 'h', marker: { color: '#636EFA' } },
            { y: bySec.map(r => r.symbol), x: bySec.map(r => r.dividends), name: 'Dividends', type: 'bar', orientation: 'h', marker: { color: '#00CC96' } },
          ]}
          layout={{ ...BASE, barmode: 'stack', height: Math.max(300, bySec.length * 28), margin: { l: 90, r: 20, t: 20, b: 40 }, xaxis: { ...BASE.xaxis, tickprefix: '€' } }}
          config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
        />
      </div>

      {/* Tax-loss harvesting */}
      {taxLoss.length > 0 && (
        <div className="card overflow-hidden">
          <SectionHeader title="Tax-Loss Harvesting Opportunities" subtitle="Securities with realised losses this period" />
          <SortableTable columns={TAXLOSS_COLS} data={taxLoss} defaultSort={{ key: 'loss', asc: true }}
            exportable exportName="tax_loss_candidates" />
        </div>
      )}

      {/* Capital gains table */}
      <Expander title={`Capital Gains (FIFO) — ${filteredCg.length} lots`}>
        <SortableTable columns={CG_COLS} data={filteredCg} defaultSort={{ key: 'sell_date', asc: false }}
          searchable searchKeys={['symbol']} pageSize={25} exportable exportName="capital_gains" />
      </Expander>

      {/* Dividends table */}
      <Expander title={`Dividends — ${filteredDivs.length} payments`}>
        <SortableTable columns={DIV_COLS} data={filteredDivs} defaultSort={{ key: 'date', asc: false }}
          searchable searchKeys={['symbol']} pageSize={25} exportable exportName="dividends" />
      </Expander>
    </div>
  )
}
