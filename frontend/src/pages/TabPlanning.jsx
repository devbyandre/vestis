// TabPlanning.jsx
import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import Plot from 'react-plotly.js'
import { holdingsApi, planningApi, portfolioApi } from '../lib/api'
import { qk } from '../lib/queryClient'
import { fmt, pnlColor, plotlyConfig } from '../lib/utils'
import { LoadingOverlay, ErrorMsg, SortableTable, SectionHeader, Expander } from '../components/ui'

const KPI_COLS = [
  { key: 'security_label', label: 'Security' },
  { key: 'sector', label: 'Sector' },
  { key: 'security_type', label: 'Type' },
  { key: 'market_value', label: 'Mkt Value', align: 'right', render: v => fmt.currency(v) },
  { key: 'cost_basis', label: 'Cost', align: 'right', render: v => fmt.currency(v) },
  { key: 'rel_perf', label: 'Rel P&L', align: 'right', render: v => <span className={pnlColor(v)}>{fmt.pctDirect(v * 100)}</span> },
  { key: 'rsi', label: 'RSI', align: 'right', render: v => v != null ? v.toFixed(1) : '—' },
  { key: 'beta', label: 'Beta', align: 'right', render: v => v != null ? v.toFixed(2) : '—' },
  { key: 'trailingPE', label: 'P/E', align: 'right', render: v => v != null ? Number(v).toFixed(1) : '—' },
  { key: 'dividendYield', label: 'Div Yield', align: 'right', render: v => v != null ? `${(Number(v) * 100).toFixed(2)}%` : '—' },
]

export default function TabPlanning() {
  const [portfolioIds, setPortfolioIds] = useState([])
  const { data: portfolios = [] } = useQuery({ queryKey: qk.portfolios(), queryFn: portfolioApi.list })
  const ids = portfolioIds.length ? portfolioIds : null

  const { data: kpis = [], isLoading, error } = useQuery({
    queryKey: qk.kpis(ids),
    queryFn: () => planningApi.kpis(ids),
  })

  const { data: rebalancing = [] } = useQuery({
    queryKey: qk.rebalancing(ids),
    queryFn: () => planningApi.rebalancing(ids),
  })

  const typeAlloc = useMemo(() => {
    const m = {}
    kpis.forEach(r => { const k = r.security_type || 'Other'; m[k] = (m[k] || 0) + (r.market_value || 0) })
    return { labels: Object.keys(m), values: Object.values(m) }
  }, [kpis])

  const BASE = { paper_bgcolor: 'transparent', plot_bgcolor: 'transparent', font: { color: '#9ca3af', size: 11 }, margin: { l: 10, r: 10, t: 20, b: 10 } }

  return (
    <div className="space-y-4">
      <SectionHeader title="Investment Planning" />

      <div className="card flex flex-wrap gap-2 items-center">
        <button className={`badge cursor-pointer ${!portfolioIds.length ? 'badge-blue' : 'bg-surface-3 text-gray-400'}`} onClick={() => setPortfolioIds([])}>All</button>
        {portfolios.map(p => (
          <button key={p.id} className={`badge cursor-pointer ${portfolioIds.includes(p.id) ? 'badge-blue' : 'bg-surface-3 text-gray-400'}`}
            onClick={() => setPortfolioIds(ids => ids.includes(p.id) ? ids.filter(i => i !== p.id) : [...ids, p.id])}>
            {p.name}
          </button>
        ))}
      </div>

      {isLoading ? <LoadingOverlay /> : error ? <ErrorMsg error={error} /> : (
        <>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="card">
              <p className="text-xs text-gray-500 mb-2">Asset Allocation</p>
              <Plot data={[{ type: 'pie', labels: typeAlloc.labels, values: typeAlloc.values, hole: 0.5, textinfo: 'label+percent', textfont: { color: '#e5e7eb', size: 11 }, marker: { colors: ['#6366f1','#22c55e','#f59e0b','#ef4444','#14b8a6'] } }]}
                layout={{ ...BASE, height: 240, showlegend: false }} config={plotlyConfig} style={{ width: '100%' }} useResizeHandler />
            </div>
            {rebalancing.length > 0 && (
              <div className="card overflow-hidden p-0">
                <div className="px-4 py-3 border-b border-surface-3"><h3 className="text-sm font-semibold">Rebalancing Suggestions</h3></div>
                <SortableTable data={rebalancing} defaultSort={{ key: 'priority', asc: false }} columns={[
                  { key: 'security_label', label: 'Security' },
                  { key: 'action', label: 'Action' },
                  { key: 'current_weight', label: 'Current', align: 'right', render: v => fmt.pct(v) },
                  { key: 'target_weight', label: 'Target', align: 'right', render: v => fmt.pct(v) },
                  { key: 'diff', label: 'Diff', align: 'right', render: v => <span className={pnlColor(v)}>{fmt.pct(v)}</span> },
                ]} />
              </div>
            )}
          </div>

          <Expander title="KPI Table" defaultOpen>
            <SortableTable columns={KPI_COLS} data={kpis} defaultSort={{ key: 'market_value', asc: false }} />
          </Expander>
        </>
      )}
    </div>
  )
}
