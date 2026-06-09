import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import Plot from 'react-plotly.js'
import { holdingsApi, portfolioApi } from '../lib/api'
import { qk } from '../lib/queryClient'
import { fmt, pnlColor, plotlyLayout, plotlyConfig } from '../lib/utils'
import {
  LoadingOverlay, ErrorMsg, MetricCard, SortableTable,
  PnlBadge, Expander, SectionHeader,
} from '../components/ui'

const PLOTLY_BASE = {
  paper_bgcolor: 'transparent', plot_bgcolor: 'transparent',
  font: { color: '#9ca3af', family: 'Inter var, Inter, sans-serif', size: 11 },
  margin: { l: 55, r: 20, t: 36, b: 40 },
  hovermode: 'x unified',
  hoverlabel: { bgcolor: '#1a1a24', bordercolor: '#2a2a3a', font: { color: '#e5e7eb', size: 11 } },
  xaxis: { gridcolor: '#1a1a24', linecolor: '#2a2a3a', showspikes: true, spikecolor: '#444', spikemode: 'across' },
  yaxis: { gridcolor: '#1a1a24', linecolor: '#2a2a3a' },
}

function PerfCharts({ ts }) {
  const daily = useMemo(() => {
    if (!ts?.length) return []
    // Group by date, summing market_value and cost_basis
    const byDate = {}
    ts.forEach(r => {
      if (!byDate[r.date]) byDate[r.date] = { date: r.date, market_value: 0, cost_basis: 0 }
      byDate[r.date].market_value += (r.market_value || 0)
      byDate[r.date].cost_basis += (r.cost_basis || 0)
    })
    const rows = Object.values(byDate).sort((a, b) => a.date < b.date ? -1 : 1)
    rows.forEach(r => {
      r.net_value = r.market_value - r.cost_basis
      r.rel_perf = r.cost_basis > 0 ? (r.market_value - r.cost_basis) / r.cost_basis : null
    })
    const firstMv = rows.find(r => r.market_value > 0)?.market_value || 1
    rows.forEach(r => { r.indexed = r.market_value / firstMv * 100 })
    return rows
  }, [ts])

  if (!daily.length) return <div className="text-gray-600 text-sm py-4">No timeseries data</div>

  const dates = daily.map(r => r.date)
  const mv = daily.map(r => r.market_value)
  const cb = daily.map(r => r.cost_basis)
  const pnl = daily.map(r => r.net_value)
  const rel = daily.map(r => r.rel_perf != null ? r.rel_perf * 100 : null)
  const idx = daily.map(r => r.indexed)

  // Annual returns
  const yearMap = {}
  daily.forEach(r => {
    const y = r.date?.slice(0, 4)
    if (y) yearMap[y] = r.market_value
  })
  const years = Object.keys(yearMap).sort()
  const annualReturns = years.slice(1).map((y, i) => {
    const prev = yearMap[years[i]]
    const curr = yearMap[y]
    return { year: y, ret: prev > 0 ? (curr - prev) / prev * 100 : null }
  }).filter(r => r.ret != null)

  // Rolling 12m
  const roll12 = daily.map((r, i) => {
    const past = daily[Math.max(0, i - 252)]
    return past?.market_value > 0 ? (r.market_value - past.market_value) / past.market_value * 100 : null
  })

  // Drawdown
  let peak = -Infinity
  const dd = daily.map(r => {
    if (r.market_value > peak) peak = r.market_value
    return peak > 0 ? (r.market_value - peak) / peak * 100 : 0
  })
  const window = Math.max(1, Math.floor(daily.length / 4))
  const drawup = daily.map((r, i) => {
    const slice = daily.slice(Math.max(0, i - window), i + 1)
    const trough = Math.min(...slice.map(s => s.market_value))
    return trough > 0 ? (r.market_value - trough) / trough * 100 : 0
  })

  const layout = (title, extra = {}) => ({
    ...PLOTLY_BASE, title: { text: title, font: { size: 13, color: '#d1d5db' } }, ...extra,
  })

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Market value vs cost basis */}
        <Plot
          data={[
            { x: dates, y: mv, name: 'Market Value', fill: 'tozeroy', fillcolor: 'rgba(76,114,176,0.25)', line: { color: '#4C72B0', width: 1.5 }, type: 'scatter' },
            { x: dates, y: cb, name: 'Cost Basis', fill: 'tozeroy', fillcolor: 'rgba(221,132,82,0.3)', line: { color: '#DD8452', width: 1.5 }, type: 'scatter' },
          ]}
          layout={layout('Portfolio Value vs Cost Basis', { yaxis: { ...PLOTLY_BASE.yaxis, tickprefix: '€', tickformat: ',.0f' } })}
          config={plotlyConfig} style={{ width: '100%', height: 280 }} useResizeHandler
        />
        {/* Net P&L */}
        <Plot
          data={[{ x: dates, y: pnl, name: 'Net P&L', fill: 'tozeroy', fillcolor: 'rgba(44,160,44,0.2)', line: { color: '#2ca02c', width: 1.5 }, type: 'scatter' }]}
          layout={layout('Net P&L Over Time', {
            yaxis: { ...PLOTLY_BASE.yaxis, tickprefix: '€', tickformat: ',.0f' },
            shapes: [{ type: 'line', x0: dates[0], x1: dates[dates.length - 1], y0: 0, y1: 0, line: { color: '#444', dash: 'dot', width: 1 } }],
          })}
          config={plotlyConfig} style={{ width: '100%', height: 280 }} useResizeHandler
        />
      </div>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Relative performance */}
        <Plot
          data={[
            { x: dates, y: rel, name: 'Rel. Performance', line: { color: '#a78bfa', width: 1.5 }, type: 'scatter' },
            // Linear trend
            (() => {
              const valid = rel.filter(v => v != null)
              if (valid.length < 2) return {}
              const n = valid.length, xi = [...valid.keys()]
              const mx = xi.reduce((a, b) => a + b, 0) / n
              const my = valid.reduce((a, b) => a + b, 0) / n
              const slope = xi.reduce((s, x, i) => s + (x - mx) * (valid[i] - my), 0) / xi.reduce((s, x) => s + (x - mx) ** 2, 0)
              const trend = dates.map((_, i) => slope * i + (my - slope * mx))
              return { x: dates, y: trend, name: 'Trend', line: { color: '#ffffff', dash: 'dash', width: 1 }, type: 'scatter' }
            })(),
          ].filter(d => d.x)}
          layout={layout('Relative Performance', {
            yaxis: { ...PLOTLY_BASE.yaxis, ticksuffix: '%' },
            shapes: [{ type: 'line', x0: dates[0], x1: dates[dates.length - 1], y0: 0, y1: 0, line: { color: '#444', dash: 'dot', width: 1 } }],
          })}
          config={plotlyConfig} style={{ width: '100%', height: 280 }} useResizeHandler
        />
        {/* Indexed */}
        <Plot
          data={[{ x: dates, y: idx, name: 'Indexed', line: { color: '#14b8a6', width: 1.5 }, type: 'scatter' }]}
          layout={layout('Indexed Portfolio Value (Base = 100)', {
            shapes: [{ type: 'line', x0: dates[0], x1: dates[dates.length - 1], y0: 100, y1: 100, line: { color: '#444', dash: 'dash', width: 1 } }],
          })}
          config={plotlyConfig} style={{ width: '100%', height: 280 }} useResizeHandler
        />
      </div>
      {/* Annual returns */}
      {annualReturns.length > 0 && (
        <Plot
          data={[{
            x: annualReturns.map(r => r.year),
            y: annualReturns.map(r => r.ret),
            type: 'bar',
            text: annualReturns.map(r => `${r.ret >= 0 ? '+' : ''}${r.ret.toFixed(1)}%`),
            textposition: 'outside',
            marker: { color: annualReturns.map(r => r.ret >= 0 ? '#26a641' : '#e05252') },
          }]}
          layout={layout('Annual Return by Year', {
            yaxis: { ...PLOTLY_BASE.yaxis, ticksuffix: '%' },
            shapes: [{ type: 'line', x0: annualReturns[0].year, x1: annualReturns[annualReturns.length - 1].year, y0: 0, y1: 0, line: { color: '#444', dash: 'dot', width: 1 } }],
          })}
          config={plotlyConfig} style={{ width: '100%', height: 280 }} useResizeHandler
        />
      )}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Rolling 12m */}
        <Plot
          data={[{ x: dates, y: roll12, name: 'Rolling 12m', line: { color: '#f0a500', width: 1.5 }, type: 'scatter' }]}
          layout={layout('Rolling 12-Month Return', {
            yaxis: { ...PLOTLY_BASE.yaxis, ticksuffix: '%' },
            shapes: [{ type: 'line', x0: dates[0], x1: dates[dates.length - 1], y0: 0, y1: 0, line: { color: '#444', dash: 'dash', width: 1 } }],
          })}
          config={plotlyConfig} style={{ width: '100%', height: 260 }} useResizeHandler
        />
        {/* Drawdown + drawup */}
        <Plot
          data={[
            { x: dates, y: dd, name: 'Drawdown', fill: 'tozeroy', fillcolor: 'rgba(255,0,0,0.15)', line: { color: 'red', width: 1 }, type: 'scatter' },
            { x: dates, y: drawup, name: 'Drawup', fill: 'tozeroy', fillcolor: 'rgba(0,200,0,0.1)', line: { color: 'green', width: 1 }, type: 'scatter' },
          ]}
          layout={layout('Drawdown & Drawup', { yaxis: { ...PLOTLY_BASE.yaxis, ticksuffix: '%' } })}
          config={plotlyConfig} style={{ width: '100%', height: 260 }} useResizeHandler
        />
      </div>
    </div>
  )
}

const HOLDINGS_COLS = [
  { key: 'security_label', label: 'Security' },
  { key: 'quantity', label: 'Qty', align: 'right', render: v => fmt.num(v, 4) },
  { key: 'avg_cost_per_share', label: 'Avg Cost', align: 'right', render: v => fmt.currency(v, 2) },
  { key: 'cost_basis', label: 'Total Cost', align: 'right', render: v => fmt.currency(v) },
  { key: 'current_price', label: 'Price', align: 'right', render: v => fmt.currency(v, 2) },
  { key: 'market_value', label: 'Market Value', align: 'right', render: v => fmt.currency(v) },
  { key: 'abs_perf', label: 'Abs P&L', align: 'right', render: v => <span className={pnlColor(v)}>{fmt.currency(v)}</span> },
  { key: 'rel_perf', label: 'Rel P&L', align: 'right', render: v => <PnlBadge value={v} multiplier={1} /> },
]

export default function TabPortfolio() {
  const [portfolioIds, setPortfolioIds] = useState([])
  const [filters, setFilters] = useState({ sector: [], secType: [], exchange: [] })

  const { data: portfolios = [] } = useQuery({ queryKey: qk.portfolios(), queryFn: portfolioApi.list })

  const selectedIds = portfolioIds.length ? portfolioIds : null

  const { data: snapshot = [], isLoading: loadingSnap, error: snapError } = useQuery({
    queryKey: qk.snapshot(selectedIds),
    queryFn: () => holdingsApi.snapshot(selectedIds, false),
  })

  const { data: ts = [], isLoading: loadingTs } = useQuery({
    queryKey: qk.timeseries({ portfolio_ids: selectedIds }),
    queryFn: () => holdingsApi.timeseries({ portfolio_ids: selectedIds, aggregate: true }),
  })

  // Filter the snapshot
  const filtered = useMemo(() => {
    let rows = snapshot
    if (filters.sector.length) rows = rows.filter(r => filters.sector.includes(r.sector))
    if (filters.secType.length) rows = rows.filter(r => filters.secType.includes(r.security_type))
    if (filters.exchange.length) rows = rows.filter(r => filters.exchange.includes(r.exchange))
    return rows
  }, [snapshot, filters])

  const totals = useMemo(() => {
    const cost = filtered.reduce((s, r) => s + (r.cost_basis || 0), 0)
    const mv = filtered.reduce((s, r) => s + (r.market_value || 0), 0)
    return { cost, mv, pnl: mv - cost, relPnl: cost > 0 ? (mv - cost) / cost : 0 }
  }, [filtered])

  // Unique filter options
  const opts = useMemo(() => ({
    sector: [...new Set(snapshot.map(r => r.sector).filter(Boolean))].sort(),
    secType: [...new Set(snapshot.map(r => r.security_type).filter(Boolean))].sort(),
    exchange: [...new Set(snapshot.map(r => r.exchange).filter(Boolean))].sort(),
  }), [snapshot])

  // Allocation donut data
  const allocationData = useMemo(() => {
    const byType = {}
    filtered.forEach(r => {
      const k = r.security_type || 'Other'
      byType[k] = (byType[k] || 0) + (r.market_value || 0)
    })
    return { labels: Object.keys(byType), values: Object.values(byType) }
  }, [filtered])

  const sectorData = useMemo(() => {
    const bySector = {}
    filtered.forEach(r => {
      const k = r.sector || 'Other'
      bySector[k] = (bySector[k] || 0) + (r.market_value || 0)
    })
    return { labels: Object.keys(bySector), values: Object.values(bySector) }
  }, [filtered])

  if (isLoading && !snapshot.length) return <LoadingOverlay label="Loading portfolio…" />
  if (snapError) return <ErrorMsg error={snapError} />

  const isLoading = loadingSnap

  return (
    <div className="space-y-5">
      <SectionHeader title="Holdings" />

      {/* Portfolio filter */}
      <div className="card">
        <div className="flex flex-wrap gap-2 items-center">
          <span className="text-xs text-gray-500 mr-1">Portfolio:</span>
          <button
            className={`badge cursor-pointer ${portfolioIds.length === 0 ? 'badge-blue' : 'bg-surface-3 text-gray-400 hover:bg-surface-4'}`}
            onClick={() => setPortfolioIds([])}
          >All</button>
          {portfolios.map(p => (
            <button key={p.id}
              className={`badge cursor-pointer ${portfolioIds.includes(p.id) ? 'badge-blue' : 'bg-surface-3 text-gray-400 hover:bg-surface-4'}`}
              onClick={() => setPortfolioIds(ids => ids.includes(p.id) ? ids.filter(i => i !== p.id) : [...ids, p.id])}
            >{p.name}</button>
          ))}
        </div>
      </div>

      {/* Filters */}
      <Expander title="Filters">
        <div className="grid grid-cols-3 gap-3">
          {[['sector', 'Sector'], ['secType', 'Security Type'], ['exchange', 'Exchange']].map(([k, label]) => (
            <div key={k}>
              <label className="label">{label}</label>
              <div className="flex flex-wrap gap-1">
                {opts[k].map(v => (
                  <button key={v}
                    className={`badge text-xs cursor-pointer ${filters[k].includes(v) ? 'badge-blue' : 'bg-surface-3 text-gray-500 hover:bg-surface-4'}`}
                    onClick={() => setFilters(f => ({
                      ...f, [k]: f[k].includes(v) ? f[k].filter(x => x !== v) : [...f[k], v]
                    }))}
                  >{v}</button>
                ))}
              </div>
            </div>
          ))}
        </div>
      </Expander>

      {isLoading ? <LoadingOverlay /> : filtered.length === 0 ? (
        <div className="card text-center text-gray-500 py-8">No holdings match current filters.</div>
      ) : (
        <>
          {/* KPI row */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <MetricCard label="Market Value" value={fmt.currency(totals.mv)} />
            <MetricCard label="Cost Basis" value={fmt.currency(totals.cost)} />
            <MetricCard label="Abs. P&L" value={fmt.currency(totals.pnl)} color={pnlColor(totals.pnl)} />
            <MetricCard label="Rel. P&L" value={fmt.pct(totals.relPnl)} color={pnlColor(totals.relPnl)} />
          </div>

          {/* Charts row */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="card">
              <p className="text-xs text-gray-500 mb-2">By Security Type</p>
              <Plot
                data={[{ type: 'pie', labels: allocationData.labels, values: allocationData.values, hole: 0.5,
                  textinfo: 'label+percent', textfont: { color: '#e5e7eb', size: 11 },
                  marker: { colors: ['#6366f1','#22c55e','#f59e0b','#ef4444','#14b8a6','#a78bfa'] } }]}
                layout={{ ...PLOTLY_BASE, margin: { l: 10, r: 10, t: 10, b: 10 }, showlegend: false, height: 220 }}
                config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
              />
            </div>
            <div className="card">
              <p className="text-xs text-gray-500 mb-2">By Sector</p>
              <Plot
                data={[{ type: 'pie', labels: sectorData.labels, values: sectorData.values, hole: 0.5,
                  textinfo: 'label+percent', textfont: { color: '#e5e7eb', size: 10 },
                  marker: { colors: ['#6366f1','#22c55e','#f59e0b','#ef4444','#14b8a6','#a78bfa','#fb923c','#38bdf8','#f472b6','#84cc16'] } }]}
                layout={{ ...PLOTLY_BASE, margin: { l: 10, r: 10, t: 10, b: 10 }, showlegend: false, height: 220 }}
                config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
              />
            </div>
          </div>

          {/* Holdings table */}
          <div className="card overflow-hidden">
            <SectionHeader title="Holdings" subtitle={`${filtered.length} positions`} />
            <SortableTable columns={HOLDINGS_COLS} data={filtered} defaultSort={{ key: 'market_value', asc: false }} />
          </div>

          {/* Performance charts */}
          <Expander title="Deeper Performance Analysis">
            {loadingTs ? <LoadingOverlay /> : <PerfCharts ts={ts} />}
          </Expander>
        </>
      )}
    </div>
  )
}
