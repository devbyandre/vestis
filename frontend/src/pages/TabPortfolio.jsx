import { useState, useMemo, lazy, Suspense } from 'react'
import { useQuery } from '@tanstack/react-query'
import { holdingsApi, portfolioApi } from '../lib/api'
import { qk } from '../lib/queryClient'
import { fmt, pnlColor, plotlyConfig } from '../lib/utils'
import {
  LoadingOverlay, ErrorMsg, MetricCard, SortableTable,
  PnlBadge, Expander, SectionHeader,
} from '../components/ui'

const Plot = lazy(() => import('react-plotly.js'))

const BASE = {
  paper_bgcolor: 'transparent', plot_bgcolor: 'transparent',
  font: { color: '#9ca3af', family: 'Inter var, Inter, sans-serif', size: 11 },
  margin: { l: 55, r: 20, t: 36, b: 40 },
  hovermode: 'closest',
  hoverlabel: { bgcolor: '#1a1a24', bordercolor: '#2a2a3a', font: { color: '#e5e7eb', size: 11 } },
  xaxis: { gridcolor: '#1a1a24', linecolor: '#2a2a3a' },
  yaxis: { gridcolor: '#1a1a24', linecolor: '#2a2a3a' },
}

function LazyPlot(props) {
  return (
    <Suspense fallback={<div className="text-gray-500 text-xs py-4">Loading chart…</div>}>
      <Plot {...props} />
    </Suspense>
  )
}

// Performance group buckets (matches Streamlit pd.cut bins)
const PERF_GROUPS = [
  { name: 'Strong Loss', min: -Infinity, max: -0.20, color: '#b91c1c' },
  { name: 'Loss', min: -0.20, max: -0.05, color: '#f87171' },
  { name: 'Neutral', min: -0.05, max: 0.05, color: '#9ca3af' },
  { name: 'Gain', min: 0.05, max: 0.20, color: '#4ade80' },
  { name: 'Strong Gain', min: 0.20, max: Infinity, color: '#16a34a' },
]
function perfGroup(rel) {
  if (rel == null) return null
  return PERF_GROUPS.find(g => rel > g.min && rel <= g.max)?.name || null
}

function PerfCharts({ ts }) {
  const daily = useMemo(() => {
    if (!ts?.length) return []
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

  const yearMap = {}
  daily.forEach(r => { const y = r.date?.slice(0, 4); if (y) yearMap[y] = r.market_value })
  const years = Object.keys(yearMap).sort()
  const annualReturns = years.slice(1).map((y, i) => {
    const prev = yearMap[years[i]], curr = yearMap[y]
    return { year: y, ret: prev > 0 ? (curr - prev) / prev * 100 : null }
  }).filter(r => r.ret != null)

  const roll12 = daily.map((r, i) => {
    const past = daily[Math.max(0, i - 252)]
    return past?.market_value > 0 ? (r.market_value - past.market_value) / past.market_value * 100 : null
  })

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
    ...BASE, hovermode: 'x unified', title: { text: title, font: { size: 13, color: '#d1d5db' } }, ...extra,
  })

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div>
          <LazyPlot
            data={[
              { x: dates, y: mv, name: 'Market Value', fill: 'tozeroy', fillcolor: 'rgba(76,114,176,0.25)', line: { color: '#4C72B0', width: 1.5 }, type: 'scatter' },
              { x: dates, y: cb, name: 'Cost Basis', fill: 'tozeroy', fillcolor: 'rgba(221,132,82,0.3)', line: { color: '#DD8452', width: 1.5 }, type: 'scatter' },
            ]}
            layout={layout('Portfolio Value vs Cost Basis', { yaxis: { ...BASE.yaxis, tickprefix: '€', tickformat: ',.0f' } })}
            config={plotlyConfig} style={{ width: '100%', height: 280 }} useResizeHandler
          />
          <p className="text-xs text-gray-600 mt-1">Market value (blue) vs invested capital (orange). Gap = unrealised P&L.</p>
        </div>
        <div>
          <LazyPlot
            data={[{ x: dates, y: pnl, name: 'Net P&L', fill: 'tozeroy', fillcolor: 'rgba(44,160,44,0.2)', line: { color: '#2ca02c', width: 1.5 }, type: 'scatter' }]}
            layout={layout('Net P&L Over Time', { yaxis: { ...BASE.yaxis, tickprefix: '€', tickformat: ',.0f' } })}
            config={plotlyConfig} style={{ width: '100%', height: 280 }} useResizeHandler
          />
          <p className="text-xs text-gray-600 mt-1">Unrealised P&L at each point. Above zero = in profit.</p>
        </div>
      </div>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div>
          <LazyPlot
            data={[{ x: dates, y: rel, name: 'Rel. Performance', line: { color: '#a78bfa', width: 1.5 }, type: 'scatter' }]}
            layout={layout('Relative Performance', { yaxis: { ...BASE.yaxis, ticksuffix: '%' } })}
            config={plotlyConfig} style={{ width: '100%', height: 280 }} useResizeHandler
          />
          <p className="text-xs text-gray-600 mt-1">(Market Value − Cost) / Cost. Above 0% = in profit.</p>
        </div>
        <div>
          <LazyPlot
            data={[{ x: dates, y: idx, name: 'Indexed', line: { color: '#14b8a6', width: 1.5 }, type: 'scatter' }]}
            layout={layout('Indexed Portfolio Value (Base = 100)')}
            config={plotlyConfig} style={{ width: '100%', height: 280 }} useResizeHandler
          />
          <p className="text-xs text-gray-600 mt-1">Value indexed to 100 at start. Total growth regardless of invested amount.</p>
        </div>
      </div>
      {annualReturns.length > 0 && (
        <div>
          <LazyPlot
            data={[{ x: annualReturns.map(r => r.year), y: annualReturns.map(r => r.ret), type: 'bar',
              text: annualReturns.map(r => `${r.ret >= 0 ? '+' : ''}${r.ret.toFixed(1)}%`), textposition: 'outside',
              marker: { color: annualReturns.map(r => r.ret >= 0 ? '#26a641' : '#e05252') } }]}
            layout={layout('Annual Return by Year', { yaxis: { ...BASE.yaxis, ticksuffix: '%' } })}
            config={plotlyConfig} style={{ width: '100%', height: 280 }} useResizeHandler
          />
          <p className="text-xs text-gray-600 mt-1">Year-on-year return based on market value change.</p>
        </div>
      )}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div>
          <LazyPlot
            data={[{ x: dates, y: roll12, name: 'Rolling 12m', line: { color: '#f0a500', width: 1.5 }, type: 'scatter' }]}
            layout={layout('Rolling 12-Month Return', { yaxis: { ...BASE.yaxis, ticksuffix: '%' } })}
            config={plotlyConfig} style={{ width: '100%', height: 260 }} useResizeHandler
          />
          <p className="text-xs text-gray-600 mt-1">Return over the trailing 12 months at each date.</p>
        </div>
        <div>
          <LazyPlot
            data={[
              { x: dates, y: dd, name: 'Drawdown', fill: 'tozeroy', fillcolor: 'rgba(255,0,0,0.15)', line: { color: 'red', width: 1 }, type: 'scatter' },
              { x: dates, y: drawup, name: 'Drawup', fill: 'tozeroy', fillcolor: 'rgba(0,200,0,0.1)', line: { color: 'green', width: 1 }, type: 'scatter' },
            ]}
            layout={layout('Drawdown & Drawup', { yaxis: { ...BASE.yaxis, ticksuffix: '%' } })}
            config={plotlyConfig} style={{ width: '100%', height: 260 }} useResizeHandler
          />
          <p className="text-xs text-gray-600 mt-1">Red = drop from peak. Green = gain from recent trough.</p>
        </div>
      </div>
    </div>
  )
}

function BubbleChart({ rows, selectedGroups, onToggleGroup }) {
  const scatter = useMemo(() => {
    return rows
      .filter(r => r.rel_perf != null)
      .map(r => ({ ...r, group: perfGroup(r.rel_perf), size: Math.abs(r.market_value || 0) }))
      .filter(r => r.group && (!selectedGroups.length || selectedGroups.includes(r.group)))
  }, [rows, selectedGroups])

  const traces = PERF_GROUPS
    .filter(g => !selectedGroups.length || selectedGroups.includes(g.name))
    .map(g => {
      const pts = scatter.filter(r => r.group === g.name)
      if (!pts.length) return null
      const maxSize = Math.max(...scatter.map(r => r.size), 1)
      return {
        x: pts.map(r => r.market_value),
        y: pts.map(r => r.rel_perf * 100),
        text: pts.map(r => r.security_label),
        name: g.name,
        mode: 'markers',
        type: 'scatter',
        marker: {
          color: g.color,
          size: pts.map(r => Math.max(8, Math.sqrt(r.size / maxSize) * 50)),
          sizemode: 'diameter',
          line: { color: '#0a0a0f', width: 1 },
          opacity: 0.8,
        },
        hovertemplate: '%{text}<br>Market Value: €%{x:,.0f}<br>Rel Perf: %{y:.2f}%<extra></extra>',
      }
    })
    .filter(Boolean)

  return (
    <div>
      <div className="flex flex-wrap gap-2 mb-3">
        {PERF_GROUPS.map(g => (
          <button key={g.name}
            className="badge text-xs cursor-pointer transition-opacity"
            style={{
              backgroundColor: (!selectedGroups.length || selectedGroups.includes(g.name)) ? g.color + '33' : 'transparent',
              color: g.color, border: `1px solid ${g.color}66`,
              opacity: (!selectedGroups.length || selectedGroups.includes(g.name)) ? 1 : 0.4,
            }}
            onClick={() => onToggleGroup(g.name)}>
            {g.name}
          </button>
        ))}
      </div>
      <LazyPlot
        data={traces}
        layout={{
          ...BASE,
          title: { text: 'Relative Performance vs Market Value', font: { size: 13, color: '#d1d5db' } },
          xaxis: { ...BASE.xaxis, title: 'Market Value (€)', tickprefix: '€', tickformat: ',.0f' },
          yaxis: { ...BASE.yaxis, title: 'Relative Performance (%)', ticksuffix: '%', zeroline: true, zerolinecolor: '#374151' },
          showlegend: true, height: 420,
        }}
        config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
      />
      <p className="text-xs text-gray-600 mt-1">Colors = performance buckets (relative perf). Bubble size = exposure (abs market value).</p>
    </div>
  )
}

function CompositionCharts({ rows, excludeUnknown }) {
  const build = (field) => {
    const agg = {}
    rows.forEach(r => {
      let k = r[field] || 'Unknown'
      if (excludeUnknown && (k === 'Unknown' || k === 'N/A' || !r[field])) return
      agg[k] = (agg[k] || 0) + (r.market_value || 0)
    })
    return { labels: Object.keys(agg), values: Object.values(agg) }
  }

  const typeData = build('security_type')
  const sectorData = build('sector')
  const industryData = build('industry')
  const exchangeData = build('exchange')

  const COLORS = ['#6366f1', '#22c55e', '#f59e0b', '#ef4444', '#14b8a6', '#a78bfa', '#fb923c', '#38bdf8', '#f472b6', '#84cc16', '#e879f9', '#facc15']

  const pie = (data, title) => (
    <div className="card">
      <p className="text-xs text-gray-500 mb-2">{title}</p>
      <LazyPlot
        data={[{ type: 'pie', labels: data.labels, values: data.values, hole: 0.5,
          textinfo: 'label+percent', textfont: { color: '#e5e7eb', size: 10 },
          marker: { colors: COLORS } }]}
        layout={{ ...BASE, margin: { l: 10, r: 10, t: 10, b: 10 }, showlegend: false, height: 240 }}
        config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
      />
    </div>
  )

  const treemap = (data, title) => (
    <div className="card">
      <p className="text-xs text-gray-500 mb-2">{title}</p>
      <LazyPlot
        data={[{ type: 'treemap', labels: data.labels, parents: data.labels.map(() => ''),
          values: data.values, textinfo: 'label+value+percent root',
          marker: { colors: COLORS }, textfont: { color: '#e5e7eb', size: 11 } }]}
        layout={{ ...BASE, margin: { l: 0, r: 0, t: 0, b: 0 }, height: 240 }}
        config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
      />
    </div>
  )

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
      {pie(typeData, 'By Security Type')}
      {treemap(sectorData, 'By Sector')}
      {treemap(industryData, 'By Industry')}
      {treemap(exchangeData, 'By Exchange')}
    </div>
  )
}

export default function TabPortfolio() {
  const [portfolioIds, setPortfolioIds] = useState([])
  const [filters, setFilters] = useState({ sector: [], industry: [], secType: [], exchange: [] })
  const [selectedGroups, setSelectedGroups] = useState([])
  const [excludeUnknown, setExcludeUnknown] = useState(true)

  const { data: portfolios = [] } = useQuery({ queryKey: qk.portfolios(), queryFn: portfolioApi.list })
  const selectedIds = portfolioIds.length ? portfolioIds : null

  const { data: snapshot = [], isLoading, error } = useQuery({
    queryKey: qk.snapshot(selectedIds),
    queryFn: () => holdingsApi.snapshot(selectedIds, false),
  })
  const { data: ts = [], isLoading: loadingTs } = useQuery({
    queryKey: qk.timeseries({ portfolio_ids: selectedIds }),
    queryFn: () => holdingsApi.timeseries({ portfolio_ids: selectedIds, aggregate: true }),
  })
  const { data: metrics } = useQuery({
    queryKey: qk.holdingsMetrics(selectedIds),
    queryFn: () => holdingsApi.metrics(selectedIds),
  })

  const filtered = useMemo(() => {
    let rows = snapshot
    if (filters.sector.length) rows = rows.filter(r => filters.sector.includes(r.sector))
    if (filters.industry.length) rows = rows.filter(r => filters.industry.includes(r.industry))
    if (filters.secType.length) rows = rows.filter(r => filters.secType.includes(r.security_type))
    if (filters.exchange.length) rows = rows.filter(r => filters.exchange.includes(r.exchange))
    return rows
  }, [snapshot, filters])

  const totals = useMemo(() => {
    const cost = filtered.reduce((s, r) => s + (r.cost_basis || 0), 0)
    const mv = filtered.reduce((s, r) => s + (r.market_value || 0), 0)
    return { cost, mv, pnl: mv - cost, relPnl: cost > 0 ? (mv - cost) / cost : 0 }
  }, [filtered])

  const opts = useMemo(() => ({
    sector: [...new Set(snapshot.map(r => r.sector).filter(Boolean))].sort(),
    industry: [...new Set(snapshot.map(r => r.industry).filter(Boolean))].sort(),
    secType: [...new Set(snapshot.map(r => r.security_type).filter(Boolean))].sort(),
    exchange: [...new Set(snapshot.map(r => r.exchange).filter(Boolean))].sort(),
  }), [snapshot])

  const HOLDINGS_COLS = [
    { key: 'security_label', label: 'Security' },
    { key: 'quantity', label: 'Qty', align: 'right', render: v => fmt.num(v, 4), exportValue: v => v },
    { key: 'cost_basis', label: 'Total Cost', align: 'right', render: v => fmt.currency(v), exportValue: v => v },
    { key: 'market_value', label: 'Market Value', align: 'right', render: v => fmt.currency(v), exportValue: v => v },
    { key: 'sector', label: 'Sector' },
    { key: 'abs_perf', label: 'Abs P&L', align: 'right', render: v => <span className={pnlColor(v)}>{fmt.currency(v)}</span>, exportValue: v => v },
    { key: 'rel_perf', label: 'Rel P&L', align: 'right', render: v => <PnlBadge value={v} multiplier={1} />, exportValue: v => v },
  ]

  const toggleGroup = (name) =>
    setSelectedGroups(g => g.includes(name) ? g.filter(x => x !== name) : [...g, name])

  if (isLoading && !snapshot.length) return <LoadingOverlay label="Loading portfolio…" />
  if (error) return <ErrorMsg error={error} />

  return (
    <div className="space-y-5">
      <SectionHeader title="Holdings" />

      <div className="card">
        <div className="flex flex-wrap gap-2 items-center">
          <span className="text-xs text-gray-500 mr-1">Portfolio:</span>
          <button className={`badge cursor-pointer ${portfolioIds.length === 0 ? 'badge-blue' : 'bg-surface-3 text-gray-400'}`} onClick={() => setPortfolioIds([])}>All</button>
          {portfolios.map(p => (
            <button key={p.id}
              className={`badge cursor-pointer ${portfolioIds.includes(p.id) ? 'badge-blue' : 'bg-surface-3 text-gray-400'}`}
              onClick={() => setPortfolioIds(ids => ids.includes(p.id) ? ids.filter(i => i !== p.id) : [...ids, p.id])}>
              {p.name}
            </button>
          ))}
        </div>
      </div>

      <Expander title="Filters">
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
          {[['sector', 'Sector'], ['industry', 'Industry'], ['secType', 'Security Type'], ['exchange', 'Exchange']].map(([k, label]) => (
            <div key={k}>
              <label className="label">{label}</label>
              <div className="flex flex-wrap gap-1">
                {opts[k].map(v => (
                  <button key={v}
                    className={`badge text-xs cursor-pointer ${filters[k].includes(v) ? 'badge-blue' : 'bg-surface-3 text-gray-500'}`}
                    onClick={() => setFilters(f => ({ ...f, [k]: f[k].includes(v) ? f[k].filter(x => x !== v) : [...f[k], v] }))}>
                    {v}
                  </button>
                ))}
              </div>
            </div>
          ))}
        </div>
      </Expander>

      {filtered.length === 0 ? (
        <div className="card text-center text-gray-500 py-8">No holdings match current filters.</div>
      ) : (
        <>
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
            <MetricCard label="Market Value" value={fmt.currency(totals.mv)} />
            <MetricCard label="Cost Basis" value={fmt.currency(totals.cost)} />
            <MetricCard label="Abs. P&L" value={fmt.currency(totals.pnl)} color={pnlColor(totals.pnl)} />
            <MetricCard label="Rel. P&L" value={fmt.pct(totals.relPnl)} color={pnlColor(totals.relPnl)} />
            <MetricCard label="Volatility" value={metrics?.volatility != null ? fmt.pct(metrics.volatility) : 'N/A'} />
            <MetricCard label="Sharpe" value={metrics?.sharpe != null ? metrics.sharpe.toFixed(2) : 'N/A'} />
          </div>

          <div className="card overflow-hidden">
            <SectionHeader title="Holdings" subtitle={`${filtered.length} positions`} />
            <SortableTable columns={HOLDINGS_COLS} data={filtered}
              defaultSort={{ key: 'market_value', asc: false }}
              searchable searchKeys={['security_label', 'sector']}
              exportable exportName="holdings" />
          </div>

          <Expander title="Performance Bubble Chart">
            <BubbleChart rows={filtered} selectedGroups={selectedGroups} onToggleGroup={toggleGroup} />
          </Expander>

          <Expander title="Portfolio Composition Charts">
            <label className="flex items-center gap-2 text-xs text-gray-400 mb-3 cursor-pointer">
              <input type="checkbox" checked={excludeUnknown} onChange={e => setExcludeUnknown(e.target.checked)} />
              Exclude 'Unknown' values
            </label>
            <CompositionCharts rows={filtered} excludeUnknown={excludeUnknown} />
          </Expander>

          <Expander title="Deeper Performance Analysis">
            {loadingTs ? <LoadingOverlay /> : <PerfCharts ts={ts} />}
          </Expander>
        </>
      )}
    </div>
  )
}
