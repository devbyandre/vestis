import { useState, useMemo, lazy, Suspense } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import toast from 'react-hot-toast'
import { planningApi, portfolioApi, settingsApi } from '../lib/api'
import { qk } from '../lib/queryClient'
import { fmt, pnlColor, plotlyConfig } from '../lib/utils'
import { LoadingOverlay, ErrorMsg, SortableTable, SectionHeader, Expander, Input, MetricCard } from '../components/ui'

const Plot = lazy(() => import('react-plotly.js'))
function LazyPlot(props) {
  return <Suspense fallback={<div className="text-gray-500 text-xs py-4">Loading chart…</div>}><Plot {...props} /></Suspense>
}
const BASE = {
  paper_bgcolor: 'transparent', plot_bgcolor: 'transparent',
  font: { color: '#9ca3af', size: 11 }, margin: { l: 50, r: 20, t: 36, b: 40 },
  xaxis: { gridcolor: '#1a1a24', linecolor: '#2a2a3a' },
  yaxis: { gridcolor: '#1a1a24', linecolor: '#2a2a3a' },
  hoverlabel: { bgcolor: '#1a1a24', bordercolor: '#2a2a3a', font: { color: '#e5e7eb', size: 11 } },
}
const AREA_COLORS = ['#6366f1', '#22c55e', '#f59e0b', '#ef4444', '#14b8a6', '#a78bfa', '#fb923c', '#38bdf8']
const ASSET_TYPES = ['Equity', 'ETF', 'Bond', 'Crypto', 'Cash', 'Commodity']

function safeJson(v, fallback) {
  if (v == null) return fallback
  if (typeof v === 'object') return v
  try { return JSON.parse(v) } catch { return fallback }
}

function TargetSettings({ settings, onSave, saving }) {
  const assetRaw = safeJson(settings?.asset_allocation_targets, {})
  const riskCfg = safeJson(settings?.target_risk_profile, {})
  const [retirementYear, setRetirementYear] = useState(settings?.retirement_year || 2047)
  const [pre, setPre] = useState(assetRaw.pre_retirement || assetRaw.current || {})
  const [post, setPost] = useState(assetRaw.post_retirement || assetRaw.retirement || {})
  const [preRisk, setPreRisk] = useState(riskCfg.pre_retirement_risk ?? 0.4)
  const [postRisk, setPostRisk] = useState(riskCfg.post_retirement_risk ?? 0.2)

  const setAsset = (which, asset) => (val) => {
    const num = parseFloat(val) || 0
    if (which === 'pre') setPre(p => ({ ...p, [asset]: num }))
    else setPost(p => ({ ...p, [asset]: num }))
  }
  const preSum = Object.values(pre).reduce((s, v) => s + (Number(v) || 0), 0)
  const postSum = Object.values(post).reduce((s, v) => s + (Number(v) || 0), 0)

  const handleSave = () => {
    onSave({
      retirement_year: Number(retirementYear),
      asset_allocation_targets: { pre_retirement: pre, post_retirement: post },
      target_risk_profile: { pre_retirement_risk: Number(preRisk), post_retirement_risk: Number(postRisk) },
    })
  }

  return (
    <div className="space-y-4">
      <div className="max-w-xs">
        <Input label="Retirement Year" type="number" value={retirementYear} onChange={setRetirementYear} min="2025" max="2100" />
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="card">
          <p className="text-xs font-semibold text-gray-300 mb-2">Pre-Retirement Asset Targets (%)
            <span className={`ml-2 ${Math.abs(preSum - 100) < 0.5 ? 'text-success' : 'text-warning'}`}>Σ {preSum.toFixed(0)}%</span>
          </p>
          {ASSET_TYPES.map(a => (
            <div key={a} className="flex items-center gap-2 mb-1">
              <span className="text-xs text-gray-400 w-24">{a}</span>
              <input type="number" className="input py-1 text-xs" value={pre[a] ?? 0}
                onChange={e => setAsset('pre', a)(e.target.value)} min="0" max="100" step="1" />
            </div>
          ))}
        </div>
        <div className="card">
          <p className="text-xs font-semibold text-gray-300 mb-2">Post-Retirement Asset Targets (%)
            <span className={`ml-2 ${Math.abs(postSum - 100) < 0.5 ? 'text-success' : 'text-warning'}`}>Σ {postSum.toFixed(0)}%</span>
          </p>
          {ASSET_TYPES.map(a => (
            <div key={a} className="flex items-center gap-2 mb-1">
              <span className="text-xs text-gray-400 w-24">{a}</span>
              <input type="number" className="input py-1 text-xs" value={post[a] ?? 0}
                onChange={e => setAsset('post', a)(e.target.value)} min="0" max="100" step="1" />
            </div>
          ))}
        </div>
      </div>
      <div className="grid grid-cols-2 gap-4 max-w-md">
        <Input label="Pre-Retirement Risk (0-1)" type="number" value={preRisk} onChange={setPreRisk} min="0" max="1" step="0.05" />
        <Input label="Post-Retirement Risk (0-1)" type="number" value={postRisk} onChange={setPostRisk} min="0" max="1" step="0.05" />
      </div>
      <button className="btn-primary" onClick={handleSave} disabled={saving}>
        {saving ? 'Saving…' : 'Save Targets'}
      </button>
    </div>
  )
}

export default function TabPlanning() {
  const qc = useQueryClient()
  const [portfolioIds, setPortfolioIds] = useState([])
  const ids = portfolioIds.length ? portfolioIds : null

  const { data: portfolios = [] } = useQuery({ queryKey: qk.portfolios(), queryFn: portfolioApi.list })
  const { data: settings } = useQuery({ queryKey: ['settings'], queryFn: settingsApi.get })
  const retirementYear = settings?.retirement_year || 2047

  const { data: kpis = [], isLoading, error } = useQuery({ queryKey: qk.kpis(ids), queryFn: () => planningApi.kpis(ids) })
  const { data: rebalancing } = useQuery({ queryKey: qk.rebalancing(ids, retirementYear), queryFn: () => planningApi.rebalancing(ids, retirementYear) })
  const { data: allocTime } = useQuery({ queryKey: qk.allocationOverTime(ids), queryFn: () => planningApi.allocationOverTime(ids) })
  const { data: riskTime = [] } = useQuery({ queryKey: qk.riskOverTime(ids), queryFn: () => planningApi.riskOverTime(ids) })

  const saveMut = useMutation({
    mutationFn: (vals) => settingsApi.update(vals),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['settings'] }); qc.invalidateQueries({ queryKey: ['planning'] }); toast.success('Targets saved') },
    onError: (e) => toast.error(e.message),
  })

  const typeAlloc = useMemo(() => {
    const m = {}
    kpis.forEach(r => { const k = r.security_type || 'Other'; m[k] = (m[k] || 0) + (r.market_value || 0) })
    return { labels: Object.keys(m), values: Object.values(m) }
  }, [kpis])

  // Rebalancing suggestions can come as {suggestions: [...]} or array
  const rebalanceRows = useMemo(() => {
    if (!rebalancing) return []
    if (Array.isArray(rebalancing)) return rebalancing
    return rebalancing.suggestions || []
  }, [rebalancing])

  const KPI_COLS = [
    { key: 'security_label', label: 'Security' },
    { key: 'sector', label: 'Sector' },
    { key: 'security_type', label: 'Type' },
    { key: 'market_value', label: 'Mkt Value', align: 'right', render: v => fmt.currency(v), exportValue: v => v },
    { key: 'rel_perf', label: 'Rel P&L', align: 'right', render: v => <span className={pnlColor(v)}>{fmt.pct(v)}</span>, exportValue: v => v },
    { key: 'rsi', label: 'RSI', align: 'right', render: v => v != null ? Number(v).toFixed(1) : '—' },
    { key: 'beta', label: 'Beta', align: 'right', render: v => v != null ? Number(v).toFixed(2) : '—' },
    { key: 'trailingPE', label: 'P/E', align: 'right', render: v => v != null ? Number(v).toFixed(1) : '—' },
    { key: 'dividendYield', label: 'Div Yld', align: 'right', render: v => v != null ? `${(Number(v) * 100).toFixed(2)}%` : '—' },
  ]
  const REBAL_COLS = [
    { key: 'symbol', label: 'Security', render: (v, r) => v || r.security_label },
    { key: 'action', label: 'Action', render: v => {
      const cls = v === 'buy' || v === 'increase' ? 'badge-green' : v === 'sell' || v === 'reduce' ? 'badge-red' : 'badge'
      return <span className={`badge ${cls}`}>{String(v).toUpperCase()}</span>
    } },
    { key: 'current_weight', label: 'Current', align: 'right', render: v => v != null ? fmt.pct(v) : '—' },
    { key: 'target_weight', label: 'Target', align: 'right', render: v => v != null ? fmt.pct(v) : '—' },
    { key: 'reason', label: 'Reason' },
  ]

  return (
    <div className="space-y-4">
      <SectionHeader title="Investment Planning" />

      <div className="card flex flex-wrap gap-2 items-center">
        <span className="text-xs text-gray-500 mr-1">Portfolio:</span>
        <button className={`badge cursor-pointer ${!portfolioIds.length ? 'badge-blue' : 'bg-surface-3 text-gray-400'}`} onClick={() => setPortfolioIds([])}>All</button>
        {portfolios.map(p => (
          <button key={p.id} className={`badge cursor-pointer ${portfolioIds.includes(p.id) ? 'badge-blue' : 'bg-surface-3 text-gray-400'}`}
            onClick={() => setPortfolioIds(ids => ids.includes(p.id) ? ids.filter(i => i !== p.id) : [...ids, p.id])}>
            {p.name}
          </button>
        ))}
      </div>

      <Expander title="Target Allocation Settings">
        {settings ? <TargetSettings settings={settings} onSave={(v) => saveMut.mutate(v)} saving={saveMut.isPending} /> : <LoadingOverlay />}
      </Expander>

      {isLoading ? <LoadingOverlay /> : error ? <ErrorMsg error={error} /> : (
        <>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="card">
              <p className="text-xs text-gray-500 mb-2">Current Asset Allocation</p>
              <LazyPlot data={[{ type: 'pie', labels: typeAlloc.labels, values: typeAlloc.values, hole: 0.5, textinfo: 'label+percent', textfont: { color: '#e5e7eb', size: 11 }, marker: { colors: AREA_COLORS } }]}
                layout={{ ...BASE, height: 260, showlegend: false, margin: { l: 10, r: 10, t: 10, b: 10 } }} config={plotlyConfig} style={{ width: '100%' }} useResizeHandler />
            </div>
            {allocTime?.dates?.length > 0 && (
              <div className="card">
                <p className="text-xs text-gray-500 mb-2">Allocation Over Time</p>
                <LazyPlot
                  data={Object.entries(allocTime.series).map(([type, vals], i) => ({
                    x: allocTime.dates, y: vals.map(v => v * 100), name: type, type: 'scatter',
                    stackgroup: 'one', line: { width: 0.5, color: AREA_COLORS[i % AREA_COLORS.length] },
                    fillcolor: AREA_COLORS[i % AREA_COLORS.length] + '99',
                  }))}
                  layout={{ ...BASE, height: 260, yaxis: { ...BASE.yaxis, ticksuffix: '%', range: [0, 100] }, legend: { font: { size: 9 }, orientation: 'h', y: -0.2 } }}
                  config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
                />
              </div>
            )}
          </div>

          {riskTime.length > 0 && (
            <div className="card">
              <p className="text-xs text-gray-500 mb-2">Portfolio Risk Over Time</p>
              <LazyPlot
                data={[{ x: riskTime.map(r => r.date), y: riskTime.map(r => r.weighted_risk ?? r.risk ?? r.value), type: 'scatter', line: { color: '#f59e0b', width: 1.5 }, fill: 'tozeroy', fillcolor: 'rgba(245,158,11,0.1)', name: 'Weighted Risk' }]}
                layout={{ ...BASE, height: 260 }} config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
              />
              <p className="text-xs text-gray-600 mt-1">Aggregated weighted risk of the portfolio over time.</p>
            </div>
          )}

          {rebalanceRows.length > 0 && (
            <div className="card overflow-hidden">
              <SectionHeader title="Suggested Rebalancing Actions" subtitle={`Based on targets & retirement year ${retirementYear}`} />
              <SortableTable columns={REBAL_COLS} data={rebalanceRows} exportable exportName="rebalancing" />
            </div>
          )}

          <Expander title="KPI Table" defaultOpen>
            <SortableTable columns={KPI_COLS} data={kpis} defaultSort={{ key: 'market_value', asc: false }}
              searchable searchKeys={['security_label', 'sector', 'security_type']} pageSize={25}
              exportable exportName="planning_kpis" />
          </Expander>
        </>
      )}
    </div>
  )
}
