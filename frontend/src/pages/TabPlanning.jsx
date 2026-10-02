import { useState, useMemo, lazy, Suspense } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import toast from 'react-hot-toast'
import { planningApi, portfolioApi, settingsApi } from '../lib/api'
import { qk } from '../lib/queryClient'
import { fmt, pnlColor, plotlyConfig, clamp } from '../lib/utils'
import { LoadingOverlay, ErrorMsg, SortableTable, SectionHeader, Expander, Input, MetricCard } from '../components/ui'

const Plot = lazy(() =>
  // Vite's dev-mode CJS interop for this package can double-wrap the
  // default export ({ default: { default: Component } }) depending on the
  // bundler version — unwrap defensively so it works either way.
  import('react-plotly.js').then(m => ({ default: m.default?.default ?? m.default }))
)
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
const AREA_COLORS = ['#6366f1', '#22c55e', '#f59e0b', '#ef4444', '#14b8a6', '#a78bfa', '#fb923c', '#38bdf8', '#f472b6', '#84cc16']
const ASSET_TYPES = ['Equity', 'ETF', 'Bond', 'Crypto', 'Cash', 'Commodity']

// security_type on holdings is yfinance's raw quoteType ("EQUITY", "ETF",
// "MUTUALFUND", "CRYPTOCURRENCY", ...) while asset_allocation_targets uses
// its own vocabulary ("Equity", "ETF", "Bonds", ...) — without this mapping
// the Actual-vs-Target asset chart compares "EQUITY" against "Equity" and
// never matches anything, so every "Actual" bar renders as zero.
const ASSET_TYPE_ALIASES = {
  EQUITY: 'Equity', ETF: 'ETF', MUTUALFUND: 'Bonds', BOND: 'Bonds',
  CRYPTOCURRENCY: 'Crypto', CURRENCY: 'Cash',
}
function remapAssetTypeKeys(obj) {
  const out = {}
  Object.entries(obj || {}).forEach(([k, v]) => {
    const key = ASSET_TYPE_ALIASES[k.toUpperCase()] || k
    out[key] = (out[key] || 0) + v
  })
  return out
}

function safeJson(v, fallback) {
  if (v == null) return fallback
  if (typeof v === 'object') return v
  try { return JSON.parse(v) } catch { return fallback }
}
function normalize(d) {
  const total = Object.values(d).reduce((s, v) => s + (Number(v) || 0), 0) || 1
  return Object.fromEntries(Object.entries(d).map(([k, v]) => [k, (Number(v) || 0) / total]))
}

// Linear glidepath from pre_retirement_risk to post_retirement_risk, flat
// after retirement — the actual formula app_streamlit.py computes inline
// (middleware.py's target_risk_curve() is dead code, never called there).
function computeGlidepath(dates, preRisk, postRisk, retirementYear) {
  if (!dates.length) return []
  const firstYear = new Date(dates[0]).getFullYear()
  const yearsToRetirement = Math.max(retirementYear - firstYear, 1)
  return dates.map(d => {
    const year = new Date(d).getFullYear()
    if (year <= retirementYear) {
      const progress = clamp((year - firstYear) / yearsToRetirement, 0, 1)
      return preRisk + (postRisk - preRisk) * progress
    }
    return postRisk
  })
}

// Groups detailed risk rows (date, weighted_risk, [categoryKey]) into a
// dates/series shape (fraction of total per date) — mirrors how
// /planning/allocation-over-time shapes its response, but computed
// client-side since risk-over-time?aggregate=false returns raw rows.
function groupRiskByCategory(rows, categoryKey) {
  const byDateCat = {}
  rows.forEach(r => {
    const d = r.date
    const cat = r[categoryKey] || 'Unknown'
    byDateCat[d] = byDateCat[d] || {}
    byDateCat[d][cat] = (byDateCat[d][cat] || 0) + (r.weighted_risk || 0)
  })
  const dates = Object.keys(byDateCat).sort()
  const cats = [...new Set(rows.map(r => r[categoryKey] || 'Unknown'))].sort()
  const series = {}
  cats.forEach(c => { series[c] = dates.map(d => {
    const total = Object.values(byDateCat[d]).reduce((s, v) => s + v, 0) || 1
    return (byDateCat[d][c] || 0) / total
  }) })
  return { dates, series }
}

function latestSnapshot(series, dates) {
  if (!dates?.length) return {}
  const out = {}
  Object.entries(series || {}).forEach(([k, arr]) => { out[k] = arr[arr.length - 1] })
  return out
}

// ── Target settings — asset/risk (existing) + full nested sector/industry/
// security targets. app_streamlit.py computes these nested weights in its
// UI but never actually persists sector/industry edits (the Save button
// writes back the stale config it loaded, not the edited state) and never
// saves or reads security-level weights at all — fixed here so this
// actually round-trips and feeds mw.suggest_rebalancing (see middleware.py).
function TargetSettings({ settings, kpis, onSave, saving }) {
  const assetRaw = safeJson(settings?.asset_allocation_targets, {})
  const riskCfg = safeJson(settings?.target_risk_profile, {})
  const sectorCfg = safeJson(settings?.target_sector_allocation, {})
  const industryCfg = safeJson(settings?.target_industry_allocation, {})
  const securityCfg = safeJson(settings?.target_security_allocation, {})

  const [retirementYear, setRetirementYear] = useState(settings?.retirement_year || 2047)
  const [pre, setPre] = useState(assetRaw.pre_retirement || assetRaw.current || {})
  const [post, setPost] = useState(assetRaw.post_retirement || assetRaw.retirement || {})
  const [preRisk, setPreRisk] = useState(riskCfg.pre_retirement_risk ?? 0.4)
  const [postRisk, setPostRisk] = useState(riskCfg.post_retirement_risk ?? 0.2)
  const [marketVol, setMarketVol] = useState(riskCfg.market_volatility ?? 0.15)

  const [sectorWeights, setSectorWeights] = useState(sectorCfg)
  const [industryWeights, setIndustryWeights] = useState(industryCfg)
  const [securityWeights, setSecurityWeights] = useState(securityCfg)
  const [expandedSectors, setExpandedSectors] = useState(
    () => Object.keys(sectorCfg).filter(s => sectorCfg[s] > 0)
  )
  const [expandedIndustries, setExpandedIndustries] = useState(() => {
    const out = {}
    for (const [sec, inds] of Object.entries(industryCfg)) out[sec] = Object.keys(inds).filter(i => inds[i] > 0)
    return out
  })

  // get_complete_taxonomy() uses a lowercase-hyphenated vocabulary
  // ("technology", "consumer-electronics") that doesn't match the Title
  // Case sector/industry values actual holdings use (from yfinance) or
  // that target_sector_allocation's own default config keys use
  // ("Technology") — sourcing this picker from real holdings instead
  // sidesteps that mismatch entirely, and only lets you target
  // sectors/industries you're actually exposed to.
  const industriesBySector = useMemo(() => {
    const out = {}
    kpis.forEach(r => {
      if (!r.sector) return
      out[r.sector] = out[r.sector] || new Set()
      if (r.industry) out[r.sector].add(r.industry)
    })
    return Object.fromEntries(Object.entries(out).map(([k, v]) => [k, [...v].sort()]))
  }, [kpis])
  const allSectors = useMemo(
    () => [...new Set([...Object.keys(sectorCfg), ...Object.keys(industriesBySector)])].sort(),
    [sectorCfg, industriesBySector]
  )
  const securitiesFor = (sec, ind) => kpis.filter(r => r.sector === sec && r.industry === ind)

  const toggleSector = (sec) => setExpandedSectors(a => a.includes(sec) ? a.filter(x => x !== sec) : [...a, sec])
  const toggleIndustry = (sec, ind) => setExpandedIndustries(prev => {
    const cur = prev[sec] || []
    return { ...prev, [sec]: cur.includes(ind) ? cur.filter(x => x !== ind) : [...cur, ind] }
  })
  const setSectorW = (sec, v) => setSectorWeights(w => ({ ...w, [sec]: parseFloat(v) || 0 }))
  const setIndustryW = (sec, ind, v) => setIndustryWeights(w => ({ ...w, [sec]: { ...(w[sec] || {}), [ind]: parseFloat(v) || 0 } }))
  const setSecurityW = (sym, v) => setSecurityWeights(w => ({ ...w, [sym]: parseFloat(v) || 0 }))

  const preSum = Object.values(pre).reduce((s, v) => s + (Number(v) || 0), 0)
  const postSum = Object.values(post).reduce((s, v) => s + (Number(v) || 0), 0)
  const setAsset = (which, asset) => (val) => {
    const num = parseFloat(val) || 0
    if (which === 'pre') setPre(p => ({ ...p, [asset]: num }))
    else setPost(p => ({ ...p, [asset]: num }))
  }

  const handleSave = () => {
    const industryOut = {}
    const securityOut = {}
    for (const sec of expandedSectors) {
      const inds = expandedIndustries[sec] || []
      const rawInd = {}
      inds.forEach(ind => { rawInd[ind] = industryWeights[sec]?.[ind] || 0 })
      const totalInd = Object.values(rawInd).reduce((s, v) => s + v, 0) || 1
      industryOut[sec] = Object.fromEntries(Object.entries(rawInd).map(([k, v]) => [k, v / totalInd]))

      for (const ind of inds) {
        if ((industryWeights[sec]?.[ind] || 0) <= 0) continue
        const secs = securitiesFor(sec, ind).map(r => r.symbol)
        const rawSec = {}
        secs.forEach(sym => { rawSec[sym] = securityWeights[sym] || 0 })
        const totalSec = Object.values(rawSec).reduce((s, v) => s + v, 0) || 1
        if (Object.values(rawSec).some(v => v > 0)) {
          Object.entries(rawSec).forEach(([sym, v]) => { securityOut[sym] = v / totalSec })
        }
      }
    }
    const sectorOut = {}
    expandedSectors.forEach(sec => { sectorOut[sec] = sectorWeights[sec] || 0 })

    onSave({
      retirement_year: Number(retirementYear),
      asset_allocation_targets: { pre_retirement: normalize(pre), post_retirement: normalize(post) },
      target_risk_profile: {
        pre_retirement_risk: Number(preRisk), post_retirement_risk: Number(postRisk), market_volatility: Number(marketVol),
      },
      target_sector_allocation: sectorOut,
      target_industry_allocation: industryOut,
      target_security_allocation: securityOut,
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
      <div className="grid grid-cols-3 gap-4 max-w-2xl">
        <Input label="Pre-Retirement Risk (0-1)" type="number" value={preRisk} onChange={setPreRisk} min="0" max="1" step="0.05" />
        <Input label="Post-Retirement Risk (0-1)" type="number" value={postRisk} onChange={setPostRisk} min="0" max="1" step="0.05" />
        <Input label="Market Volatility (0-1)" type="number" value={marketVol} onChange={setMarketVol} min="0" max="1" step="0.05" />
      </div>

      <div className="card">
        <p className="text-xs font-semibold text-gray-300 mb-1">Sector / Industry / Security Targets</p>
        <p className="text-xs text-gray-600 mb-3">Click a sector to set its target weight; industries and securities within it become editable once it has weight &gt; 0.</p>
        <div className="flex flex-wrap gap-1.5 mb-3">
          {allSectors.map(sec => (
            <button key={sec} className={`badge text-xs cursor-pointer ${expandedSectors.includes(sec) ? 'badge-blue' : 'bg-surface-3 text-gray-500'}`}
              onClick={() => toggleSector(sec)}>{sec}</button>
          ))}
        </div>
        <div className="space-y-3">
          {expandedSectors.map(sec => (
            <div key={sec} className="pl-3 border-l-2 border-surface-3">
              <div className="flex items-center gap-2">
                <span className="text-xs text-gray-300 w-48 truncate">{sec}</span>
                <input type="number" className="input py-1 text-xs max-w-24" value={sectorWeights[sec] ?? 0}
                  onChange={e => setSectorW(sec, e.target.value)} min="0" max="1" step="0.05" />
              </div>
              {(sectorWeights[sec] || 0) > 0 && (
                <div className="pl-4 mt-2 space-y-2">
                  <div className="flex flex-wrap gap-1.5">
                    {(industriesBySector[sec] || Object.keys(industryWeights[sec] || {})).map(ind => (
                      <button key={ind} className={`badge text-xs cursor-pointer ${expandedIndustries[sec]?.includes(ind) ? 'badge-blue' : 'bg-surface-3 text-gray-500'}`}
                        onClick={() => toggleIndustry(sec, ind)}>{ind}</button>
                    ))}
                  </div>
                  {(expandedIndustries[sec] || []).map(ind => (
                    <div key={ind} className="pl-3 border-l-2 border-surface-4">
                      <div className="flex items-center gap-2">
                        <span className="text-xs text-gray-400 w-48 truncate">{sec} → {ind}</span>
                        <input type="number" className="input py-1 text-xs max-w-24" value={industryWeights[sec]?.[ind] ?? 0}
                          onChange={e => setIndustryW(sec, ind, e.target.value)} min="0" max="1" step="0.05" />
                      </div>
                      {(industryWeights[sec]?.[ind] || 0) > 0 && (
                        <div className="pl-4 mt-1 space-y-1">
                          {securitiesFor(sec, ind).map(r => (
                            <div key={r.symbol} className="flex items-center gap-2">
                              <span className="text-xs text-gray-500 w-48 truncate">{r.security_label || r.symbol}</span>
                              <input type="number" className="input py-1 text-xs max-w-24" value={securityWeights[r.symbol] ?? 0}
                                onChange={e => setSecurityW(r.symbol, e.target.value)} min="0" max="1" step="0.05" />
                            </div>
                          ))}
                          {securitiesFor(sec, ind).length === 0 && (
                            <p className="text-xs text-gray-600">No current holdings in this sector/industry.</p>
                          )}
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>
          ))}
          {expandedSectors.length === 0 && <p className="text-xs text-gray-600">No sector targets set yet.</p>}
        </div>
      </div>

      <button className="btn-primary" onClick={handleSave} disabled={saving}>
        {saving ? 'Saving…' : 'Save Targets'}
      </button>
    </div>
  )
}

function areaChart(title, dates, series, colors = AREA_COLORS, height = 260) {
  return (
    <div className="card">
      <p className="text-xs text-gray-500 mb-2">{title}</p>
      <LazyPlot
        data={Object.entries(series).map(([name, vals], i) => ({
          x: dates, y: vals.map(v => v * 100), name, type: 'scatter',
          stackgroup: 'one', line: { width: 0.5, color: colors[i % colors.length] },
          fillcolor: colors[i % colors.length] + '99',
        }))}
        layout={{ ...BASE, height, yaxis: { ...BASE.yaxis, ticksuffix: '%', range: [0, 100] }, legend: { font: { size: 9 }, orientation: 'h', y: -0.2 } }}
        config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
      />
    </div>
  )
}

function actualVsTargetBar(title, actual, targets, height = 260) {
  const labels = [...new Set([...Object.keys(actual), ...targets.flatMap(t => Object.keys(t.data))])].sort()
  return (
    <div className="card">
      <p className="text-xs text-gray-500 mb-2">{title}</p>
      <LazyPlot
        data={[
          { x: labels, y: labels.map(l => (actual[l] || 0) * 100), name: 'Actual', type: 'bar', marker: { color: '#38bdf8' } },
          ...targets.map((t, i) => ({
            x: labels, y: labels.map(l => (t.data[l] || 0) * 100), name: t.name, type: 'bar',
            marker: { color: AREA_COLORS[(i + 2) % AREA_COLORS.length] },
          })),
        ]}
        layout={{ ...BASE, height, barmode: 'group', yaxis: { ...BASE.yaxis, ticksuffix: '%' }, legend: { font: { size: 9 }, orientation: 'h', y: -0.2 } }}
        config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
      />
    </div>
  )
}

function categoryBarWithTarget(title, values, targetPct, height = 240) {
  const labels = Object.keys(values).sort()
  return (
    <div className="card">
      <p className="text-xs text-gray-500 mb-2">{title}</p>
      <LazyPlot
        data={[{ x: labels, y: labels.map(l => values[l] * 100), type: 'bar', marker: { color: '#a78bfa' } }]}
        layout={{
          ...BASE, height, yaxis: { ...BASE.yaxis, ticksuffix: '%' },
          shapes: labels.length ? [{ type: 'line', x0: -0.5, x1: labels.length - 0.5, y0: targetPct * 100, y1: targetPct * 100, line: { color: '#f59e0b', dash: 'dash', width: 1.5 } }] : [],
        }}
        config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
      />
      <p className="text-xs text-gray-600 mt-1">Dashed line = pre-retirement target risk.</p>
    </div>
  )
}

export default function TabPlanning() {
  const qc = useQueryClient()
  const [portfolioIds, setPortfolioIds] = useState([])
  const [showCurrentOnly, setShowCurrentOnly] = useState(false)
  const [typeFilter, setTypeFilter] = useState([])
  const ids = portfolioIds.length ? portfolioIds : null

  const { data: portfolios = [] } = useQuery({ queryKey: qk.portfolios(), queryFn: portfolioApi.list })
  const { data: settings } = useQuery({ queryKey: ['settings'], queryFn: settingsApi.get })
  const retirementYear = settings?.retirement_year || 2047
  const riskCfg = safeJson(settings?.target_risk_profile, {})
  const preRisk = riskCfg.pre_retirement_risk ?? 0.4
  const postRisk = riskCfg.post_retirement_risk ?? 0.2
  const assetTargets = safeJson(settings?.asset_allocation_targets, {})
  const sectorTargets = safeJson(settings?.target_sector_allocation, {})
  const industryTargetsNested = safeJson(settings?.target_industry_allocation, {})
  const industryTargetsFlat = useMemo(() => {
    const out = {}
    Object.values(industryTargetsNested).forEach(inds => Object.entries(inds || {}).forEach(([ind, w]) => { out[ind] = (out[ind] || 0) + w }))
    return out
  }, [industryTargetsNested])

  const { data: kpisRaw = [], isLoading, error } = useQuery({ queryKey: qk.kpis(ids), queryFn: () => planningApi.kpis(ids) })
  const { data: rebalancing } = useQuery({ queryKey: qk.rebalancing(ids, retirementYear), queryFn: () => planningApi.rebalancing(ids, retirementYear) })
  // One request computing all 4 groupings server-side from a single shared
  // holdings-timeseries fetch, instead of 4 separate round trips that each
  // redundantly re-fetch and re-process the same underlying data.
  const { data: allocAll } = useQuery({ queryKey: qk.allocationOverTimeAll(ids), queryFn: () => planningApi.allocationOverTimeAll(ids) })
  const allocTime = allocAll?.security_type
  const sectorTime = allocAll?.sector
  const industryTime = allocAll?.industry
  const symbolTime = allocAll?.symbol
  const { data: riskTime = [] } = useQuery({ queryKey: qk.riskOverTime(ids, true), queryFn: () => planningApi.riskOverTime(ids, true) })
  const { data: riskDetailedRaw = [] } = useQuery({ queryKey: qk.riskOverTime(ids, false), queryFn: () => planningApi.riskOverTime(ids, false) })

  const saveMut = useMutation({
    mutationFn: (vals) => settingsApi.update(vals),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['settings'] }); qc.invalidateQueries({ queryKey: ['planning'] }); qc.invalidateQueries({ queryKey: ['analytics', 'rebalancing'] }); toast.success('Targets saved') },
    onError: (e) => toast.error(e.message),
  })

  const currentSymbols = useMemo(() => new Set(kpisRaw.map(r => r.symbol)), [kpisRaw])
  const typeOptions = useMemo(() => [...new Set(kpisRaw.map(r => r.security_type).filter(Boolean))].sort(), [kpisRaw])

  const kpis = useMemo(() => {
    let rows = kpisRaw
    if (typeFilter.length) rows = rows.filter(r => typeFilter.includes(r.security_type))
    return rows
  }, [kpisRaw, typeFilter])

  // "Current holdings only" + type filter apply to the risk-detail-derived
  // charts (10-13) and the top-10-securities chart, where raw per-security
  // rows are available client-side to filter before re-aggregating. The
  // server-aggregated sector/industry/asset-type distribution-over-time
  // charts (already grouped across all-time holdings server-side) aren't
  // re-filterable this way without a further backend change, so they show
  // full history regardless of these two filters.
  const riskDetailed = useMemo(() => {
    let rows = riskDetailedRaw
    if (showCurrentOnly) rows = rows.filter(r => currentSymbols.has(r.symbol))
    if (typeFilter.length) rows = rows.filter(r => typeFilter.includes(r.security_type))
    return rows
  }, [riskDetailedRaw, showCurrentOnly, typeFilter, currentSymbols])

  const typeAlloc = useMemo(() => {
    const m = {}
    kpis.forEach(r => { const k = r.security_type || 'Other'; m[k] = (m[k] || 0) + (r.market_value || 0) })
    return { labels: Object.keys(m), values: Object.values(m) }
  }, [kpis])

  const rebalanceRows = useMemo(() => {
    if (!rebalancing) return []
    if (Array.isArray(rebalancing)) return rebalancing
    return rebalancing.suggestions || []
  }, [rebalancing])

  // Symbol-level distribution, optionally restricted to currently-held symbols
  const symbolTimeFiltered = useMemo(() => {
    if (!symbolTime?.dates?.length) return symbolTime
    if (!showCurrentOnly) return symbolTime
    const series = {}
    Object.entries(symbolTime.series).forEach(([sym, vals]) => {
      if (sym === 'Other' || currentSymbols.has(sym)) series[sym] = vals
    })
    return { dates: symbolTime.dates, series }
  }, [symbolTime, showCurrentOnly, currentSymbols])

  const riskByType = useMemo(() => groupRiskByCategory(riskDetailed, 'security_type'), [riskDetailed])
  const riskBySector = useMemo(() => groupRiskByCategory(riskDetailed, 'sector'), [riskDetailed])
  const riskByIndustry = useMemo(() => groupRiskByCategory(riskDetailed, 'industry'), [riskDetailed])
  const riskBySymbolAll = useMemo(() => groupRiskByCategory(riskDetailed, 'symbol'), [riskDetailed])
  const riskTop10Symbols = useMemo(() => {
    const latest = latestSnapshot(riskBySymbolAll.series, riskBySymbolAll.dates)
    const top = Object.entries(latest).sort((a, b) => b[1] - a[1]).slice(0, 10).map(([k]) => k)
    const series = {}
    top.forEach(k => { series[k] = riskBySymbolAll.series[k] })
    return { dates: riskBySymbolAll.dates, series }
  }, [riskBySymbolAll])

  const glidepath = useMemo(() => computeGlidepath(riskTime.map(r => r.date), preRisk, postRisk, retirementYear), [riskTime, preRisk, postRisk, retirementYear])

  const latestRiskByType = useMemo(() => latestSnapshot(riskByType.series, riskByType.dates), [riskByType])
  const latestRiskBySector = useMemo(() => latestSnapshot(riskBySector.series, riskBySector.dates), [riskBySector])
  const latestRiskByIndustry = useMemo(() => latestSnapshot(riskByIndustry.series, riskByIndustry.dates), [riskByIndustry])
  const latestRiskBySecurity = useMemo(() => latestSnapshot(riskBySymbolAll.series, riskBySymbolAll.dates), [riskBySymbolAll])

  const deviationBySecurity = useMemo(() => {
    const out = {}
    Object.entries(latestRiskBySecurity).forEach(([sym, frac]) => {
      out[sym] = preRisk > 0 ? ((frac - preRisk) / preRisk) * 100 : 0
    })
    return out
  }, [latestRiskBySecurity, preRisk])

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
    { key: '_action', label: 'Action', render: (_, r) => {
      const v = r.pct_change
      const cls = v > 0 ? 'badge-green' : v < 0 ? 'badge-red' : 'badge'
      const label = v > 0 ? 'Increase' : v < 0 ? 'Reduce' : 'Hold'
      return <span className={`badge ${cls}`}>{label}</span>
    } },
    { key: 'pct_change', label: '% Change', align: 'right', render: v => <span className={pnlColor(v)}>{fmt.pct(v)}</span> },
    { key: 'market_value_change', label: '€ Change', align: 'right', render: v => v != null ? fmt.currency(v) : '—' },
    { key: 'reasons', label: 'Reason', render: v => Array.isArray(v) ? v.join('; ') : (v || '—') },
    { key: 'impact_allocation', label: 'Impact Allocation', align: 'right', render: v => v != null ? fmt.pct(v) : '—' },
    { key: 'impact_risk', label: 'Impact Risk', align: 'right', render: v => v != null ? fmt.pct(v) : '—' },
    { key: 'priority_score', label: 'Priority', align: 'right', render: v => v != null ? Number(v).toFixed(2) : '—' },
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

      <div className="card flex flex-wrap gap-4 items-center">
        <label className="flex items-center gap-2 text-xs text-gray-400 cursor-pointer">
          <input type="checkbox" checked={showCurrentOnly} onChange={e => setShowCurrentOnly(e.target.checked)} />
          Current holdings only (risk charts + top securities)
        </label>
        <div className="flex items-center gap-1.5">
          <span className="text-xs text-gray-500">Type:</span>
          {typeOptions.map(t => (
            <button key={t} className={`badge text-xs cursor-pointer ${typeFilter.includes(t) ? 'badge-blue' : 'bg-surface-3 text-gray-500'}`}
              onClick={() => setTypeFilter(f => f.includes(t) ? f.filter(x => x !== t) : [...f, t])}>{t}</button>
          ))}
        </div>
      </div>

      <Expander title="Target Allocation Settings">
        {settings ? <TargetSettings settings={settings} kpis={kpisRaw} onSave={(v) => saveMut.mutate(v)} saving={saveMut.isPending} /> : <LoadingOverlay />}
      </Expander>

      {isLoading ? <LoadingOverlay /> : error ? <ErrorMsg error={error} /> : (
        <>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="card">
              <p className="text-xs text-gray-500 mb-2">Current Asset Allocation</p>
              <LazyPlot data={[{ type: 'pie', labels: typeAlloc.labels, values: typeAlloc.values, hole: 0.5, textinfo: 'label+percent', textfont: { color: '#e5e7eb', size: 11 }, marker: { colors: AREA_COLORS } }]}
                layout={{ ...BASE, height: 260, showlegend: false, margin: { l: 10, r: 10, t: 10, b: 10 } }} config={plotlyConfig} style={{ width: '100%' }} useResizeHandler />
            </div>
            {allocTime?.dates?.length > 0 && areaChart('Allocation Over Time (by Type)', allocTime.dates, allocTime.series)}
          </div>

          {actualVsTargetBar('Asset Allocation: Actual vs Target', remapAssetTypeKeys(latestSnapshot(allocTime?.series, allocTime?.dates)), [
            { name: 'Target (Pre)', data: safeJson(settings?.asset_allocation_targets, {}).pre_retirement || {} },
            { name: 'Target (Post)', data: safeJson(settings?.asset_allocation_targets, {}).post_retirement || {} },
          ])}

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {sectorTime?.dates?.length > 0 && areaChart('Sector Distribution Over Time', sectorTime.dates, sectorTime.series)}
            {actualVsTargetBar('Sector: Actual vs Target', latestSnapshot(sectorTime?.series, sectorTime?.dates), [{ name: 'Target', data: sectorTargets }])}
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {industryTime?.dates?.length > 0 && areaChart('Industry Distribution Over Time', industryTime.dates, industryTime.series)}
            {actualVsTargetBar('Industry: Actual vs Target', latestSnapshot(industryTime?.series, industryTime?.dates), [{ name: 'Target', data: industryTargetsFlat }])}
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {symbolTimeFiltered?.dates?.length > 0 && areaChart('Top 10 Securities Over Time', symbolTimeFiltered.dates, symbolTimeFiltered.series)}
            {symbolTimeFiltered?.dates?.length > 0 && (
              <div className="card">
                <p className="text-xs text-gray-500 mb-2">Top 10 Securities — Current Allocation</p>
                <LazyPlot
                  data={[{
                    x: Object.keys(latestSnapshot(symbolTimeFiltered.series, symbolTimeFiltered.dates)),
                    y: Object.values(latestSnapshot(symbolTimeFiltered.series, symbolTimeFiltered.dates)).map(v => v * 100),
                    type: 'bar', marker: { color: '#38bdf8' },
                  }]}
                  layout={{ ...BASE, height: 260, yaxis: { ...BASE.yaxis, ticksuffix: '%' } }}
                  config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
                />
              </div>
            )}
          </div>

          {riskTime.length > 0 && (
            <div className="card">
              <p className="text-xs text-gray-500 mb-2">Portfolio Risk vs Target</p>
              <LazyPlot
                data={[
                  { x: riskTime.map(r => r.date), y: riskTime.map(r => (r.weighted_risk ?? r.risk ?? r.value) * 100), type: 'scatter', line: { color: '#f59e0b', width: 1.5 }, fill: 'tozeroy', fillcolor: 'rgba(245,158,11,0.1)', name: 'Actual' },
                  { x: riskTime.map(r => r.date), y: glidepath.map(v => v * 100), type: 'scatter', line: { color: '#38bdf8', width: 1.5, dash: 'dash' }, name: 'Target (glidepath)' },
                ]}
                layout={{ ...BASE, height: 260, yaxis: { ...BASE.yaxis, ticksuffix: '%' } }} config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
              />
              <p className="text-xs text-gray-600 mt-1">Target linearly declines from the pre- to post-retirement risk setting by your retirement year.</p>
            </div>
          )}

          {riskDetailed.length > 0 && (
            <Expander title="Risk Breakdown Over Time" defaultOpen>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {areaChart('Risk by Asset Type', riskByType.dates, riskByType.series)}
                {areaChart('Risk by Sector', riskBySector.dates, riskBySector.series)}
                {areaChart('Risk by Industry', riskByIndustry.dates, riskByIndustry.series)}
                {areaChart('Top 10 Securities by Risk Contribution', riskTop10Symbols.dates, riskTop10Symbols.series)}
              </div>
            </Expander>
          )}

          {riskDetailed.length > 0 && (
            <Expander title="Risk vs Target by Category" defaultOpen>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {categoryBarWithTarget('Risk by Asset Type (current)', latestRiskByType, preRisk)}
                {categoryBarWithTarget('Risk by Sector (current)', latestRiskBySector, preRisk)}
                {categoryBarWithTarget('Risk by Industry (current)', latestRiskByIndustry, preRisk)}
                {categoryBarWithTarget('Risk by Security (current)', latestRiskBySecurity, preRisk)}
              </div>
              <div className="card mt-4">
                <p className="text-xs text-gray-500 mb-2">Risk Deviation vs Target (by Security)</p>
                <LazyPlot
                  data={[{
                    x: Object.keys(deviationBySecurity), y: Object.values(deviationBySecurity), type: 'bar',
                    marker: { color: Object.values(deviationBySecurity).map(v => v > 0 ? '#ef4444' : '#22c55e') },
                  }]}
                  layout={{ ...BASE, height: 260, yaxis: { ...BASE.yaxis, ticksuffix: '%' } }} config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
                />
                <p className="text-xs text-gray-600 mt-1">% deviation of each security's risk share from the pre-retirement target. Red = contributing more risk than target, green = less.</p>
              </div>
            </Expander>
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
