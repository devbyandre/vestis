import { useState, lazy, Suspense } from 'react'
import { useQuery, useQueries } from '@tanstack/react-query'
import { analyticsApi, securitiesApi } from '../lib/api'
import { qk } from '../lib/queryClient'
import { fmt, plotlyConfig, distributionStats, normalPdf, linspace } from '../lib/utils'
import { LoadingOverlay, ErrorMsg, MetricCard, SectionHeader, Expander, SortableTable } from '../components/ui'

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
  font: { color: '#9ca3af', family: 'Inter var, Inter, sans-serif', size: 11 },
  xaxis: { gridcolor: '#1a1a24', linecolor: '#2a2a3a', showspikes: true, spikemode: 'across', spikecolor: '#444' },
  yaxis: { gridcolor: '#1a1a24', linecolor: '#2a2a3a' },
  margin: { l: 55, r: 20, t: 36, b: 40 },
  hovermode: 'x unified',
  hoverlabel: { bgcolor: '#1a1a24', bordercolor: '#2a2a3a', font: { color: '#e5e7eb', size: 11 } },
}

const LOOKBACKS = [
  { value: 90, label: '3M' }, { value: 180, label: '6M' }, { value: 365, label: '1Y' },
  { value: 730, label: '2Y' }, { value: 1825, label: '5Y' },
]
const SMA_CHOICES = [5, 10, 20, 50, 100, 200]
const EMA_CHOICES = [5, 10, 20, 50, 100, 200]
const SMA_COLORS = { 5: '#f59e0b', 10: '#fb923c', 20: '#facc15', 50: '#84cc16', 100: '#14b8a6', 200: '#38bdf8' }
const EMA_COLORS = { 5: '#a78bfa', 10: '#c084fc', 20: '#e879f9', 50: '#f472b6', 100: '#fb7185', 200: '#f87171' }

// ── Return Distribution ──────────────────────────────────────────────────────
// Histogram of daily returns + fitted normal curve + ±1σ/±2σ lines, matching
// app_streamlit.py's per-symbol Return Distribution section.
function ReturnDistribution({ symbol, closes }) {
  const returns = closes
    .map((c, i) => (i > 0 && closes[i - 1] ? (c - closes[i - 1]) / closes[i - 1] : null))
    .filter(v => v != null)

  if (returns.length < 5) return <div className="text-gray-600 text-sm py-4">Not enough data.</div>

  const { mean, std, skew, kurtosis } = distributionStats(returns)
  const xs = linspace(Math.min(...returns), Math.max(...returns), 200)
  const ys = xs.map(x => normalPdf(x, mean, std))
  const maxY = Math.max(...ys, 0.001) * 1.1

  const sigmaLine = (mult, color) => {
    const x = mean + mult * std
    return { x0: x, x1: x, y0: 0, y1: maxY, color }
  }
  const shapes = [-2, -1, 1, 2].map(mult => {
    const l = sigmaLine(mult, '#f59e0b')
    return { type: 'line', x0: l.x0, x1: l.x1, y0: l.y0, y1: l.y1, line: { color: l.color, dash: 'dot', width: 1 } }
  })

  return (
    <div>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-3">
        <MetricCard label="Mean" value={fmt.pct(mean, 4)} />
        <MetricCard label="Std Dev" value={fmt.pct(std, 4)} />
        <MetricCard label="Skew" value={skew != null ? skew.toFixed(2) : '—'} />
        <MetricCard label="Kurtosis" value={kurtosis != null ? kurtosis.toFixed(2) : '—'} />
      </div>
      <LazyPlot
        data={[
          { x: returns, type: 'histogram', nbinsx: 50, histnorm: 'probability density', marker: { color: '#38bdf8', opacity: 0.6 }, name: 'Returns' },
          { x: xs, y: ys, type: 'scatter', mode: 'lines', name: 'Normal fit', line: { color: '#ef4444', dash: 'dash', width: 1.5 } },
        ]}
        layout={{
          ...BASE, title: { text: `${symbol} — Daily Return Distribution`, font: { color: '#d1d5db', size: 13 } },
          xaxis: { ...BASE.xaxis, tickformat: '.1%' }, shapes, height: 300, showlegend: false,
        }}
        config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
      />
    </div>
  )
}

// ── Fundamentals / KPI grid ───────────────────────────────────────────────────
// Matches app_streamlit.py's "Technical KPIs & Financial Overview" — 7 groups.
const FUNDAMENTALS_GROUPS = [
  { title: 'Risk & Performance', fields: [
    ['Volatility', m => fmt.pct(m.metrics?.volatility)],
    ['Sharpe', m => m.metrics?.sharpe != null ? m.metrics.sharpe.toFixed(2) : '—'],
    ['Sortino', m => m.metrics?.sortino != null ? m.metrics.sortino.toFixed(2) : '—'],
    ['Max Drawdown', m => fmt.pct(m.metrics?.max_drawdown)],
    ['CAGR', m => fmt.pct(m.metrics?.cagr)],
    ['Calmar', m => m.metrics?.calmar != null ? m.metrics.calmar.toFixed(2) : '—'],
    ['Treynor', m => m.metrics?.treynor != null ? m.metrics.treynor.toFixed(2) : '—'],
  ] },
  { title: 'Market Data', fields: [
    ['Price', m => fmt.currency(m.basic?.regularMarketPrice, 2)],
    ['52w Low', m => fmt.currency(m.basic?.fiftyTwoWeekLow, 2)],
    ['52w High', m => fmt.currency(m.basic?.fiftyTwoWeekHigh, 2)],
  ] },
  { title: 'Valuation', fields: [
    ['Trailing P/E', m => m.basic?.trailingPE != null ? Number(m.basic.trailingPE).toFixed(1) : '—'],
    ['Forward P/E', m => m.basic?.forwardPE != null ? Number(m.basic.forwardPE).toFixed(1) : '—'],
    ['Enterprise Value', m => fmt.currency(m.basic?.enterpriseValue, 0)],
    ['Profit Margin', m => fmt.pct(m.basic?.profitMargins)],
    ['Operating Margin', m => fmt.pct(m.basic?.operatingMargins)],
  ] },
  { title: 'Revenue & Profits', fields: [
    ['Total Revenue', m => fmt.currency(m.basic?.totalRevenue, 0)],
    ['Revenue / Share', m => fmt.currency(m.basic?.revenuePerShare, 2)],
    ['Gross Profits', m => fmt.currency(m.basic?.grossProfits, 0)],
    ['EBITDA', m => fmt.currency(m.basic?.ebitda, 0)],
  ] },
  { title: 'Balance Sheet', fields: [
    ['Total Cash', m => fmt.currency(m.basic?.totalCash, 0)],
    ['Total Debt', m => fmt.currency(m.basic?.totalDebt, 0)],
    ['Current Ratio', m => m.basic?.currentRatio != null ? Number(m.basic.currentRatio).toFixed(2) : '—'],
    ['Book Value', m => fmt.currency(m.basic?.bookValue, 2)],
  ] },
  { title: 'Cash Flow', fields: [
    ['Operating CF', m => fmt.currency(m.basic?.operatingCashflow, 0)],
    ['Free Cash Flow', m => fmt.currency(m.basic?.freeCashflow, 0)],
  ] },
  { title: 'Shares', fields: [
    ['Shares Outstanding', m => m.basic?.sharesOutstanding != null ? fmt.num(m.basic.sharesOutstanding, 0) : '—'],
    ['Market Cap', m => fmt.currency(m.basic?.marketCap, 0)],
  ] },
]

function FundamentalsGrid({ basic, metrics }) {
  const ctx = { basic, metrics }
  return (
    <div className="space-y-4">
      {FUNDAMENTALS_GROUPS.map(g => (
        <div key={g.title}>
          <p className="text-xs font-semibold text-gray-400 mb-2">{g.title}</p>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            {g.fields.map(([label, fn]) => (
              <MetricCard key={label} label={label} value={fn(ctx)} />
            ))}
          </div>
        </div>
      ))}
    </div>
  )
}

const COMPARE_COLORS = ['#38bdf8', '#f59e0b', '#22c55e', '#a78bfa', '#ef4444', '#f472b6']

// ── Compare mode ──────────────────────────────────────────────────────────────
// Overlaid indexed price + RSI (comparable across symbols regardless of raw
// price level), small-multiples volume + return distribution, and one
// comparison table per fundamentals group — mirrors app_streamlit.py's
// multi-security branch (which falls back to per-group st.dataframe tables
// instead of the single-symbol metric view).
function TechnicalCompare({ securities }) {
  const [symbols, setSymbols] = useState([])
  const [pick, setPick] = useState('')
  const [lookback, setLookback] = useState(365)

  const addSymbol = (s) => {
    const sym = s.trim().toUpperCase()
    if (sym && !symbols.includes(sym) && symbols.length < 6) setSymbols(arr => [...arr, sym])
    setPick('')
  }
  const removeSymbol = (s) => setSymbols(arr => arr.filter(x => x !== s))

  const indicatorQueries = useQueries({
    queries: symbols.map(sym => ({
      queryKey: qk.indicators(sym, { lookback_days: lookback, compare: true }),
      queryFn: () => analyticsApi.indicators(sym, { lookback_days: lookback, show_bb: false, show_crossovers: false }),
    })),
  })
  const basicQueries = useQueries({
    queries: symbols.map(sym => ({
      queryKey: qk.security(sym),
      queryFn: () => securitiesApi.getBasic(sym),
    })),
  })

  const perSymbol = symbols.map((sym, i) => {
    const series = indicatorQueries[i]?.data?.series || []
    const closes = series.map(r => r.adj_close ?? r.close)
    const dates = series.map(r => r.date)
    const rsi = series.map(r => r.rsi)
    const volumes = series.map(r => r.volume)
    const metrics = indicatorQueries[i]?.data?.metrics || {}
    const basic = basicQueries[i]?.data || {}
    const base = closes.find(c => c != null) || 1
    const indexed = closes.map(c => c != null ? (c / base) * 100 : null)
    return { sym, series, closes, dates, rsi, volumes, metrics, basic, indexed, isLoading: indicatorQueries[i]?.isLoading }
  })

  const anyLoading = perSymbol.some(p => p.isLoading)
  const ready = perSymbol.filter(p => p.dates.length > 0)

  const compareCols = (fields) => [
    { key: 'sym', label: 'Security' },
    ...fields.map(([label, fn]) => ({
      key: label, label, align: 'right', render: (_, r) => fn({ basic: r.basic, metrics: r.metrics }),
    })),
  ]

  return (
    <div className="space-y-4">
      <div className="card space-y-2">
        <label className="label">Securities (2-6)</label>
        <div className="flex flex-wrap gap-2 items-center">
          {symbols.map(s => (
            <span key={s} className="badge badge-blue cursor-pointer" onClick={() => removeSymbol(s)}>{s} ×</span>
          ))}
          {symbols.length < 6 && (
            <input className="input max-w-40 text-sm" list="tech-compare-list" value={pick} placeholder="Add symbol…"
              onChange={e => setPick(e.target.value.toUpperCase())}
              onKeyDown={e => e.key === 'Enter' && addSymbol(pick)}
              onBlur={() => pick && addSymbol(pick)} />
          )}
          <datalist id="tech-compare-list">
            {securities.map(s => <option key={s.symbol || s.yahoo_ticker} value={s.symbol || s.yahoo_ticker} />)}
          </datalist>
        </div>
        <div className="flex gap-1">
          {LOOKBACKS.map(lb => (
            <button key={lb.value} className={`btn text-xs px-2 py-1 ${lookback === lb.value ? 'btn-primary' : 'btn-ghost'}`}
              onClick={() => setLookback(lb.value)}>{lb.label}</button>
          ))}
        </div>
      </div>

      {symbols.length < 2 && <div className="card text-center text-gray-500 py-8">Add at least 2 securities to compare.</div>}
      {symbols.length >= 2 && anyLoading && <LoadingOverlay label="Loading comparison…" />}

      {symbols.length >= 2 && ready.length >= 2 && (
        <>
          <div className="card p-2">
            <LazyPlot
              data={ready.map((p, i) => ({
                x: p.dates, y: p.indexed, type: 'scatter', name: p.sym,
                line: { color: COMPARE_COLORS[i % COMPARE_COLORS.length], width: 1.5 },
              }))}
              layout={{ ...BASE, title: { text: 'Indexed Price (Base = 100)', font: { color: '#d1d5db', size: 13 } }, height: 340 }}
              config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
            />
            <p className="text-xs text-gray-600 mt-1 px-2">Indexed to 100 at the start of the period so securities at very different price levels are comparable.</p>
          </div>

          <div className="card p-2">
            <LazyPlot
              data={ready.map((p, i) => ({
                x: p.dates, y: p.rsi, type: 'scatter', name: p.sym,
                line: { color: COMPARE_COLORS[i % COMPARE_COLORS.length], width: 1.5 },
              }))}
              layout={{
                ...BASE, title: { text: 'RSI (14)', font: { color: '#d1d5db', size: 13 } },
                yaxis: { ...BASE.yaxis, range: [0, 100] }, height: 220,
                shapes: [
                  { type: 'line', x0: ready[0].dates[0], x1: ready[0].dates[ready[0].dates.length - 1], y0: 70, y1: 70, line: { color: '#ef4444', dash: 'dot', width: 1 } },
                  { type: 'line', x0: ready[0].dates[0], x1: ready[0].dates[ready[0].dates.length - 1], y0: 30, y1: 30, line: { color: '#22c55e', dash: 'dot', width: 1 } },
                ],
              }}
              config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
            />
          </div>

          <Expander title="Volume (per security)">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              {ready.map((p, i) => (
                <div key={p.sym} className="card p-2">
                  <LazyPlot
                    data={[{ x: p.dates, y: p.volumes, type: 'bar', name: p.sym, marker: { color: COMPARE_COLORS[i % COMPARE_COLORS.length] } }]}
                    layout={{ ...BASE, title: { text: p.sym, font: { color: '#d1d5db', size: 12 } }, height: 180 }}
                    config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
                  />
                </div>
              ))}
            </div>
          </Expander>

          <Expander title="Return Distribution (per security)">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              {ready.map(p => (
                <div key={p.sym} className="card p-2">
                  <ReturnDistribution symbol={p.sym} closes={p.closes} />
                </div>
              ))}
            </div>
          </Expander>

          <Expander title="Fundamentals Comparison" defaultOpen>
            <div className="space-y-4">
              {FUNDAMENTALS_GROUPS.map(g => (
                <div key={g.title} className="overflow-x-auto">
                  <p className="text-xs font-semibold text-gray-400 mb-2">{g.title}</p>
                  <SortableTable columns={compareCols(g.fields)} data={ready} />
                </div>
              ))}
            </div>
          </Expander>
        </>
      )}
    </div>
  )
}

function TechnicalDeepDive({ securities }) {
  const [symbol, setSymbol] = useState('')
  const [lookback, setLookback] = useState(365)
  const [smaPeriods, setSmaPeriods] = useState([50, 200])
  const [emaPeriods, setEmaPeriods] = useState([])
  const [showBB, setShowBB] = useState(false)
  const [showCrossovers, setShowCrossovers] = useState(true)
  const [showExtrema, setShowExtrema] = useState(false)
  const [showVolume, setShowVolume] = useState(true)

  const opts = {
    sma_periods: smaPeriods.join(','),
    ema_periods: emaPeriods.join(','),
    bb_window: 20,
    show_bb: showBB,
    show_crossovers: showCrossovers,
    show_extrema: showExtrema,
    lookback_days: lookback,
  }

  const { data: result, isLoading, error } = useQuery({
    queryKey: qk.indicators(symbol, opts),
    queryFn: () => analyticsApi.indicators(symbol, opts),
    enabled: !!symbol,
  })
  const { data: basic } = useQuery({
    queryKey: qk.security(symbol),
    queryFn: () => securitiesApi.getBasic(symbol),
    enabled: !!symbol,
  })

  const series = result?.series || []
  const metrics = result?.metrics || {}
  const crossovers = result?.crossovers || { buy: [], sell: [] }
  const extrema = result?.extrema || { min: [], max: [] }

  const dates = series.map(r => r.date)
  const closes = series.map(r => r.adj_close ?? r.close)
  const opens = series.map(r => r.open)
  const highs = series.map(r => r.high)
  const lows = series.map(r => r.low)
  const volumes = series.map(r => r.volume)
  const rsi = series.map(r => r.rsi)

  const toggle = (arr, setArr, val) =>
    setArr(a => a.includes(val) ? a.filter(x => x !== val) : [...a, val])

  return (
    <div className="space-y-4">
      {/* Controls */}
      <div className="card space-y-3">
        <div className="flex flex-wrap gap-4 items-end">
          <div className="flex-1 min-w-48">
            <label className="label">Security</label>
            <input className="input" list="tech-sec-list" value={symbol}
              onChange={e => setSymbol(e.target.value.toUpperCase())} placeholder="e.g. AAPL" />
            <datalist id="tech-sec-list">
              {securities.map(s => <option key={s.symbol || s.yahoo_ticker} value={s.symbol || s.yahoo_ticker} />)}
            </datalist>
          </div>
          <div className="flex gap-1">
            {LOOKBACKS.map(lb => (
              <button key={lb.value}
                className={`btn text-xs px-2 py-1 ${lookback === lb.value ? 'btn-primary' : 'btn-ghost'}`}
                onClick={() => setLookback(lb.value)}>{lb.label}</button>
            ))}
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          <div>
            <label className="label">SMAs</label>
            <div className="flex flex-wrap gap-1">
              {SMA_CHOICES.map(w => (
                <button key={w} onClick={() => toggle(smaPeriods, setSmaPeriods, w)}
                  className={`badge text-xs cursor-pointer ${smaPeriods.includes(w) ? 'badge-blue' : 'bg-surface-3 text-gray-500'}`}>
                  {w}
                </button>
              ))}
            </div>
          </div>
          <div>
            <label className="label">EMAs</label>
            <div className="flex flex-wrap gap-1">
              {EMA_CHOICES.map(e => (
                <button key={e} onClick={() => toggle(emaPeriods, setEmaPeriods, e)}
                  className={`badge text-xs cursor-pointer ${emaPeriods.includes(e) ? 'badge-blue' : 'bg-surface-3 text-gray-500'}`}>
                  {e}
                </button>
              ))}
            </div>
          </div>
        </div>

        <div className="flex flex-wrap gap-2 text-xs">
          {[['Bollinger', showBB, setShowBB], ['Crossovers', showCrossovers, setShowCrossovers],
            ['Local Min/Max', showExtrema, setShowExtrema], ['Volume', showVolume, setShowVolume]].map(([label, val, set]) => (
            <button key={label} onClick={() => set(v => !v)}
              className={`badge cursor-pointer ${val ? 'badge-blue' : 'bg-surface-3 text-gray-500'}`}>{label}</button>
          ))}
        </div>
      </div>

      {!symbol && <div className="card text-center text-gray-500 py-12">Select a security to view technical analysis.</div>}
      {symbol && isLoading && <LoadingOverlay label={`Loading ${symbol}…`} />}
      {symbol && error && <ErrorMsg error={error} />}

      {series.length > 0 && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <MetricCard label="Volatility" value={metrics.volatility != null ? fmt.pct(metrics.volatility) : '—'} color={metrics.volatility > 0.3 ? 'text-warning' : ''} />
            <MetricCard label="Sharpe" value={metrics.sharpe != null ? metrics.sharpe.toFixed(2) : '—'} color={metrics.sharpe < 0 ? 'text-red-400' : ''} />
            {metrics.sortino != null && <MetricCard label="Sortino" value={metrics.sortino.toFixed(2)} />}
            <MetricCard label="Max DD" value={metrics.max_drawdown != null ? fmt.pct(metrics.max_drawdown) : '—'} color="text-red-400" />
          </div>

          {/* Price chart */}
          <div className="card p-2">
            <LazyPlot
              data={[
                {
                  type: 'candlestick', x: dates, open: opens, high: highs, low: lows, close: closes, name: symbol,
                  increasing: { line: { color: '#22c55e' }, fillcolor: '#22c55e' },
                  decreasing: { line: { color: '#ef4444' }, fillcolor: '#ef4444' },
                },
                ...smaPeriods.map(w => ({
                  x: dates, y: series.map(r => r[`sma_${w}`]), type: 'scatter', name: `SMA ${w}`,
                  line: { color: SMA_COLORS[w] || '#f59e0b', width: 1.3, dash: 'dash' },
                })),
                ...emaPeriods.map(e => ({
                  x: dates, y: series.map(r => r[`ema_${e}`]), type: 'scatter', name: `EMA ${e}`,
                  line: { color: EMA_COLORS[e] || '#a78bfa', width: 1.3, dash: 'dot' },
                })),
                ...(showBB && series[0]?.bb_upper != null ? [
                  { x: dates, y: series.map(r => r.bb_upper), type: 'scatter', name: 'BB Upper', line: { color: '#38bdf8', width: 1, dash: 'dot' } },
                  { x: dates, y: series.map(r => r.bb_lower), type: 'scatter', name: 'BB Lower', fill: 'tonexty', fillcolor: 'rgba(56,189,248,0.05)', line: { color: '#38bdf8', width: 1, dash: 'dot' } },
                ] : []),
                ...(showCrossovers && crossovers.buy.length ? [{
                  x: crossovers.buy.map(c => c.date), y: crossovers.buy.map(c => c.price),
                  type: 'scatter', mode: 'markers', name: 'Buy Signal',
                  marker: { color: '#22c55e', size: 11, symbol: 'triangle-up', line: { color: '#0a0a0f', width: 1 } },
                }] : []),
                ...(showCrossovers && crossovers.sell.length ? [{
                  x: crossovers.sell.map(c => c.date), y: crossovers.sell.map(c => c.price),
                  type: 'scatter', mode: 'markers', name: 'Sell Signal',
                  marker: { color: '#ef4444', size: 11, symbol: 'triangle-down', line: { color: '#0a0a0f', width: 1 } },
                }] : []),
                ...(showExtrema && extrema.max.length ? [{
                  x: extrema.max.map(c => c.date), y: extrema.max.map(c => c.price),
                  type: 'scatter', mode: 'markers', name: 'Local Max',
                  marker: { color: '#facc15', size: 7, symbol: 'circle' },
                }] : []),
                ...(showExtrema && extrema.min.length ? [{
                  x: extrema.min.map(c => c.date), y: extrema.min.map(c => c.price),
                  type: 'scatter', mode: 'markers', name: 'Local Min',
                  marker: { color: '#38bdf8', size: 7, symbol: 'circle' },
                }] : []),
              ]}
              layout={{
                ...BASE, title: { text: symbol, font: { color: '#d1d5db', size: 14 } },
                xaxis: { ...BASE.xaxis, rangeslider: { visible: false } },
                yaxis: { ...BASE.yaxis, tickprefix: '€' },
                legend: { bgcolor: 'transparent', font: { color: '#9ca3af', size: 10 }, orientation: 'h', y: -0.15 },
                height: 460,
              }}
              config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
            />
            {showCrossovers && (crossovers.buy.length > 0 || crossovers.sell.length > 0) && (
              <p className="text-xs text-gray-600 mt-1 px-2">
                ▲ {crossovers.buy.length} buy signals, ▼ {crossovers.sell.length} sell signals (first SMA × first EMA, or first two SMAs).
              </p>
            )}
          </div>

          {showVolume && (
            <div className="card p-2">
              <LazyPlot
                data={[{
                  type: 'bar', x: dates, y: volumes, name: 'Volume',
                  marker: { color: closes.map((c, i) => i > 0 && c >= closes[i - 1] ? 'rgba(34,197,94,0.5)' : 'rgba(239,68,68,0.5)') },
                }]}
                layout={{ ...BASE, title: { text: 'Volume', font: { color: '#d1d5db', size: 13 } }, height: 160 }}
                config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
              />
            </div>
          )}

          {rsi[0] != null && (
            <div className="card p-2">
              <LazyPlot
                data={[{ type: 'scatter', x: dates, y: rsi, name: 'RSI', line: { color: '#a78bfa', width: 1.5 } }]}
                layout={{
                  ...BASE, title: { text: 'RSI (14)', font: { color: '#d1d5db', size: 13 } },
                  yaxis: { ...BASE.yaxis, range: [0, 100] },
                  shapes: [
                    { type: 'line', x0: dates[0], x1: dates[dates.length - 1], y0: 70, y1: 70, line: { color: '#ef4444', dash: 'dot', width: 1 } },
                    { type: 'line', x0: dates[0], x1: dates[dates.length - 1], y0: 30, y1: 30, line: { color: '#22c55e', dash: 'dot', width: 1 } },
                  ],
                  height: 200,
                }}
                config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
              />
            </div>
          )}

          <Expander title="Return Distribution">
            <ReturnDistribution symbol={symbol} closes={closes} />
          </Expander>

          <Expander title="Fundamentals & Financial Overview" defaultOpen>
            {basic ? <FundamentalsGrid basic={basic} metrics={metrics} /> : <LoadingOverlay label="Loading fundamentals…" />}
          </Expander>
        </>
      )}
    </div>
  )
}

export default function TabTechnical() {
  const [mode, setMode] = useState('deep-dive')
  const { data: securities = [] } = useQuery({ queryKey: qk.securities(), queryFn: securitiesApi.list })

  return (
    <div className="space-y-4">
      <SectionHeader title="Technical Analysis" action={
        <div className="flex gap-1">
          <button className={`btn text-xs px-3 py-1.5 ${mode === 'deep-dive' ? 'btn-primary' : 'btn-ghost'}`}
            onClick={() => setMode('deep-dive')}>Deep Dive</button>
          <button className={`btn text-xs px-3 py-1.5 ${mode === 'compare' ? 'btn-primary' : 'btn-ghost'}`}
            onClick={() => setMode('compare')}>Compare</button>
        </div>
      } />
      {mode === 'deep-dive'
        ? <TechnicalDeepDive securities={securities} />
        : <TechnicalCompare securities={securities} />}
    </div>
  )
}
