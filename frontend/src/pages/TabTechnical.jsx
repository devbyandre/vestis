import { useState, lazy, Suspense } from 'react'
import { useQuery } from '@tanstack/react-query'
import { analyticsApi, securitiesApi } from '../lib/api'
import { qk } from '../lib/queryClient'
import { fmt, plotlyConfig } from '../lib/utils'
import { LoadingOverlay, ErrorMsg, MetricCard, SectionHeader } from '../components/ui'

const Plot = lazy(() => import('react-plotly.js'))
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

export default function TabTechnical() {
  const [symbol, setSymbol] = useState('')
  const [lookback, setLookback] = useState(365)
  const [smaPeriods, setSmaPeriods] = useState([50, 200])
  const [emaPeriods, setEmaPeriods] = useState([])
  const [showBB, setShowBB] = useState(false)
  const [showCrossovers, setShowCrossovers] = useState(true)
  const [showExtrema, setShowExtrema] = useState(false)
  const [showVolume, setShowVolume] = useState(true)

  const { data: securities = [] } = useQuery({ queryKey: qk.securities(), queryFn: securitiesApi.list })

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
      <SectionHeader title="Technical Analysis" />

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
        </>
      )}
    </div>
  )
}
