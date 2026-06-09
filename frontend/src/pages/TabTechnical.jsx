import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import Plot from 'react-plotly.js'
import { analyticsApi, securitiesApi } from '../lib/api'
import { qk } from '../lib/queryClient'
import { fmt, plotlyConfig } from '../lib/utils'
import { LoadingOverlay, ErrorMsg, MetricCard, Expander, SectionHeader } from '../components/ui'

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
  { value: 30, label: '1M' }, { value: 90, label: '3M' },
  { value: 180, label: '6M' }, { value: 365, label: '1Y' },
  { value: 730, label: '2Y' }, { value: 1825, label: '5Y' },
]

export default function TabTechnical() {
  const [symbol, setSymbol] = useState('')
  const [lookback, setLookback] = useState(365)
  const [showBB, setShowBB] = useState(true)
  const [showSMA, setShowSMA] = useState(true)
  const [showEMA, setShowEMA] = useState(false)
  const [showVolume, setShowVolume] = useState(true)

  const { data: securities = [] } = useQuery({ queryKey: qk.securities(), queryFn: securitiesApi.list })

  const enabled = !!symbol
  const { data: result, isLoading, error } = useQuery({
    queryKey: qk.indicators(symbol, { lookback_days: lookback }),
    queryFn: () => analyticsApi.indicators(symbol, { lookback_days: lookback }),
    enabled,
  })

  const series = result?.series || []
  const metrics = result?.metrics || {}

  const dates = series.map(r => r.date)
  const closes = series.map(r => r.adj_close ?? r.close)
  const opens = series.map(r => r.open)
  const highs = series.map(r => r.high)
  const lows = series.map(r => r.low)
  const volumes = series.map(r => r.volume)
  const rsi14 = series.map(r => r.rsi_14)
  const sma20 = series.map(r => r.sma_20)
  const ema20 = series.map(r => r.ema_20)
  const bbUpper = series.map(r => r.bb_upper)
  const bbLower = series.map(r => r.bb_lower)
  const bbMid = series.map(r => r.bb_mid)

  return (
    <div className="space-y-4">
      <SectionHeader title="Technical Analysis" />

      {/* Controls */}
      <div className="card flex flex-wrap gap-4 items-end">
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
              onClick={() => setLookback(lb.value)}>
              {lb.label}
            </button>
          ))}
        </div>
        <div className="flex gap-2 text-xs">
          {[['SMA 20', showSMA, setShowSMA], ['EMA 20', showEMA, setShowEMA],
            ['Bollinger', showBB, setShowBB], ['Volume', showVolume, setShowVolume]].map(([label, val, set]) => (
            <button key={label} onClick={() => set(v => !v)}
              className={`badge cursor-pointer ${val ? 'badge-blue' : 'bg-surface-3 text-gray-500'}`}>
              {label}
            </button>
          ))}
        </div>
      </div>

      {!symbol && (
        <div className="card text-center text-gray-500 py-12">Select a security to view technical analysis.</div>
      )}

      {symbol && isLoading && <LoadingOverlay label={`Loading ${symbol}…`} />}
      {symbol && error && <ErrorMsg error={error} />}

      {series.length > 0 && (
        <>
          {/* Risk metrics */}
          <div className="grid grid-cols-2 md:grid-cols-6 gap-3">
            {[
              ['Sharpe', metrics.sharpe, 2],
              ['Sortino', metrics.sortino, 2],
              ['Calmar', metrics.calmar, 2],
              ['Max DD', metrics.max_drawdown, 1, '%', 100],
              ['Volatility', metrics.volatility, 1, '%', 100],
              ['CAGR', metrics.cagr, 1, '%', 100],
            ].map(([label, v, dec, suf = '', mul = 1]) => (
              <MetricCard key={label} label={label}
                value={v != null ? `${(v * mul).toFixed(dec)}${suf}` : '—'}
                color={v != null && v < 0 ? 'text-red-400' : ''} />
            ))}
          </div>

          {/* Candlestick / price chart */}
          <div className="card p-2">
            <Plot
              data={[
                {
                  type: 'candlestick',
                  x: dates, open: opens, high: highs, low: lows, close: closes,
                  name: symbol,
                  increasing: { line: { color: '#22c55e' }, fillcolor: '#22c55e' },
                  decreasing: { line: { color: '#ef4444' }, fillcolor: '#ef4444' },
                },
                ...(showSMA && sma20[0] != null ? [{ x: dates, y: sma20, type: 'scatter', name: 'SMA 20', line: { color: '#f59e0b', width: 1.5 } }] : []),
                ...(showEMA && ema20[0] != null ? [{ x: dates, y: ema20, type: 'scatter', name: 'EMA 20', line: { color: '#a78bfa', width: 1.5 } }] : []),
                ...(showBB && bbUpper[0] != null ? [
                  { x: dates, y: bbUpper, type: 'scatter', name: 'BB Upper', line: { color: '#38bdf8', width: 1, dash: 'dot' } },
                  { x: dates, y: bbMid, type: 'scatter', name: 'BB Mid', line: { color: '#38bdf8', width: 1 } },
                  { x: dates, y: bbLower, type: 'scatter', name: 'BB Lower', fill: 'tonexty', fillcolor: 'rgba(56,189,248,0.05)', line: { color: '#38bdf8', width: 1, dash: 'dot' } },
                ] : []),
              ]}
              layout={{
                ...BASE,
                title: { text: symbol, font: { color: '#d1d5db', size: 14 } },
                xaxis: { ...BASE.xaxis, rangeslider: { visible: false } },
                yaxis: { ...BASE.yaxis, tickprefix: '€' },
                legend: { bgcolor: 'transparent', font: { color: '#9ca3af', size: 10 } },
                height: 420,
              }}
              config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
            />
          </div>

          {/* Volume */}
          {showVolume && (
            <div className="card p-2">
              <Plot
                data={[{
                  type: 'bar', x: dates, y: volumes, name: 'Volume',
                  marker: { color: closes.map((c, i) => i > 0 && c >= closes[i - 1] ? 'rgba(34,197,94,0.5)' : 'rgba(239,68,68,0.5)') },
                }]}
                layout={{ ...BASE, title: { text: 'Volume', font: { color: '#d1d5db', size: 13 } }, height: 160 }}
                config={plotlyConfig} style={{ width: '100%' }} useResizeHandler
              />
            </div>
          )}

          {/* RSI */}
          {rsi14[0] != null && (
            <div className="card p-2">
              <Plot
                data={[{ type: 'scatter', x: dates, y: rsi14, name: 'RSI 14', line: { color: '#a78bfa', width: 1.5 } }]}
                layout={{
                  ...BASE,
                  title: { text: 'RSI (14)', font: { color: '#d1d5db', size: 13 } },
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
