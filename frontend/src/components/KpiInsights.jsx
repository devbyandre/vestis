import { useState } from 'react'
import { clsx } from 'clsx'
import { CheckCircle2, MinusCircle, AlertTriangle, Info } from 'lucide-react'
import { fmt } from '../lib/utils'
import { evaluateGroups, insightContext, pricePosition, compactEur } from '../lib/kpiInsights'

// Rating → colour + icon. The word next to the icon carries the meaning, so
// it never relies on colour alone.
const RATING = {
  good: { icon: CheckCircle2, text: 'text-green-400', badge: 'badge-green' },
  ok:   { icon: MinusCircle,  text: 'text-amber-400', badge: 'badge-yellow' },
  bad:  { icon: AlertTriangle, text: 'text-red-400',  badge: 'badge-red' },
}

function RatingTag({ rating, word }) {
  if (!rating) return null
  const { icon: Icon, text } = RATING[rating]
  return (
    <span className={clsx('inline-flex items-center gap-1 text-xs whitespace-nowrap', text)}>
      <Icon size={12} /> {word}
    </span>
  )
}

function CashDebtBar({ basic }) {
  const cash = Number(basic.totalCash), debt = Number(basic.totalDebt)
  if (!(cash >= 0) || !(debt >= 0) || cash + debt === 0) return null
  const max = Math.max(cash, debt)
  return (
    <div className="space-y-1 mt-1 text-xs text-gray-400">
      {[['Cash', cash, 'bg-green-500/60'], ['Debt', debt, 'bg-red-500/60']].map(([label, v, color]) => (
        <div key={label} className="flex items-center gap-2">
          <span className="w-9">{label}</span>
          <div className="flex-1 h-2 rounded bg-surface-3 overflow-hidden">
            <div className={clsx('h-full rounded', color)} style={{ width: `${(v / max) * 100}%` }} />
          </div>
          <span className="w-20 text-right tabular-nums">{compactEur(v)}</span>
        </div>
      ))}
    </div>
  )
}

function KpiTile({ kpi, basic, explain }) {
  return (
    <div className="metric-card">
      <div className="flex flex-wrap items-center justify-between gap-x-2 gap-y-0.5">
        <span className="metric-label inline-flex items-center gap-1" title={kpi.hint}>
          {kpi.label} <Info size={11} className="text-gray-600" />
        </span>
        <RatingTag rating={kpi.rating} word={kpi.word} />
      </div>
      <div className={clsx('font-semibold tabular-nums', kpi.kind === 'cashDebt' ? 'text-sm text-gray-200' : 'text-xl')}>
        {kpi.display}
      </div>
      {kpi.kind === 'cashDebt' && <CashDebtBar basic={basic} />}
      {explain && <p className="text-xs text-gray-500 leading-snug">{kpi.hint}</p>}
    </div>
  )
}

function PricePosition({ basic, closes, dates, sma200, periodLabel }) {
  const { range, trend, peak } = pricePosition({ basic, closes, dates, sma200 })
  if (!range && !trend && !peak) return null
  return (
    <div className="card space-y-3">
      <div>
        <p className="text-sm font-semibold text-gray-200">Where the price stands</p>
        <p className="text-xs text-gray-500">Is it near its highs or lows, and above or below its long-term trend?</p>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {range && (
          <div>
            <p className="metric-label mb-2" title="Today’s price within the lowest and highest price of the last 52 weeks.">
              52-week range
            </p>
            <div className="relative h-2 rounded bg-gradient-to-r from-red-500/40 via-surface-4 to-green-500/40">
              <div className="absolute -top-1 h-4 w-1 rounded bg-gray-100" style={{ left: `calc(${range.pos * 100}% - 2px)` }} />
            </div>
            <div className="flex justify-between text-xs text-gray-500 mt-1 tabular-nums">
              <span>{fmt.currency(range.lo, 2)}</span>
              <span className="text-gray-300">{fmt.currency(range.price, 2)}</span>
              <span>{fmt.currency(range.hi, 2)}</span>
            </div>
            <p className="text-xs text-gray-400 mt-1">
              {fmt.pct(range.fromHigh)} from the high, {fmt.pct(range.fromLow)} above the low
            </p>
          </div>
        )}
        {trend && (
          <div>
            <p className="metric-label mb-1" title="The 200-day moving average is the usual yardstick for the long-term trend. Above it = uptrend, below = downtrend.">
              vs. 200-day average
            </p>
            <p className={clsx('text-xl font-semibold tabular-nums', trend.vsSma >= 0 ? 'text-green-400' : 'text-red-400')}>
              {fmt.pct(trend.vsSma)}
            </p>
            <p className="text-xs text-gray-400">
              {trend.vsSma >= 0 ? 'Above' : 'Below'} its long-term trend ({fmt.currency(trend.sma, 2)})
              {Math.abs(trend.vsSma) > 0.2 && ' — unusually far, moves back towards the average are common'}
            </p>
          </div>
        )}
        {peak && (
          <div>
            <p className="metric-label mb-1" title="How far the latest close is below the highest close in the selected period.">
              From {periodLabel} high
            </p>
            <p className={clsx('text-xl font-semibold tabular-nums', peak.fromPeak > -0.05 ? 'text-gray-200' : 'text-red-400')}>
              {peak.fromPeak > -0.005 ? 'at the high' : fmt.pct(peak.fromPeak)}
            </p>
            <p className="text-xs text-gray-400">
              Peak {fmt.currency(peak.price, 2)} on {new Date(peak.date).toLocaleDateString('de-DE')}
            </p>
          </div>
        )}
      </div>
    </div>
  )
}

export default function KpiInsights({ basic, metrics, closes, dates, sma200, periodLabel, periodDays }) {
  const [explain, setExplain] = useState(false)
  const groups = evaluateGroups(insightContext({ basic, metrics, closes, periodDays }))

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between gap-2">
        <p className="text-xs text-gray-500">
          Ratings are general rules of thumb — compare with peers in the same sector. Hover a label for its meaning.
        </p>
        <button className={`btn text-xs px-2 py-1 whitespace-nowrap ${explain ? 'btn-primary' : 'btn-ghost'}`}
          onClick={() => setExplain(e => !e)}>
          <Info size={12} /> {explain ? 'Hide explanations' : 'Explain all'}
        </button>
      </div>

      <PricePosition basic={basic || {}} closes={closes} dates={dates} sma200={sma200} periodLabel={periodLabel} />

      {groups.map(g => {
        const shown = g.kpis.filter(k => k.value != null)
        if (!shown.length) return null
        const v = g.verdict && RATING[g.verdict.level]
        return (
          <div key={g.id} className="card space-y-3">
            <div className="flex flex-wrap items-start justify-between gap-2">
              <div>
                <p className="text-sm font-semibold text-gray-200">
                  {g.title}{g.periodBased && <span className="text-gray-500 font-normal"> · {periodLabel}</span>}
                </p>
                <p className="text-xs text-gray-500">{g.question}</p>
              </div>
              {v && (
                <span className={clsx('badge inline-flex items-center gap-1', v.badge)}
                  title={`Summary of the ${g.verdict.basis} rated figures below`}>
                  <v.icon size={12} /> {g.verdict.label}
                </span>
              )}
            </div>
            <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-5 gap-3">
              {shown.map(k => <KpiTile key={k.label} kpi={k} basic={basic || {}} explain={explain} />)}
            </div>
          </div>
        )
      })}
    </div>
  )
}
