// Turns raw fundamentals (securities cache, already in EUR) and the price-based
// risk metrics into ratios with a rating, a plain-language hint and a verdict
// per group. Thresholds are generic rules of thumb, not sector-aware — the
// hints say so where it matters (banks, growth stocks).
import { fmt } from './utils'

const num = v => (v == null || v === '' || isNaN(Number(v)) ? null : Number(v))
const ratio = (a, b) => {
  const x = num(a), y = num(b)
  return x != null && y != null && y !== 0 ? x / y : null
}

// Lower is better: v < good → good, v <= ok → ok, else bad
const lowerBetter = (good, ok) => v => v == null ? null : v < good ? 'good' : v <= ok ? 'ok' : 'bad'
// Higher is better: v > good → good, v >= ok → ok, else bad
const higherBetter = (good, ok) => v => v == null ? null : v > good ? 'good' : v >= ok ? 'ok' : 'bad'

const x1 = v => v == null ? '—' : `${v.toFixed(1)}×`
const x2 = v => v == null ? '—' : v.toFixed(2)
const pct = (v, d = 1) => v == null ? '—' : fmt.pct(v, d).replace('+', '')
const signedPct = (v, d = 1) => v == null ? '—' : fmt.pct(v, d)

const compactEur = v => new Intl.NumberFormat('de-DE', {
  style: 'currency', currency: 'EUR', notation: 'compact', maximumFractionDigits: 1,
}).format(v)

const VALUATION_WORDS = { good: 'cheap', ok: 'fair', bad: 'expensive' }

export { compactEur }

export const KPI_GROUPS = [
  {
    id: 'valuation',
    title: 'Valuation',
    question: 'How much am I paying for the business?',
    verdicts: { good: 'Looks cheap', ok: 'Fairly valued', bad: 'Looks expensive' },
    kpis: [
      {
        label: 'P/E', value: ({ basic }) => { const v = num(basic.trailingPE); return v != null && v > 0 ? v : null },
        format: x1, rate: lowerBetter(15, 25), words: VALUATION_WORDS,
        hint: 'Price ÷ last 12 months’ earnings per share — how many years of today’s profit you pay for. <15 cheap · 15–25 typical · >25 expensive (fast growers are often higher). Empty when the company makes a loss.',
      },
      {
        label: 'Forward P/E', value: ({ basic }) => { const v = num(basic.forwardPE); return v != null && v > 0 ? v : null },
        format: x1, rate: lowerBetter(15, 25), words: VALUATION_WORDS,
        hint: 'Like P/E, but on analysts’ expected earnings for the next 12 months. Lower than the P/E means profits are expected to grow.',
      },
      {
        // EV built from market cap + debt − cash: Yahoo's own enterpriseValue
        // is wrong for ADRs (TSM: reporting-currency EV off by the ADR ratio).
        label: 'EV / EBITDA', value: ({ basic }) => {
          const e = num(basic.ebitda)
          if (!(e > 0)) return null
          const mc = num(basic.marketCap), debt = num(basic.totalDebt), cash = num(basic.totalCash)
          const ev = mc != null && debt != null && cash != null ? mc + debt - cash : num(basic.enterpriseValue)
          return ratio(ev, e)
        },
        format: x1, rate: lowerBetter(10, 15), words: VALUATION_WORDS,
        hint: 'Company value incl. debt ÷ operating profit before depreciation. Unlike P/E it isn’t distorted by debt or tax. <10 cheap · 10–15 typical · >15 expensive. Not meaningful for banks/insurers.',
      },
      {
        label: 'Price / Sales', value: ({ basic }) => ratio(basic.marketCap, basic.totalRevenue),
        format: x1, rate: lowerBetter(2, 5), words: VALUATION_WORDS,
        hint: 'Market cap ÷ yearly revenue — what you pay per €1 of sales. Useful when there are no profits yet. <2 cheap · 2–5 typical · >5 expensive (software is usually higher).',
      },
      {
        label: 'Price / Book', value: ({ basic }) => ratio(basic.regularMarketPrice, num(basic.bookValue) > 0 ? basic.bookValue : null),
        format: x2, rate: lowerBetter(1.5, 3), words: VALUATION_WORDS,
        hint: 'Share price ÷ net assets per share. <1 means you pay less than the accounting value of the assets. Most useful for banks and asset-heavy firms.',
      },
      {
        label: 'FCF yield', value: ({ basic }) => ratio(basic.freeCashflow, basic.marketCap),
        format: v => pct(v), rate: higherBetter(0.05, 0.02), words: VALUATION_WORDS,
        hint: 'Free cash flow ÷ market cap — the cash the business generates per €100 you invest, after investments. >5% attractive · 2–5% typical · <2% expensive or cash-hungry.',
      },
      {
        label: 'Dividend yield', value: ({ basic }) => num(basic.dividendYield),
        format: v => pct(v, 2), rate: v => v == null ? null : v > 0.03 ? 'good' : null, words: { good: 'high' },
        noVerdict: true,
        hint: 'Yearly dividend ÷ share price. >3% is high. Not part of the verdict — many good companies pay little and reinvest instead.',
      },
    ],
  },
  {
    id: 'quality',
    title: 'Profitability',
    question: 'How good is the business at making money?',
    verdicts: { good: 'Highly profitable', ok: 'Average profitability', bad: 'Weak profitability' },
    kpis: [
      {
        label: 'Gross margin', value: ({ basic }) => ratio(basic.grossProfits, basic.totalRevenue),
        format: v => pct(v), rate: higherBetter(0.4, 0.2),
        hint: 'What’s left of each €1 of sales after direct production costs. >40% suggests pricing power (brands, software); <20% is typical for retail and commodities.',
      },
      {
        label: 'Operating margin', value: ({ basic }) => num(basic.operatingMargins),
        format: v => pct(v), rate: higherBetter(0.15, 0.05),
        hint: 'Profit from the core business per €1 of sales, before interest and tax. >15% strong · 5–15% average · <5% thin.',
      },
      {
        label: 'Net margin', value: ({ basic }) => num(basic.profitMargins),
        format: v => pct(v), rate: higherBetter(0.2, 0.1),
        hint: 'Bottom-line profit per €1 of sales. >20% strong · 10–20% solid · <10% thin (same thresholds as the Portfolio KPI table).',
      },
      {
        label: 'Return on equity', value: ({ basic }) => num(basic.returnOnEquity),
        format: v => pct(v), rate: higherBetter(0.15, 0.08),
        hint: 'Profit ÷ shareholders’ equity — how well management turns your capital into profit. >15% strong · 8–15% average. Very high values can come from lots of debt.',
      },
      {
        label: 'FCF margin', value: ({ basic }) => ratio(basic.freeCashflow, basic.totalRevenue),
        format: v => pct(v), rate: higherBetter(0.1, 0.03),
        hint: 'Free cash flow per €1 of sales — profit that actually arrives as cash. >10% strong · 3–10% average · <3% weak.',
      },
    ],
  },
  {
    id: 'health',
    title: 'Financial health',
    question: 'Can it carry its debt?',
    verdicts: { good: 'Solid balance sheet', ok: 'Acceptable debt', bad: 'Stretched balance sheet' },
    kpis: [
      {
        label: 'Net debt / EBITDA',
        value: ({ basic }) => {
          const debt = num(basic.totalDebt), cash = num(basic.totalCash), e = num(basic.ebitda)
          if (debt == null || cash == null || e == null) return null
          const net = debt - cash
          if (net <= 0) return 0          // net cash
          return e > 0 ? net / e : Infinity // debt but no operating profit
        },
        format: v => v == null ? '—' : v === 0 ? 'net cash' : v === Infinity ? 'n/a' : x1(v),
        rate: v => v == null ? null : v < 2 ? 'good' : v <= 3.5 ? 'ok' : 'bad',
        hint: 'Years of operating profit needed to pay off debt minus cash. <2 comfortable · 2–3.5 manageable · >3.5 heavy. “net cash” means more cash than debt. Not meaningful for banks.',
      },
      {
        label: 'Current ratio', value: ({ basic }) => num(basic.currentRatio),
        format: x2, rate: higherBetter(1.5, 1),
        hint: 'Short-term assets ÷ short-term liabilities. <1 means bills due within a year exceed what it can turn into cash in that time.',
      },
      {
        label: 'Cash vs debt', kind: 'cashDebt',
        value: ({ basic }) => num(basic.totalDebt) != null && num(basic.totalCash) != null ? num(basic.totalCash) - num(basic.totalDebt) : null,
        format: v => v == null ? '—' : `${v >= 0 ? 'Net cash' : 'Net debt'} ${compactEur(Math.abs(v))}`,
        rate: () => null,
        hint: 'Cash on hand compared with total borrowings.',
      },
    ],
  },
  {
    id: 'risk',
    title: 'Risk & return',
    question: 'How has the share behaved?',
    periodBased: true,
    verdicts: { good: 'Good risk / return', ok: 'Mixed risk / return', bad: 'Poor risk / return' },
    kpis: [
      {
        // Annualising a few months exaggerates wildly (-27% in 4 months → -55% p.a.),
        // so periods under a year show the plain total return.
        label: 'Return',
        value: ({ metrics, periodReturn, annualise }) => annualise ? num(metrics.cagr) : num(periodReturn),
        format: (v, { annualise } = {}) => v == null ? '—' : `${signedPct(v)}${annualise ? ' p.a.' : ''}`,
        rate: v => v == null ? null : v > 0.08 ? 'good' : v >= 0 ? 'ok' : 'bad',
        hint: 'Price return over the selected period, per year for 1Y and longer (dividends not included). The long-term stock market average is ~7–8% p.a.',
      },
      {
        label: 'Volatility', value: ({ metrics }) => num(metrics.volatility),
        format: v => pct(v), rate: lowerBetter(0.2, 0.35), words: { good: 'calm', ok: 'moderate', bad: 'jumpy' },
        hint: 'How much the price typically swings in a year. ~15% is a broad index, 20–35% a typical single stock, >35% is a wild ride.',
      },
      {
        label: 'Sharpe', value: ({ metrics }) => num(metrics.sharpe),
        format: x2, rate: v => v == null ? null : v > 1 ? 'good' : v >= 0 ? 'ok' : 'bad',
        hint: 'Return per unit of volatility — were the swings worth it? >1 good · 0–1 so-so · <0 you were paid nothing for the risk.',
      },
      {
        label: 'Max drawdown', value: ({ metrics }) => metrics.max_drawdown != null ? -Math.abs(num(metrics.max_drawdown)) : null,
        format: v => signedPct(v), rate: v => v == null ? null : v > -0.2 ? 'good' : v >= -0.35 ? 'ok' : 'bad',
        words: { good: 'mild', ok: 'notable', bad: 'deep' },
        hint: 'Worst fall from a high to a later low in the period — the pain you would have had to sit through.',
      },
      {
        label: 'Beta', value: ({ basic }) => num(basic.beta),
        format: x2, rate: lowerBetter(1, 1.2), words: { good: 'defensive', ok: 'market-like', bad: 'amplifies market' },
        hint: 'Sensitivity to the overall market: 1 moves with it, 1.5 moves 50% more, 0.5 half as much. Based on several years, not the selected period.',
      },
    ],
  },
]

const DEFAULT_WORDS = { good: 'good', ok: 'fair', bad: 'weak' }

export function evaluateKpi(kpi, ctx) {
  let value = null
  try { value = kpi.value(ctx) } catch { value = null }
  if (value != null && Number.isNaN(value)) value = null
  const rating = value == null ? null : kpi.rate(value)
  return { ...kpi, value, display: kpi.format(value, ctx), rating, word: rating ? (kpi.words ?? DEFAULT_WORDS)[rating] : null }
}

// good = +1, ok = 0, bad = -1 averaged over the rated KPIs; needs ≥2 ratings
// so a single number doesn't decide the whole group.
export function groupVerdict(group, evaluated) {
  const rated = evaluated.filter(k => k.rating && !k.noVerdict)
  if (rated.length < 2) return null
  const score = rated.reduce((s, k) => s + (k.rating === 'good' ? 1 : k.rating === 'bad' ? -1 : 0), 0) / rated.length
  const level = score > 0.3 ? 'good' : score < -0.3 ? 'bad' : 'ok'
  return { level, label: group.verdicts[level], basis: rated.length }
}

// closes = chart series for the selected period; periodDays = lookback length
export function insightContext({ basic, metrics, closes = [], periodDays = 365 }) {
  const valid = closes.filter(c => c != null)
  const periodReturn = valid.length > 1 ? valid[valid.length - 1] / valid[0] - 1 : null
  return { basic: basic || {}, metrics: metrics || {}, periodReturn, annualise: periodDays >= 365 }
}

export function evaluateGroups(ctx) {
  return KPI_GROUPS.map(g => {
    const kpis = g.kpis.map(k => evaluateKpi(k, ctx))
    return { ...g, kpis, verdict: groupVerdict(g, kpis) }
  })
}

// Where the price stands: 52-week range, long-term trend and distance from the
// period high. closes/dates are the chart series (adjusted, EUR).
export function pricePosition({ basic = {}, closes = [], dates = [], sma200 = null }) {
  const price = num(basic.regularMarketPrice)
  const lo = num(basic.fiftyTwoWeekLow), hi = num(basic.fiftyTwoWeekHigh)
  const range = price != null && lo != null && hi != null && hi > lo
    ? { lo, hi, price, pos: Math.min(1, Math.max(0, (price - lo) / (hi - lo))), fromHigh: price / hi - 1, fromLow: price / lo - 1 }
    : null

  const valid = closes.map((c, i) => [c, dates[i]]).filter(([c]) => c != null)
  const last = valid.length ? valid[valid.length - 1][0] : null

  let trend = null
  let sma = sma200
  if (sma == null && valid.length >= 200) sma = valid.slice(-200).reduce((s, [c]) => s + c, 0) / 200
  if (sma != null && last != null) trend = { sma, vsSma: last / sma - 1 }

  let peak = null
  if (valid.length) {
    const [maxC, maxD] = valid.reduce((m, cur) => (cur[0] > m[0] ? cur : m), valid[0])
    peak = { price: maxC, date: maxD, fromPeak: last / maxC - 1 }
  }
  return { range, trend, peak }
}
