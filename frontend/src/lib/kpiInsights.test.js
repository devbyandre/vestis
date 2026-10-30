import { describe, it, expect } from 'vitest'
import { evaluateGroups, insightContext, pricePosition } from './kpiInsights'

const group = (groups, id) => groups.find(g => g.id === id)
const kpi = (groups, id, label) => group(groups, id).kpis.find(k => k.label === label)

describe('evaluateGroups', () => {
  it('derives ratios from raw fundamentals and rates them', () => {
    const groups = evaluateGroups({
      basic: { trailingPE: 12, enterpriseValue: 200, ebitda: 10, marketCap: 100, totalRevenue: 20,
               freeCashflow: 8, totalDebt: 30, totalCash: 50, grossProfits: 10 },
      metrics: {},
    })
    expect(kpi(groups, 'valuation', 'P/E')).toMatchObject({ value: 12, rating: 'good', word: 'cheap' })
    // EV = market cap 100 + debt 30 − cash 50 = 80 → 8× EBITDA (Yahoo's EV of 200 is ignored)
    expect(kpi(groups, 'valuation', 'EV / EBITDA')).toMatchObject({ value: 8, rating: 'good', word: 'cheap' })
    expect(kpi(groups, 'valuation', 'Price / Sales').value).toBe(5)
    expect(kpi(groups, 'valuation', 'FCF yield')).toMatchObject({ value: 0.08, rating: 'good' })
    expect(kpi(groups, 'health', 'Net debt / EBITDA')).toMatchObject({ display: 'net cash', rating: 'good' })
    expect(kpi(groups, 'quality', 'Gross margin').value).toBe(0.5)
  })

  it('hides P/E for loss-making companies and needs two ratings for a verdict', () => {
    const groups = evaluateGroups({ basic: { trailingPE: -4, forwardPE: 10 }, metrics: {} })
    expect(kpi(groups, 'valuation', 'P/E').value).toBeNull()
    expect(group(groups, 'valuation').verdict).toBeNull()
  })

  it('summarises a group and ignores the dividend in the verdict', () => {
    const groups = evaluateGroups({ basic: { trailingPE: 40, forwardPE: 35, dividendYield: 0.05 }, metrics: {} })
    expect(group(groups, 'valuation').verdict).toMatchObject({ level: 'bad', label: 'Looks expensive', basis: 2 })
  })
})

describe('pricePosition', () => {
  it('places the price in its 52-week range and finds the period high', () => {
    const { range, peak, trend } = pricePosition({
      basic: { regularMarketPrice: 75, fiftyTwoWeekLow: 50, fiftyTwoWeekHigh: 100 },
      closes: [80, 100, 90], dates: ['2026-01-01', '2026-02-01', '2026-03-01'], sma200: 60,
    })
    expect(range.pos).toBe(0.5)
    expect(range.fromHigh).toBeCloseTo(-0.25)
    expect(peak).toMatchObject({ price: 100, date: '2026-02-01' })
    expect(peak.fromPeak).toBeCloseTo(-0.1)
    expect(trend.vsSma).toBeCloseTo(0.5)
  })
})

describe('insightContext', () => {
  it('shows total return under a year and CAGR from a year on', () => {
    const short = evaluateGroups(insightContext({ metrics: { cagr: -0.55 }, closes: [100, 73], periodDays: 180 }))
    expect(kpi(short, 'risk', 'Return')).toMatchObject({ display: '-27.0%' })
    const long = evaluateGroups(insightContext({ metrics: { cagr: 0.1 }, closes: [100, 73], periodDays: 365 }))
    expect(kpi(long, 'risk', 'Return').display).toBe('+10.0% p.a.')
  })
})
