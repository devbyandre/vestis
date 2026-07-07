import { http, HttpResponse } from 'msw'
import { setupServer } from 'msw/node'

// Default happy-path (mostly "empty state") responses for every endpoint
// api.js calls. Individual tests can override a handler with server.use(...)
// for a specific case; the point of these defaults is that every Tab can
// render its empty/loading state without the request itself failing.
export const handlers = [
  http.get('/api/portfolios', () => HttpResponse.json([{ id: 1, name: 'Default' }])),
  http.post('/api/portfolios', () => HttpResponse.json({ id: 2, name: 'New' })),
  http.put('/api/portfolios/rename', () => HttpResponse.json({ ok: true })),
  http.delete('/api/portfolios', () => HttpResponse.json({ ok: true })),

  http.get('/api/securities', () => HttpResponse.json([])),
  http.get('/api/securities/all', () => HttpResponse.json([])),
  http.get('/api/securities/symbols', () => HttpResponse.json({ symbols: [] })),
  http.get('/api/securities/:symbol', () => HttpResponse.json({})),
  http.get('/api/securities/:symbol/basic', () => HttpResponse.json({})),
  http.get('/api/securities/:symbol/price/latest', () => HttpResponse.json({ price: null })),
  http.get('/api/securities/:symbol/price/series', () => HttpResponse.json([])),

  http.get('/api/holdings/snapshot', () => HttpResponse.json([])),
  http.get('/api/holdings/timeseries', () => HttpResponse.json([])),
  http.get('/api/holdings/risk-timeseries', () => HttpResponse.json([])),
  http.get('/api/holdings/metrics', () =>
    HttpResponse.json({ volatility: null, sharpe: null, max_drawdown: null })),

  http.get('/api/transactions', () => HttpResponse.json([])),
  http.get('/api/transactions/summary', () => HttpResponse.json({
    count: 0, total_quantity: 0, total_buys: 0, num_buys: 0,
    total_sells: 0, num_sells: 0, total_fees: 0,
  })),
  http.post('/api/transactions', () => HttpResponse.json({ ok: true })),
  http.put('/api/transactions/:id', () => HttpResponse.json({ ok: true })),
  http.delete('/api/transactions/:id', () => HttpResponse.json({ ok: true })),
  http.post('/api/transactions/split', () => HttpResponse.json({ ok: true, portfolios_applied: [] })),

  http.get('/api/analytics/capital-gains', () => HttpResponse.json([])),
  http.get('/api/analytics/dividends', () => HttpResponse.json([])),
  http.get('/api/analytics/revenues-summary', () => HttpResponse.json({
    total_gains: 0, total_dividends: 0, estimated_tax: 0, tax_rate: 0.26375,
    by_year: [], by_security: [], tax_loss_candidates: [],
    capital_gains: [], dividends: [],
  })),
  http.get('/api/analytics/rebalancing', () => HttpResponse.json([])),
  http.get('/api/analytics/indicators/:symbol', () => HttpResponse.json({
    series: [], metrics: {}, crossovers: { buy: [], sell: [] }, extrema: { min: [], max: [] },
  })),
  http.get('/api/analytics/crossovers/:symbol', () => HttpResponse.json([])),
  http.get('/api/analytics/local-extrema/:symbol', () => HttpResponse.json([])),

  http.get('/api/watchlist', () => HttpResponse.json([])),
  http.get('/api/watchlist/symbols', () => HttpResponse.json({ symbols: [] })),
  http.post('/api/watchlist', () => HttpResponse.json({ ok: true })),
  http.delete('/api/watchlist', () => HttpResponse.json({ ok: true })),

  http.get('/api/alerts', () => HttpResponse.json([])),
  http.post('/api/alerts', () => HttpResponse.json({ id: 1 })),
  http.put('/api/alerts/:id', () => HttpResponse.json({ ok: true })),
  http.delete('/api/alerts/:id', () => HttpResponse.json({ ok: true })),

  http.get('/api/planning/kpis', () => HttpResponse.json([])),
  http.get('/api/planning/taxonomy', () => HttpResponse.json({})),
  http.get('/api/planning/portfolio-symbols', () => HttpResponse.json([])),
  http.get('/api/planning/allocation-over-time', () => HttpResponse.json({ dates: [], series: {} })),
  http.get('/api/planning/risk-over-time', () => HttpResponse.json([])),

  http.get('/api/settings', () => HttpResponse.json({
    tax_rate: 0.25, dcf_discount_rate: 0.10, retirement_year: 2047, dnd: false,
    telegram_bot_token_set: false, telegram_chat_id_set: false,
  })),
  http.put('/api/settings', () => HttpResponse.json({ ok: true })),
]

export const server = setupServer(...handlers)
