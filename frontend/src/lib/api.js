import axios from 'axios'

const BASE = '/api'

export const api = axios.create({ baseURL: BASE, timeout: 30000 })

// ── Generic helpers ──────────────────────────────────────────────────────────

const get = (url, params) => api.get(url, { params }).then(r => r.data)
const post = (url, data) => api.post(url, data).then(r => r.data)
const put = (url, data) => api.put(url, data).then(r => r.data)
const del = (url, data) => api.delete(url, { data }).then(r => r.data)

// ── Portfolios ────────────────────────────────────────────────────────────────
export const portfolioApi = {
  list: () => get('/portfolios'),
  create: (name) => post('/portfolios', { name }),
  rename: (old_name, new_name) => put('/portfolios/rename', { old_name, new_name }),
  delete: (name, reassign_to) => del('/portfolios', { name, reassign_to }),
}

// ── Securities ────────────────────────────────────────────────────────────────
export const securitiesApi = {
  list: () => get('/securities'),
  all: () => get('/securities/all'),
  symbols: () => get('/securities/symbols'),
  get: (symbol) => get(`/securities/${symbol}`),
  getBasic: (symbol) => get(`/securities/${symbol}/basic`),
  latestPrice: (symbol) => get(`/securities/${symbol}/price/latest`),
  priceSeries: (symbol, start, end) => get(`/securities/${symbol}/price/series`, { start, end }),
}

// ── Holdings ──────────────────────────────────────────────────────────────────
export const holdingsApi = {
  snapshot: (portfolio_ids, aggregate = true) =>
    get('/holdings/snapshot', {
      portfolio_ids: portfolio_ids?.join(','),
      aggregate,
    }),
  timeseries: (opts = {}) =>
    get('/holdings/timeseries', {
      portfolio_ids: opts.portfolio_ids?.join(','),
      sectors: opts.sectors?.join(','),
      security_types: opts.security_types?.join(','),
      aggregate: opts.aggregate ?? true,
    }),
  riskTimeseries: (portfolio_ids) =>
    get('/holdings/risk-timeseries', { portfolio_ids: portfolio_ids?.join(',') }),
}

// ── Transactions ──────────────────────────────────────────────────────────────
export const transactionsApi = {
  list: (portfolio_ids) =>
    get('/transactions', { portfolio_ids: portfolio_ids?.join(',') }),
  add: (tx) => post('/transactions', tx),
  edit: (tx_id, tx) => put(`/transactions/${tx_id}`, tx),
  delete: (tx_id) => del(`/transactions/${tx_id}`),
  addSplit: (split) => post('/transactions/split', split),
}

// ── Analytics ─────────────────────────────────────────────────────────────────
export const analyticsApi = {
  capitalGains: (portfolio_ids, year) =>
    get('/analytics/capital-gains', { portfolio_ids: portfolio_ids?.join(','), year }),
  dividends: (portfolio_ids, year) =>
    get('/analytics/dividends', { portfolio_ids: portfolio_ids?.join(','), year }),
  rebalancing: (portfolio_ids) =>
    get('/analytics/rebalancing', { portfolio_ids: portfolio_ids?.join(',') }),
  indicators: (symbol, opts = {}) =>
    get(`/analytics/indicators/${symbol}`, opts),
  crossovers: (symbol, short, long_) =>
    get(`/analytics/crossovers/${symbol}`, { short, long_ }),
  localExtrema: (symbol, lookback_days) =>
    get(`/analytics/local-extrema/${symbol}`, { lookback_days }),
}

// ── Watchlist ─────────────────────────────────────────────────────────────────
export const watchlistApi = {
  list: () => get('/watchlist'),
  symbols: () => get('/watchlist/symbols'),
  add: (symbol, name, isin) => post('/watchlist', { symbol, name, isin }),
  remove: (symbol) => del('/watchlist', { symbol }),
}

// ── Alerts ────────────────────────────────────────────────────────────────────
export const alertsApi = {
  list: (active_only = false) => get('/alerts', { active_only }),
  create: (alert) => post('/alerts', alert),
  edit: (id, data) => put(`/alerts/${id}`, data),
  delete: (id) => del(`/alerts/${id}`),
}

// ── Planning ──────────────────────────────────────────────────────────────────
export const planningApi = {
  kpis: (portfolio_ids) =>
    get('/planning/kpis', { portfolio_ids: portfolio_ids?.join(',') }),
  taxonomy: () => get('/planning/taxonomy'),
  portfolioSymbols: (portfolio_ids) =>
    get('/planning/portfolio-symbols', { portfolio_ids: portfolio_ids?.join(',') }),
}

// ── Settings ──────────────────────────────────────────────────────────────────
export const settingsApi = {
  get: () => get('/settings'),
  update: (settings) => put('/settings', { settings }),
}
