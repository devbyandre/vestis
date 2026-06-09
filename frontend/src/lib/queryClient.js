import { QueryClient } from '@tanstack/react-query'

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 5 * 60 * 1000,      // 5 min — data considered fresh
      gcTime: 30 * 60 * 1000,         // 30 min — keep in cache
      retry: 1,
      refetchOnWindowFocus: false,
    },
    mutations: {
      onError: (err) => {
        console.error('Mutation error:', err)
      },
    },
  },
})

// Structured query keys — invalidate by scope
export const qk = {
  portfolios: () => ['portfolios'],
  securities: () => ['securities'],
  security: (symbol) => ['securities', symbol],
  snapshot: (ids) => ['holdings', 'snapshot', ids],
  timeseries: (opts) => ['holdings', 'timeseries', opts],
  riskTs: (ids) => ['holdings', 'risk', ids],
  transactions: (ids) => ['transactions', ids],
  capitalGains: (ids, year) => ['analytics', 'capital-gains', ids, year],
  dividends: (ids, year) => ['analytics', 'dividends', ids, year],
  rebalancing: (ids) => ['analytics', 'rebalancing', ids],
  indicators: (symbol, opts) => ['analytics', 'indicators', symbol, opts],
  watchlist: () => ['watchlist'],
  alerts: () => ['alerts'],
  kpis: (ids) => ['planning', 'kpis', ids],
  settings: () => ['settings'],
}
