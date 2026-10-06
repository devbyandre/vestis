import { describe, it, expect, beforeEach } from 'vitest'
import { screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { Toaster } from 'react-hot-toast'
import { server } from '../test/mswServer'
import { renderWithProviders } from '../test/renderWithProviders'
import TabNews from './TabNews'

const feed = {
  items: [
    { uid: 'a', title: 'Apple beats estimates', publisher: 'Reuters', url: 'https://example.com/a',
      published_at: new Date(Date.now() - 3 * 3600_000).toISOString(), symbols: ['AAPL'], sentiment: 0.6, label: 'positive' },
    { uid: 'b', title: 'Tesla recall widens', publisher: 'WSJ', url: 'https://example.com/b',
      published_at: new Date(Date.now() - 26 * 3600_000).toISOString(), symbols: ['TSLA'], sentiment: -0.5, label: 'negative' },
  ],
  summary: [
    { symbol: 'AAPL', name: 'Apple', scope: 'holdings', count: 1, avg_sentiment: 0.6, positive: 1, negative: 0, neutral: 0, latest: '2026-01-01T00:00:00Z' },
    { symbol: 'TSLA', name: 'Tesla', scope: 'holdings', count: 1, avg_sentiment: -0.5, positive: 0, negative: 1, neutral: 0, latest: '2026-01-01T00:00:00Z' },
    { symbol: 'IWDA.AS', name: 'iShares', scope: 'holdings', count: 0, avg_sentiment: null, positive: 0, negative: 0, neutral: 0, latest: null },
  ],
  overall: { count: 2, avg_sentiment: 0.05, positive: 1, negative: 1, neutral: 0 },
}

beforeEach(() => window.history.replaceState(null, '', '/'))

describe('TabNews', () => {
  it('renders articles with safe external links, sentiment badges and no-coverage rows', async () => {
    server.use(http.get('/api/news', () => HttpResponse.json(feed)))
    renderWithProviders(<TabNews />)

    const link = await screen.findByRole('link', { name: /Apple beats estimates/ })
    expect(link).toHaveAttribute('href', 'https://example.com/a')
    expect(link).toHaveAttribute('rel', 'noopener noreferrer')
    expect(link).toHaveAttribute('target', '_blank')
    expect(screen.getByText('positive')).toBeInTheDocument()
    expect(screen.getByText('negative')).toBeInTheDocument()
    expect(screen.getByText('3 h ago')).toBeInTheDocument()
    expect(screen.getByText('no coverage')).toBeInTheDocument()
    expect(screen.getAllByText('Apple').length).toBeGreaterThan(0)     // names, not just tickers
    expect(screen.getAllByText('Tesla').length).toBeGreaterThan(0)
  })

  it('passes scope, days and symbol to the API', async () => {
    const seen = []
    server.use(http.get('/api/news', ({ request }) => {
      seen.push(Object.fromEntries(new URL(request.url).searchParams))
      return HttpResponse.json(feed)
    }))
    const user = userEvent.setup()
    renderWithProviders(<TabNews />)
    await screen.findByRole('link', { name: /Apple beats/ })
    expect(seen[0]).toEqual({ scope: 'all', days: '7' })

    await user.click(screen.getByRole('button', { name: 'Holdings' }))
    await user.click(screen.getByRole('button', { name: '30 days' }))
    await user.click(screen.getAllByRole('button', { name: /^Apple/ })[0])
    await screen.findByRole('button', { name: /Refresh AAPL/ })
    expect(seen.at(-1)).toEqual({ scope: 'holdings', days: '30', symbol: 'AAPL' })
    expect(window.location.search).toContain('symbol=AAPL')
  })

  it('refresh without a symbol starts a background run', async () => {
    let body = null
    server.use(http.post('/api/news/refresh', async ({ request }) => {
      body = await request.json()
      return HttpResponse.json({ started: true })
    }))
    const user = userEvent.setup()
    renderWithProviders(<><Toaster /><TabNews /></>)
    await user.click(await screen.findByRole('button', { name: 'Refresh' }))
    await screen.findByText(/in the background/)
    expect(body).toEqual({})
  })

  it('refresh with a symbol sends it and reports the stored count', async () => {
    let body = null
    server.use(
      http.get('/api/news', () => HttpResponse.json(feed)),
      http.post('/api/news/refresh', async ({ request }) => {
        body = await request.json()
        return HttpResponse.json({ checked: 1, stored: 4, skipped: 0, errors: 0, pruned: 0 })
      }),
    )
    window.history.replaceState(null, '', '/?symbol=TSLA')
    const user = userEvent.setup()
    renderWithProviders(<><Toaster /><TabNews /></>)
    await user.click(await screen.findByRole('button', { name: 'Refresh TSLA' }))
    await screen.findByText('Fetched 4 headlines')
    expect(body).toEqual({ symbol: 'TSLA' })
  })

  it('Market shows general headlines instead of the per-security list', async () => {
    const seen = []
    server.use(http.get('/api/news', ({ request }) => {
      const scope = new URL(request.url).searchParams.get('scope')
      seen.push(scope)
      return HttpResponse.json(scope === 'market'
        ? { items: [{ ...feed.items[1], uid: 'm1', title: 'Oil slips as demand cools', symbols: [] }], summary: [], overall: feed.overall }
        : feed)
    }))
    const user = userEvent.setup()
    renderWithProviders(<TabNews />)
    await screen.findByText('By security')
    await user.click(screen.getByRole('button', { name: 'Market' }))
    expect(await screen.findByRole('link', { name: /Oil slips/ })).toBeInTheDocument()
    expect(screen.getByText('Market news')).toBeInTheDocument()
    expect(screen.queryByText('By security')).not.toBeInTheDocument()
    expect(seen).toContain('market')
  })

  it('shows an error when the feed fails', async () => {
    server.use(http.get('/api/news', () => HttpResponse.json({ detail: 'boom' }, { status: 500 })))
    renderWithProviders(<TabNews />)
    expect(await screen.findByText("boom")).toBeInTheDocument()
  })
})
