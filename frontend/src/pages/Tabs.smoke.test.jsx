// Smoke-render every Tab page against the default (empty-state) MSW mocks.
// The goal isn't asserting exact content — it's catching the "API response
// shape drifted, component throws on render" failure mode that previously
// had zero coverage anywhere in the stack.
import { describe, it, expect } from 'vitest'
import { screen } from '@testing-library/react'
import { renderWithProviders } from '../test/renderWithProviders'

import TabPortfolio from './TabPortfolio'
import TabRevenues from './TabRevenues'
import TabPlanning from './TabPlanning'
import TabTechnical from './TabTechnical'
import TabNews from './TabNews'
import TabTransactions from './TabTransactions'
import TabWatchlist from './TabWatchlist'
import TabAlerts from './TabAlerts'
import TabSettings from './TabSettings'

const cases = [
  ['TabPortfolio', TabPortfolio, 'Holdings'],
  ['TabRevenues', TabRevenues, 'Revenues & Taxes'],
  ['TabPlanning', TabPlanning, 'Investment Planning'],
  ['TabTechnical', TabTechnical, 'Technical Analysis'],
  ['TabTransactions', TabTransactions, 'Transactions'],
  ['TabWatchlist', TabWatchlist, 'Watchlist'],
  ['TabAlerts', TabAlerts, 'Alerts Manager'],
  ['TabSettings', TabSettings, 'Settings'],
]

describe.each(cases)('%s', (_name, Component, heading) => {
  it('renders its section heading without throwing, given empty API data', async () => {
    renderWithProviders(<Component />)
    expect(await screen.findByText(heading)).toBeInTheDocument()
  })
})

describe('TabNews', () => {
  it('renders its placeholder', () => {
    renderWithProviders(<TabNews />)
    expect(screen.getByText(/Coming soon/i)).toBeInTheDocument()
  })
})
