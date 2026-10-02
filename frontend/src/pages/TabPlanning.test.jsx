import { describe, it, expect } from 'vitest'
import { screen, waitFor } from '@testing-library/react'
import { http, HttpResponse, delay } from 'msw'
import { server } from '../test/mswServer'
import { renderWithProviders } from '../test/renderWithProviders'
import TabPlanning from './TabPlanning'

const kpi = { symbol: 'AAA', security_label: 'A (AAA)', security_type: 'EQUITY', market_value: 100 }

describe('TabPlanning progressive loading', () => {
  it('shows per-section skeletons while the slow history queries are pending, then removes them', async () => {
    server.use(
      http.get('/api/planning/kpis', () => HttpResponse.json([kpi])),
      http.get('/api/planning/allocation-over-time-all', async () => {
        await delay(150)
        return HttpResponse.json({ security_type: { dates: [], series: {} }, sector: { dates: [], series: {} },
          industry: { dates: [], series: {} }, symbol: { dates: [], series: {} } })
      }),
      http.get('/api/planning/risk-over-time', async () => {
        await delay(150)
        return HttpResponse.json([])
      }),
    )
    renderWithProviders(<TabPlanning />)

    // KPI-driven content is up while the history charts still show placeholders
    expect(await screen.findByText('Current Asset Allocation')).toBeInTheDocument()
    expect((await screen.findAllByRole('status')).length).toBeGreaterThan(0)
    expect(screen.getByText('Loading allocation history…')).toBeInTheDocument()

    await waitFor(() => expect(screen.queryAllByRole('status')).toHaveLength(0))
  })
})
