import { describe, it, expect } from 'vitest'
import { screen, fireEvent } from '@testing-library/react'
import { http, HttpResponse } from 'msw'
import { server } from '../test/mswServer'
import { renderWithProviders } from '../test/renderWithProviders'
import TaxSelling from './TaxSelling'

const sell = (o) => ({ portfolio: 'Scalable', symbol: 'NVDA', name: 'NVIDIA', category: 'stock',
  quantity: 5.92, proceeds: 853.68, gain: 817.74, oldest_lot: '2020-01-29', ...o })

const base = {
  year: 2026, tax_rate: 0.26375, allowance: 1000, fund_partial_exemption: 0.3,
  status: { taxable_base: 182, allowance_used: 182, allowance_left: 818, tax_so_far: 0, carry_stock: 0, carry_general: 0 },
  allowance_plan: { sells: [sell()], gain: 817.74, taxable_gain: 817.74, future_tax_saved: 215.68 },
  loss_plan: null, cash_plan: null,
  crypto: [{ portfolio: 'Crypto', symbol: 'BTC-EUR', name: 'Bitcoin', quantity: 0.1, bought: '2018-09-04',
    value: 9000, gain: 8500, tax_free_from: '2019-09-05', tax_free_now: true }],
}

describe('TaxSelling', () => {
  it('shows the allowance plan and asks the API for a cash plan', async () => {
    let requested = null
    server.use(http.get('/api/planning/tax-sales', ({ request }) => {
      const t = new URL(request.url).searchParams.get('target_amount')
      requested = t
      return HttpResponse.json(t ? { ...base, cash_plan: {
        target: 5000, proceeds: 5000, shortfall: 0, gain: -200, tax: 0, effective_rate: 0, naive_tax: 120,
        sells: [sell({ symbol: 'BAYN.DE', name: 'Bayer', proceeds: 5000, gain: -200 })] } } : base)
    }))
    renderWithProviders(<TaxSelling portfolioIds={null} />)

    expect(await screen.findByText('NVIDIA')).toBeInTheDocument()
    expect(screen.getByText('No tax due this year so far — nothing to offset.')).toBeInTheDocument()
    expect(screen.getByText('now')).toBeInTheDocument()          // BTC already tax-free

    fireEvent.change(screen.getByPlaceholderText('e.g. 10000'), { target: { value: '5.000' } })
    fireEvent.click(screen.getByText('Plan sales'))
    expect(await screen.findByText('Bayer')).toBeInTheDocument()
    expect(requested).toBe('5000')                                // German thousands separator understood
    expect(screen.getByText('You save')).toBeInTheDocument()
  })
})
