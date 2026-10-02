import { describe, it, expect } from 'vitest'
import { screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { server } from '../test/mswServer'
import { renderWithProviders } from '../test/renderWithProviders'
import { Toaster } from 'react-hot-toast'
import TabAlerts from './TabAlerts'

describe('TabAlerts — type-specific form', () => {
  it('builds typed params for a new RSI alert and shows a plain-language preview', async () => {
    let posted = null
    server.use(
      http.get('/api/securities', () => HttpResponse.json([{ id: 7, symbol: 'TSM', name: 'TSMC' }])),
      http.post('/api/alerts', async ({ request }) => { posted = await request.json(); return HttpResponse.json({ id: 1 }) }),
    )
    const user = userEvent.setup()
    renderWithProviders(<><Toaster /><TabAlerts /></>)

    await user.click(await screen.findByRole('button', { name: /New Alert/i }))
    await screen.findByText(/You will be notified when/)
    await user.selectOptions(await screen.findByRole('option', { name: /TSM/ }).then(o => o.closest('select')), '7')
    await user.selectOptions(screen.getByDisplayValue('Price'), 'rsi')
    await user.selectOptions(screen.getByDisplayValue(/Overbought/), 'below')

    expect(screen.getByDisplayValue('30')).toBeInTheDocument()   // default level follows the zone
    expect(screen.getByText('RSI falls below 30')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Save' }))
    await screen.findByText('Alert created')
    expect(posted).toMatchObject({ security_id: 7, alert_type: 'rsi', params: { threshold: 30, direction: 'below' } })
  })

  it('blocks submission with a readable error instead of sending bad params', async () => {
    let called = false
    server.use(
      http.get('/api/securities', () => HttpResponse.json([{ id: 7, symbol: 'TSM', name: 'TSMC' }])),
      http.post('/api/alerts', () => { called = true; return HttpResponse.json({ id: 1 }) }),
    )
    const user = userEvent.setup()
    renderWithProviders(<><Toaster /><TabAlerts /></>)
    await user.click(await screen.findByRole('button', { name: /New Alert/i }))
    await user.selectOptions(await screen.findByRole('option', { name: /TSM/ }).then(o => o.closest('select')), '7')
    await user.click(screen.getByRole('button', { name: 'Save' }))   // price threshold is empty
    expect(await screen.findByText('Price (EUR) is required')).toBeInTheDocument()
    expect(called).toBe(false)
  })
})
