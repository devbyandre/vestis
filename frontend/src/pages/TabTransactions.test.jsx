import { describe, it, expect } from 'vitest'
import { screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { server } from '../test/mswServer'
import { renderWithProviders } from '../test/renderWithProviders'
import { Toaster } from 'react-hot-toast'
import TabTransactions from './TabTransactions'

describe('TabTransactions — portfolio management', () => {
  it('creates a new portfolio and it appears in the list', async () => {
    let portfolios = [{ id: 1, name: 'Default' }]
    server.use(
      http.get('/api/portfolios', () => HttpResponse.json(portfolios)),
      http.post('/api/portfolios', async ({ request }) => {
        const body = await request.json()
        const created = { id: 2, name: body.name }
        portfolios = [...portfolios, created]
        return HttpResponse.json(created)
      }),
    )

    const user = userEvent.setup()
    renderWithProviders(<TabTransactions />)

    await user.click(await screen.findByText('Manage Portfolios'))
    await screen.findByText('Default', { selector: 'span' })

    await user.click(screen.getByRole('button', { name: /New Portfolio/i }))
    const input = screen.getByPlaceholderText('e.g. Satellite')
    await user.type(input, 'Satellite')
    expect(input).toHaveValue('Satellite')
    const createBtn = screen.getByRole('button', { name: 'Create' })
    expect(createBtn).not.toBeDisabled()
    await user.click(createBtn)

    expect(await screen.findByText('Satellite', { selector: 'span' })).toBeInTheDocument()
  })

  it('disables delete when there is only one portfolio', async () => {
    server.use(http.get('/api/portfolios', () => HttpResponse.json([{ id: 1, name: 'Default' }])))
    renderWithProviders(<TabTransactions />)

    await userEvent.setup().click(await screen.findByText('Manage Portfolios'))
    const nameSpan = await screen.findByText('Default', { selector: 'span' })
    const deleteBtn = within(nameSpan.closest('div').parentElement).getAllByRole('button')[1]
    expect(deleteBtn).toBeDisabled()
  })
})

describe('TabTransactions — editing', () => {
  it('saves a corrected price with the fields the API expects', async () => {
    let body = null
    server.use(
      http.get('/api/transactions', () => HttpResponse.json([{
        id: 28, portfolio_id: 1, portfolio: 'Default', symbol: 'BABA', security_label: 'Alibaba',
        date: '2018-06-13', type: 'buy', tx_type: 'buy', quantity: 6, price: 539.7, fees: 10, tx_cost: 10, total_cost: 3248.2,
      }])),
      http.put('/api/transactions/:id', async ({ request, params }) => {
        body = { id: params.id, ...(await request.json()) }
        return HttpResponse.json({ ok: true })
      }),
    )
    const user = userEvent.setup()
    renderWithProviders(<><Toaster /><TabTransactions /></>)
    await user.click(await screen.findByRole('button', { name: 'Edit transaction' }))
    const price = screen.getByDisplayValue('539.7')
    await user.clear(price)
    await user.type(price, '179.9')
    await user.click(screen.getByRole('button', { name: 'Save' }))
    await screen.findByText('Transaction updated')
    expect(body).toMatchObject({ id: '28', tx_type: 'buy', tx_date: '2018-06-13', quantity: 6, price: 179.9, fees: 10 })
  })
})
