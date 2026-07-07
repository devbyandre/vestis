import { describe, it, expect } from 'vitest'
import { screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { server } from '../test/mswServer'
import { renderWithProviders } from '../test/renderWithProviders'
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
