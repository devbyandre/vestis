import { describe, it, expect } from 'vitest'
import { screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { server } from '../test/mswServer'
import { renderWithProviders } from '../test/renderWithProviders'
import TabSettings from './TabSettings'

describe('TabSettings — quiet hours', () => {
  it('shows the window only when enabled and saves it', async () => {
    let saved = null
    server.use(http.put('/api/settings', async ({ request }) => { saved = (await request.json()).settings; return HttpResponse.json({ ok: true }) }))
    const user = userEvent.setup()
    renderWithProviders(<TabSettings />)

    const toggle = await screen.findByRole('checkbox', { name: /Quiet hours/ })
    expect(screen.queryByText('Until')).not.toBeInTheDocument()
    await user.click(toggle)
    expect(screen.getByText('Until')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Save Settings' }))
    await screen.findByRole('button', { name: 'Save Settings' })
    expect(saved).toMatchObject({ dnd: true })
  })
})
