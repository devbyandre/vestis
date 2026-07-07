import { Suspense } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render } from '@testing-library/react'

// A fresh, isolated QueryClient per render — no retries/refetch noise, and
// no cache bleed between tests (unlike importing the app's shared singleton).
export function renderWithProviders(ui) {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false, refetchOnWindowFocus: false, gcTime: 0 },
      mutations: { retry: false },
    },
  })
  return render(
    <QueryClientProvider client={queryClient}>
      <Suspense fallback={<div>loading…</div>}>{ui}</Suspense>
    </QueryClientProvider>
  )
}
