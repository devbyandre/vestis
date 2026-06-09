import React from 'react'
import ReactDOM from 'react-dom/client'
import { QueryClientProvider } from '@tanstack/react-query'
import { Toaster } from 'react-hot-toast'
import { queryClient } from './lib/queryClient'
import App from './App'
import './index.css'

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <App />
      <Toaster
        position="top-right"
        toastOptions={{
          style: {
            background: '#1a1a24',
            color: '#e5e7eb',
            border: '1px solid #2a2a3a',
            fontSize: '13px',
          },
          success: { iconTheme: { primary: '#22c55e', secondary: '#1a1a24' } },
          error: { iconTheme: { primary: '#ef4444', secondary: '#1a1a24' } },
        }}
      />
    </QueryClientProvider>
  </React.StrictMode>
)
