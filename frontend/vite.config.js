import { fileURLToPath, URL } from 'node:url'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: process.env.VITEST
      ? {
          // jsdom has no canvas/WebGL — stub the chart lib out under test.
          'react-plotly.js': fileURLToPath(new URL('./src/test/plotlyStub.jsx', import.meta.url)),
        }
      : {},
  },
  test: {
    environment: 'jsdom',
    setupFiles: './src/test/setup.js',
    globals: true,
  },
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: "http://localhost:8505",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ''),
      },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: false,
  },
})
