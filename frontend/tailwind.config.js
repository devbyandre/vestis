/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        surface: {
          0: '#0a0a0f',
          1: '#111118',
          2: '#1a1a24',
          3: '#22222f',
          4: '#2a2a3a',
        },
        accent: {
          DEFAULT: '#6366f1',
          dim: '#4f52d4',
          bright: '#818cf8',
        },
        success: '#22c55e',
        warning: '#f59e0b',
        danger: '#ef4444',
        muted: '#6b7280',
      },
      fontFamily: {
        sans: ['Inter var', 'Inter', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'monospace'],
      },
    },
  },
  plugins: [],
}
