// Number formatting
export const fmt = {
  currency: (v, decimals = 0) => {
    if (v == null || isNaN(v)) return '—'
    return new Intl.NumberFormat('de-DE', {
      style: 'currency',
      currency: 'EUR',
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    }).format(v)
  },
  pct: (v, decimals = 1) => {
    if (v == null || isNaN(v)) return '—'
    return `${v >= 0 ? '+' : ''}${(v * 100).toFixed(decimals)}%`
  },
  pctDirect: (v, decimals = 1) => {
    if (v == null || isNaN(v)) return '—'
    return `${v >= 0 ? '+' : ''}${Number(v).toFixed(decimals)}%`
  },
  num: (v, decimals = 2) => {
    if (v == null || isNaN(v)) return '—'
    return Number(v).toLocaleString('de-DE', {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    })
  },
  date: (v) => {
    if (!v) return '—'
    return new Date(v).toLocaleDateString('de-DE', {
      day: '2-digit', month: '2-digit', year: 'numeric',
    })
  },
}

// Color helpers
export const pnlColor = (v) => {
  if (v == null) return 'text-gray-400'
  return v >= 0 ? 'text-green-400' : 'text-red-400'
}

export const pnlBg = (v) => {
  if (v == null) return ''
  return v >= 0 ? 'bg-green-500/10' : 'bg-red-500/10'
}

// Plotly dark theme config
export const plotlyLayout = (overrides = {}) => ({
  paper_bgcolor: 'transparent',
  plot_bgcolor: 'transparent',
  font: { color: '#9ca3af', family: 'Inter var, Inter, system-ui, sans-serif', size: 12 },
  xaxis: {
    gridcolor: '#1a1a24',
    linecolor: '#2a2a3a',
    tickcolor: '#2a2a3a',
    ...overrides.xaxis,
  },
  yaxis: {
    gridcolor: '#1a1a24',
    linecolor: '#2a2a3a',
    tickcolor: '#2a2a3a',
    ...overrides.yaxis,
  },
  margin: { l: 50, r: 20, t: 40, b: 40, ...(overrides.margin || {}) },
  hovermode: 'x unified',
  hoverlabel: {
    bgcolor: '#1a1a24',
    bordercolor: '#2a2a3a',
    font: { color: '#e5e7eb', size: 12 },
  },
  legend: {
    bgcolor: 'transparent',
    font: { color: '#9ca3af' },
    ...overrides.legend,
  },
  ...overrides,
})

export const plotlyConfig = {
  displayModeBar: true,
  displaylogo: false,
  modeBarButtonsToRemove: ['select2d', 'lasso2d', 'autoScale2d'],
  responsive: true,
}

// Sort helpers
export const sortBy = (arr, key, asc = true) =>
  [...arr].sort((a, b) => {
    const va = a[key] ?? -Infinity
    const vb = b[key] ?? -Infinity
    return asc ? va - vb : vb - va
  })

// Group by
export const groupBy = (arr, key) =>
  arr.reduce((acc, item) => {
    const k = item[key]
    ;(acc[k] = acc[k] || []).push(item)
    return acc
  }, {})

// Clamp
export const clamp = (v, min, max) => Math.min(Math.max(v, min), max)
