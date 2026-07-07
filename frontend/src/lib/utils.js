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

// KPI threshold colors — shared by Watchlist and Portfolio "Holdings KPIs"
// tables. Thresholds match app_streamlit.py's display_kpi_table().
export const kpiColor = {
  beta: (v) => v == null || isNaN(v) ? '' : v < 1.0 ? 'text-green-400' : v <= 1.2 ? 'text-orange-400' : 'text-red-400',
  pe: (v) => v == null || isNaN(v) ? '' : v < 15 ? 'text-green-400' : v <= 25 ? 'text-orange-400' : 'text-red-400',
  pb: (v) => v == null || isNaN(v) ? '' : v < 1.5 ? 'text-green-400' : v <= 3 ? 'text-orange-400' : 'text-red-400',
  divYield: (v) => v == null || isNaN(v) ? '' : v > 0.03 ? 'text-green-400' : v >= 0.01 ? 'text-orange-400' : 'text-red-400',
  profitMargin: (v) => v == null || isNaN(v) ? '' : v > 0.2 ? 'text-green-400' : v >= 0.1 ? 'text-orange-400' : 'text-red-400',
}

export const temperatureBadgeClass = (t) => {
  if (t === 'Hot') return 'bg-red-500/20 text-red-300'
  if (t === 'Warm') return 'bg-orange-500/20 text-orange-300'
  if (t === 'Cold') return 'bg-blue-500/20 text-blue-300'
  return 'bg-surface-3 text-gray-500'
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
