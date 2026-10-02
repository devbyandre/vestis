// Per-type alert definitions: drive the form fields, defaults, validation and
// the human-readable description. Param names match app/middleware/alerts.py.
// `scale` converts a displayed value to the stored one (mos: 25 % -> 0.25).

const DIRECTION_PRICE = [['above', 'rises above'], ['below', 'falls below']]

export const ALERT_SCHEMAS = {
  price: {
    label: 'Price',
    help: 'Fires when the price crosses the threshold, and again after it has crossed back.',
    fields: [
      { key: 'direction', label: 'Alert when price', type: 'select', options: DIRECTION_PRICE, default: 'above' },
      { key: 'threshold', label: 'Price (EUR)', type: 'number', step: 'any', min: 0, required: true },
    ],
    describe: p => p.mode === 'relative'
      ? `Price ${(p.threshold * 100).toFixed(1)}% vs. last price (automatic)`
      : `Price ${p.direction === 'below' ? 'falls below' : 'rises above'} ${p.threshold ?? '?'}`,
  },
  rsi: {
    label: 'RSI',
    help: 'Daily RSI(14). Fires when it enters the zone, then stays quiet until it has left it by 5 points.',
    fields: [
      { key: 'direction', label: 'Zone', type: 'select', default: 'above',
        options: [['above', 'Overbought — RSI rises above'], ['below', 'Oversold — RSI falls below']] },
      { key: 'threshold', label: 'RSI level', type: 'number', min: 1, max: 99, default: 70, required: true },
    ],
    describe: p => `RSI ${p.direction === 'below' ? 'falls below' : 'rises above'} ${p.threshold ?? (p.direction === 'below' ? 30 : 70)}`,
  },
  ma_crossover: {
    label: 'Moving-average cross',
    help: 'Golden cross: fast average crosses above the slow one. Death cross: the reverse. Needs a 0.5 % gap to count.',
    fields: [
      { key: 'crossover_type', label: 'Cross', type: 'select', default: 'golden',
        options: [['golden', 'Golden cross (bullish)'], ['death', 'Death cross (bearish)']] },
      { key: 'short', label: 'Fast average (days)', type: 'number', min: 2, default: 50, required: true },
      { key: 'long', label: 'Slow average (days)', type: 'number', min: 3, default: 200, required: true },
    ],
    validate: p => (Number(p.short) >= Number(p.long) ? 'Fast average must be shorter than the slow average' : null),
    describe: p => `${p.crossover_type === 'death' ? 'Death' : 'Golden'} cross SMA${p.short ?? 50}/SMA${p.long ?? 200}`,
  },
  '52w': {
    label: '52-week high / low',
    help: 'Fires on a new 52-week extreme (once per day).',
    fields: [
      { key: 'type', label: 'Extreme', type: 'select', default: 'high', options: [['high', 'New 52-week high'], ['low', 'New 52-week low']] },
    ],
    describe: p => `New 52-week ${p.type === 'low' ? 'low' : 'high'}`,
  },
  volume_spike: {
    label: 'Volume spike',
    help: 'Fires when the latest day’s volume is a multiple of the recent average.',
    fields: [
      { key: 'multiplier', label: 'Times the average', type: 'number', step: 'any', min: 1.1, default: 2, required: true },
      { key: 'lookback', label: 'Average over (days)', type: 'number', min: 5, max: 250, default: 20, required: true },
    ],
    describe: p => `Volume ≥ ${p.multiplier ?? 2}× the ${p.lookback ?? 20}-day average`,
  },
  pct_change: {
    label: 'Price move (%)',
    help: 'Fires when the close moved by at least this much over N trading days (once per day).',
    fields: [
      { key: 'direction', label: 'Direction', type: 'select', default: 'down', options: [['down', 'Drop'], ['up', 'Rise']] },
      { key: 'pct', label: 'Move of at least (%)', type: 'number', step: 'any', min: 0.1, max: 100, default: 5, required: true },
      { key: 'days', label: 'Over (trading days)', type: 'number', min: 1, max: 60, default: 1, required: true },
    ],
    describe: p => {
      const d = Number(p.days || 1)
      return `${p.direction === 'up' ? 'Rise' : 'Drop'} of ≥${p.pct ?? 5}% over ${d} trading day${d > 1 ? 's' : ''}`
    },
  },
  earnings_soon: {
    label: 'Earnings coming up',
    help: 'Fires once per earnings date, N days ahead.',
    fields: [{ key: 'days', label: 'Days ahead', type: 'number', min: 1, max: 60, default: 3, required: true }],
    describe: p => `Earnings within ${p.days ?? 3} days`,
  },
  mos: {
    label: 'Margin of safety',
    help: 'Fires when the price is this far below the estimated fair value; re-arms 5 points lower.',
    fields: [{ key: 'threshold_pct', label: 'Margin of safety (%)', type: 'number', step: 'any', min: 1, max: 100,
               default: 25, scale: 0.01, required: true }],
    describe: p => `Margin of safety ≥ ${Math.round((p.threshold_pct ?? 0.25) * 100)}%`,
  },
}

export const ALERT_TYPES = Object.keys(ALERT_SCHEMAS)

// Automatic relative-price alerts store fractions the form can't represent.
export const isEditable = a => !!ALERT_SCHEMAS[a.alert_type] && !String(a.params).includes('"relative"')

const parse = p => {
  if (!p) return {}
  if (typeof p === 'object') return p
  try { return JSON.parse(p) } catch { return {} }
}

const round = x => Math.round(x * 1e8) / 1e8

// Stored params -> form strings (applies defaults for missing keys)
export function toFormValues(type, params) {
  const p = parse(params)
  const out = {}
  for (const f of ALERT_SCHEMAS[type]?.fields ?? []) {
    const stored = p[f.key]
    const v = stored != null ? (f.scale ? round(Number(stored) / f.scale) : stored) : (f.default ?? '')
    out[f.key] = String(v)
  }
  return out
}

// Form strings -> params to send. Keys the form doesn't know (mode,
// rearm_gap, ...) are carried over from `base` so editing never drops them.
export function toParams(type, values, base = {}) {
  const out = { ...parse(base) }
  for (const f of ALERT_SCHEMAS[type].fields) {
    const raw = values[f.key]
    out[f.key] = f.type === 'number' ? round(Number(raw) * (f.scale ?? 1)) : raw
  }
  return out
}

// Returns an error message, or null when the values are valid.
export function validateForm(type, values) {
  const schema = ALERT_SCHEMAS[type]
  for (const f of schema.fields) {
    if (f.type !== 'number') continue
    const raw = values[f.key]
    if (raw === '' || raw == null) return `${f.label} is required`
    const n = Number(raw)
    if (!Number.isFinite(n)) return `${f.label} must be a number`
    if (f.min != null && n < f.min) return `${f.label} must be at least ${f.min}`
    if (f.max != null && n > f.max) return `${f.label} must be at most ${f.max}`
  }
  return schema.validate?.(values) ?? null
}

export function describeAlert(type, params) {
  const schema = ALERT_SCHEMAS[type]
  if (!schema) return type
  return schema.describe(parse(params))
}
