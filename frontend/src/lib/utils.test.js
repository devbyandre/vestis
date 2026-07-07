import { describe, expect, it } from 'vitest'
import { fmt, pnlColor, pnlBg, sortBy, groupBy, clamp } from './utils'

describe('fmt.currency', () => {
  it('formats a positive value in EUR', () => {
    expect(fmt.currency(1234)).toContain('1.234')
  })
  it('returns em dash for null/NaN', () => {
    expect(fmt.currency(null)).toBe('—')
    expect(fmt.currency(NaN)).toBe('—')
  })
})

describe('fmt.pct', () => {
  it('adds a + sign for positive values and formats as a percentage', () => {
    expect(fmt.pct(0.1234)).toBe('+12.3%')
  })
  it('does not add a + sign for negative values', () => {
    expect(fmt.pct(-0.05)).toBe('-5.0%')
  })
  it('returns em dash for null/NaN', () => {
    expect(fmt.pct(null)).toBe('—')
  })
})

describe('fmt.num', () => {
  it('formats with the requested decimal places', () => {
    expect(fmt.num(1234.5, 2)).toBe('1.234,50')
  })
})

describe('fmt.date', () => {
  it('formats an ISO date as DD.MM.YYYY', () => {
    expect(fmt.date('2024-01-08')).toBe('08.01.2024')
  })
  it('returns em dash for falsy input', () => {
    expect(fmt.date(null)).toBe('—')
    expect(fmt.date('')).toBe('—')
  })
})

describe('pnlColor / pnlBg', () => {
  it('is green for positive/zero, red for negative', () => {
    expect(pnlColor(10)).toContain('green')
    expect(pnlColor(0)).toContain('green')
    expect(pnlColor(-1)).toContain('red')
  })
  it('returns a neutral class for null', () => {
    expect(pnlColor(null)).toBe('text-gray-400')
    expect(pnlBg(null)).toBe('')
  })
})

describe('sortBy', () => {
  const rows = [{ v: 3 }, { v: 1 }, { v: 2 }]
  it('sorts ascending by default', () => {
    expect(sortBy(rows, 'v').map(r => r.v)).toEqual([1, 2, 3])
  })
  it('sorts descending when asc=false', () => {
    expect(sortBy(rows, 'v', false).map(r => r.v)).toEqual([3, 2, 1])
  })
  it('does not mutate the input array', () => {
    const copy = [...rows]
    sortBy(rows, 'v')
    expect(rows).toEqual(copy)
  })
})

describe('groupBy', () => {
  it('groups items by key', () => {
    const rows = [{ k: 'a', v: 1 }, { k: 'b', v: 2 }, { k: 'a', v: 3 }]
    const grouped = groupBy(rows, 'k')
    expect(grouped.a).toHaveLength(2)
    expect(grouped.b).toHaveLength(1)
  })
})

describe('clamp', () => {
  it('clamps within range', () => {
    expect(clamp(5, 0, 10)).toBe(5)
    expect(clamp(-5, 0, 10)).toBe(0)
    expect(clamp(50, 0, 10)).toBe(10)
  })
})
