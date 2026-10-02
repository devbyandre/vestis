import { describe, it, expect } from 'vitest'
import { ALERT_TYPES, toFormValues, toParams, validateForm, describeAlert, isEditable } from './alertSchema'

describe('alertSchema', () => {
  it('has defaults for every type that validate once required fields are filled', () => {
    for (const t of ALERT_TYPES) {
      const v = toFormValues(t, {})
      if (t === 'price') v.threshold = '150'
      expect(validateForm(t, v), t).toBeNull()
    }
  })

  it('round-trips stored params and keeps keys the form does not know', () => {
    const stored = { threshold: 30, direction: 'below', rearm_gap: 8 }
    const form = toFormValues('rsi', stored)
    expect(form).toEqual({ direction: 'below', threshold: '30' })
    expect(toParams('rsi', form, stored)).toEqual(stored)
  })

  it('shows margin of safety as percent but stores a fraction', () => {
    expect(toFormValues('mos', { threshold_pct: 0.25 })).toEqual({ threshold_pct: '25' })
    expect(toParams('mos', { threshold_pct: '30' })).toEqual({ threshold_pct: 0.3 })
  })

  it('rejects out-of-range and inconsistent values', () => {
    expect(validateForm('rsi', { direction: 'above', threshold: '150' })).toMatch(/at most 99/)
    expect(validateForm('pct_change', { direction: 'down', pct: '', days: '1' })).toMatch(/required/)
    expect(validateForm('ma_crossover', { crossover_type: 'golden', short: '200', long: '50' })).toMatch(/shorter/)
  })

  it('describes alerts in plain words, including legacy params', () => {
    expect(describeAlert('rsi', '{"threshold":30,"direction":"below"}')).toBe('RSI falls below 30')
    expect(describeAlert('pct_change', { pct: 5, days: 3, direction: 'down' })).toBe('Drop of ≥5% over 3 trading days')
    expect(describeAlert('ma_crossover', {})).toBe('Golden cross SMA50/SMA200')
    expect(describeAlert('mystery', {})).toBe('mystery')
  })

  it('does not offer editing for unknown types or automatic relative alerts', () => {
    expect(isEditable({ alert_type: 'rsi', params: '{}' })).toBe(true)
    expect(isEditable({ alert_type: 'split_pending', params: '{}' })).toBe(false)
    expect(isEditable({ alert_type: 'price', params: '{"mode":"relative","threshold":-0.05}' })).toBe(false)
  })
})
