import { describe, it, expect, beforeEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useUrlParam, setUrlParams, getUrlParam } from './urlState'

describe('urlState', () => {
  beforeEach(() => window.history.replaceState(null, '', '/'))

  it('reads the initial value from the URL (deep links)', () => {
    window.history.replaceState(null, '', '/?tab=technical&symbol=TSM')
    const { result } = renderHook(() => useUrlParam('symbol', ''))
    expect(result.current[0]).toBe('TSM')
  })

  it('falls back to the default and keeps it out of the URL', () => {
    const { result } = renderHook(() => useUrlParam('tab', 'portfolio'))
    expect(result.current[0]).toBe('portfolio')
    act(() => result.current[1]('alerts'))
    expect(getUrlParam('tab')).toBe('alerts')
    act(() => result.current[1]('portfolio'))
    expect(getUrlParam('tab')).toBeNull()
  })

  it('keeps hooks on the same param in sync', () => {
    const a = renderHook(() => useUrlParam('symbol', ''))
    const b = renderHook(() => useUrlParam('symbol', ''))
    act(() => setUrlParams({ symbol: 'V' }))
    expect(a.result.current[0]).toBe('V')
    expect(b.result.current[0]).toBe('V')
  })

  it('push adds a history entry, default replaces', () => {
    const before = window.history.length
    act(() => setUrlParams({ tab: 'alerts' }))
    expect(window.history.length).toBe(before)
    act(() => setUrlParams({ tab: 'technical' }, { push: true }))
    expect(window.history.length).toBe(before + 1)
  })
})
