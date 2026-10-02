// Minimal URL query-param state — no router needed for a single-page tab app.
// Lets links like /?tab=technical&symbol=TSM (e.g. from Telegram alerts) open
// a specific view, and makes the browser back button / bookmarks work.
import { useState, useEffect, useCallback } from 'react'

const EVENT = 'vestis:urlchange'

export function getUrlParam(key) {
  return new URLSearchParams(window.location.search).get(key)
}

// updates: { key: value | null } — null/'' removes the param.
// push=true adds a history entry (back button returns to the previous view).
export function setUrlParams(updates, { push = false } = {}) {
  const params = new URLSearchParams(window.location.search)
  for (const [k, v] of Object.entries(updates)) {
    if (v === null || v === undefined || v === '') params.delete(k)
    else params.set(k, v)
  }
  const qs = params.toString()
  const url = `${window.location.pathname}${qs ? `?${qs}` : ''}${window.location.hash}`
  if (url === `${window.location.pathname}${window.location.search}${window.location.hash}`) return
  window.history[push ? 'pushState' : 'replaceState'](null, '', url)
  window.dispatchEvent(new Event(EVENT))
}

// useState-like hook backed by a query param.
export function useUrlParam(key, defaultValue = '', { push = false } = {}) {
  const read = useCallback(() => getUrlParam(key) ?? defaultValue, [key, defaultValue])
  const [value, setValue] = useState(read)

  useEffect(() => {
    const sync = () => setValue(read())
    window.addEventListener('popstate', sync)
    window.addEventListener(EVENT, sync)
    return () => {
      window.removeEventListener('popstate', sync)
      window.removeEventListener(EVENT, sync)
    }
  }, [read])

  const update = useCallback(v => {
    setUrlParams({ [key]: v === defaultValue ? null : v }, { push })
    setValue(v)
  }, [key, defaultValue, push])

  return [value, update]
}
