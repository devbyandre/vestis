import { useQuery } from '@tanstack/react-query'
import { portfolioApi } from '../lib/api'
import { qk } from '../lib/queryClient'
import { clsx } from 'clsx'

export function PortfolioFilter({ selected, onChange, multi = true }) {
  const { data: portfolios = [], isLoading } = useQuery({
    queryKey: qk.portfolios(),
    queryFn: portfolioApi.list,
  })

  if (isLoading) return <div className="text-xs text-gray-600 animate-pulse">Loading portfolios…</div>

  if (multi) {
    return (
      <div className="flex flex-wrap gap-2">
        <button
          className={clsx(
            'badge cursor-pointer transition-all',
            selected.length === 0 ? 'badge-blue' : 'bg-surface-3 text-gray-400 hover:bg-surface-4'
          )}
          onClick={() => onChange([])}
        >
          All
        </button>
        {portfolios.map(p => {
          const active = selected.includes(p.id)
          return (
            <button
              key={p.id}
              className={clsx(
                'badge cursor-pointer transition-all',
                active ? 'badge-blue' : 'bg-surface-3 text-gray-400 hover:bg-surface-4'
              )}
              onClick={() => onChange(
                active ? selected.filter(id => id !== p.id) : [...selected, p.id]
              )}
            >
              {p.name}
            </button>
          )
        })}
      </div>
    )
  }

  return (
    <select
      className="select text-sm"
      value={selected ?? ''}
      onChange={e => onChange(e.target.value ? Number(e.target.value) : null)}
    >
      <option value="">All portfolios</option>
      {portfolios.map(p => (
        <option key={p.id} value={p.id}>{p.name}</option>
      ))}
    </select>
  )
}
