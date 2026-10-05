// TabNews.jsx — headlines for holdings/watchlist with a rough headline sentiment
import { useRef, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient, keepPreviousData } from '@tanstack/react-query'
import { RefreshCw, ExternalLink, LineChart } from 'lucide-react'
import { clsx } from 'clsx'
import toast from 'react-hot-toast'
import { newsApi } from '../lib/api'
import { qk } from '../lib/queryClient'
import { timeAgo, sentimentLabel, sentimentBadgeClass, fmtSentiment } from '../lib/utils'
import { useUrlParam, setUrlParams } from '../lib/urlState'
import { LoadingOverlay, ErrorMsg, SectionHeader, MetricCard } from '../components/ui'

const SCOPES = [['all', 'All'], ['holdings', 'Holdings'], ['watchlist', 'Watchlist'], ['market', 'Market']]
const DAYS = [1, 3, 7, 30]

function Chips({ options, value, onChange }) {
  return (
    <div className="flex flex-wrap gap-1.5">
      {options.map(([v, label]) => (
        <button key={v} type="button"
          className={clsx('badge cursor-pointer', value === v ? 'badge-blue' : 'bg-surface-3 text-gray-400')}
          onClick={() => onChange(v)}>
          {label}
        </button>
      ))}
    </div>
  )
}

// Diverging bar: negative share grows to the left of the centre line, positive to the right.
function SentimentBar({ positive, negative, neutral }) {
  const total = positive + negative + neutral
  if (!total) return null
  const pct = n => `${(n / total) * 50}%`
  return (
    <div className="relative h-1.5 w-full rounded bg-surface-3" aria-hidden="true">
      <div className="absolute inset-y-0 bg-danger/70 rounded-l" style={{ right: '50%', width: pct(negative) }} />
      <div className="absolute inset-y-0 bg-success/70 rounded-r" style={{ left: '50%', width: pct(positive) }} />
      <div className="absolute inset-y-0 left-1/2 w-px bg-gray-600" />
    </div>
  )
}

function SecurityRow({ row, active, onSelect }) {
  const covered = row.count > 0
  return (
    <button type="button" onClick={() => onSelect(row.symbol)}
      className={clsx('w-full text-left px-3 py-2 rounded border transition-colors',
        active ? 'border-accent bg-accent/5' : 'border-transparent hover:bg-surface-2')}>
      <div className="flex items-center justify-between gap-2">
        <span className="text-sm font-medium text-gray-200 truncate">{row.symbol}</span>
        {covered
          ? <span className={clsx('badge', sentimentBadgeClass(row.avg_sentiment))}>{fmtSentiment(row.avg_sentiment)}</span>
          : <span className="text-xs text-gray-600">no coverage</span>}
      </div>
      {covered && (
        <>
          <div className="my-1.5"><SentimentBar {...row} /></div>
          <div className="text-xs text-gray-500">
            {row.count} {row.count === 1 ? 'article' : 'articles'} · {row.positive} pos · {row.negative} neg
            {row.latest && <> · {timeAgo(row.latest)}</>}
          </div>
        </>
      )}
    </button>
  )
}

function Article({ item, onSelectSymbol }) {
  const label = sentimentLabel(item.sentiment)
  return (
    <article className="card py-3">
      <div className="flex items-start justify-between gap-3">
        <a href={item.url} target="_blank" rel="noopener noreferrer"
          className="text-sm font-medium text-gray-100 hover:text-accent-bright inline-flex gap-1.5 items-start">
          <span>{item.title}</span>
          <ExternalLink size={12} className="mt-1 shrink-0 text-gray-600" />
        </a>
        <span className={clsx('badge shrink-0', sentimentBadgeClass(item.sentiment))}
          title={`Headline score ${fmtSentiment(item.sentiment)}`}>
          {label}
        </span>
      </div>
      <div className="mt-2 flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-gray-500">
        {item.publisher && <span>{item.publisher}</span>}
        <span title={item.published_at}>{timeAgo(item.published_at)}</span>
        {item.symbols.map(s => (
          <button key={s} type="button" className="badge bg-surface-3 text-gray-400 cursor-pointer"
            onClick={() => onSelectSymbol(s)}>
            {s}
          </button>
        ))}
      </div>
    </article>
  )
}

export default function TabNews() {
  const qc = useQueryClient()
  const [scope, setScope] = useUrlParam('scope', 'all')
  const [daysParam, setDaysParam] = useUrlParam('days', '7')
  const [symbol, setSymbol] = useUrlParam('symbol', '')
  const days = DAYS.includes(Number(daysParam)) ? Number(daysParam) : 7
  const timers = useRef([])
  useEffect(() => () => timers.current.forEach(clearTimeout), [])

  const { data, isLoading, isFetching, error } = useQuery({
    queryKey: qk.news(scope, days, symbol),
    queryFn: () => newsApi.feed({ scope, days, symbol }),
    placeholderData: keepPreviousData,
  })

  const refresh = useMutation({
    mutationFn: () => newsApi.refresh(symbol),
    onSuccess: res => {
      if (res?.started) {
        toast.success('Refreshing news in the background — this takes a few minutes')
        // Results trickle in while the run proceeds; re-read a couple of times.
        timers.current.push(
          setTimeout(() => qc.invalidateQueries({ queryKey: ['news'] }), 10_000),
          setTimeout(() => qc.invalidateQueries({ queryKey: ['news'] }), 45_000),
        )
      } else {
        toast.success(res?.stored ? `Fetched ${res.stored} headlines` : 'No new headlines found')
        qc.invalidateQueries({ queryKey: ['news'] })
      }
    },
    onError: e => toast.error(e?.response?.data?.detail || 'News refresh failed'),
  })

  const items = data?.items ?? []
  const summary = data?.summary ?? []
  const overall = data?.overall
  const selectSymbol = s => setSymbol(s === symbol ? '' : s)
  const isMarket = scope === 'market'

  return (
    <div className="space-y-5">
      <SectionHeader
        title="News & Sentiment"
        subtitle="Recent headlines for your holdings and watchlist, scored from the headline text only"
        action={
          <button className="btn-ghost text-xs flex items-center gap-1.5"
            disabled={refresh.isPending} onClick={() => refresh.mutate()}>
            <RefreshCw size={13} className={clsx(refresh.isPending && 'animate-spin')} />
            {symbol ? `Refresh ${symbol}` : 'Refresh'}
          </button>
        }
      />

      <div className="flex flex-wrap items-center gap-x-6 gap-y-2">
        <Chips options={SCOPES} value={scope} onChange={setScope} />
        <Chips options={DAYS.map(d => [d, d === 1 ? '24 h' : `${d} days`])} value={days}
          onChange={d => setDaysParam(String(d))} />
        {symbol && (
          <div className="flex items-center gap-2">
            <span className="badge badge-blue">{symbol}</span>
            <button className="text-xs text-gray-500 hover:text-gray-300" onClick={() => setSymbol('')}>clear</button>
            <button className="text-xs text-accent-bright flex items-center gap-1"
              onClick={() => setUrlParams({ tab: 'technical', symbol }, { push: true })}>
              <LineChart size={12} /> Open in Technical
            </button>
          </div>
        )}
        {isFetching && !isLoading && <span className="text-xs text-gray-600">Updating…</span>}
      </div>

      {isLoading && <LoadingOverlay label="Loading news…" />}
      {error && <ErrorMsg error={error} label="Failed to load news" />}

      {data && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <MetricCard label="Articles" value={overall?.count ?? 0} />
            <MetricCard label="Positive" value={overall?.positive ?? 0} color="text-success" />
            <MetricCard label="Negative" value={overall?.negative ?? 0} color="text-danger" />
            <MetricCard label="Avg sentiment" value={fmtSentiment(overall?.avg_sentiment)}
              color={overall?.avg_sentiment == null ? '' : overall.avg_sentiment >= 0.2 ? 'text-success'
                : overall.avg_sentiment <= -0.2 ? 'text-danger' : ''} />
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-3 gap-5">
            {isMarket ? (
              <div className="card lg:col-span-1 self-start text-xs text-gray-500 space-y-2">
                <h3 className="text-sm font-semibold text-gray-200">Market news</h3>
                <p>General market headlines from CNBC, MarketWatch, Investing.com, Nasdaq and the FT.</p>
                <p>Headlines that name one of your companies also appear under that security.</p>
              </div>
            ) : (
            <div className="card lg:col-span-1 self-start">
              <h3 className="text-sm font-semibold text-gray-200 mb-2">By security</h3>
              {summary.length === 0
                ? <p className="text-xs text-gray-500">Nothing to track yet — add holdings or watchlist entries.</p>
                : (
                  <div className="space-y-0.5 max-h-[32rem] overflow-y-auto">
                    {summary.map(r => (
                      <SecurityRow key={r.symbol} row={r} active={r.symbol === symbol} onSelect={selectSymbol} />
                    ))}
                  </div>
                )}
              <p className="text-xs text-gray-600 mt-3">
                Sources: Yahoo Finance by ticker, Google News by company name and market news feeds.
                ETFs rarely make headlines of their own — see Market for the wider picture.
              </p>
            </div>
            )}

            <div className="lg:col-span-2 space-y-3">
              {items.length === 0 ? (
                <div className="card text-center py-12 text-gray-500">
                  <p className="text-sm">No headlines in this period.</p>
                  <p className="text-xs mt-1">Use Refresh to fetch the latest ones, or widen the time range.</p>
                </div>
              ) : items.map(it => (
                <Article key={it.uid} item={it} onSelectSymbol={selectSymbol} />
              ))}
            </div>
          </div>
        </>
      )}
    </div>
  )
}
