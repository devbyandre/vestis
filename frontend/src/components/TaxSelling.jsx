import { useState } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { clsx } from 'clsx'
import { Gift, Scissors, Banknote, Bitcoin, Info } from 'lucide-react'
import { planningApi } from '../lib/api'
import { qk } from '../lib/queryClient'
import { fmt, pnlColor } from '../lib/utils'
import { LoadingOverlay, ErrorMsg, MetricCard, SortableTable } from './ui'

const CATEGORY_LABEL = { stock: 'Stock', fund: 'ETF/fund', other: 'Other', crypto: 'Crypto' }

const SELL_COLS = [
  { key: 'portfolio', label: 'Account' },
  { key: 'name', label: 'Security', render: (v, r) => <span title={r.symbol}>{v} <span className="text-gray-600">({r.symbol})</span></span> },
  { key: 'category', label: 'Type', render: v => <span className="text-gray-400">{CATEGORY_LABEL[v] ?? v}</span> },
  { key: 'quantity', label: 'Shares', align: 'right', render: v => fmt.num(v, v < 10 ? 4 : 2), exportValue: v => v },
  { key: 'proceeds', label: 'Proceeds', align: 'right', render: v => fmt.currency(v), exportValue: v => v },
  { key: 'gain', label: 'Gain / loss', align: 'right', render: v => <span className={pnlColor(v)}>{fmt.currency(v)}</span>, exportValue: v => v },
  { key: 'oldest_lot', label: 'From lot bought', align: 'right', render: v => new Date(v).toLocaleDateString('de-DE') },
]

function Section({ icon: Icon, title, subtitle, children }) {
  return (
    <div className="card space-y-3">
      <div className="flex items-start gap-2">
        <Icon size={16} className="text-accent-bright mt-0.5 shrink-0" />
        <div>
          <p className="text-sm font-semibold text-gray-200">{title}</p>
          {subtitle && <p className="text-xs text-gray-500">{subtitle}</p>}
        </div>
      </div>
      {children}
    </div>
  )
}

function Empty({ children }) {
  return <p className="text-sm text-gray-500">{children}</p>
}

function AllowanceBar({ used, total }) {
  const pct = total > 0 ? Math.min(100, (used / total) * 100) : 0
  return (
    <div className="h-2 rounded bg-surface-3 overflow-hidden" title={`${fmt.currency(used)} of ${fmt.currency(total)} used`}>
      <div className="h-full rounded bg-accent" style={{ width: `${pct}%` }} />
    </div>
  )
}

export default function TaxSelling({ portfolioIds }) {
  const [amountInput, setAmountInput] = useState('')
  const [target, setTarget] = useState(null)

  const { data, isLoading, error, isFetching } = useQuery({
    queryKey: qk.taxSales(portfolioIds, target),
    queryFn: () => planningApi.taxSales(portfolioIds, target),
    placeholderData: keepPreviousData,
  })

  if (isLoading) return <LoadingOverlay label="Working out this year's tax position…" />
  if (error) return <ErrorMsg error={error} />

  const st = data.status
  const ratePct = `${(data.tax_rate * 100).toFixed(2).replace(/\.?0+$/, '')}%`
  const ap = data.allowance_plan
  const lp = data.loss_plan
  const cp = data.cash_plan
  const carried = (st.carry_stock || 0) + (st.carry_general || 0)

  const runCash = () => {
    const v = parseFloat(String(amountInput).replace(/\./g, '').replace(',', '.'))
    setTarget(v > 0 ? v : null)
  }

  return (
    <div className="space-y-4">
      {/* Year status */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        <MetricCard label={`Taxable income ${data.year}`} value={fmt.currency(st.taxable_base)}
          sub="gains + dividends after losses, so far" />
        <div className="metric-card">
          <span className="metric-label">Allowance left</span>
          <div className="metric-value text-green-400">{fmt.currency(st.allowance_left)}</div>
          <AllowanceBar used={st.allowance_used} total={data.allowance} />
          <div className="text-xs text-gray-600">{fmt.currency(st.allowance_used)} of {fmt.currency(data.allowance)} used</div>
        </div>
        <MetricCard label="Tax due so far" value={fmt.currency(st.tax_so_far)} color={st.tax_so_far > 0 ? 'text-warning' : ''}
          sub={`at ${ratePct}, above the allowance`} />
        <MetricCard label="Losses carried forward" value={fmt.currency(carried)} color={carried < 0 ? 'text-red-400' : ''}
          sub={carried < 0 ? `stocks ${fmt.currency(st.carry_stock)} · other ${fmt.currency(st.carry_general)}` : 'none from earlier years'} />
      </div>

      {/* 1. Allowance */}
      <Section icon={Gift} title="Use this year's allowance"
        subtitle="Unused allowance is lost on 31 December. Selling and immediately buying back realises gains tax-free and raises your purchase price — so you pay less tax when you sell for real later.">
        {ap ? (
          <>
            <p className="text-sm text-gray-300">
              Sell &amp; rebuy the shares below to realise <b>{fmt.currency(ap.taxable_gain)}</b> of taxable gains for free
              {Math.abs(ap.gain - ap.taxable_gain) > 1 && <> ({fmt.currency(ap.gain)} actual gain — ETF gains count only partly)</>}.
              That saves about <b className="text-green-400">{fmt.currency(ap.future_tax_saved)}</b> of tax later.
            </p>
            <SortableTable columns={SELL_COLS} defaultSort={{ key: 'proceeds', asc: false }} data={ap.sells} exportable exportName="allowance_sells" />
            <p className="text-xs text-gray-500">
              The broker only applies the exemption order (Freistellungsauftrag) you gave it — check it has enough left, otherwise it
              withholds tax that you only get back with your tax return. Leave a few days before year-end for the trades to settle.
            </p>
          </>
        ) : st.allowance_left > 1
          ? <Empty>No positions with gains to realise.</Empty>
          : <Empty>Your allowance for {data.year} is already used up.</Empty>}
      </Section>

      {/* 2. Losses */}
      <Section icon={Scissors} title="Harvest losses"
        subtitle="Realising losses offsets gains you've already been taxed on this year — the broker refunds the tax right away.">
        {lp ? (
          <>
            <p className="text-sm text-gray-300">
              Selling these realises <b className="text-red-400">{fmt.currency(lp.loss)}</b> of losses and gets back
              about <b className="text-green-400">{fmt.currency(lp.tax_saved)}</b> of this year's tax. You can buy back right away.
            </p>
            <SortableTable columns={SELL_COLS} defaultSort={{ key: 'proceeds', asc: false }} data={lp.sells} exportable exportName="loss_harvest_sells" />
            <p className="text-xs text-gray-500">Losses on single stocks only offset gains on stocks, so they're only suggested when there are stock gains to offset.</p>
          </>
        ) : st.tax_so_far > 0
          ? <Empty>None of your positions with losses can reduce this year's tax (losses on single stocks only offset gains on stocks).</Empty>
          : <Empty>No tax due this year so far — nothing to offset.</Empty>}
      </Section>

      {/* 3. Cash */}
      <Section icon={Banknote} title="Raise cash with the least tax"
        subtitle="Picks the sales that cost the least tax — losses and low-gain lots first, then the allowance.">
        <div className="flex flex-wrap items-end gap-2">
          <div>
            <label className="label">Amount needed (€)</label>
            <input className="input w-40" inputMode="decimal" value={amountInput} placeholder="e.g. 10000"
              onChange={e => setAmountInput(e.target.value)} onKeyDown={e => e.key === 'Enter' && runCash()} />
          </div>
          <button className="btn btn-primary text-xs px-3 py-2" onClick={runCash} disabled={isFetching}>
            {isFetching && target ? 'Planning…' : 'Plan sales'}
          </button>
        </div>
        {cp && (
          <>
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
              <MetricCard label="Raised" value={fmt.currency(cp.proceeds)}
                sub={cp.shortfall > 1 ? `${fmt.currency(cp.shortfall)} short — not enough to sell` : undefined}
                color={cp.shortfall > 1 ? 'text-warning' : ''} />
              <MetricCard label="Tax on these sales" value={fmt.currency(cp.tax)} sub={`${fmt.pct(cp.effective_rate, 1).replace('+', '')} of the amount`} />
              <MetricCard label="Selling evenly instead" value={cp.naive_tax != null ? fmt.currency(cp.naive_tax) : '—'}
                sub="same share of every position" />
              <MetricCard label="You save" value={cp.naive_tax != null ? fmt.currency(Math.max(0, cp.naive_tax - cp.tax)) : '—'}
                color="text-green-400" />
            </div>
            {cp.gain < -1 && (
              <p className="text-xs text-gray-500">
                These sales realise {fmt.currency(Math.abs(cp.gain))} of losses. Any loss not used this year is carried forward
                and reduces tax on future gains (losses on single stocks only against stock gains).
              </p>
            )}
            {cp.sells.length > 0 && <SortableTable columns={SELL_COLS} defaultSort={{ key: 'proceeds', asc: false }} data={cp.sells} exportable exportName="cash_sells" />}
          </>
        )}
      </Section>

      {/* 4. Crypto */}
      {data.crypto.length > 0 && (
        <Section icon={Bitcoin} title="Crypto holding periods"
          subtitle="Crypto held for more than a year is tax-free to sell, without using the allowance. Sold earlier, gains are taxed at your personal income-tax rate.">
          <SortableTable data={data.crypto} defaultSort={{ key: 'tax_free_from', asc: true }} columns={[
            { key: 'portfolio', label: 'Account' },
            { key: 'symbol', label: 'Coin' },
            { key: 'bought', label: 'Bought', render: v => new Date(v).toLocaleDateString('de-DE') },
            { key: 'quantity', label: 'Amount', align: 'right', render: v => fmt.num(v, 6) },
            { key: 'value', label: 'Value', align: 'right', render: v => fmt.currency(v) },
            { key: 'gain', label: 'Gain', align: 'right', render: v => <span className={pnlColor(v)}>{fmt.currency(v)}</span> },
            { key: 'tax_free_from', label: 'Tax-free from', align: 'right', render: (v, r) => (
              <span className={clsx(r.tax_free_now ? 'text-green-400' : 'text-amber-400')}>
                {r.tax_free_now ? 'now' : new Date(v).toLocaleDateString('de-DE')}
              </span>
            ) },
          ]} />
        </Section>
      )}

      <p className="text-xs text-gray-600 flex gap-1.5">
        <Info size={12} className="shrink-0 mt-0.5" />
        <span>
          Estimate under German rules: FIFO per account, separate loss pot for stocks, {fmt.currency(data.allowance)} allowance,
          {' '}{Math.round(data.fund_partial_exemption * 100)}% of ETF gains tax-free (all ETFs treated as equity funds), tax rate {ratePct} — all
          adjustable in Settings. Not included: advance lump sum (Vorabpauschale), foreign withholding tax, trading fees.
          The allowance is per person — select only your own accounts above. Not tax advice.
        </span>
      </p>
    </div>
  )
}
