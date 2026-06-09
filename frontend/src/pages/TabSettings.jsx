import { useState, useEffect } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { settingsApi } from '../lib/api'
import { qk } from '../lib/queryClient'
import toast from 'react-hot-toast'
import { LoadingOverlay, ErrorMsg, SectionHeader, Input } from '../components/ui'

function Section({ title, children }) {
  return (
    <div className="card space-y-3">
      <h3 className="text-sm font-semibold text-gray-200 border-b border-surface-3 pb-2">{title}</h3>
      {children}
    </div>
  )
}

function Field({ label, value, onChange, type = 'text', hint, password }) {
  return (
    <div>
      <label className="label">{label}</label>
      <input
        className="input"
        type={password ? 'password' : type}
        value={value ?? ''}
        onChange={e => onChange(e.target.value)}
        step={type === 'number' ? 'any' : undefined}
      />
      {hint && <p className="text-xs text-gray-600 mt-0.5">{hint}</p>}
    </div>
  )
}

export default function TabSettings() {
  const { data: cfg, isLoading, error } = useQuery({ queryKey: qk.settings(), queryFn: settingsApi.get })
  const [form, setForm] = useState(null)

  useEffect(() => { if (cfg) setForm({ ...cfg }) }, [cfg])

  const updateMut = useMutation({
    mutationFn: (settings) => settingsApi.update(settings),
    onSuccess: () => toast.success('Settings saved'),
    onError: e => toast.error(e.message),
  })

  if (isLoading) return <LoadingOverlay />
  if (error) return <ErrorMsg error={error} />
  if (!form) return null

  const set = (k) => (v) => setForm(f => ({ ...f, [k]: v }))

  const handleSave = () => updateMut.mutate(form)

  return (
    <div className="space-y-4 max-w-2xl">
      <SectionHeader title="Settings" subtitle="Stored in config.json. Some changes require app restart." />

      <Section title="News">
        <div className="grid grid-cols-2 gap-3">
          <Field label="Max items" type="number" value={form.news_max_items} onChange={set('news_max_items')} />
          <Field label="Min fetch interval (min)" type="number" value={form.news_min_fetch_minutes} onChange={set('news_min_fetch_minutes')} />
        </div>
      </Section>

      <Section title="Telegram">
        <div className="grid grid-cols-2 gap-3">
          <Field label="Bot token" value={form.telegram_bot_token || ''} onChange={set('telegram_bot_token')} password
            hint={form.telegram_bot_token_set ? '✓ Currently set' : 'Not configured'} />
          <Field label="Chat ID" value={form.telegram_chat_id || ''} onChange={set('telegram_chat_id')} password
            hint={form.telegram_chat_id_set ? '✓ Currently set' : 'Not configured'} />
        </div>
        <label className="flex items-center gap-2 cursor-pointer text-sm text-gray-300">
          <input type="checkbox" checked={!!form.dnd} onChange={e => set('dnd')(e.target.checked)}
            className="rounded border-surface-3 bg-surface-2" />
          Enable Do Not Disturb (skip immediate alerts during DND hours)
        </label>
      </Section>

      <Section title="Tax & Valuation">
        <div className="grid grid-cols-3 gap-3">
          <Field label="Tax rate (fraction)" type="number" value={form.tax_rate} onChange={set('tax_rate')} hint="e.g. 0.25 = 25%" />
          <Field label="Valuation cache (hours)" type="number" value={form.valuation_cache_hours} onChange={set('valuation_cache_hours')} />
          <Field label="KPI cache (hours)" type="number" value={form.kpi_cache_hours} onChange={set('kpi_cache_hours')} />
        </div>
      </Section>

      <Section title="Yahoo Finance / Fetch Throttling">
        <div className="grid grid-cols-2 gap-3">
          <Field label="Max requests/min" type="number" value={form.yf_max_req_per_min} onChange={set('yf_max_req_per_min')} />
          <Field label="Base sleep (sec)" type="number" value={form.yf_base_sleep_sec} onChange={set('yf_base_sleep_sec')} />
        </div>
      </Section>

      <Section title="DCF Parameters">
        <div className="grid grid-cols-2 gap-3">
          <Field label="Projection years" type="number" value={form.dcf_projection_years} onChange={set('dcf_projection_years')} />
          <Field label="Discount rate" type="number" value={form.dcf_discount_rate} onChange={set('dcf_discount_rate')} hint="e.g. 0.10 = 10%" />
          <Field label="Terminal growth" type="number" value={form.dcf_terminal_growth} onChange={set('dcf_terminal_growth')} hint="e.g. 0.025 = 2.5%" />
        </div>
        <label className="flex items-center gap-2 cursor-pointer text-sm text-gray-300">
          <input type="checkbox" checked={!!form.dcf_conservative} onChange={e => set('dcf_conservative')(e.target.checked)}
            className="rounded border-surface-3 bg-surface-2" />
          Conservative mode
        </label>
      </Section>

      <button className="btn-primary" onClick={handleSave} disabled={updateMut.isPending}>
        {updateMut.isPending ? 'Saving…' : 'Save Settings'}
      </button>
    </div>
  )
}
