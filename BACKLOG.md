# Vestis — UI/UX Backlog

Planning document only — nothing here is implemented. Items are grounded in the
current React app state (post Streamlit-parity work) as of 2026-07. Each entry
notes the *why* and a rough size (S/M/L) so it's easy to triage later.

## How to use this file

Pick an item, discuss/refine scope, then it becomes a real task. Cross items
off (or move to a "Done" section) as they ship, rather than deleting the
history of why they were proposed.

---

## 1. Feature gaps (things stubbed or explicitly deferred)

- **News & Sentiment follow-ups (S)** — sources are Yahoo (ticker), Google
  News (company name) and market RSS feeds. Open: ETFs rarely have news of
  their own, and the scorer reads English headlines only (most come out
  neutral). Swapping in an ML model only needs `score_headline` replaced.

- **Recurring/scheduled investments (DCA planner) (M)** — flagged as a
  backlog feature early on ("repeating investments"). UI-wise: a form on
  Transactions or Planning to define "invest €X in SYMBOL every N
  weeks/months," a preview of the schedule, and a way to see upcoming
  scheduled buys distinctly from historical transactions. Needs a small
  backend scheduling table + cron entry; UI is the smaller half of this.

- **More event-based alert types (M)** — user asked for "more event
  notifications" as a backlog item. Current alert types (`price`, `rsi`,
  `ma_crossover`, `52w`, `volume_spike`, `pct_change`, `earnings_soon`,
  `mos`) already cover a lot. Candidates worth discussing: dividend
  ex-date/payment reminders, analyst rating changes, insider transaction
  filings, new 52-week volume record (distinct from price), portfolio-level
  alerts (e.g. "portfolio drawdown exceeds X%") rather than only
  security-level ones.

## 2. Alerts UX polish

- **Type-specific alert param forms instead of raw JSON (M)** —
  `TabAlerts.jsx`'s `AlertForm` currently has one freeform JSON textarea for
  `params` regardless of `alert_type`, with a hint comment showing example
  keys. Real users will mistype keys/values with no validation until save.
  Since `ALERT_TYPES` and each type's expected params are already enumerated
  in the "Alert Type Descriptions" expander, that same list can drive a
  small per-type field set (e.g. `price` → threshold number + direction
  select) instead of a JSON blob.

- **DND is all-or-nothing, not time-scheduled (S)** — `config.json`'s `dnd`
  field and the Settings tab checkbox are a single boolean ("skip immediate
  alerts during DND hours") but there's no actual start/end time configured
  anywhere in the UI — the "hours" the tooltip refers to aren't
  user-editable. Either add quiet-hours start/end fields, or reword the
  copy so it's not implying a schedule that doesn't exist yet.

- **In-app notification center (M)** — right now the only notification
  channel is Telegram. A small bell icon + dropdown showing recently
  triggered alerts (using the same `last_triggered` data already in
  `alerts`) would let users check alert history without leaving the app or
  relying on Telegram being configured at all.

## 3. Performance / perceived-loading UX

- **Planning tab's parallel-query load time (M)** — during this session's
  verification, `TabPlanning.jsx`'s 8 parallel `useQuery` calls took
  15-17 seconds end-to-end on real data, dominated by one server-side
  `merge_asof` across ~1,268 days of forward-filled holdings history. This
  was invisible in the UI beyond a generic loading state. Worth: a
  per-section skeleton/loading state (so charts that resolve fast aren't
  blocked behind the slowest one), and/or a backend cache for the expensive
  `allocation-over-time`/`risk-over-time` computation so repeat visits are
  instant. This directly serves the original ask for "more reactive with
  background loading."

- **Skeleton loaders over spinners (S)** — most tabs currently show a
  single centered `LoadingOverlay` for the whole page while data loads.
  Section-level skeletons (matching the eventual layout) feel faster and
  reduce layout shift when data arrives.

## 4. Technical Analysis / Planning follow-ons

- **Compare mode: correlation matrix (M)** — `TabTechnical.jsx`'s Compare
  mode (2-6 securities) currently shows overlaid indexed price, RSI, small-
  multiple volume/histograms, and a KPI table. A pairwise return-correlation
  heatmap would be a natural, cheap addition (all data already fetched
  client-side per symbol) and is a common ask when comparing candidates for
  diversification.

- **Custom date-range picker on timeseries charts (M)** — technical and
  planning charts currently use whatever range the underlying query
  returns. A shared date-range control (e.g. 1M/3M/1Y/5Y/All + custom)
  reused across Technical, Portfolio, and Planning tabs would match
  standard finance-app UX expectations.

## 5. General polish

- **Mobile responsiveness pass (M)** — the app hasn't been explicitly
  tested on narrow viewports this session (all verification used a
  1200×900 desktop viewport via Playwright). Plotly charts in particular
  are prone to overflow/illegible-label issues on phone-width screens.
  Worth a dedicated pass once feature parity work settles down.

- **Dark-mode-only currently — confirm intentional (S)** — the app appears
  to ship one (dark) theme. If that's a deliberate choice, no action; if a
  light theme is wanted for daytime/outdoor use, it's a Tailwind config
  change plus a toggle in Settings.

- **Portfolio-vs-portfolio compare (M)** — Technical Analysis got a
  Compare mode this session; the Portfolio tab itself only supports
  viewing one portfolio (or "all") at a time. A side-by-side compare of
  two portfolios' allocation/performance could reuse the same
  small-multiples pattern built for Technical Compare.

- **Global search / command palette (M)** — with Portfolio, Technical,
  Planning, Watchlist, Transactions, Alerts, Revenues, News and Settings
  tabs now all present, a `Cmd+K`-style quick-jump (to a tab, or straight
  to a security's Technical Deep Dive) would cut down on manual tab
  clicking as the app has grown.

- **Empty/onboarding states (S)** — worth checking what a genuinely empty
  portfolio (zero transactions) looks like across each tab — this session's
  dev-stack work used seeded synthetic data throughout, so first-run empty
  states weren't verified.
