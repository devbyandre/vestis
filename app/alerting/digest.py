"""Daily/weekly portfolio digest: snapshot, movers, earnings, alerts since last digest."""
import json
import logging
from typing import Callable, Optional

import pandas as pd

import middleware as mw
from config_utils import get_config, set_config
from .messages import md, vestis_link, describe_alert
from .news_updates import news_digest_lines

Notify = Callable[[str, Optional[tuple]], bool]


def send_digest(notify: Notify, freq: str = "daily") -> None:
    now = pd.Timestamp.utcnow().replace(tzinfo=None)
    key = f"last_digest_sent_{freq}"
    last_sent_raw = get_config(key)
    if last_sent_raw:
        since_ts = pd.Timestamp(last_sent_raw)
        if since_ts.tzinfo is not None:
            since_ts = since_ts.tz_convert("UTC").tz_localize(None)
    else:
        since_ts = now - pd.Timedelta(days=1)
    parts = []

    # ── Portfolio snapshot ─────────────────────────────────────────────────
    try:
        snap = mw.get_latest_holdings_snapshot(aggregate=True)
        if not snap.empty:
            total_mv   = snap['market_value'].sum()
            total_cost = snap['cost_basis'].sum()
            total_pnl  = total_mv - total_cost
            pnl_pct    = (total_pnl / total_cost * 100) if total_cost else 0
            pnl_icon   = "📈" if total_pnl >= 0 else "📉"
            parts.append("📊 *Daily Portfolio Digest*")
            parts.append(f"_{now.strftime('%A, %d %b %Y')}_")
            parts.append("")
            parts.append(f"💰 Total value: *€{total_mv:,.0f}*")
            parts.append(f"{pnl_icon} Total P&L: *€{total_pnl:+,.0f}* ({pnl_pct:+.1f}%)")
            parts.append("")

            # ── Today's movers — 1-day price change per holding ──────────
            try:
                movers = []
                for _, r in snap.iterrows():
                    sym = r.get('symbol', '')
                    if not sym:
                        continue
                    data = mw.fetch_symbol_data(sym)
                    if not data or 'last_price' not in data:
                        continue
                    # Get price from 1 trading day ago
                    prices = mw.db.get_price_history(sym, lookback_days=5)
                    if prices is None or len(prices) < 2:
                        continue
                    prices = prices.sort_values('date')
                    prev_price = float(prices.iloc[-2]['adj_close'] or prices.iloc[-2]['close'] or 0)
                    curr_price = float(data['last_price'])
                    if prev_price > 0:
                        day_chg = (curr_price - prev_price) / prev_price * 100
                        movers.append({
                            'symbol': sym,
                            'name': r.get('security_label') or r.get('name') or sym,
                            'day_chg': day_chg,
                            'market_value': float(r.get('market_value', 0))
                        })

                if movers:
                    movers_df = pd.DataFrame(movers).sort_values('day_chg', ascending=False)
                    top_day = movers_df.head(3)
                    bot_day = movers_df.tail(3)
                    parts.append("📈 *Today's top movers:*")
                    for _, r in top_day.iterrows():
                        label = r.get('name') or r['symbol']
                        parts.append(f"  • {md(label)}: {r['day_chg']:+.1f}%")
                    parts.append("📉 *Today's biggest drops:*")
                    for _, r in bot_day.iterrows():
                        label = r.get('name') or r['symbol']
                        parts.append(f"  • {md(label)}: {r['day_chg']:+.1f}%")
                    parts.append("")
            except Exception:
                logging.exception("Error building daily movers for digest")

            # ── All-time top/bottom (since purchase) ─────────────────────
            snap['pnl_pct'] = (snap['market_value'] - snap['cost_basis']) / snap['cost_basis'].replace(0, float('nan')) * 100
            snap_valid = snap.dropna(subset=['pnl_pct'])
            if not snap_valid.empty:
                top3 = snap_valid.nlargest(3, 'pnl_pct')
                bot3 = snap_valid.nsmallest(3, 'pnl_pct')
                parts.append("🏆 *Best performers (all-time):*")
                for _, r in top3.iterrows():
                    mv = float(r.get('market_value', 0))
                    label = r.get('security_label') or r.get('name') or r.get('symbol', '?')
                    parts.append(f"  • {md(str(label))}: {r['pnl_pct']:+.1f}% (€{mv:,.0f})")
                parts.append("⚠️ *Underperformers (all-time):*")
                for _, r in bot3.iterrows():
                    mv = float(r.get('market_value', 0))
                    label = r.get('security_label') or r.get('name') or r.get('symbol', '?')
                    parts.append(f"  • {md(str(label))}: {r['pnl_pct']:+.1f}% (€{mv:,.0f})")
                parts.append("")

            # ── Upcoming earnings (next 7 days) ──────────────────────────
            try:
                earnings_soon = []
                for _, r in snap.iterrows():
                    sym = r.get('symbol', '')
                    if not sym:
                        continue
                    sec_id = r.get('security_id') or r.get('id')
                    if not sec_id:
                        continue
                    edt = mw.earnings_date(mw.get_security_cache_row(int(sec_id)))
                    if edt is None:
                        continue
                    diff = (edt - now.replace(tzinfo=None)).days
                    if 0 <= diff <= 7:
                        sec_name = r.get('security_label') or r.get('name') or sym
                        earnings_soon.append((sym, sec_name, diff, edt.strftime('%b %d')))
                if earnings_soon:
                    parts.append("📅 *Earnings coming up:*")
                    for sym, name, days, date_str in sorted(earnings_soon, key=lambda x: x[2]):
                        label = "tomorrow" if days == 1 else f"in {days} days" if days > 1 else "today"
                        display = name or sym
                        parts.append(f"  • {md(display)}: {date_str} ({label})")
                    parts.append("")
            except Exception:
                logging.exception("Error building earnings calendar for digest")

    except Exception:
        logging.exception("Error building portfolio snapshot for digest")

    # ── News on holdings since last digest ────────────────────────────────
    try:
        parts.extend(news_digest_lines(since_ts))
    except Exception:
        logging.exception("Digest: news section failed")

    # ── Alerts fired since last digest ────────────────────────────────────
    try:
        notify_mode = f"digest_{freq}"
        rows = mw.get_alert_log_entries(since_ts.isoformat(), notify_mode)
        if rows:
            parts.append("🔔 *Alerts since last digest:*")
            for r in rows:
                try:
                    sec = mw.db.get_security_by_id(int(r.get('security_id'))) or {}
                    basic = mw.get_security_basic(sec.get('symbol')) if sec.get('symbol') else {}
                    name = basic.get('longName') or basic.get('shortName')
                    sym = f"{name} ({sec['symbol']})" if name else sec.get('symbol', '?')
                except Exception:
                    sym = '?'
                desc = describe_alert(r.get('alert_type', ''), r.get('params', '{}'))
                ts   = str(r.get('triggered_at', ''))[:16].replace('T', ' ')
                line = f"  • *{md(sym)}* — {desc}"
                try:
                    detail = json.loads(r.get('payload') or '{}').get('detail')
                except Exception:
                    detail = None
                if detail:
                    line += f" ({md(detail)})"
                if r.get('note'):
                    line += f" — {md(r['note'])}"
                if ts:
                    line += f" ({ts})"
                parts.append(line)
            parts.append("")
        else:
            parts.append("🔔 *No alerts fired since last digest*")
            parts.append("")
    except Exception:
        logging.exception("Error building alert digest")

    parts.append(f"_Generated {now.strftime('%Y-%m-%d %H:%M UTC')}_")
    body = "\n".join(parts)
    if notify(body, vestis_link("🔔 Alert history in Vestis", tab="alerts", mode="history")):
        set_config(key, now.isoformat())
        logging.info("Digest (%s) sent", freq)
