"""Automatic alerts: trailing stops, recovery, sudden-drop, earnings, watchlist crosses."""
import json
import logging

import middleware as mw

LEGACY_DIP_NOTE = "Price dip - potential buy"


def ensure_alert(security_id, alert_type, params, note="",
                  notify_mode="immediate", cooldown_seconds=14400):
    alerts = mw.get_automatic_alerts()
    existing = alerts[
        (alerts['security_id'] == int(security_id)) &
        (alerts['alert_type'] == alert_type)
    ]
    params_str = json.dumps(params, sort_keys=True)
    if not existing.empty:
        alert = existing.iloc[0]
        current_params = json.loads(alert.get('params') or '{}')
        if json.dumps(current_params, sort_keys=True) != params_str or alert.get('note', '') != note:
            mw.edit_alert(alert_id=int(alert['id']), params=params, note=note, automatic=True)
    else:
        mw.create_alert(security_id=int(security_id), alert_type=alert_type,
                        params=params, note=note, notify_mode=notify_mode,
                        cooldown_seconds=cooldown_seconds, automatic=True)

def maintain_alerts():
    try:
        holdings = mw.get_holdings()
    except Exception:
        logging.exception("Could not load holdings for maintain_alerts")
        return

    # An old version created "Price dip - potential buy" alerts with thresholds
    # in the listing currency (alerts are evaluated in EUR); nothing creates
    # them any more, so switch any remaining ones off.
    try:
        legacy = mw.get_automatic_alerts()
        if not legacy.empty:
            for _, alert in legacy[legacy['note'] == LEGACY_DIP_NOTE].iterrows():
                mw.toggle_alert(int(alert['id']), False)
                logging.info("Deactivated legacy price-dip alert %s", alert['id'])
    except Exception:
        logging.exception("Could not deactivate legacy price-dip alerts")

    # Deactivate automatic alerts for securities no longer held — otherwise
    # a price/trailing-stop alert created while a position was open keeps
    # firing forever after it's fully sold, since nothing else clears it.
    # Watchlist golden-cross alerts are the exception: they're for securities
    # NOT held, so excluding them here keeps them from being deactivated and
    # recreated (with fresh history, so no cooldown) on every run.
    try:
        held_ids = set(int(x) for x in holdings['security_id']) if not holdings.empty else set()
        watch_ids = set()
        for symbol in mw.get_watchlist_symbols():
            sec = mw.get_security(symbol)
            if sec:
                watch_ids.add(int(sec['id']))
        stale = mw.get_automatic_alerts()
        if not stale.empty:
            sec_ids = stale['security_id'].astype(int)
            watch_alert = (stale['alert_type'] == 'ma_crossover') & sec_ids.isin(watch_ids)
            stale = stale[~sec_ids.isin(held_ids) & ~watch_alert]
            for _, alert in stale.iterrows():
                mw.toggle_alert(int(alert['id']), False)
                logging.info("Deactivated stale automatic alert %s for security_id=%s (no longer held)",
                             alert['id'], alert['security_id'])
    except Exception:
        logging.exception("Could not clean up stale automatic alerts")

    for _, row in holdings.iterrows():
        symbol      = row['symbol']
        security_id = row['security_id']
        data = mw.fetch_symbol_data(symbol)
        if not data or 'last_price' not in data:
            logging.warning("fetch_symbol_data: no price history for %s", symbol)
            continue
        current_price = float(data['last_price'])
        buy_price = float(row.get('avg_cost') or row.get('buy_price') or current_price)

        # Trailing stop: only for positions >5% in profit
        # Threshold only moves UP — never down — so it truly trails
        if current_price > buy_price * 1.05:
            new_stop = round(current_price * 0.95, 4)
            existing = mw.get_automatic_alerts()
            ex = existing[
                (existing['security_id'] == int(security_id)) &
                (existing['alert_type'] == 'price')
            ]
            if not ex.empty:
                try:
                    old_stop = float(json.loads(ex.iloc[0].get('params') or '{}').get('threshold', 0))
                except Exception:
                    old_stop = 0
                # Only raise the stop, never lower it
                if new_stop > old_stop:
                    ensure_alert(security_id, "price",
                                  {"threshold": new_stop, "mode": "absolute", "direction": "below"},
                                  note=f"Trailing stop (95% of {current_price:.2f})",
                                  cooldown_seconds=14400)
            else:
                ensure_alert(security_id, "price",
                              {"threshold": new_stop, "mode": "absolute", "direction": "below"},
                              note=f"Trailing stop (95% of {current_price:.2f})",
                              cooldown_seconds=14400)

        # Recovery alert: position underwater, alert when it crosses back to buy price
        elif current_price < buy_price * 0.95:
            ensure_alert(security_id, "price",
                          {"threshold": buy_price, "mode": "absolute", "direction": "above"},
                          note="Recovery to buy price",
                          notify_mode="digest_daily",
                          cooldown_seconds=86400)

        # Sudden drop alert: >5% drop in last 24h — always on for all holdings
        ensure_alert(security_id, "pct_change",
                      {"pct": 5, "days": 1, "direction": "down"},
                      note="Sudden drop >5% in 24h",
                      cooldown_seconds=14400)

        # Earnings soon: alert 3 days before earnings for any holding
        ensure_alert(security_id, "earnings_soon",
                      {"days": 3},
                      note="Earnings in 3 days",
                      notify_mode="digest_daily",
                      cooldown_seconds=86400)

    # Watchlist: golden cross alert
    try:
        watchlist = mw.get_watchlist_symbols()
        for symbol in watchlist:
            sec = mw.get_security(symbol)
            if not sec:
                continue
            security_id = sec['id']
            ensure_alert(security_id, "ma_crossover",
                          {"short": 20, "long": 50, "ma_type": "SMA", "crossover_type": "golden"},
                          note="Golden cross — potential entry",
                          notify_mode="digest_daily",
                          cooldown_seconds=86400)
    except Exception:
        logging.exception("Error maintaining watchlist alerts")

    # ensure_alert only sees active alerts, so an inactive automatic alert (stale
    # after a sale, or switched off in the UI) gets a fresh twin; prune those.
    try:
        removed = mw.dedupe_auto_alerts()
        if removed:
            logging.info("Removed %d duplicate automatic alerts", removed)
    except Exception:
        logging.exception("Could not remove duplicate automatic alerts")
