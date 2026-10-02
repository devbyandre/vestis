"""
db_utils package — split by domain from the original single-file db_utils.py.

Re-exports every public (and a few underscore-prefixed but de-facto public,
e.g. tests reach into db._read_sql/db.get_engine directly) name so every
existing call site across the repo (`import db_utils as db; db.func(...)`)
keeps working completely unchanged.
"""

from .core import (
    get_engine,
    get_conn,
    _raw_conn,
    _ph,
    _adapt_sql,
    _read_sql,
    to_iso_date,
    to_iso_ts,
    _IS_POSTGRES,
)

from .portfolios import (
    get_portfolio_by_name,
    list_portfolios,
    insert_portfolio,
    rename_portfolio,
    delete_portfolio,
    reassign_transactions,
)

from .securities import (
    get_security_id,
    list_securities,
    get_all_symbols,
    insert_security,
    get_security_by_symbol,
    get_security_by_id,
    list_securities_metadata,
    update_security,
)

from .securities_cache import (
    store_security_cache,
    store_lazy_security,
    get_security_cache,
    get_last_info_update,
    get_last_prices_update,
)

from .transactions import (
    insert_transaction,
    update_transaction,
    list_transactions,
    list_transactions_detailed,
    get_first_buy_date,
    get_recorded_splits,
    store_split_alert,
    clear_split_alert,
    list_transactions_for_security_any_portfolio,
    list_transactions_for_security,
    delete_transaction,
    delete_security_if_no_transactions,
    get_transaction_by_id,
)

from .watchlist import (
    get_watchlist,
    delete_watchlist_item,
)

from .prices import (
    get_latest_price,
    get_price_series,
    get_price_history,
    get_price_bars,
    store_prices,
)

from .dividends import (
    get_dividends,
    store_dividends,
)

from .financials import (
    store_financials,
)

from .valuations import (
    store_dcf,
    get_cached_dcf,
    get_cashflow_payloads,
)

from .alerts import (
    get_all_alerts,
    create_alert,
    update_alert,
    toggle_alert_active,
    delete_alert,
    log_alert_trigger,
    set_alert_state,
    get_last_alert_log,
    last_trigger_time,
    get_active_alerts,
    get_alert_by_id,
    get_alerts_for_digest,
    get_alert_history,
)

from .holdings import (
    insert_holdings_timeseries,
    list_portfolios_holding_security,
    get_holdings_from_transactions,
    list_prices_for_security,
    clear_holdings_timeseries,
    get_holdings_timeseries,
    recompute_holdings_timeseries,
    get_security_risk_timeseries,
    update_security_risk_timeseries,
    get_portfolio_risk_timeseries,
    get_portfolio_risk_timeseries_detailed,
)

from .fx import (
    get_all_security_currencies,
    get_earliest_price_or_dividend_date,
    store_fx_rates,
    get_latest_fx_date,
    get_latest_fx_rate,
    get_fx_series,
)
