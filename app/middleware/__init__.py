"""
middleware package — split by domain from the original single-file middleware.py.

Re-exports every public name so every existing call site across the repo
(`import middleware as mw; mw.func(...)`) keeps working completely unchanged.
"""

# The original flat middleware.py did `import db_utils as db` at module level,
# making `db` a real attribute of the middleware module — tests reach into
# mw.db.* directly to monkeypatch specific db_utils functions in isolation.
# Every submodule below does its own `import db_utils as db` too; since
# that's just a name binding to the one shared db_utils module object (not a
# copy), mutating attributes via mw.db.* affects what the submodules see too.
import db_utils as db

from .indicators import (
    sma,
    ema,
    bollinger,
    rsi,
    local_min_max,
    calc_returns,
    find_crossovers,
    volatility,
    max_drawdown,
    sharpe_ratio,
    sortino_ratio,
    cagr,
    calmar_ratio,
    treynor_ratio,
    information_ratio,
)

from .securities import (
    get_all_securities,
    get_security,
    add_new_security,
    add_security,
    get_security_basic,
)

from .transactions import (
    add_transaction,
    edit_transaction,
    remove_transaction,
    list_transactions,
    list_transactions_detailed,
    apply_splits_to_transactions,
    add_split_transaction,
)

from .watchlist import (
    get_watchlist,
    calc_security_KPIs,
    delete_security_from_watchlist,
    get_watchlist_symbols,
)

from .valuation import (
    extract_fcf_from_cashflow_payloads,
    compute_dcf_raw,
    compute_dcf_cached,
)

from .revenues import (
    calc_dividends_for_portfolio,
    calc_capital_gains_fifo,
)

from .alerts import (
    get_alerts,
    get_all_alerts_for_ui,
    get_automatic_alerts,
    create_alert,
    edit_alert,
    toggle_alert,
    delete_alert,
    log_trigger,
    last_trigger,
    get_alert_log_entries,
    fetch_symbol_data,
    evaluate_alert,
    save_alert_state,
)

from .holdings import (
    get_holdings,
    get_latest_holdings_snapshot,
    holdings_timeseries,
    recompute_all_holdings_timeseries,
    store_prices,
    fetch_portfolio_risk_timeseries,
    update_portfolio_risk_timeseries_for_portfolios,
)

from .portfolios import (
    create_portfolio,
    rename_portfolio,
    list_portfolios,
    delete_and_reassign_portfolio,
    get_all_symbols,
    get_portfolio_symbols,
)

from .rebalancing import (
    suggest_rebalancing,
    get_complete_taxonomy,
)

from .fx import (
    store_fx_rates,
)
