#!/usr/bin/env python3
# db_utils/core.py – SQLite ➜ PostgreSQL drop-in replacement: connection bootstrap
#
# Connection source priority:
#   1. DATABASE_URL env var  (Docker / production)
#   2. config.json "db_url"  (optional override)
#   3. config.json "db_path" (legacy SQLite path – still works for local dev)

import os
import logging
import re
from contextlib import contextmanager
from typing import Optional

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def _build_url() -> str:
    """Return the database URL to use, with PostgreSQL preferred."""
    # 1. Docker / environment variable (highest priority)
    url = os.environ.get("DATABASE_URL", "").strip()
    if url:
        return url

    # 2. config.json explicit url
    try:
        from config_utils import get_config
        url = get_config("db_url") or ""
        if url:
            return url
    except Exception:
        pass

    # 3. Fallback: SQLite from config "db_path"
    try:
        from config_utils import get_config
        db_path = os.path.expanduser(get_config("db_path") or "portfolio.db")
    except Exception:
        db_path = "portfolio.db"
    return f"sqlite:///{db_path}"


_DB_URL: str = _build_url()
_IS_POSTGRES: bool = _DB_URL.startswith("postgresql") or _DB_URL.startswith("postgres")

_SAFE_URL = re.sub(r"://([^:/@]+):[^@]*@", r"://\1:***@", _DB_URL)
logging.info(f"db_utils: using {'PostgreSQL' if _IS_POSTGRES else 'SQLite'} → {_SAFE_URL[:80]}")

# ── SQLAlchemy engine (used for pd.read_sql_query) ────────────────────────────
from sqlalchemy import create_engine, text as sa_text
from sqlalchemy.pool import NullPool

_engine = create_engine(
    _DB_URL,
    poolclass=NullPool,   # each call gets a fresh connection → thread-safe without extra work
    future=True,
)


def get_engine():
    return _engine


# ── Low-level connection context manager ─────────────────────────────────────
# Returns a DBAPI2-compatible connection so existing code (conn.execute,
# conn.executemany, conn.commit) keeps working unchanged.

@contextmanager
def _conn_ctx():
    """Yield a raw DBAPI connection and commit/rollback automatically."""
    raw = _engine.raw_connection()
    try:
        yield raw
        raw.commit()
    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()


def get_conn():
    """
    Return a new DBAPI connection.

    Callers that use  `with get_conn() as conn:`  still work because psycopg2
    and sqlite3 connections both support the context-manager protocol
    (commit on __exit__ success, rollback on exception).
    """
    return _conn_ctx()


def _raw_conn():
    """Return raw DBAPI connection for functions managing their own lifecycle."""
    return _engine.raw_connection()


# ── Database availability (deploys restart Postgres) ─────────────────────────

# Messages that mean the server is unreachable — not a bad query. Matching on
# the message because SQLite raises the same OperationalError type for
# "no such table" and friends.
_CONNECTION_ERROR_MARKERS = (
    "the database system is shutting down",
    "the database system is starting up",
    "the database system is in recovery mode",
    "terminating connection due to administrator command",
    "server closed the connection unexpectedly",
    "could not connect to server",
    "connection refused",
    "could not translate host name",
    "connection timed out",
    "ssl connection has been closed unexpectedly",
)


def is_connection_error(exc: BaseException) -> bool:
    """True if exc (or anything it was raised from) is a lost/refused DB connection."""
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if getattr(exc, "connection_invalidated", False):
            return True
        msg = str(exc).lower()
        if any(m in msg for m in _CONNECTION_ERROR_MARKERS):
            return True
        exc = exc.__cause__ or exc.__context__
    return False


def wait_for_db(timeout: float = 90, interval: float = 5) -> bool:
    """Poll until the database answers (True) or timeout seconds pass (False)."""
    import time
    deadline = time.monotonic() + timeout
    while True:
        try:
            with _engine.connect() as con:
                con.execute(sa_text("SELECT 1"))
            return True
        except Exception as exc:
            if time.monotonic() >= deadline:
                logging.error("Database still unreachable after %ss: %s", timeout, exc)
                return False
            logging.warning("Database unreachable, retrying in %ss: %s", interval, str(exc).splitlines()[0])
            time.sleep(interval)


def db_outage_recovered(exc: BaseException, timeout: float = 90) -> bool:
    """True if exc was a lost DB connection and the database is back within timeout
    — e.g. Postgres restarting during a deploy. Not worth an alert; the next
    scheduled run picks up where this one stopped."""
    return is_connection_error(exc) and wait_for_db(timeout)


# ── SQL dialect helper ────────────────────────────────────────────────────────

def _ph() -> str:
    """Return the correct placeholder for the active dialect."""
    return "%s" if _IS_POSTGRES else "?"


def _adapt_sql(sql: str) -> str:
    """Convert SQLite-style ? placeholders to %s for PostgreSQL."""
    if _IS_POSTGRES:
        return sql.replace("?", "%s")
    return sql


# ── Pandas read helper ────────────────────────────────────────────────────────

def _read_sql(sql: str, params=None) -> pd.DataFrame:
    """Execute a SELECT and return a DataFrame, dialect-agnostic."""
    adapted = _adapt_sql(sql)
    with _engine.connect() as con:
        if params:
            # SQLAlchemy 2.x requires named params as dicts with sa_text.
            # Works for both SQLite (?) and Postgres (%s) — convert to :p0, :p1...
            named_sql = adapted
            named_params = {}
            i = 0
            placeholder = "%s" if _IS_POSTGRES else "?"
            while placeholder in named_sql:
                named_sql = named_sql.replace(placeholder, f":p{i}", 1)
                named_params[f"p{i}"] = params[i]
                i += 1
            return pd.read_sql_query(sa_text(named_sql), con, params=named_params)
        return pd.read_sql_query(sa_text(adapted), con)


# ── Date helpers ──────────────────────────────────────────────────────────────

def to_iso_date(val: Optional[str]) -> Optional[str]:
    if val is None:
        return None
    return pd.to_datetime(val, utc=True).date().isoformat()


def to_iso_ts(val) -> Optional[str]:
    if val is None:
        return None
    return pd.to_datetime(val, utc=True).isoformat()
