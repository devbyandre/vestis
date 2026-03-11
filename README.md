<div align="center">

# 🏛️ Vestis

**Self-hosted stock portfolio analytics for your home server.**

[![CI](https://github.com/YOUR_USERNAME/vestis/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/vestis/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue?logo=python&logoColor=white)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/database-PostgreSQL%2016-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Streamlit](https://img.shields.io/badge/UI-Streamlit-ff4b4b?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ed?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

[Features](#-features) · [Quick Start](#-quick-start) · [Architecture](#-architecture) · [Configuration](#%EF%B8%8F-configuration) · [Development](#%EF%B8%8F-development) · [Contributing](#-contributing)

---

![Vestis Dashboard](https://placehold.co/900x480/1a1a2e/4f8ef7?text=Vestis+Dashboard+Screenshot)

*Track your entire portfolio — holdings, performance, valuations and alerts — from a single self-hosted dashboard.*

</div>

---

## ✨ Features

| | |
|---|---|
| 📊 **Portfolio Dashboard** | Holdings, P&L, CAGR, max drawdown and Sharpe ratio across multiple portfolios |
| 📈 **Technical Analysis** | SMA, EMA, RSI, Bollinger Bands, MA crossovers — all charted interactively |
| 🔔 **Smart Alerts** | Price, RSI, 52-week high/low, volume spike and MA crossover alerts via Telegram |
| 💹 **DCF Valuation** | Discounted Cash Flow model with margin-of-safety ratings per security |
| 🌍 **FX-Aware** | All values normalised to EUR using live Yahoo Finance exchange rates |
| 🗂️ **Watchlist** | Research securities before buying — scored by fundamentals (Hot / Warm / Cold) |
| 🐳 **Docker-native** | One command starts everything: dashboard, database, data fetcher and Telegram worker |
| 🐘 **PostgreSQL backend** | Production-grade database for multi-user concurrency and long-term history |

---

## 🚀 Quick Start

### Prerequisites

- [Docker + Docker Compose v2](https://docs.docker.com/get-docker/)
- A [Telegram bot token](https://core.telegram.org/bots#how-do-i-create-a-bot) *(optional — only needed for alert notifications)*

### 1 — Clone and configure

```bash
git clone https://github.com/YOUR_USERNAME/vestis.git
cd vestis
cp .env.example .env
```

Open `.env` and set at minimum:

```dotenv
POSTGRES_PASSWORD=change_me_to_something_strong
```

### 2 — Start

```bash
docker compose up -d --build
```

Database tables are created automatically on the first run. Then open:

```
http://localhost:8501
```

That's it. 🎉

---

## 🏗️ Architecture

```
┌───────────────────────────────────────────────────────────────┐
│  docker compose                                               │
│                                                               │
│  ┌──────────────┐  healthy   ┌────────────────────────────┐  │
│  │  PostgreSQL   │ ◄───────── │  db-init  (runs once)      │  │
│  │    :5432      │            └────────────────────────────┘  │
│  └──────┬────────┘                          │                 │
│         │ depends_on                        │                 │
│  ┌──────▼──────────────────────────────────────────────────┐  │
│  │  app              Streamlit dashboard   :8501            │  │
│  │  data-fetcher     Yahoo Finance → DB    (supercronic)    │  │
│  │  telegram-worker  Alert evaluation      (supercronic)    │  │
│  └─────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────┘
```

All four services share a single Docker image — only `CMD` differs. No extra base image bloat.

**Data flow:**

```
Yahoo Finance API
       │
       ▼
 data_fetcher.py ──► PostgreSQL ◄── app_streamlit.py ──► Browser
                          │
                  telegram_worker.py ──► Telegram
```

---

## ⚙️ Configuration

Vestis uses two configuration mechanisms:

### Environment variables — `.env` (never commit this file)

| Variable | Required | Default | Description |
|---|---|---|---|
| `POSTGRES_PASSWORD` | ✅ | — | Database password |
| `POSTGRES_USER` | | `vestis` | Database user |
| `POSTGRES_DB` | | `vestis` | Database name |
| `APP_PORT` | | `8501` | Host port for the dashboard |
| `TELEGRAM_BOT_TOKEN` | | *(empty)* | Enables Telegram notifications |
| `TELEGRAM_CHAT_ID` | | *(empty)* | Your Telegram chat / group ID |

### App settings — `config.json`

Configure in the dashboard under **⚙️ Settings**, or edit `config.json` directly:

| Key | Default | Description |
|---|---|---|
| `tax_rate` | `0.25` | Capital gains tax rate for P&L calculations |
| `dcf_discount_rate` | `0.10` | WACC used in DCF model |
| `dcf_terminal_growth` | `0.025` | Terminal growth rate |
| `dcf_projection_years` | `10` | DCF forecast horizon |
| `dcf_conservative` | `true` | Applies a 40% haircut to FCF growth estimates |
| `kpi_cache_hours` | `24` | How long KPI data is cached before re-fetching |
| `yf_max_req_per_min` | `45` | Yahoo Finance rate limiter |
| `retirement_year` | `2047` | Used for the portfolio risk glidepath |
| `dnd` | `false` | Do-not-disturb — suppress Telegram notifications |

---

## 📅 Cron Schedules

Edit `cron/fetcher.cron` and `cron/telegram.cron` (standard cron syntax, times in UTC). Apply by restarting the service:

```bash
docker compose restart data-fetcher
```

| Service | Default | Action |
|---|---|---|
| `data-fetcher` — full | 17:30 UTC Mon–Fri | Prices, KPIs, FX rates, financials |
| `data-fetcher` — prices | 09:00 UTC Mon–Fri | Quick pre-market price refresh |
| `telegram-worker` — immediate | Every 5 min Mon–Fri | Evaluate and fire instant alerts |
| `telegram-worker` — digest | 19:00 UTC Mon–Fri | Send daily summary |

---

## 🗄️ Migrating from an Existing SQLite Database

If you have a `portfolio.db` from a previous install:

```bash
# Install the migration tool (one-time)
pip install pgloader          # Linux/Mac
# or: brew install pgloader   # macOS

# Start Vestis first
docker compose up -d

# Run the migration
pgloader portfolio.db \
  postgresql://vestis:YOUR_PASSWORD@localhost:5432/vestis

docker compose restart
```

---

## 🛠️ Development

### Run tests

```bash
pip install -r requirements.txt

# Unit tests — no DB, no network, < 3 seconds (also used by the pre-commit hook)
pytest tests/test_unit.py -v

# Integration tests — spins up a temporary SQLite DB per test
pytest tests/test_integration.py -v

# Smoke tests — import checks and schema validation
pytest tests/test_smoke.py -v

# Full suite with coverage
pytest --cov=app --cov-report=term-missing
```

### Install the pre-commit hook

Runs the unit tests automatically before every `git commit`:

```bash
cp scripts/pre-commit-hook.sh .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

### Project layout

```
vestis/
├── app/
│   ├── app_streamlit.py        # Streamlit dashboard (main UI)
│   ├── db_utils.py             # Database abstraction (SQLite + PostgreSQL)
│   ├── middleware.py           # Business logic, indicators, alert evaluation
│   ├── data_fetcher.py         # Yahoo Finance data ingestion
│   ├── telegram_worker.py      # Alert notifications via Telegram
│   ├── config_utils.py         # Settings management
│   ├── article_utils.py        # News article helpers
│   └── setup/db_init.py        # Schema creation (run once at startup)
├── tests/
│   ├── test_unit.py            # Pure logic tests — no infrastructure needed
│   ├── test_integration.py     # DB layer tests — SQLite + Postgres
│   └── test_smoke.py           # App startup and import validation
├── cron/
│   ├── fetcher.cron            # data_fetcher schedule
│   └── telegram.cron           # telegram_worker schedule
├── scripts/
│   └── pre-commit-hook.sh      # Local pre-commit gate
├── .github/workflows/ci.yml    # CI pipeline
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── pytest.ini
└── .env.example
```

---

## 🔒 Security Notes

- **No secrets in the codebase.** `config.json` contains only non-sensitive app settings. All credentials live in `.env`, which is listed in `.gitignore`.
- **CI runs without any secrets.** The GitHub Actions pipeline uses an in-memory SQLite database, so no credentials are required to run the full test suite on public runners.
- **Telegram credentials** are injected via environment variables at runtime, never stored in committed files.

---

## 🤝 Contributing

Contributions are welcome! To get started:

1. Fork the repo and `git clone` your fork
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make your changes and add/update tests
4. Run the test suite: `pytest`
5. Open a pull request with a clear description

For larger changes, please open an issue first to discuss the approach.

---

## 📄 License

MIT © [DevByAndre](https://github.com/YOUR_USERNAME) — see [LICENSE](LICENSE) for details.

---

<div align="center">
<sub>
Market data provided by <a href="https://finance.yahoo.com">Yahoo Finance</a> via <a href="https://github.com/ranaroussi/yfinance">yfinance</a>.
Vestis is not financial advice. Always do your own research.
</sub>
</div>
