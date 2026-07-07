# Local development

This is the **local, disposable** dev setup — fully separate from the
production stack Coolify deploys (`docker-compose.yaml`, which is wired to
`coolify_net`, Traefik and Authentik and should not be run on a laptop).

Everything below uses `docker-compose.dev.yml` and its own Postgres volume,
so it can never touch production data.

## Branch workflow

Day-to-day work happens on `develop` (or a feature branch off it). Only merge
to `main` once things are validated locally and CI is green — a push to
`main` triggers `scripts/webhook_server.py` on the homeserver, which runs
`git pull && docker compose up -d --build` immediately, regardless of CI
status. Treat merging to `main` as "deploying to prod."

## 1 — Bring up the dev stack

```bash
cp .env.dev.example .env.dev     # defaults are fine for local use
docker compose -f docker-compose.dev.yml --env-file .env.dev up -d --build
```

This starts:
- `db` — Postgres 16, own volume (`vestis_dev_pgdata`), host port `5433`
- `db-init` — creates the schema, then exits
- `api` — FastAPI with `--reload`, mounted source (`./app`, `./api/main.py`), host port `8505`
- `streamlit` — the legacy dashboard, `--server.runOnSave`, host port `8501` (optional, useful for parity checks against React)

Edits to `app/` or `api/main.py` apply live — no rebuild needed for either service.

## 2 — Seed synthetic data

```bash
docker compose -f docker-compose.dev.yml exec api python /app/scripts/seed_dev_data.py
```

Creates a handful of synthetic transactions on real tickers (AAPL, MSFT,
VWCE.DE, ...) in the `Default` portfolio, then pulls real prices/KPIs/FX for
those tickers from Yahoo Finance — so you get realistic-looking holdings
without any real personal financial data ever touching a dev machine. Safe
to re-run; it skips seeding if transactions already exist.

## 3 — Run the frontend

The frontend dev server runs directly on the host (much faster HMR loop than
containerizing nginx + a full build):

```bash
cd frontend
npm install
npm run dev
```

Open `http://localhost:5173`. Vite already proxies `/api` to `localhost:8505`
(see `frontend/vite.config.js`), which matches the port the dev `api`
service publishes — no config changes needed.

## 4 — Run tests

```bash
# Python — unit / integration / smoke / API
pip install -r requirements.txt -r requirements-test.txt -r api/requirements-api.txt
pytest

# Frontend — unit + component tests
cd frontend
npm test
```

## Tearing down

```bash
docker compose -f docker-compose.dev.yml down          # keep the DB volume
docker compose -f docker-compose.dev.yml down -v        # also wipe the DB volume
```
