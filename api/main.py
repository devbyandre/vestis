"""
Vestis FastAPI — exposes all middleware functions as REST endpoints.
Runs on port 8503, sits alongside the Streamlit app on 8502.
Do not modify middleware.py or db_utils.py — this layer only wraps them.
Routes live in routers/, grouped by domain; this module only wires them up.
"""
import sys, os
sys.path.insert(0, "/app")

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import (
    portfolios, securities, holdings, transactions, analytics,
    planning, watchlist, alerts, settings, health,
)

app = FastAPI(title="Vestis API", version="1.0.0")

# In production, nginx proxies /api/* to this service same-origin (see
# nginx.conf) and the API's own host port isn't published — the browser
# never makes a cross-origin request to it at all. CORS_ALLOWED_ORIGINS
# exists for local dev, where the Vite dev server (localhost:5173) talks
# to this API directly (localhost:8505), which is a real cross-origin
# request. Defaults to the dev origin only; set explicitly for anything else.
_cors_origins = [
    o.strip()
    for o in os.environ.get("CORS_ALLOWED_ORIGINS", "http://localhost:5173").split(",")
    if o.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

for _router_module in (
    portfolios, securities, holdings, transactions, analytics,
    planning, watchlist, alerts, settings, health,
):
    app.include_router(_router_module.router)
