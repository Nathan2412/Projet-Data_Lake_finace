"""
API Gateway du Data Lake Financier.

Endpoints standard :
  GET  /health       – état des services
  GET  /stats        – métriques de remplissage
  GET  /raw          – données brutes (MinIO + Elasticsearch)
  GET  /staging      – données transformées avec indicateurs
  GET  /curated      – données enrichies avec anomalies

Endpoints avancés :
  POST /ingest       – ingestion synchrone avec benchmark
  POST /ingest_fast  – ingestion optimisée (async + cache + parallélisation)
"""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import health, stats, raw, staging, curated, ingest, ingest_fast

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("API Gateway démarrée – Data Lake Financier")
    yield
    log.info("API Gateway arrêtée")


app = FastAPI(
    title="Financial Data Lake API",
    description=(
        "API Gateway du data lake financier (Yahoo Finance / yfinance). "
        "Expose les trois zones du data lake (Raw, Staging, Curated) "
        "et fournit des endpoints d'ingestion standard et optimisé."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router,      tags=["Health"])
app.include_router(stats.router,       tags=["Stats"])
app.include_router(raw.router,         tags=["Raw"])
app.include_router(staging.router,     tags=["Staging"])
app.include_router(curated.router,     tags=["Curated"])
app.include_router(ingest.router,      tags=["Ingest"])
app.include_router(ingest_fast.router, tags=["Ingest Fast"])


@app.get("/", include_in_schema=False)
def root():
    return {
        "message": "Financial Data Lake API",
        "docs":    "/docs",
        "health":  "/health",
    }
