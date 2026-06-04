"""Endpoint /health : vérifie l'état de tous les services du data lake."""
import logging
from fastapi import APIRouter
from pydantic import BaseModel

from dependencies import get_pg_conn, get_minio, get_es, get_redis, BUCKET_RAW_FILE, BUCKET_RAW_API

log = logging.getLogger(__name__)
router = APIRouter()


class ServiceStatus(BaseModel):
    status: str
    details: str | None = None


class HealthResponse(BaseModel):
    overall: str
    services: dict[str, ServiceStatus]


@router.get("/health", response_model=HealthResponse, summary="Vérification de l'état des services")
def health_check() -> HealthResponse:
    """
    Vérifie la connectivité de tous les services :
    PostgreSQL, MinIO, Elasticsearch, Redis.
    """
    services: dict[str, ServiceStatus] = {}

    # PostgreSQL
    try:
        conn = get_pg_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.close()
        services["postgresql"] = ServiceStatus(status="ok")
    except Exception as exc:
        log.error("PostgreSQL health check failed: %s", exc)
        services["postgresql"] = ServiceStatus(status="error", details=str(exc))

    # MinIO
    try:
        minio = get_minio()
        buckets = [b.name for b in minio.list_buckets()]
        missing = [b for b in [BUCKET_RAW_FILE, BUCKET_RAW_API] if b not in buckets]
        if missing:
            services["minio"] = ServiceStatus(status="warning", details=f"Buckets manquants : {missing}")
        else:
            services["minio"] = ServiceStatus(status="ok", details=f"Buckets : {buckets}")
    except Exception as exc:
        log.error("MinIO health check failed: %s", exc)
        services["minio"] = ServiceStatus(status="error", details=str(exc))

    # Elasticsearch
    try:
        es = get_es()
        info = es.info()
        services["elasticsearch"] = ServiceStatus(
            status="ok",
            details=f"version {info['version']['number']}"
        )
    except Exception as exc:
        log.error("Elasticsearch health check failed: %s", exc)
        services["elasticsearch"] = ServiceStatus(status="error", details=str(exc))

    # Redis
    try:
        r = get_redis()
        r.ping()
        services["redis"] = ServiceStatus(status="ok")
    except Exception as exc:
        log.error("Redis health check failed: %s", exc)
        services["redis"] = ServiceStatus(status="error", details=str(exc))

    overall = "ok" if all(s.status == "ok" for s in services.values()) else "degraded"
    return HealthResponse(overall=overall, services=services)
