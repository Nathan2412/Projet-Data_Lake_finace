"""Endpoint /raw : accès aux données brutes (MinIO + Elasticsearch)."""
import logging
from fastapi import APIRouter, Query, HTTPException

from dependencies import get_minio, get_es, BUCKET_RAW_FILE, BUCKET_RAW_API, ES_INDEX

log = logging.getLogger(__name__)
router = APIRouter()


@router.get("/raw", summary="Données brutes de la zone Raw")
def get_raw(
    ticker:  str | None = Query(None,  description="Filtrer par ticker (ex: AAPL, ^GSPC)"),
    source:  str | None = Query(None,  description="Source : 'file' ou 'api'"),
    limit:   int        = Query(100,   ge=1, le=1000, description="Nombre de documents à retourner"),
    from_date: str | None = Query(None, description="Date de début (YYYY-MM-DD)"),
    to_date:   str | None = Query(None, description="Date de fin (YYYY-MM-DD)"),
) -> dict:
    """
    Retourne les données brutes de la zone Raw depuis Elasticsearch.
    Supporte le filtrage par ticker, source et plage de dates.
    """
    es = get_es()

    if not es.indices.exists(index=ES_INDEX):
        return {"documents": [], "total": 0, "note": "Index non encore créé"}

    # Construction de la requête ES
    must_clauses = []

    if ticker:
        must_clauses.append({"term": {"ticker": ticker.upper()}})

    if source:
        source_value = "yfinance_file" if source == "file" else "yfinance_api"
        must_clauses.append({"term": {"source": source_value}})

    if from_date or to_date:
        date_range: dict = {}
        if from_date:
            date_range["gte"] = from_date
        if to_date:
            date_range["lte"] = to_date
        must_clauses.append({"range": {"date": date_range}})

    query = {
        "query":  {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}},
        "sort":   [{"date": {"order": "desc"}}],
        "size":   limit,
    }

    try:
        resp = es.search(index=ES_INDEX, body=query)
    except Exception as exc:
        log.error("ES search error: %s", exc)
        raise HTTPException(status_code=500, detail=f"Erreur Elasticsearch : {exc}")

    total = resp["hits"]["total"]["value"]
    docs  = [h["_source"] for h in resp["hits"]["hits"]]

    return {
        "total":     total,
        "returned":  len(docs),
        "documents": docs,
    }


@router.get("/raw/objects", summary="Liste des objets MinIO dans la zone Raw")
def get_raw_objects(
    bucket: str = Query("file", description="'file' ou 'api'"),
    ticker: str | None = Query(None, description="Filtrer par ticker"),
) -> dict:
    """Liste les objets stockés dans MinIO (zone Raw)."""
    bucket_name = BUCKET_RAW_FILE if bucket == "file" else BUCKET_RAW_API
    minio = get_minio()

    try:
        prefix = f"{ticker.upper()}/" if ticker else ""
        objects = [
            {
                "name":         obj.object_name,
                "size_bytes":   obj.size,
                "last_modified": str(obj.last_modified),
            }
            for obj in minio.list_objects(bucket_name, prefix=prefix, recursive=True)
        ]
    except Exception as exc:
        log.error("MinIO list error: %s", exc)
        raise HTTPException(status_code=500, detail=f"Erreur MinIO : {exc}")

    return {
        "bucket":  bucket_name,
        "objects": objects,
        "count":   len(objects),
    }
