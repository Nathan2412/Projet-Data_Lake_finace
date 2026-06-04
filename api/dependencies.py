"""Clients partagés et injection de dépendances FastAPI."""
import os
import psycopg2
import psycopg2.extras
import redis
from minio import Minio
from elasticsearch import Elasticsearch
from functools import lru_cache

POSTGRES = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "user":     os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
    "dbname":   os.getenv("POSTGRES_DB", "airflow"),
}

MINIO_CONFIG = {
    "endpoint":   f"{os.getenv('MINIO_HOST', 'localhost')}:{os.getenv('MINIO_PORT', '9000')}",
    "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    "secure":     False,
}

ES_URL   = f"http://{os.getenv('ES_HOST', 'localhost')}:{os.getenv('ES_PORT', '9200')}"
REDIS_CONFIG = {
    "host": os.getenv("REDIS_HOST", "localhost"),
    "port": int(os.getenv("REDIS_PORT", 6379)),
    "db":   0,
}

BUCKET_RAW_FILE = "raw-financial-data"
BUCKET_RAW_API  = "raw-api-data"
ES_INDEX        = "raw_financial_events"


def get_pg_conn():
    return psycopg2.connect(**POSTGRES)


def get_minio() -> Minio:
    return Minio(**MINIO_CONFIG)


def get_es() -> Elasticsearch:
    return Elasticsearch(ES_URL)


def get_redis() -> redis.Redis:
    return redis.Redis(**REDIS_CONFIG, decode_responses=True)
