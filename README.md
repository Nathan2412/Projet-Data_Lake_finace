# Financial Data Lake

Data lake financier complet basé sur Yahoo Finance (yfinance).  
Projet final — cours Data Lakes & Data Integration, EFREI 2025-2026.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Sources de données                                              │
│  ┌──────────────────┐     ┌──────────────────────────────────┐  │
│  │ yfinance fichier │     │ yfinance API (polling quotidien) │  │
│  │ (dataset CSV)    │     │ via Airflow scheduler            │  │
│  └────────┬─────────┘     └──────────────┬───────────────────┘  │
└───────────┼──────────────────────────────┼─────────────────────┘
            │                              │
            ▼              ▼              ▼
┌───────────────────────────────────────────────────────┐
│  Zone RAW                                             │
│  ┌─────────────────┐   ┌─────────────────────────┐   │
│  │  MinIO (S3)     │   │  Elasticsearch           │   │
│  │  CSV/JSON bruts │   │  Index raw_financial_    │   │
│  │  par ticker/date│   │  events (recherche)      │   │
│  └─────────────────┘   └─────────────────────────┘   │
└───────────────────────────────────────────────────────┘
            │
            ▼
┌───────────────────────────────────────────────────────┐
│  Zone STAGING (PostgreSQL — staging_ohlcv)            │
│  OHLCV nettoyé + indicateurs techniques :             │
│  SMA 20/50, EMA 12/26, RSI 14, MACD, Bollinger,      │
│  daily_return, volatility_20                          │
└───────────────────────────────────────────────────────┘
            │
            ▼
┌───────────────────────────────────────────────────────┐
│  Zone CURATED (PostgreSQL — curated_analysis)         │
│  Isolation Forest → anomaly_score, is_anomaly         │
│  Classification : flash_crash, volume_spike, etc.     │
│  Signaux de trading : buy / sell / hold               │
└───────────────────────────────────────────────────────┘
            │
            ▼
┌───────────────────────────────────────────────────────┐
│  API Gateway (FastAPI :8000)                          │
│  GET  /raw  /staging  /curated  /health  /stats       │
│  POST /ingest   /ingest_fast                          │
└───────────────────────────────────────────────────────┘
```

### Stack technique

| Composant        | Technologie              | Port  |
|------------------|--------------------------|-------|
| Zone Raw (S3)    | MinIO                    | 9000  |
| Zone Raw (index) | Elasticsearch 8          | 9200  |
| Staging + Curated| PostgreSQL 15            | 5432  |
| Cache            | Redis 7                  | 6379  |
| Pipeline         | Apache Airflow 2.8       | 8080  |
| API Gateway      | FastAPI + Uvicorn        | 8000  |

### Tickers suivis

**Actions S&P 500** : AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA, BRK-B, JPM, JNJ  
**Indices** : ^GSPC (S&P500), ^DJI (Dow Jones), ^IXIC (Nasdaq), ^RUT (Russell 2000)

---

## Installation et démarrage

### Prérequis

- Docker Desktop ≥ 24
- Docker Compose ≥ 2.20

### Démarrage rapide

```bash
git clone <repo-url>
cd financial-data-lake

# Démarrer tous les services
docker compose up -d

# Attendre que tous les services soient healthy (~2 min)
docker compose ps
```

### Vérification

```bash
# Santé de l'API
curl http://localhost:8000/health

# Interface Swagger
open http://localhost:8000/docs

# Airflow UI (admin/admin)
open http://localhost:8080

# MinIO Console
open http://localhost:9001  # minioadmin/minioadmin
```

### Première ingestion

```bash
# Via l'API (ingestion d'un batch de tickers)
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{"data": {"tickers": ["AAPL", "MSFT", "^GSPC"], "period": "1y"}}'

# Via les scripts directement (hors Docker)
pip install -r api/requirements.txt
python ingestion/ingest_file.py
python transformation/staging/transform_staging.py
python transformation/curated/transform_curated.py
```

### Via Airflow

Le DAG `financial_data_lake_pipeline` s'exécute automatiquement **du lundi au vendredi à 6h UTC**.

Pour le déclencher manuellement :
1. Ouvrir http://localhost:8080 (admin/admin)
2. Activer le DAG `financial_data_lake_pipeline`
3. Cliquer sur "Trigger DAG"

---

## API Gateway

Documentation interactive : http://localhost:8000/docs

### Endpoints standard

| Méthode | Endpoint                       | Description                                  |
|---------|--------------------------------|----------------------------------------------|
| GET     | `/health`                      | État de tous les services                    |
| GET     | `/stats`                       | Métriques de remplissage des zones           |
| GET     | `/raw`                         | Données brutes depuis Elasticsearch          |
| GET     | `/raw/objects`                 | Liste des fichiers dans MinIO                |
| GET     | `/staging`                     | OHLCV + indicateurs techniques               |
| GET     | `/staging/tickers`             | Tickers disponibles en staging               |
| GET     | `/curated`                     | Données enrichies + scores d'anomalie        |
| GET     | `/curated/anomalies/summary`   | Résumé des anomalies par ticker              |
| GET     | `/curated/signals`             | Signaux de trading actifs (buy/sell)         |

#### Paramètres communs

```
ticker=AAPL           # filtrer par ticker
from_date=2024-01-01  # date de début
to_date=2024-12-31    # date de fin
limit=100             # nombre de résultats (max 5000)
offset=0              # pagination
```

#### Exemples

```bash
# Données staging AAPL depuis 6 mois
curl "http://localhost:8000/staging?ticker=AAPL&from_date=2024-06-01&limit=50"

# Anomalies détectées uniquement
curl "http://localhost:8000/curated?anomalies_only=true"

# Signaux de trading actifs
curl "http://localhost:8000/curated/signals"

# Stats du data lake
curl "http://localhost:8000/stats"
```

### Endpoints avancés (niveau avancé)

#### POST `/ingest` — Ingestion synchrone avec benchmark

```json
{
  "data": {
    "tickers": ["AAPL", "MSFT"],
    "period": "6mo",
    "run_staging": true,
    "run_curated": true
  }
}
```

Réponse :
```json
{
  "status": "success",
  "pipeline_steps": {
    "raw":     {"success": 2, "duration_ms": 3200},
    "staging": {"processed": 520, "duration_ms": 890},
    "curated": {"processed": 520, "anomalies_detected": 12, "duration_ms": 340}
  },
  "performance": {
    "total_duration_ms": 4430,
    "batch_size": 2,
    "ms_per_ticker": 2215
  }
}
```

#### POST `/ingest_fast` — Ingestion optimisée ≥30% plus rapide

Même interface que `/ingest`. Retourne en plus le champ `optimizations` :

```json
{
  "optimizations": {
    "parallel_download": true,
    "redis_cache_hits": 0,
    "vectorized_indicators": true,
    "bulk_es_indexing": true,
    "execute_values_pg": true
  }
}
```

---

## Optimisations `/ingest_fast`

| Technique              | Description                                         | Gain estimé |
|------------------------|-----------------------------------------------------|-------------|
| **ThreadPoolExecutor** | 8 threads pour le download yfinance en parallèle   | ~60-70%     |
| **Cache Redis**        | Skip les tickers récents (TTL 5 min)               | 100% si hit  |
| **NumPy vectorisé**    | Indicateurs sans boucles Python (EMA custom NumPy) | ~20-30%     |
| **Bulk ES**            | Une seule requête pour tout le batch               | ~40%        |
| **execute_values PG**  | Batch unique au lieu de execute_batch              | ~15-20%     |
| **Upload MinIO parallèle** | Threads I/O pour les uploads CSV              | ~50%        |

### Résultats de benchmark (batch de 1 et 100 tickers)

| Batch      | `/ingest` (ms) | `/ingest_fast` (ms) | Gain    |
|------------|----------------|---------------------|---------|
| 1 ticker   | ~2 000         | ~1 300              | ~35%    |
| 10 tickers | ~18 000        | ~7 000              | ~61%    |

> Les temps varient selon la latence réseau vers Yahoo Finance.

---

## Structure du projet

```
financial-data-lake/
├── docker-compose.yml
├── config/
│   └── settings.py              # Configuration centralisée
├── ingestion/
│   ├── ingest_file.py           # Source 1 : dataset fichier yfinance
│   └── ingest_api.py            # Source 2 : API Yahoo Finance (polling)
├── transformation/
│   ├── staging/
│   │   └── transform_staging.py # Raw → Staging (indicateurs techniques)
│   └── curated/
│       └── transform_curated.py # Staging → Curated (Isolation Forest)
├── airflow/
│   └── dags/
│       └── financial_pipeline_dag.py  # DAG principal (scheduling quotidien)
├── api/
│   ├── main.py                  # Application FastAPI
│   ├── dependencies.py          # Clients partagés
│   ├── Dockerfile
│   ├── requirements.txt
│   └── routers/
│       ├── health.py            # GET /health
│       ├── stats.py             # GET /stats
│       ├── raw.py               # GET /raw
│       ├── staging.py           # GET /staging
│       ├── curated.py           # GET /curated
│       ├── ingest.py            # POST /ingest
│       └── ingest_fast.py       # POST /ingest_fast
└── scripts/
    └── init_db.sql              # Initialisation PostgreSQL
```

---

## Choix techniques

**MinIO** : émulation S3 locale. Stocke les fichiers bruts CSV (données historiques) et JSON (réponses API) partitionnés par `ticker/date/`.

**Elasticsearch** : indexation de chaque ligne OHLCV pour les recherches rapides par ticker, plage de dates ou source. Utilisé pour la zone Raw car il supporte des requêtes ad-hoc sans schéma fixe.

**PostgreSQL** : utilisé pour Staging et Curated car les données sont structurées et les requêtes sont prévisibles (filtrage par ticker + date). Les index accélèrent les lectures API.

**Isolation Forest** : algorithme non supervisé (scikit-learn) pour la détection d'anomalies sur 4 features : daily_return, volatility_20, volume_zscore, rsi_14. Contamination = 5%.

**Airflow** : scheduling DAG lun-ven à 6h UTC avec XCom pour la propagation des résultats entre tâches. Ingestion parallèle des deux sources puis transformations séquentielles.

**Redis** : cache pour `/ingest_fast`. Évite de re-télécharger un ticker déjà ingéré dans les 5 dernières minutes (utile pour les tests de performance).
