FROM python:3.11-slim

WORKDIR /app

# Installation des dépendances
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Code de l'API et modules partagés
COPY api/           /app/
COPY config/        /app/config/
COPY ingestion/     /app/ingestion/
COPY transformation/ /app/transformation/

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
