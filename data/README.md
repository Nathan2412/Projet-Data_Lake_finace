# Source fichier finance

`finance_dataset.csv` est l'instantane fichier utilise par le pipeline Airflow.

- Actif : Apple Inc. (`AAPL`)
- Periode : 2022-01-03 au 2024-11-19
- Granularite : quotidienne
- Colonnes : ticker, date, open, high, low, close, adj_close, volume
- Source d'origine : Yahoo Finance, republiee sous licence Apache-2.0 par
  `FarhanAli97/Apple-AAPL-Stock-Data-1980-to-December-2024`
- URL : https://github.com/FarhanAli97/Apple-AAPL-Stock-Data-1980-to-December-2024

Le fichier est fige et versionne afin que la source fichier reste reproductible et
distincte de l'ingestion API quotidienne effectuee avec `yfinance`.
