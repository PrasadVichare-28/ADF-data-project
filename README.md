# Handbook-Style Credit Card Fraud Simulator (Azure-ready)

This repo contains a *Handbook-style* transaction simulator (customers, terminals, timestamps, fraud bursts) and **Azure Data Factory (ADF)** templates to ingest daily CSVs from **GitHub (raw)** into **Azure Data Lake/Blob**.

## Quickstart
```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python simulate_fraud.py --out ./data/daily --start 2025-01-01 --days 7 --customers 2000 --terminals 500 --seed 42
```
Push `data/daily/*.csv` to GitHub. Raw URL pattern:
```
https://raw.githubusercontent.com/<owner>/<repo>/<branch>/data/daily/transactions_YYYYMMDD.csv
```

## ADF (copy GitHub → ADLS)
- Import `adf_templates/linkedservice_http_github_raw.json` and `linkedservice_adls.json`
- Import datasets: `dataset_github_http_csv.json`, `dataset_adls_csv.json`
- Import pipeline: `pipeline_copy_github_to_adls.json` (loop over date list)
