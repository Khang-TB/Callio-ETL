# Callio ETL

This repository provides a refactored Python package for running the Callio → BigQuery ETL pipeline outside of Jupyter notebooks.

## Usage

1. Provide the required configuration via environment variables (see `callio_etl/config.py` for defaults). At minimum you must set `SERVICE_ACCOUNT_KEY_JSON` to the BigQuery service account credentials.
2. Install dependencies listed in the notebook (e.g. `pandas`, `requests`, `google-cloud-bigquery`, `gspread`).
3. Run the CLI:

```bash
python -m callio_etl --mode once --job all
```

Use `--mode daemon` to keep the scheduler loop running continuously.
