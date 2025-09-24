# Callio ETL

This repository provides a refactored Python package for running the Callio → BigQuery ETL pipeline outside of Jupyter notebooks.

## Usage

1. Copy `.env.example` to `.env` and provide the required secrets:
   * `SERVICE_ACCOUNT_KEY_JSON` (or `SERVICE_ACCOUNT_KEY_FILE`) – BigQuery service account credentials.
   * `CALLIO_ACCOUNTS_JSON` (or `CALLIO_ACCOUNTS_FILE`) – JSON array of Callio tenants with `tenant`, `email`, `password` fields.
   * Optional overrides such as `BQ_PROJECT_ID`, `BQ_DATASET_ID`, `LOG_LEVEL`, etc.
2. Install dependencies listed in the notebook (e.g. `pandas`, `requests`, `google-cloud-bigquery`).
3. Run the CLI:

```bash
python -m callio_etl --mode once --job all
```

Use `--mode daemon` to keep the scheduler loop running continuously. When running a single tick you can limit the work to a specific flow with `--job customer`, `--job call`, or `--job staffgroup`.
