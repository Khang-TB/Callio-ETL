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

Running a single tick exits immediately, making it suitable for **cron-style scheduling**. Limit the work to a specific flow with `--job customer`, `--job call`, or `--job staffgroup` if you split the cadence across multiple invocations. Use `--mode daemon` only if you still want a long-running worker that idles between slots.

### Scheduler cadence

When the daemon is running it now follows a fixed daily cadence instead of a simple interval loop. By default the customer and call synchronisations execute at **09:30, 11:00, 13:00, 15:00, and 18:00 (UTC+7)**. These correspond to **02:30, 04:00, 06:00, 08:00, and 11:00 UTC**.

The staff and group snapshots are taken only once per day at **09:30 (UTC+7)** / **02:30 UTC**.

You can change the schedule via environment variables (times are expressed in UTC and use the `HH:MM` format):

* `SCHEDULER_RUN_TIMES_UTC` – comma separated list that controls the customer + call jobs. Example: `SCHEDULER_RUN_TIMES_UTC="00:00,12:00"`.
* `SCHEDULER_STAFF_GROUP_TIME_UTC` – single run time for the staff/group snapshot. If omitted it defaults to the earliest entry from `SCHEDULER_RUN_TIMES_UTC`.

### Cron-based execution

To avoid keeping a Python process alive all day you can wire the CLI into `cron` (or another scheduler) and invoke it at the required times. The defaults map the requested **09:30, 11:00, 13:00, 15:00, 18:00 (UTC+7)** cadence to **02:30, 04:00, 06:00, 08:00, 11:00 UTC**. A minimal crontab (server clock in UTC) looks like this:

```
# Customer + call log at every slot (staff/group automatically run only at the first slot)
30 2 * * * cd /path/to/Callio-ETL && python -m callio_etl --mode once --job all >> /var/log/callio-etl.log 2>&1
0 4,6,8,11 * * * cd /path/to/Callio-ETL && python -m callio_etl --mode once --job all >> /var/log/callio-etl.log 2>&1
```

If your server runs in a different timezone, either convert the times accordingly or prefix the crontab with `CRON_TZ=Asia/Bangkok` to express the entries in UTC+7. Each invocation loads configuration from `.env`, performs the requested job(s), writes to BigQuery, and exits without an idle loop.
