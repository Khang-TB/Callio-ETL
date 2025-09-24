"""Configuration models for the Callio ETL pipeline."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass(frozen=True)
class Account:
    tenant: str
    email: str
    password: str


@dataclass(frozen=True)
class CallioAPIConfig:
    base_url: str
    timeout: int
    page_size: int
    accounts: Tuple[Account, ...]

    @classmethod
    def from_env(cls) -> "CallioAPIConfig":
        base_url = os.getenv("CALLIO_API_BASE_URL", "https://clientapi.phonenet.io")
        timeout = int(os.getenv("API_TIMEOUT", "90"))
        page_size = int(os.getenv("API_PAGE_SIZE", "500"))

        raw_accounts = os.getenv("CALLIO_ACCOUNTS_JSON")
        if raw_accounts:
            account_dicts = json.loads(raw_accounts)
        else:
            account_dicts = [
                {"tenant": "hot1new", "email": "hot1new@gmail.com", "password": "Huyhoang@123"},
                {"tenant": "hot1old", "email": "hot1old@gmail.com", "password": "Huyhoang@123"},
                {"tenant": "hot2", "email": "hot2@gmail.com", "password": "Huyhoang@123"},
            ]

        accounts = tuple(Account(**item) for item in account_dicts)
        return cls(base_url=base_url, timeout=timeout, page_size=page_size, accounts=accounts)

    def find_account(self, tenant: str) -> Optional[Account]:
        return next((acc for acc in self.accounts if acc.tenant == tenant), None)


@dataclass(frozen=True)
class BigQueryConfig:
    project_id: str
    dataset_id: str
    location: str
    service_account_json: str

    @classmethod
    def from_env(cls) -> "BigQueryConfig":
        service_account_json = os.getenv("SERVICE_ACCOUNT_KEY_JSON")
        if not service_account_json:
            raise RuntimeError(
                "SERVICE_ACCOUNT_KEY_JSON environment variable is required. "
                "Provide the JSON credentials (or a path) before running."
            )

        project_id = os.getenv("BQ_PROJECT_ID", "rio-system-migration")
        dataset_id = os.getenv("BQ_DATASET_ID", "dev_callio")
        location = os.getenv("BQ_LOCATION", "asia-southeast1")
        return cls(
            project_id=project_id,
            dataset_id=dataset_id,
            location=location,
            service_account_json=service_account_json,
        )


@dataclass(frozen=True)
class RankMappingConfig:
    sheet_id: str
    tab_name: str
    ranges: Tuple[str, ...]

    @classmethod
    def from_env(cls) -> "RankMappingConfig":
        sheet_id = os.getenv("RANK_SHEET_ID", "1U8B9tglIj21GRbC-TJfB549hx7xE-kP6X99ls-Ihsho")
        tab_name = os.getenv("RANK_TAB_NAME", "XẾP HẠNG HIỆN TẠI")
        ranges = tuple(r.strip() for r in os.getenv("RANK_RANGES_A1", "A1:Q").split(";") if r.strip())
        return cls(sheet_id=sheet_id, tab_name=tab_name, ranges=ranges)


@dataclass(frozen=True)
class SchedulerConfig:
    customer_interval_minutes: int = 15
    call_interval_minutes: int = 15
    staff_daily_hour: int = 9
    rank_daily_hour: int = 9


@dataclass(frozen=True)
class WindowConfig:
    overlap_ms: int
    days_if_empty: int

    @classmethod
    def from_env(cls) -> "WindowConfig":
        overlap_ms = int(os.getenv("OVERLAP_MS", str(3 * 60 * 1000)))
        days_if_empty = int(os.getenv("DAYS_TO_FETCH_IF_EMPTY", "30"))
        return cls(overlap_ms=overlap_ms, days_if_empty=days_if_empty)


@dataclass(frozen=True)
class PipelineConfig:
    api: CallioAPIConfig
    bigquery: BigQueryConfig
    rank_mapping: RankMappingConfig
    scheduler: SchedulerConfig
    window: WindowConfig
    log_level: str
    limit_records_per_endpoint: Optional[int]

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        limit_records = os.getenv("LIMIT_RECORDS_PER_ENDPOINT")
        limit_records_per_endpoint = int(limit_records) if limit_records else None
        return cls(
            api=CallioAPIConfig.from_env(),
            bigquery=BigQueryConfig.from_env(),
            rank_mapping=RankMappingConfig.from_env(),
            scheduler=SchedulerConfig(),
            window=WindowConfig.from_env(),
            log_level=log_level,
            limit_records_per_endpoint=limit_records_per_endpoint,
        )
