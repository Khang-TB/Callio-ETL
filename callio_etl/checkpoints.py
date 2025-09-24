"""Checkpoint and update log helpers."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd

from .bigquery_service import BigQueryService


@dataclass
class UpdateLogEntry:
    table_name: str
    tenant: str
    updated_at: datetime
    rows_loaded: int
    max_update_time: Optional[int]
    mode: str


@dataclass
class UpdateLogBuffer:
    bq: BigQueryService
    logger: any
    _pending: List[UpdateLogEntry] = field(default_factory=list)

    def add(self, tenant: str, table: str, rows: int, max_update: Optional[int], mode: str) -> None:
        self._pending.append(
            UpdateLogEntry(
                table_name=table,
                tenant=tenant,
                updated_at=datetime.now(timezone.utc),
                rows_loaded=int(rows),
                max_update_time=int(max_update) if max_update is not None else None,
                mode=mode,
            )
        )

    def flush(self) -> None:
        if not self._pending:
            return
        self.bq.ensure_update_log()
        df = pd.DataFrame([entry.__dict__ for entry in self._pending])
        self.bq.load_append(df, "update_log")
        self.logger.info("📝 Flushed update_log: %d rows", len(self._pending))
        self._pending.clear()


@dataclass
class CheckpointStore:
    bq: BigQueryService
    logger: any
    _checkpoints: Dict[Tuple[str, str], Optional[int]] = field(default_factory=dict)
    _last_run: Dict[Tuple[str, str], Optional[datetime]] = field(default_factory=dict)

    def warm(self) -> None:
        self.bq.ensure_update_log()
        sql = f"""
            SELECT table_name, tenant,
                   MAX(max_updateTime) AS ck,
                   MAX(updated_at)     AS last_run_at
            FROM `{self.bq.fqn('update_log')}`
            GROUP BY table_name, tenant
        """
        for row in self.bq.execute_query(sql):
            table = str(row["table_name"]).lower()
            tenant = row["tenant"]
            checkpoint = int(row["ck"]) if row["ck"] is not None else None
            last_run_at = row["last_run_at"]
            if isinstance(last_run_at, datetime) and last_run_at.tzinfo is None:
                last_run_at = last_run_at.replace(tzinfo=timezone.utc)
            self._checkpoints[(table, tenant)] = checkpoint
            self._last_run[(table, tenant)] = last_run_at
        self.logger.info("🔥 Warmed checkpoint cache: %d keys", len(self._checkpoints))

    @staticmethod
    def _normalize_table(name: str) -> str:
        return name.strip().lower().replace("-", "_").replace(" ", "_")

    def get_checkpoint(self, table: str, tenant: str) -> Optional[int]:
        return self._checkpoints.get((self._normalize_table(table), tenant))

    def set_checkpoint(self, table: str, tenant: str, value: Optional[int]) -> None:
        self._checkpoints[(self._normalize_table(table), tenant)] = value

    def get_last_run_any(self, table: str) -> Optional[datetime]:
        normalized = self._normalize_table(table)
        values = [dt for (tbl, _), dt in self._last_run.items() if tbl == normalized and dt is not None]
        return max(values) if values else None

    def get_last_run_by_prefix(self, prefix: str) -> Optional[datetime]:
        normalized = self._normalize_table(prefix)
        values = [dt for (tbl, _), dt in self._last_run.items() if tbl.startswith(normalized) and dt is not None]
        return max(values) if values else None
