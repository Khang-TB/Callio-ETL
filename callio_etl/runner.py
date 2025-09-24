"""High level orchestration for the Callio ETL pipeline."""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple

import pandas as pd
from google.cloud import bigquery

from .api import CallioAPI
from .bigquery_service import BigQueryService
from .checkpoints import CheckpointStore, UpdateLogBuffer
from .config import PipelineConfig
from .logging_utils import configure_logging
from .utils import (
    compute_row_hash,
    derive_cf0_string_from_df,
    extract_user_group_id,
    extract_user_id,
    extract_user_name,
    ms_to_iso,
    safe_eval,
)


@dataclass
class CallioETLRunner:
    config: PipelineConfig

    def __post_init__(self) -> None:
        self.logger = configure_logging(self.config.log_level)
        self.bq = BigQueryService(self.config.bigquery, self.logger)
        self.api = CallioAPI(self.config.api, self.logger)
        self.log_buffer = UpdateLogBuffer(self.bq, self.logger)
        self.checkpoints = CheckpointStore(self.bq, self.logger)

    # ------------------------------------------------------------------
    # Bootstrap helpers
    # ------------------------------------------------------------------
    def bootstrap(self) -> None:
        self.bq.ensure_dataset()
        self.bq.ensure_update_log()
        self.bq.ensure_table_schema_call_log()
        self.bq.ensure_table_schema_staff()
        self.bq.ensure_table_schema_group()
        self.bq.ensure_table_schema_customer()
        self.bq.ensure_table_schema_customer_staging()
        self.checkpoints.warm()

    # ------------------------------------------------------------------
    # Customer flow
    # ------------------------------------------------------------------
    def run_customer_for_tenant(
        self, tenant: str
    ) -> Tuple[Tuple[Optional[datetime.date], Optional[datetime.date]], int, Optional[int]]:
        table = "customer"
        checkpoint = self.checkpoints.get_checkpoint(table, tenant)
        if checkpoint is None:
            checkpoint = int(
                (datetime.now(timezone.utc) - timedelta(days=self.config.window.days_if_empty)).timestamp() * 1000
            )
        cutoff = checkpoint - self.config.window.overlap_ms if checkpoint else checkpoint
        self.logger.info(
            "[%s][customer] ck=%s (%s) overlap=%s → cutoff=%s",
            tenant,
            checkpoint,
            ms_to_iso(checkpoint),
            self.config.window.overlap_ms,
            ms_to_iso(cutoff),
        )

        account = self.config.api.find_account(tenant)
        email = account.email if account else None
        password = account.password if account else None

        docs = self.api.fetch_desc_until(
            "customer",
            tenant,
            email,
            password,
            "updateTime",
            cutoff,
            limit_records=self.config.limit_records_per_endpoint,
            log_prefix=f"[{tenant}][customer]",
        )
        if not docs:
            self.log_buffer.add(tenant, table, 0, None, "NOOP")
            return (None, None), 0, checkpoint

        df = pd.DataFrame(docs)
        df["user_id"] = extract_user_id(df)
        df["user_name"] = extract_user_name(df)
        df["user_group_id"] = extract_user_group_id(df)
        if "customField_0_val" not in df.columns:
            df["customField_0_val"] = derive_cf0_string_from_df(df)
        df["customField_0_val"] = df["customField_0_val"].astype("string")

        columns = [
            "_id",
            "assignedTime",
            "createTime",
            "updateTime",
            "name",
            "phone",
            "user_id",
            "user_name",
            "user_group_id",
            "tenant",
            "row_hash",
            "customField_0_val",
            "NgayUpdate",
            "NgayAssign",
        ]
        for column in columns:
            if column not in df.columns:
                df[column] = None
        out = df[columns].copy()
        out["tenant"] = tenant
        out["NgayUpdate"] = pd.to_datetime(pd.to_numeric(out["updateTime"], errors="coerce"), unit="ms", utc=True).dt.date
        out["NgayAssign"] = pd.to_datetime(pd.to_numeric(out["assignedTime"], errors="coerce"), unit="ms", utc=True).dt.date
        out["row_hash"] = compute_row_hash(out)

        self.bq.ensure_table_schema_customer_staging()
        rows = self.bq.load_append(out, "stg_customer")
        max_update = int(pd.to_numeric(out["updateTime"], errors="coerce").max()) if rows else None
        date_min = pd.to_datetime(out["NgayUpdate"]).min().date() if rows else None
        date_max = pd.to_datetime(out["NgayUpdate"]).max().date() if rows else None
        self.log_buffer.add(tenant, table, rows, None, "STAGED")
        self.logger.info("[%s][customer] STAGED rows=%s window=[%s..%s]", tenant, rows, date_min, date_max)
        return (date_min, date_max), rows, max_update

    def merge_customer_window(self, start: datetime.date, end: datetime.date) -> None:
        self.bq.ensure_table_schema_customer()
        self.bq.ensure_table_schema_customer_staging()

        stg = self.bq.fqn("stg_customer")
        tgt = self.bq.fqn("customer")
        sql = f"""
        DECLARE d_from DATE DEFAULT @d_from;
        DECLARE d_to   DATE DEFAULT @d_to;

        CREATE TEMP TABLE _S AS
        SELECT *
        FROM `{stg}`
        WHERE NgayUpdate BETWEEN d_from AND d_to;

        MERGE `{tgt}` T
        USING (
          SELECT tenant, `_id`, assignedTime, createTime, updateTime, name, phone,
                 user_id, user_name, user_group_id,
                 row_hash, customField_0_val, NgayUpdate, NgayAssign
          FROM _S
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY tenant, `_id`
            ORDER BY SAFE_CAST(updateTime AS INT64) DESC
          ) = 1
        ) S
        ON T.tenant = S.tenant
           AND T._id = S._id
           AND T.NgayUpdate BETWEEN d_from AND d_to
        WHEN MATCHED AND (
              T.row_hash IS NULL OR T.row_hash != S.row_hash
              OR SAFE_CAST(S.updateTime AS INT64) >= SAFE_CAST(T.updateTime AS INT64)
              OR T.updateTime IS NULL
        ) THEN
          UPDATE SET
            assignedTime = S.assignedTime,
            createTime   = S.createTime,
            updateTime   = S.updateTime,
            name         = S.name,
            phone        = S.phone,
            user_id      = S.user_id,
            user_name    = S.user_name,
            user_group_id= S.user_group_id,
            row_hash     = S.row_hash,
            customField_0_val = S.customField_0_val,
            NgayUpdate   = S.NgayUpdate,
            NgayAssign   = S.NgayAssign
        WHEN NOT MATCHED BY TARGET THEN
          INSERT (tenant, _id, assignedTime, createTime, updateTime, name, phone, user_id,
                  user_name, user_group_id, row_hash, customField_0_val, NgayUpdate, NgayAssign)
          VALUES (S.tenant, S._id, S.assignedTime, S.createTime, S.updateTime, S.name, S.phone, S.user_id,
                  S.user_name, S.user_group_id, S.row_hash, S.customField_0_val, S.NgayUpdate, S.NgayAssign);
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("d_from", "DATE", start),
                bigquery.ScalarQueryParameter("d_to", "DATE", end),
            ]
        )
        self.bq.execute_query(sql, job_config=job_config)
        cleanup_sql = f"DELETE FROM `{stg}` WHERE NgayUpdate BETWEEN @d_from AND @d_to"
        self.bq.execute_query(cleanup_sql, job_config=job_config)
        self.logger.info("🧩 MERGE customer done for window [%s..%s]", start, end)

    # ------------------------------------------------------------------
    # Call log flow
    # ------------------------------------------------------------------
    def run_call_for_tenant(self, tenant: str) -> None:
        table = "call_log"
        checkpoint = self.checkpoints.get_checkpoint(table, tenant)
        if checkpoint is None:
            checkpoint = int(
                (datetime.now(timezone.utc) - timedelta(days=self.config.window.days_if_empty)).timestamp() * 1000
            )
        cutoff = checkpoint
        self.logger.info(
            "[%s][call_log] ck=%s (%s) → cutoff=%s (%s)",
            tenant,
            checkpoint,
            ms_to_iso(checkpoint),
            cutoff,
            ms_to_iso(cutoff),
        )

        account = self.config.api.find_account(tenant)
        email = account.email if account else None
        password = account.password if account else None

        docs = self.api.fetch_desc_until(
            "call",
            tenant,
            email,
            password,
            "createTime",
            cutoff,
            limit_records=self.config.limit_records_per_endpoint,
            log_prefix=f"[{tenant}][call_log]",
        )
        if not docs:
            self.log_buffer.add(tenant, table, 0, checkpoint, "NOOP")
            return

        df = pd.DataFrame(docs)
        if "_id" in df.columns and not df.empty:
            df = df.drop_duplicates(subset=["_id"], keep="last")

        keep = [
            "_id",
            "chargeTime",
            "createTime",
            "direction",
            "fromNumber",
            "toNumber",
            "startTime",
            "endTime",
            "duration",
            "billDuration",
            "hangupCause",
            "answerTime",
        ]
        out = pd.DataFrame({column: df.get(column, pd.Series([None] * len(df))) for column in keep})
        if "fromUser" in df.columns:
            out["fromUser__id"] = df["fromUser"].apply(
                lambda value: (safe_eval(value) or {}).get("_id") if isinstance(safe_eval(value), dict) else None
            )
            out["fromUser__name"] = df["fromUser"].apply(
                lambda value: (safe_eval(value) or {}).get("name") if isinstance(safe_eval(value), dict) else None
            )
        else:
            out["fromUser__id"] = None
            out["fromUser__name"] = None
        if "fromGroup" in df.columns:
            out["fromGroup__id"] = df["fromGroup"].apply(
                lambda value: (safe_eval(value) or {}).get("_id") if isinstance(safe_eval(value), dict) else None
            )
        else:
            out["fromGroup__id"] = None

        out["NgayTao"] = pd.to_datetime(pd.to_numeric(out["createTime"], errors="coerce"), unit="ms", utc=True).dt.date
        out["tenant"] = tenant
        out["row_hash"] = compute_row_hash(out)

        self.bq.ensure_table_schema_call_log()
        loaded = self.bq.load_append(out, "call_log")
        max_create = int(out["createTime"].max()) if "createTime" in out.columns and not out["createTime"].empty else checkpoint
        if max_create is not None and (self.checkpoints.get_checkpoint(table, tenant) is None or max_create > checkpoint):
            self.checkpoints.set_checkpoint(table, tenant, max_create)
        new_ck = self.checkpoints.get_checkpoint(table, tenant)
        self.log_buffer.add(tenant, table, loaded, new_ck, "APPEND")
        self.logger.info("[%s][call_log] APPEND loaded=%s new_ck=%s (%s)", tenant, loaded, new_ck, ms_to_iso(new_ck))

    # ------------------------------------------------------------------
    # Staff & group helpers
    # ------------------------------------------------------------------
    def _prepare_staff_dataframe(self, tenant: str) -> pd.DataFrame:
        df = self.api.fetch_staff(tenant)
        if df.empty:
            return df
        out = pd.DataFrame()
        out["_id"] = df.get("_id")
        out["email"] = df.get("email")
        out["name"] = df.get("name")
        out["updateTime"] = df.get("updateTime")
        out["createTime"] = df.get("createTime")
        if "group" in df.columns:
            out["group_id"] = df["group"].apply(
                lambda value: (safe_eval(value) or {}).get("_id") if isinstance(safe_eval(value), dict) else None
            )
        else:
            out["group_id"] = None
        out["tenant"] = tenant
        out["row_hash"] = compute_row_hash(out)
        return out

    def _prepare_group_dataframe(self, tenant: str) -> pd.DataFrame:
        df = self.api.fetch_group(tenant)
        if df.empty:
            return df
        out = pd.DataFrame()
        if "_id" in df.columns:
            out["group_id"] = df["_id"].astype("string")
        elif "id" in df.columns:
            out["group_id"] = df["id"].astype("string")
        else:
            out["group_id"] = None
        out["name"] = df.get("name")
        out["tenant"] = tenant
        out["row_hash"] = compute_row_hash(out)
        return out

    def snapshot_staff_group(self) -> None:
        staff_all = []
        for account in self.config.api.accounts:
            token = self.api.get_token(account.tenant, account.email, account.password)
            if not token:
                self.log_buffer.add(account.tenant, "staff", 0, None, "ERROR_LOGIN")
                continue
            try:
                df = self._prepare_staff_dataframe(account.tenant)
                if isinstance(df, pd.DataFrame) and not df.empty:
                    staff_all.append(df)
            except Exception:  # pragma: no cover - network path
                self.logger.exception("[%s] staff error", account.tenant)

        if staff_all:
            df_all = pd.concat(staff_all, ignore_index=True)
            if "name" in df_all.columns:
                df_all = df_all[df_all["name"].notna() & (df_all["name"].astype(str).str.strip() != "")]
            else:
                self.logger.warning("[staff] Missing 'name' column — skipping load/merge")
                df_all = pd.DataFrame()

            if not df_all.empty:
                rows = self.bq.load_append(df_all, "stg_staff")
                self.log_buffer.add("ALL", "staff", rows, None, "STAGED")
                try:
                    self.merge_staff_from_staging("stg_staff", "staff")
                    self.log_buffer.add("ALL", "staff", rows, None, "MERGED")
                except Exception:  # pragma: no cover - BQ failure path
                    self.logger.exception("[staff] MERGE failed")
        else:
            self.log_buffer.add("ALL", "staff", 0, None, "NOOP")
            self.logger.info("[staff] NOOP (no rows)")

        group_all = []
        for account in self.config.api.accounts:
            token = self.api.get_token(account.tenant, account.email, account.password)
            if not token:
                self.log_buffer.add(account.tenant, "group", 0, None, "ERROR_LOGIN")
                continue
            try:
                df = self._prepare_group_dataframe(account.tenant)
                if isinstance(df, pd.DataFrame) and not df.empty:
                    group_all.append(df)
            except Exception:  # pragma: no cover
                self.logger.exception("[%s] group error", account.tenant)

        if group_all:
            df_all = pd.concat(group_all, ignore_index=True)
            rows = self.bq.load_truncate(df_all, "group")
            self.log_buffer.add("ALL", "group", rows, None, "TRUNCATE")
            self.logger.info("[group] TRUNCATE rows=%s", rows)
        else:
            self.log_buffer.add("ALL", "group", 0, None, "NOOP")
            self.logger.info("[group] NOOP (no rows)")

    def merge_staff_from_staging(self, stg_name: str = "stg_staff", tgt_name: str = "staff") -> None:
        full_stg = self.bq.fqn(stg_name)
        full_tgt = self.bq.fqn(tgt_name)

        try:
            stg_table = self.bq.client.get_table(full_stg)
        except Exception:
            self.logger.info("ℹ️ No staged staff to merge.")
            return

        stg_columns = [field.name for field in stg_table.schema]
        if "name" not in stg_columns or "tenant" not in stg_columns:
            self.logger.warning("⚠️ Staging %s missing tenant/name columns — skipping MERGE", full_stg)
            self.bq.client.delete_table(full_stg, not_found_ok=True)
            return

        try:
            tgt_table = self.bq.client.get_table(full_tgt)
        except Exception:
            table = bigquery.Table(full_tgt, schema=stg_table.schema)
            table.clustering_fields = ["tenant", "name"]
            self.bq.client.create_table(table)
            tgt_table = self.bq.client.get_table(full_tgt)
            self.logger.info("✅ Created target: %s", full_tgt)

        tgt_columns = [field.name for field in tgt_table.schema]
        exclude = {"tenant", "name"}
        cols_common = sorted((set(stg_columns) & set(tgt_columns)) - exclude)
        has_hash = "row_hash" in stg_columns and "row_hash" in tgt_columns
        stg_has_upd = "updateTime" in stg_columns
        tgt_has_upd = "updateTime" in tgt_columns

        conditions = []
        if has_hash:
            conditions.append("(T.row_hash IS NULL OR T.row_hash != S.row_hash)")
        if stg_has_upd and tgt_has_upd:
            conditions.append("(SAFE_CAST(S.updateTime AS INT64) >= SAFE_CAST(T.updateTime AS INT64) OR T.updateTime IS NULL)")
        when_matched = " AND ".join(conditions) if conditions else "TRUE"

        projection = ["tenant", "name"]
        if has_hash:
            projection.append("row_hash")
        if stg_has_upd:
            projection.append("updateTime")
        for column in cols_common:
            if column not in projection:
                projection.append(column)
        projection_sql = ", ".join([f"`{col}`" for col in projection])

        set_clause = ", ".join([f"T.`{col}` = S.`{col}`" for col in cols_common]) or "T.`name` = T.`name`"
        insert_cols = ["tenant", "name"] + cols_common
        insert_vals = ["S.`tenant`", "S.`name`"] + [f"S.`{col}`" for col in cols_common]
        order_expr = "SAFE_CAST(updateTime AS INT64) DESC" if stg_has_upd else "name"

        sql = f"""
        MERGE `{full_tgt}` T
        USING (
          SELECT {projection_sql}
          FROM `{full_stg}`
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY tenant, name
            ORDER BY {order_expr}
          ) = 1
        ) S
        ON T.tenant = S.tenant AND T.name = S.name
        WHEN MATCHED AND ({when_matched}) THEN
          UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
          INSERT ({', '.join([f'`{col}`' for col in insert_cols])})
          VALUES ({', '.join(insert_vals)})
        """
        self.bq.execute_query(sql)
        self.logger.info("🧩 MERGE done: %s → %s", full_stg, full_tgt)
        self.bq.client.delete_table(full_stg, not_found_ok=True)

    # ------------------------------------------------------------------
    # Scheduler helpers
    # ------------------------------------------------------------------
    def next_daily(self, base: datetime, hour_utc: int) -> datetime:
        x = base.replace(minute=0, second=0, microsecond=0)
        target = x.replace(hour=hour_utc)
        if target <= base:
            target += timedelta(days=1)
        return target

    def plan_initial_windows(
        self,
        now: datetime,
        staff_last: Optional[datetime],
        group_last: Optional[datetime],
    ) -> Tuple[datetime, datetime, datetime]:
        next_customer = now
        next_call = now
        if (staff_last is None or (now - staff_last > timedelta(days=1))) or (
            group_last is None or (now - group_last > timedelta(days=1))
        ):
            next_staffgrp = now
        else:
            next_staffgrp = self.next_daily(now, self.config.scheduler.staff_daily_hour)
        return next_customer, next_call, next_staffgrp

    def run_tick(
        self,
        next_customer: datetime,
        next_call: datetime,
        next_staffgrp: datetime,
    ) -> Tuple[datetime, datetime, datetime]:
        loop_start = datetime.now(timezone.utc)

        if loop_start >= next_customer:
            self.logger.info("▶ Run customer (all tenants) | interval=%sm", self.config.scheduler.customer_interval_minutes)
            window_min, window_max = None, None
            staged_stats: Dict[str, Dict[str, Optional[int]]] = {}
            for account in self.config.api.accounts:
                token = self.api.get_token(account.tenant, account.email, account.password)
                if not token:
                    self.log_buffer.add(account.tenant, "customer", 0, None, "ERROR_LOGIN")
                    continue
                try:
                    (d_from, d_to), rows, max_update = self.run_customer_for_tenant(account.tenant)
                    if d_from and d_to:
                        window_min = d_from if (window_min is None or d_from < window_min) else window_min
                        window_max = d_to if (window_max is None or d_to > window_max) else window_max
                    staged_stats[account.tenant] = {"rows": rows, "max_update": max_update}
                except Exception:  # pragma: no cover - network failure path
                    self.logger.exception("[%s] customer error", account.tenant)

            if window_min and window_max:
                try:
                    self.merge_customer_window(window_min, window_max)
                    for tenant, stats in staged_stats.items():
                        max_update = stats.get("max_update")
                        if max_update is not None:
                            self.checkpoints.set_checkpoint("customer", tenant, max_update)
                            self.log_buffer.add(tenant, "customer", stats.get("rows", 0), max_update, "MERGED")
                            self.logger.info(
                                "[%s][customer] MERGED window [%s..%s] → CK=%s",
                                tenant,
                                window_min,
                                window_max,
                                ms_to_iso(max_update),
                            )
                except Exception:  # pragma: no cover - BQ failure path
                    self.logger.exception("MERGE customer window [%s..%s] failed", window_min, window_max)

            next_customer = loop_start + timedelta(minutes=self.config.scheduler.customer_interval_minutes)

        if loop_start >= next_call:
            self.logger.info("▶ Run call_log (all tenants) | interval=%sm", self.config.scheduler.call_interval_minutes)
            for account in self.config.api.accounts:
                token = self.api.get_token(account.tenant, account.email, account.password)
                if not token:
                    ck = self.checkpoints.get_checkpoint("call_log", account.tenant)
                    self.log_buffer.add(account.tenant, "call_log", 0, ck, "ERROR_LOGIN")
                    continue
                try:
                    self.run_call_for_tenant(account.tenant)
                except Exception:  # pragma: no cover - network failure path
                    self.logger.exception("[%s] call_log error", account.tenant)
            next_call = loop_start + timedelta(minutes=self.config.scheduler.call_interval_minutes)

        if loop_start >= next_staffgrp:
            self.logger.info("▶ Daily snapshot: staff + group (all tenants)")
            self.snapshot_staff_group()
            next_staffgrp = self.next_daily(loop_start + timedelta(seconds=1), self.config.scheduler.staff_daily_hour)

        self.log_buffer.flush()
        return next_customer, next_call, next_staffgrp

    # ------------------------------------------------------------------
    # Entrypoints
    # ------------------------------------------------------------------
    def run_forever(self) -> None:  # pragma: no cover - daemon mode
        self.bootstrap()
        now = datetime.now(timezone.utc)
        staff_last = self.checkpoints.get_last_run_any("staff")
        group_last = self.checkpoints.get_last_run_any("group")
        next_customer, next_call, next_staffgrp = self.plan_initial_windows(now, staff_last, group_last)
        self.logger.info(
            "⏱️ Schedule boot | customer/call: now | staff/group: %s",
            next_staffgrp,
        )

        while True:
            try:
                next_customer, next_call, next_staffgrp = self.run_tick(
                    next_customer,
                    next_call,
                    next_staffgrp,
                )
                next_due = min(next_customer, next_call, next_staffgrp)
                wait_seconds = int((next_due - datetime.now(timezone.utc)).total_seconds())
                wait_seconds = max(1, min(300, wait_seconds))
                self.logger.info("⏳ Idle %ss — next due @ %s UTC", wait_seconds, next_due.isoformat())
                time.sleep(wait_seconds)
            except KeyboardInterrupt:
                self.logger.warning("⛔ Stopped by user")
                break
            except Exception:
                self.logger.exception("Loop-level error; retrying in 10s")
                time.sleep(10)

    def run_once(self, job: str = "all") -> None:
        self.bootstrap()
        now = datetime.now(timezone.utc)
        staff_last = self.checkpoints.get_last_run_any("staff")
        group_last = self.checkpoints.get_last_run_any("group")
        next_customer, next_call, next_staffgrp = self.plan_initial_windows(now, staff_last, group_last)

        far_future = now + timedelta(days=365 * 10)
        if job != "all":
            if job != "customer":
                next_customer = far_future
            if job != "call":
                next_call = far_future
            if job != "staffgroup":
                next_staffgrp = far_future

        self.run_tick(next_customer, next_call, next_staffgrp)
