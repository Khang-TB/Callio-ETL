"""BigQuery helper abstraction used by the Callio ETL pipeline."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pandas as pd
from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery
from google.oauth2 import service_account

from .config import BigQueryConfig
from .utils import ensure_unique_columns


@dataclass
class BigQueryService:
    config: BigQueryConfig
    logger: any

    def __post_init__(self) -> None:
        info = json.loads(self.config.service_account_json)
        project = info.get("project_id") or self.config.project_id
        credentials = service_account.Credentials.from_service_account_info(info)
        self.client = bigquery.Client(project=project, credentials=credentials)

    # ------------------------------------------------------------------
    # Dataset helpers
    # ------------------------------------------------------------------
    def fqn(self, table: str) -> str:
        return f"{self.client.project}.{self.config.dataset_id}.{table}"

    def ensure_dataset(self) -> None:
        dataset_id = f"{self.client.project}.{self.config.dataset_id}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = self.config.location
        try:
            self.client.create_dataset(dataset)
            self.logger.info("✅ Created dataset %s", self.config.dataset_id)
        except Conflict:
            pass

    # ------------------------------------------------------------------
    # Table bootstrap
    # ------------------------------------------------------------------
    def ensure_table_schema_call_log(self) -> None:
        table_id = self.fqn("call_log")
        try:
            self.client.get_table(table_id)
            return
        except NotFound:
            schema = [
                bigquery.SchemaField("_id", "STRING"),
                bigquery.SchemaField("chargeTime", "INT64"),
                bigquery.SchemaField("createTime", "INT64"),
                bigquery.SchemaField("direction", "STRING"),
                bigquery.SchemaField("fromNumber", "STRING"),
                bigquery.SchemaField("toNumber", "STRING"),
                bigquery.SchemaField("startTime", "INT64"),
                bigquery.SchemaField("endTime", "INT64"),
                bigquery.SchemaField("duration", "INT64"),
                bigquery.SchemaField("billDuration", "INT64"),
                bigquery.SchemaField("hangupCause", "STRING"),
                bigquery.SchemaField("answerTime", "FLOAT64"),
                bigquery.SchemaField("fromUser__id", "STRING"),
                bigquery.SchemaField("fromUser__name", "STRING"),
                bigquery.SchemaField("fromGroup__id", "STRING"),
                bigquery.SchemaField("tenant", "STRING"),
                bigquery.SchemaField("row_hash", "STRING"),
                bigquery.SchemaField("NgayTao", "DATE"),
            ]
            table = bigquery.Table(table_id, schema=schema)
            table.clustering_fields = ["tenant"]
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="NgayTao",
            )
            self.client.create_table(table)
            self.logger.info("✅ Created table %s", table_id)

    def ensure_table_schema_staff(self) -> None:
        table_id = self.fqn("staff")
        try:
            self.client.get_table(table_id)
            return
        except NotFound:
            schema = [
                bigquery.SchemaField("_id", "STRING"),
                bigquery.SchemaField("email", "STRING"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("updateTime", "INT64"),
                bigquery.SchemaField("createTime", "INT64"),
                bigquery.SchemaField("group_id", "STRING"),
                bigquery.SchemaField("tenant", "STRING"),
                bigquery.SchemaField("row_hash", "STRING"),
            ]
            table = bigquery.Table(table_id, schema=schema)
            table.clustering_fields = ["tenant", "name"]
            self.client.create_table(table)
            self.logger.info("✅ Created table %s", table_id)

    def ensure_table_schema_group(self) -> None:
        table_id = self.fqn("group")
        try:
            self.client.get_table(table_id)
            return
        except NotFound:
            schema = [
                bigquery.SchemaField("group_id", "STRING"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("tenant", "STRING"),
                bigquery.SchemaField("row_hash", "STRING"),
            ]
            table = bigquery.Table(table_id, schema=schema)
            table.clustering_fields = ["tenant", "group_id"]
            self.client.create_table(table)
            self.logger.info("✅ Created table %s", table_id)

    def ensure_table_schema_rank_mapping(self) -> None:
        table_id = self.fqn("rank_mapping")
        try:
            self.client.get_table(table_id)
            return
        except NotFound:
            schema = [
                bigquery.SchemaField("code", "STRING"),
                bigquery.SchemaField("grade", "STRING"),
                bigquery.SchemaField("target_day", "INT64"),
                bigquery.SchemaField("target_week", "INT64"),
                bigquery.SchemaField("target_month", "INT64"),
                bigquery.SchemaField("week_key", "STRING"),
                bigquery.SchemaField("week_start", "DATE"),
                bigquery.SchemaField("snapshot_at", "TIMESTAMP"),
            ]
            table = bigquery.Table(table_id, schema=schema)
            table.clustering_fields = ["code"]
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="week_start",
            )
            self.client.create_table(table)
            self.logger.info("✅ Created table %s", table_id)

    def ensure_table_schema_customer(self) -> None:
        table_id = self.fqn("customer")
        try:
            self.client.get_table(table_id)
            return
        except NotFound:
            schema = [
                bigquery.SchemaField("_id", "STRING"),
                bigquery.SchemaField("assignedTime", "INT64"),
                bigquery.SchemaField("createTime", "INT64"),
                bigquery.SchemaField("updateTime", "INT64"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("phone", "STRING"),
                bigquery.SchemaField("user_id", "STRING"),
                bigquery.SchemaField("user_name", "STRING"),
                bigquery.SchemaField("user_group_id", "STRING"),
                bigquery.SchemaField("tenant", "STRING"),
                bigquery.SchemaField("row_hash", "STRING"),
                bigquery.SchemaField("customField_0_val", "STRING"),
                bigquery.SchemaField("NgayUpdate", "DATE"),
                bigquery.SchemaField("NgayAssign", "DATE"),
            ]
            table = bigquery.Table(table_id, schema=schema)
            table.clustering_fields = ["tenant", "_id"]
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="NgayUpdate",
            )
            self.client.create_table(table)
            self.logger.info("✅ Created table %s", table_id)

    def ensure_table_schema_customer_staging(self) -> None:
        table_id = self.fqn("stg_customer")
        try:
            self.client.get_table(table_id)
            return
        except NotFound:
            schema = [
                bigquery.SchemaField("_id", "STRING"),
                bigquery.SchemaField("assignedTime", "INT64"),
                bigquery.SchemaField("createTime", "INT64"),
                bigquery.SchemaField("updateTime", "INT64"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("phone", "STRING"),
                bigquery.SchemaField("user_id", "STRING"),
                bigquery.SchemaField("user_name", "STRING"),
                bigquery.SchemaField("user_group_id", "STRING"),
                bigquery.SchemaField("tenant", "STRING"),
                bigquery.SchemaField("row_hash", "STRING"),
                bigquery.SchemaField("customField_0_val", "STRING"),
                bigquery.SchemaField("NgayUpdate", "DATE"),
                bigquery.SchemaField("NgayAssign", "DATE"),
            ]
            table = bigquery.Table(table_id, schema=schema)
            self.client.create_table(table)
            self.logger.info("✅ Created table %s", table_id)

    def ensure_update_log(self) -> None:
        table_id = self.fqn("update_log")
        try:
            self.client.get_table(table_id)
            return
        except NotFound:
            schema = [
                bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("tenant", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("rows_loaded", "INT64"),
                bigquery.SchemaField("max_updateTime", "INT64"),
                bigquery.SchemaField("mode", "STRING"),
            ]
            table = bigquery.Table(table_id, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="updated_at",
            )
            self.client.create_table(table)
            self.logger.info("✅ Created table %s", table_id)

    # ------------------------------------------------------------------
    # Loading helpers
    # ------------------------------------------------------------------
    def load_dataframe(
        self,
        df: pd.DataFrame,
        table: str,
        write_disposition: bigquery.WriteDisposition,
        autodetect: bool = True,
        allow_schema_updates: bool = True,
    ) -> int:
        if df.empty:
            return 0
        df = ensure_unique_columns(df)
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            autodetect=autodetect,
        )
        if allow_schema_updates:
            job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
            ]
        self.client.load_table_from_dataframe(
            df,
            self.fqn(table),
            job_config=job_config,
            location=self.config.location,
        ).result()
        return int(len(df))

    def load_append(self, df: pd.DataFrame, table: str) -> int:
        return self.load_dataframe(
            df,
            table,
            bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,
            allow_schema_updates=True,
        )

    def load_truncate(self, df: pd.DataFrame, table: str) -> int:
        return self.load_dataframe(
            df,
            table,
            bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
            allow_schema_updates=False,
        )

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------
    def execute_query(
        self,
        sql: str,
        *,
        job_config: Optional[bigquery.QueryJobConfig] = None,
    ) -> bigquery.table.RowIterator:
        return self.client.query(sql, job_config=job_config, location=self.config.location).result()

    def delete_rows_between(self, table: str, field: str, start: datetime.date, end: datetime.date) -> None:
        sql = f"DELETE FROM `{self.fqn(table)}` WHERE {field} BETWEEN @start AND @end"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start", "DATE", start),
                bigquery.ScalarQueryParameter("end", "DATE", end),
            ]
        )
        self.execute_query(sql, job_config=job_config)
