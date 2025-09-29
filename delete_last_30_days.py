"""
This script deletes the last 30 days of data from the call_log and customer tables,
and clears the update_log table.
"""

import os
from datetime import datetime, timedelta, timezone
import dotenv

from callio_etl.bigquery_service import BigQueryService
from callio_etl.config import PipelineConfig
from callio_etl.logging_utils import configure_logging

def delete_last_30_days():
    """
    Deletes the last 30 days of data from the call_log and customer tables,
    and clears the update_log table.
    """
    # Load environment variables from .env file
    dotenv.load_dotenv()

    # Initialize configuration and services
    config = PipelineConfig.from_env()
    logger = configure_logging(config.log_level)
    bq_service = BigQueryService(config.bigquery, logger)

    # Define the date range
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=30)

    logger.info(f"Deleting data from {start_date} to {end_date}")

    # Delete data from the customer table
    try:
        logger.info("Deleting data from the 'customer' table...")
        bq_service.delete_rows_between("customer", "NgayUpdate", start_date, end_date)
        logger.info("Successfully deleted data from the 'customer' table.")
    except Exception as e:
        logger.error(f"Error deleting data from the 'customer' table: {e}")

    # Delete data from the call_log table
    try:
        logger.info("Deleting data from the 'call_log' table...")
        bq_service.delete_rows_between("call_log", "NgayTao", start_date, end_date)
        logger.info("Successfully deleted data from the 'call_log' table.")
    except Exception as e:
        logger.error(f"Error deleting data from the 'call_log' table: {e}")

    # Clear the update_log table
    try:
        logger.info("Clearing the 'update_log' table...")
        table_fqn = bq_service.fqn("update_log")
        sql = f"TRUNCATE TABLE `{table_fqn}`"
        bq_service.execute_query(sql)
        logger.info("Successfully cleared the 'update_log' table.")
    except Exception as e:
        logger.error(f"Error clearing the 'update_log' table: {e}")


import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Deletes the last 30 days of data from the call_log and customer tables, and clears the update_log table."
    )
    parser.add_argument("--yes", action="store_true", help="Confirm the destructive operation.")
    args = parser.parse_args()

    if args.yes:
        delete_last_30_days()
    else:
        print("This script will delete the last 30 days of data from the 'customer' and 'call_log' tables, and clear the 'update_log' table.")
        print("This is a destructive operation and cannot be undone.")
        print("Please review the code and make sure you have a backup of your data before proceeding.")
        print("Run the script with the --yes flag to confirm the operation.")
