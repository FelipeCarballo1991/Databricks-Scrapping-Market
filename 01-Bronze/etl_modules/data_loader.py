import json
import logging
from datetime import datetime

import pandas as pd
from pyspark.sql import functions as F

from etl_modules.config import (
    AUDIT_SCHEMA,
    BRONZE_PRICES_TABLE,
    BRONZE_SCHEMA,
    ETL_LOGS_TABLE,
    PIPELINE_NAME,
    RUN_METRICS_TABLE,
    SCRAPING_ERRORS_TABLE,
    SOURCE_NAME,
)


def ensure_objects(spark) -> None:
    """Create required schemas and Delta tables if they do not exist."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {AUDIT_SCHEMA}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ETL_LOGS_TABLE} (
          event_ts TIMESTAMP,
          pipeline STRING,
          source STRING,
          status STRING,
          batch_id STRING,
          rows_read BIGINT,
          rows_written BIGINT,
          error_type STRING,
          error_message STRING,
          stacktrace STRING
        )
        USING DELTA
        """
    )
    spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {SCRAPING_ERRORS_TABLE} (
          event_ts TIMESTAMP,
          pipeline STRING,
          source STRING,
          batch_id STRING,
          corte STRING,
          supermercado STRING,
          url STRING,
          error_type STRING,
          error_message STRING
        )
        USING DELTA
        """
    )
    spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {RUN_METRICS_TABLE} (
          event_ts TIMESTAMP,
          pipeline STRING,
          source STRING,
          batch_id STRING,
          ingestion_timestamp STRING,
          products_configured BIGINT,
          urls_target BIGINT,
          rows_success BIGINT,
          rows_error BIGINT,
          success_rate_pct DOUBLE
        )
        USING DELTA
        """
    )
    spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {BRONZE_PRICES_TABLE} (
          supermercado STRING,
          nombre STRING,
          descripcion STRING,
          precio STRING,
          categoria STRING,
          url STRING,
          fecha_extraccion DATE,
          batch_id STRING,
          ingestion_timestamp TIMESTAMP,
          load_date DATE
        )
        USING DELTA
        """
    )


def prepare_result_df(resultados: list, batch_id: str, ingestion_timestamp: str) -> pd.DataFrame:
    """Build the bronze-ready pandas DataFrame adding batch traceability fields."""
    df_resultados = pd.DataFrame(resultados)
    df_resultados["batch_id"] = batch_id
    df_resultados["ingestion_timestamp"] = ingestion_timestamp
    return df_resultados


def write_event_log(
    spark,
    batch_id: str,
    status: str,
    rows_read=None,
    rows_written=None,
    error_type=None,
    error_message=None,
    stacktrace=None,
) -> None:
    """Append one operational ETL event (STARTED/SUCCESS/FAILED/SUMMARY) to audit logs."""
    log_row = pd.DataFrame(
        [
            {
                "event_ts": datetime.utcnow(),
                "pipeline": PIPELINE_NAME,
                "source": SOURCE_NAME,
                "status": status,
                "batch_id": batch_id,
                "rows_read": rows_read,
                "rows_written": rows_written,
                "error_type": error_type,
                "error_message": error_message,
                "stacktrace": stacktrace,
            }
        ]
    )
    spark.createDataFrame(log_row).write.format("delta").mode("append").saveAsTable(ETL_LOGS_TABLE)
    logging.info(
        {
            "status": status,
            "batch_id": batch_id,
            "rows_read": rows_read,
            "rows_written": rows_written,
            "error_type": error_type,
            "error_message": error_message,
        }
    )


def write_scraping_errors(spark, errores_scraping: list, batch_id: str) -> None:
    """Persist row-level scraping failures to audit for later troubleshooting."""
    if not errores_scraping:
        return

    errors_df = spark.createDataFrame(pd.DataFrame(errores_scraping))
    errors_df = (
        errors_df.withColumn("event_ts", F.current_timestamp())
        .withColumn("pipeline", F.lit(PIPELINE_NAME))
        .withColumn("source", F.lit(SOURCE_NAME))
        .withColumn("batch_id", F.lit(batch_id))
    )
    errors_df.write.format("delta").mode("append").saveAsTable(SCRAPING_ERRORS_TABLE)
    logging.warning("Se registraron %s errores de scraping en audit.scraping_errors", len(errores_scraping))


def write_bronze_prices(spark, df_resultados: pd.DataFrame) -> None:
    """Load scraped prices into bronze Delta table using append mode.
    
    Stores precio as STRING to capture numeric values, "Sin stock", and other text statuses.
    Type casting and normalization deferred to silver layer.
    """
    spark_df = spark.createDataFrame(df_resultados)
    # Convert precio to string (as-is from scraping) and format dates
    spark_df = (
        spark_df.withColumn("precio", F.col("precio").cast("string"))
        .withColumn("fecha_extraccion", F.to_date("fecha_extraccion", "dd-MM-yyyy"))
        .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
        .withColumn("load_date", F.current_date())
    )
    spark_df.write.format("delta").mode("append").saveAsTable(BRONZE_PRICES_TABLE)


def write_run_metrics(
    spark,
    batch_id: str,
    ingestion_timestamp: str,
    total_productos_configurados: int,
    total_urls_objetivo: int,
    total_exitos: int,
    total_errores: int,
    tasa_exito: float,
) -> None:
    """Write aggregated run metrics (one row per batch) to audit.run_metrics."""
    metrics_row = pd.DataFrame(
        [
            {
                "event_ts": datetime.utcnow(),
                "pipeline": PIPELINE_NAME,
                "source": SOURCE_NAME,
                "batch_id": batch_id,
                "ingestion_timestamp": ingestion_timestamp,
                "products_configured": total_productos_configurados,
                "urls_target": total_urls_objetivo,
                "rows_success": total_exitos,
                "rows_error": total_errores,
                "success_rate_pct": float(round(tasa_exito, 2)),
            }
        ]
    )
    spark.createDataFrame(metrics_row).write.format("delta").mode("append").saveAsTable(RUN_METRICS_TABLE)


def write_summary_log(
    spark,
    batch_id: str,
    ingestion_timestamp: str,
    total_productos_configurados: int,
    total_urls_objetivo: int,
    total_exitos: int,
    total_errores: int,
    tasa_exito: float,
) -> None:
    """Write a SUMMARY event into audit.etl_logs with serialized run metrics payload."""
    summary_payload = {
        "batch_id": batch_id,
        "ingestion_timestamp": ingestion_timestamp,
        "products_configured": total_productos_configurados,
        "urls_target": total_urls_objetivo,
        "rows_success": total_exitos,
        "rows_error": total_errores,
        "success_rate_pct": round(tasa_exito, 2),
    }

    summary_log_row = pd.DataFrame(
        [
            {
                "event_ts": datetime.utcnow(),
                "pipeline": PIPELINE_NAME,
                "source": SOURCE_NAME,
                "status": "SUMMARY",
                "batch_id": batch_id,
                "rows_read": total_urls_objetivo,
                "rows_written": total_exitos,
                "error_type": "RUN_METRICS",
                "error_message": json.dumps(summary_payload, ensure_ascii=False),
                "stacktrace": None,
            }
        ]
    )

    spark.createDataFrame(summary_log_row).write.format("delta").mode("append").saveAsTable(ETL_LOGS_TABLE)
    logging.info({"status": "SUMMARY", **summary_payload})
