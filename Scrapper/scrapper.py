import uuid
import traceback
from datetime import datetime

import pandas as pd
from pyspark.sql import SparkSession

from etl_modules.config import CATALOG
from etl_modules.data_loader import (
    ensure_objects,
    prepare_result_df,
    write_bronze_prices,
    write_event_log,
    write_run_metrics,
    write_scraping_errors,
    write_summary_log,
)
from etl_modules.logging_utils import (
    log_execution_targets,
    log_runtime_context,
    print_run_summary,
    setup_logging,
)
from etl_modules.scrappers import run_coto_scraping
from products import URLS


def get_spark_session():
    """Get active Spark session or create one if script runs standalone."""
    session = SparkSession.getActiveSession()
    if session is None:
        session = SparkSession.builder.getOrCreate()
    return session


def main():
    """Orchestrate end-to-end extract run: scrape, load, audit and summary."""
    # 1) Initialize logging and Spark context.
    setup_logging()
    spark = get_spark_session()

    # 2) Force Spark session to the configured catalog and log resolved targets.
    spark.sql(f"USE CATALOG {CATALOG}")
    log_runtime_context()
    log_execution_targets(spark)

    # 3) Create run identifiers used across bronze and audit tables.
    batch_id = str(uuid.uuid4())
    ingestion_timestamp = datetime.now().isoformat(timespec="seconds")

    # 4) Run scraping and build dataframe with traceability fields.
    resultados, errores_scraping = run_coto_scraping(URLS)
    df_resultados = prepare_result_df(resultados, batch_id, ingestion_timestamp)

    # 5) Compute run-level metrics for console summary and audit tables.
    total_productos_configurados = len(URLS)
    total_urls_objetivo = sum(len(item["urls"]) for item in URLS.values())
    total_exitos = len(resultados)
    total_errores = len(errores_scraping)
    total_registros_df = len(df_resultados)
    tasa_exito = (total_exitos / total_urls_objetivo * 100) if total_urls_objetivo else 0
    df_errores = pd.DataFrame(errores_scraping)

    # 6) Ensure destination schemas/tables exist before writing.
    ensure_objects(spark)

    try:
        # Guardrail: fail early if scraping produced no valid rows.
        if df_resultados.empty:
            raise ValueError("El scraping no devolvió filas; no se puede cargar una tabla vacía.")

        # 7) Persist start event, bronze data and URL-level errors.
        write_event_log(
            spark,
            batch_id=batch_id,
            status="STARTED",
            rows_read=len(df_resultados),
            rows_written=0,
        )

        write_bronze_prices(spark, df_resultados)
        write_scraping_errors(spark, errores_scraping, batch_id)

        write_event_log(
            spark,
            batch_id=batch_id,
            status="SUCCESS",
            rows_read=len(df_resultados),
            rows_written=len(df_resultados),
        )
    except Exception as exc:
        # 8) On failure, still persist scraping errors and failed run event.
        write_scraping_errors(spark, errores_scraping, batch_id)
        write_event_log(
            spark,
            batch_id=batch_id,
            status="FAILED",
            rows_read=len(df_resultados),
            rows_written=0,
            error_type=type(exc).__name__,
            error_message=str(exc),
            stacktrace=traceback.format_exc(),
        )
        raise

    # 9) Print human-friendly summary for interactive runs.
    print_run_summary(
        batch_id=batch_id,
        ingestion_timestamp=ingestion_timestamp,
        total_productos_configurados=total_productos_configurados,
        total_urls_objetivo=total_urls_objetivo,
        total_exitos=total_exitos,
        total_errores=total_errores,
        total_registros_df=total_registros_df,
        tasa_exito=tasa_exito,
        df_resultados=df_resultados,
        df_errores=df_errores,
    )

    # 10) Persist summary payload and structured run metrics into audit.
    write_summary_log(
        spark,
        batch_id=batch_id,
        ingestion_timestamp=ingestion_timestamp,
        total_productos_configurados=total_productos_configurados,
        total_urls_objetivo=total_urls_objetivo,
        total_exitos=total_exitos,
        total_errores=total_errores,
        tasa_exito=tasa_exito,
    )
    write_run_metrics(
        spark,
        batch_id=batch_id,
        ingestion_timestamp=ingestion_timestamp,
        total_productos_configurados=total_productos_configurados,
        total_urls_objetivo=total_urls_objetivo,
        total_exitos=total_exitos,
        total_errores=total_errores,
        tasa_exito=tasa_exito,
    )


if __name__ == "__main__":
    # Allows direct execution as a script and reuse via imports in notebooks/jobs.
    main()
