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
    session = SparkSession.getActiveSession()
    if session is None:
        session = SparkSession.builder.getOrCreate()
    return session


def main():
    setup_logging()
    spark = get_spark_session()
    spark.sql(f"USE CATALOG {CATALOG}")
    log_runtime_context()
    log_execution_targets(spark)

    batch_id = str(uuid.uuid4())
    ingestion_timestamp = datetime.now().isoformat(timespec="seconds")

    resultados, errores_scraping = run_coto_scraping(URLS)
    df_resultados = prepare_result_df(resultados, batch_id, ingestion_timestamp)

    total_productos_configurados = len(URLS)
    total_urls_objetivo = sum(len(item["urls"]) for item in URLS.values())
    total_exitos = len(resultados)
    total_errores = len(errores_scraping)
    total_registros_df = len(df_resultados)
    tasa_exito = (total_exitos / total_urls_objetivo * 100) if total_urls_objetivo else 0
    df_errores = pd.DataFrame(errores_scraping)

    ensure_objects(spark)

    try:
        if df_resultados.empty:
            raise ValueError("El scraping no devolvió filas; no se puede cargar una tabla vacía.")

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
    main()
