import logging

from etl_modules.config import (
    AUDIT_SCHEMA,
    BRONZE_PRICES_TABLE,
    BRONZE_SCHEMA,
    CATALOG,
    ETL_LOGS_TABLE,
    ENV,
    PIPELINE_NAME,
    RUN_METRICS_TABLE,
    SCRAPING_ERRORS_TABLE,
    SOURCE_NAME,
)


def setup_logging() -> None:
    """Configure the base logger format and level for the pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def log_runtime_context() -> None:
    """Log resolved configuration values (env/catalog/schemas/tables) at startup."""
    logging.info("Runtime context loaded")
    logging.info("ENV=%s", ENV)
    logging.info("CATALOG=%s", CATALOG)
    logging.info("PIPELINE_NAME=%s", PIPELINE_NAME)
    logging.info("SOURCE_NAME=%s", SOURCE_NAME)
    logging.info("AUDIT_SCHEMA=%s", AUDIT_SCHEMA)
    logging.info("BRONZE_SCHEMA=%s", BRONZE_SCHEMA)
    logging.info("ETL_LOGS_TABLE=%s", ETL_LOGS_TABLE)
    logging.info("SCRAPING_ERRORS_TABLE=%s", SCRAPING_ERRORS_TABLE)
    logging.info("RUN_METRICS_TABLE=%s", RUN_METRICS_TABLE)
    logging.info("BRONZE_PRICES_TABLE=%s", BRONZE_PRICES_TABLE)


def log_execution_targets(spark) -> None:
    """Log active Databricks catalog/schema and expected target tables."""
    target_row = spark.sql(
        "SELECT current_catalog() AS current_catalog, current_schema() AS current_schema"
    ).collect()[0]

    logging.info("Databricks current_catalog()=%s", target_row["current_catalog"])
    logging.info("Databricks current_schema()=%s", target_row["current_schema"])
    logging.info("Expected ETL_LOGS_TABLE=%s", ETL_LOGS_TABLE)
    logging.info("Expected SCRAPING_ERRORS_TABLE=%s", SCRAPING_ERRORS_TABLE)
    logging.info("Expected RUN_METRICS_TABLE=%s", RUN_METRICS_TABLE)
    logging.info("Expected BRONZE_PRICES_TABLE=%s", BRONZE_PRICES_TABLE)


def print_run_summary(
    batch_id: str,
    ingestion_timestamp: str,
    total_productos_configurados: int,
    total_urls_objetivo: int,
    total_exitos: int,
    total_errores: int,
    total_registros_df: int,
    tasa_exito: float,
    df_resultados,
    df_errores,
) -> None:
    """Print a human-readable run summary with success and error breakdown."""
    print("=" * 80)
    print(f"RESUMEN DE CORRIDA - EXTRACT {SOURCE_NAME.upper()}")
    print("=" * 80)
    print(f"Batch ID: {batch_id}")
    print(f"Fecha de ingestión: {ingestion_timestamp}")
    print(f"Productos configurados: {total_productos_configurados}")
    print(f"URLs objetivo: {total_urls_objetivo}")
    print(f"Registros extraídos correctamente: {total_exitos}")
    print(f"Registros en df_resultados: {total_registros_df}")
    print(f"Errores de scraping: {total_errores}")
    print(f"Tasa de éxito sobre URLs objetivo: {tasa_exito:.2f}%")
    print("-")

    if total_exitos:
        print("Productos cargados con éxito:")
        print(df_resultados[["nombre", "descripcion", "precio", "url"]].head(10).to_string(index=False))
    else:
        print("No se cargaron productos con éxito.")

    print("-")
    if total_errores:
        print("Errores de scraping detectados:")
        print(df_errores[["corte", "supermercado", "error_type", "error_message"]].to_string(index=False))
    else:
        print("No hubo errores de scraping.")

    print("=" * 80)
