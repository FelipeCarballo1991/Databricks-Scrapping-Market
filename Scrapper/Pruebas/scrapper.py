import pandas as pd
import requests
import json
import re
import uuid
import logging
import traceback
from datetime import datetime
from urllib.parse import urlparse
from productos import URLS
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from pyspark.sql import functions as F

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# Función Scrapping (Coto)
def coto_scraping(url,corte):
    try:
        params = {
            "Dy": "1",
            "idSucursal": "200",
            "format": "json"
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "application/json"
        }

        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
        except requests.exceptions.SSLError as e:
            logging.warning("SSL error en %s: %s", url, e)
            response = requests.get(url, headers=headers, params=params, timeout=10, verify=False)

        data = response.json()

        datadto_price_str = data['contents'][0]['Main'][0]['record']['attributes']['sku.dtoPrice'][0]
        dtoPrice_json = json.loads(datadto_price_str)

        precio = dtoPrice_json['precioLista']
        nombre = data['contents'][0]['Main'][0]['record']['attributes']['product.displayName'][0]

        return {
            "fecha_extraccion": datetime.now().strftime("%d-%m-%Y"),
            "supermercado": "Coto",
            "nombre": corte,
            "descripcion": nombre,
            "precio": precio,
            "url": url
        }
    except Exception as e:
        logging.exception("Error con %s", url)
        return None


# Función principal
batch_id = str(uuid.uuid4())
ingestion_timestamp = datetime.now().isoformat(timespec="seconds")

resultados = []
errores_scraping = []
for corte in URLS.keys():
    url = URLS[corte]['urls']
    nombre = URLS[corte]['nombre']
    for supermercado in url.keys():
        url_actual = url[supermercado]

        try:
            info = coto_scraping(url_actual, nombre)
            if info:
                resultados.append(info)
            else:
                errores_scraping.append({
                    "corte": nombre,
                    "supermercado": supermercado,
                    "url": url_actual,
                    "error_type": "NoData",
                    "error_message": "No se pudo extraer información"
                })
                logging.warning("No se pudo extraer info para %s - %s", nombre, url_actual)
        except Exception as e:
            errores_scraping.append({
                "corte": nombre,
                "supermercado": supermercado,
                "url": url_actual,
                "error_type": type(e).__name__,
                "error_message": str(e)
            })
            logging.exception("Fallo inesperado en %s - %s", nombre, url_actual)


df_resultados = pd.DataFrame(resultados)
df_resultados["batch_id"] = batch_id
df_resultados["ingestion_timestamp"] = ingestion_timestamp


spark.sql("CREATE SCHEMA IF NOT EXISTS supermercadoetl.audit")
spark.sql("""
CREATE TABLE IF NOT EXISTS supermercadoetl.audit.etl_logs (
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
""")
spark.sql("""
CREATE TABLE IF NOT EXISTS supermercadoetl.audit.scraping_errors (
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
""")
spark.sql("""
CREATE TABLE IF NOT EXISTS supermercadoetl.audit.run_metrics (
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
""")

def log_event(status, rows_read=None, rows_written=None, error_type=None, error_message=None, stacktrace=None):
    log_row = pd.DataFrame([{
        "event_ts": datetime.utcnow(),
        "pipeline": "extract_coto_precios",
        "source": "Coto",
        "status": status,
        "batch_id": batch_id,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "error_type": error_type,
        "error_message": error_message,
        "stacktrace": stacktrace
    }])
    log_df = spark.createDataFrame(log_row)
    log_df.write.format("delta").mode("append").saveAsTable("supermercadoetl.audit.etl_logs")
    logging.info({
        "status": status,
        "batch_id": batch_id,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "error_type": error_type,
        "error_message": error_message,
    })

def write_scraping_errors(df_errors):
    if df_errors.empty:
        return
    errors_df = spark.createDataFrame(df_errors)
    errors_df = (
        errors_df
        .withColumn("event_ts", F.current_timestamp())
        .withColumn("pipeline", F.lit("extract_coto_precios"))
        .withColumn("source", F.lit("Coto"))
        .withColumn("batch_id", F.lit(batch_id))
    )
    errors_df.write.format("delta").mode("append").saveAsTable("supermercadoetl.audit.scraping_errors")
    logging.warning("Se registraron %s errores de scraping en audit.scraping_errors", df_errors.shape[0])

def write_run_metrics():
    metrics_row = pd.DataFrame([{
        "event_ts": datetime.utcnow(),
        "pipeline": "extract_coto_precios",
        "source": "Coto",
        "batch_id": batch_id,
        "ingestion_timestamp": ingestion_timestamp,
        "products_configured": total_productos_configurados,
        "urls_target": total_urls_objetivo,
        "rows_success": total_exitos,
        "rows_error": total_errores,
        "success_rate_pct": float(round(tasa_exito, 2))
    }])
    metrics_df = spark.createDataFrame(metrics_row)
    metrics_df.write.format("delta").mode("append").saveAsTable("supermercadoetl.audit.run_metrics")
    logging.info({
        "status": "SUMMARY",
        "batch_id": batch_id,
        "ingestion_timestamp": ingestion_timestamp,
        "products_configured": total_productos_configurados,
        "urls_target": total_urls_objetivo,
        "rows_success": total_exitos,
        "rows_error": total_errores,
        "success_rate_pct": round(tasa_exito, 2),
    })

try:
    if df_resultados.empty:
        raise ValueError("El scraping no devolvió filas; no se puede cargar una tabla vacía.")

    log_event("STARTED", rows_read=len(df_resultados), rows_written=0)

    spark_df = spark.createDataFrame(df_resultados)
    spark_df = (
        spark_df
        .withColumn("precio", F.col("precio").cast("double"))
        .withColumn("fecha_extraccion", F.to_date("fecha_extraccion", "dd-MM-yyyy"))
        .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
        .withColumn("load_date", F.current_date())
    )

    spark.sql("""
    CREATE TABLE IF NOT EXISTS supermercadoetl.bronze.precios_scraping (
      supermercado STRING,
      nombre STRING,
      descripcion STRING,
      precio DOUBLE,
      url STRING,
      fecha_extraccion DATE,
      batch_id STRING,
      ingestion_timestamp TIMESTAMP,
      load_date DATE
    )
    USING DELTA
    """)

    (
        spark_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable("supermercadoetl.bronze.precios_scraping")
    )

    if errores_scraping:
        write_scraping_errors(pd.DataFrame(errores_scraping))
    log_event("SUCCESS", rows_read=len(df_resultados), rows_written=len(df_resultados))

except Exception as e:
    if errores_scraping:
        write_scraping_errors(pd.DataFrame(errores_scraping))
    log_event(
        "FAILED",
        rows_read=len(df_resultados),
        rows_written=0,
        error_type=type(e).__name__,
        error_message=str(e),
        stacktrace=traceback.format_exc()
    )
    raise


total_productos_configurados = len(URLS)
total_urls_objetivo = sum(len(item['urls']) for item in URLS.values())
total_exitos = len(resultados)
total_errores = len(errores_scraping)
total_registros_df = len(df_resultados)
tasa_exito = (total_exitos / total_urls_objetivo * 100) if total_urls_objetivo else 0

print("=" * 80)
print("RESUMEN DE CORRIDA - EXTRACT COTO")
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
    df_errores = pd.DataFrame(errores_scraping)
    print(df_errores[["corte", "supermercado", "error_type", "error_message"]].to_string(index=False))
else:
    print("No hubo errores de scraping.")

print("=" * 80)

summary_payload = {
    "batch_id": batch_id,
    "ingestion_timestamp": ingestion_timestamp,
    "products_configured": total_productos_configurados,
    "urls_target": total_urls_objetivo,
    "rows_success": total_exitos,
    "rows_error": total_errores,
    "success_rate_pct": round(tasa_exito, 2),
}

summary_log_row = pd.DataFrame([{
    "event_ts": datetime.utcnow(),
    "pipeline": "extract_coto_precios",
    "source": "Coto",
    "status": "SUMMARY",
    "batch_id": batch_id,
    "rows_read": total_urls_objetivo,
    "rows_written": total_exitos,
    "error_type": "RUN_METRICS",
    "error_message": json.dumps(summary_payload, ensure_ascii=False),
    "stacktrace": None
}])

summary_log_df = spark.createDataFrame(summary_log_row)
summary_log_df.write.format("delta").mode("append").saveAsTable("supermercadoetl.audit.etl_logs")
write_run_metrics()
logging.info({"status": "SUMMARY", **summary_payload})
