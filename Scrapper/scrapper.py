import pandas as pd
import requests
import json
import re
import uuid
from datetime import datetime
from urllib.parse import urlparse
from productos import URLS
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, DateType



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
        except requests.exceptions.SSLError:
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
        print(f"Error con {url}: {e}")
        return None
    

#Scrappeo precio de los productos
resultados = []
for corte in URLS.keys():
    url = (URLS[corte]['urls'])
    nombre = (URLS[corte]['nombre'])
    for supermercado in (url.keys()):
        url = (url[supermercado])

        info = coto_scraping(url,nombre)

        if info:
            resultados.append(info)
        

# Genero un id único por cada carga
batch_id = str(uuid.uuid4())
ingestion_timestamp = datetime.now().isoformat(timespec="seconds")

df_resultados = pd.DataFrame(resultados)
df_resultados["batch_id"] = batch_id
df_resultados["ingestion_timestamp"] = ingestion_timestamp


#Transformo a spark_df
spark_df = spark.createDataFrame(df_resultados)

# Si ya tienes spark_df, normaliza tipos antes de escribir
spark_df = (
    spark_df
    .withColumn("precio", F.col("precio").cast("double"))
    .withColumn("fecha_extraccion", F.to_date("fecha_extraccion", "dd-MM-yyyy"))
    .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
    .withColumn("load_date", F.current_date())  # útil para particionar/controlar cargas
)

# Crear tabla Delta si no existe (primera vez)
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

# Carga incremental diaria (append)
(
    spark_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable("supermercadoetl.bronze.precios_scraping")
)


