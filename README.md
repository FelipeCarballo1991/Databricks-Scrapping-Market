# Databricks-Scrapping-Market

Proyecto de web scraping y análisis de precios de supermercados utilizando la arquitectura medallón de Databricks.

## Descripción

Este proyecto implementa un pipeline de datos completo para extraer, transformar y analizar precios de productos de supermercados. Los datos se procesan siguiendo la arquitectura medallón (Bronze-Silver-Gold) para garantizar calidad, trazabilidad y escalabilidad.

## Arquitectura del Proyecto

### 📊 Estructura de Carpetas

```
Databricks-Scrapping-Market/
├── 01-Bronze/          # Capa de ingesta de datos crudos
├── 02-Silver/          # Capa de datos limpios y transformados
├── 03-Gold/            # Capa de datos agregados (en desarrollo)
└── README.md
```

---

## 🥉 01-Bronze: Capa de Ingesta

Esta carpeta contiene los scripts de web scraping que extraen los datos crudos de los sitios web de supermercados.

**Componentes principales:**
* `scrapper.py` - Script principal de scraping
* `products.py` - Definiciones y modelos de productos
* `etl_modules/` - Módulos auxiliares para el proceso ETL
* `Pruebas/` - Scripts de prueba y validación
* `scrapper_documentation.drawio` - Documentación visual del proceso

**Función:** Extracción de datos crudos de precios de productos desde fuentes web y carga en formato original sin transformaciones.

---

## 🥈 02-Silver: Capa de Transformación

Esta carpeta contiene las transformaciones que limpian y estandarizan los datos crudos de la capa Bronze.

**Componentes principales:**
* `bronze_to_silver_precios_prod.sql` - Pipeline de transformación para producción
* `bronze_to_silver_precios_dev.sql` - Pipeline de transformación para desarrollo
* `data_exploration` - Notebook de exploración y análisis de datos
* `README.md` - Documentación específica de la capa Silver

**Función:** Limpieza, validación, deduplicación y estandarización de datos. Los datos se estructuran en formato analítico optimizado.

---

## 🥇 03-Gold: Capa de Agregación

**Estado:** 🚧 En desarrollo

Esta carpeta contendrá las agregaciones finales y métricas de negocio listas para consumo en dashboards y reportes.

**Objetivo:** Generar tablas agregadas con métricas clave como:
* Comparativas de precios entre supermercados
* Tendencias históricas de precios
* Análisis de categorías de productos
* KPIs de negocio

---

## 🚀 Tecnologías Utilizadas

* **Databricks** - Plataforma de procesamiento de datos
* **Apache Spark** - Motor de procesamiento distribuido
* **Python** - Lenguaje para web scraping y transformaciones
* **SQL** - Transformaciones de datos
* **Delta Lake** - Almacenamiento de datos con ACID

---

## 📝 Notas

* El proyecto sigue las mejores prácticas de la arquitectura medallón
* Se mantienen ambientes separados (dev/prod) para garantizar estabilidad
* Los datos crudos se preservan en Bronze para trazabilidad completa
