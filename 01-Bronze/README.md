# Sistema de Scraping de Precios de Supermercados

## Descripción General
Sistema ETL automatizado que extrae precios de productos de supermercados (actualmente Coto) y los almacena en Delta Lake usando Databricks. El sistema incluye auditoría completa, manejo de errores y métricas de ejecución.

## Arquitectura

### Estructura del Proyecto

```
Precios Supermercados/
├── Databricks-Scrapping-Market/01-Bronze/
│   ├── scrapper.py                    # Orquestador principal del ETL
│   ├── products.py                    # Configuración de URLs y productos
│   ├── etl_modules/
│   │   ├── __init__.py               # Módulo Python
│   │   ├── config.py                  # Configuración de entornos y tablas
│   │   ├── scrappers.py              # Lógica de scraping por supermercado
│   │   ├── data_loader.py            # Escritura a Delta Lake
│   │   └── logging_utils.py          # Utilidades de logging y monitoreo
│   ├── Pruebas/                      # Carpeta de pruebas
│   └── scrapper_documentation.drawio  # Diagramas de arquitectura
├── Extract (Notebook)                 # Notebook alternativo de extracción
├── Transform (Notebook)               # Transformaciones y análisis
├── URL_Productos (Notebook)           # Gestión de URLs
└── Agregar productos (Notebook)       # Agregar nuevos productos
```

## Componentes Principales

### 1. scrapper.py (Orquestador Principal)
Coordina el flujo completo de extracción:
* Inicializa logging y contexto Spark
* Genera identificadores de lote (batch_id)
* Ejecuta scraping con run_coto_scraping()
* Gestiona errores y persistencia
* Escribe eventos de auditoría
* Genera métricas de ejecución

### 2. products.py (Configuración de Productos)
Define el diccionario URLS con la estructura:
```python
URLS = {
    "bife_americano": {
        "nombre": "Bife Americano",
        "unidad": "kg",
        "categoria": "Carnes",
        "urls": {
            "Coto": "https://www.cotodigital.com.ar/..."
        }
    }
}
```

### 3. etl_modules/

#### config.py
* Gestión de entornos (dev/prod)
* Catálogos: supermercadoetl_dev / supermercadoetl_prod
* Definición de tablas (audit.\*, bronze.\*)
* Parámetros de request HTTP

#### scrappers.py
* `coto_scraping()`: Extrae precio de una URL de Coto
* `run_coto_scraping()`: Itera sobre todos los productos configurados
* Manejo de SSL y timeouts
* Registro de errores por URL

#### data_loader.py
* `ensure_objects()`: Crea schemas y tablas Delta si no existen
* `write_bronze_prices()`: Carga datos a bronze.precios_scraping
* `write_event_log()`: Eventos STARTED/SUCCESS/FAILED
* `write_scraping_errors()`: Errores a nivel de URL
* `write_run_metrics()`: Métricas agregadas por ejecución

#### logging_utils.py
* Configuración de logging estructurado
* Impresión de contexto de ejecución
* Resumen de runs para consola

## Esquema de Datos

### Bronze Layer

#### bronze.precios_scraping
| Columna               | Tipo      | Descripción                          |
|-----------------------|-----------|--------------------------------------|
| supermercado          | STRING    | Nombre del supermercado (Coto)       |
| nombre                | STRING    | Nombre del producto                  |
| descripcion           | STRING    | Descripción completa del producto    |
| precio                | STRING    | Precio (puede incluir "Sin stock")   |
| categoria             | STRING    | Categoría del producto (Carnes, etc.)|
| url                   | STRING    | URL fuente                           |
| fecha_extraccion      | DATE      | Fecha de scraping                    |
| batch_id              | STRING    | UUID único por ejecución             |
| ingestion_timestamp   | TIMESTAMP | Timestamp de ingesta                 |
| load_date             | DATE      | Fecha de carga a Delta               |

### Audit Layer

#### audit.etl_logs
Eventos operacionales del ETL (STARTED, SUCCESS, FAILED, SUMMARY)

#### audit.scraping_errors
Errores a nivel de URL individual

#### audit.run_metrics
Métricas agregadas por ejecución:
* products_configured: Productos configurados
* urls_target: URLs objetivo
* rows_success: Registros exitosos
* rows_error: Errores
* success_rate_pct: Tasa de éxito

## Configuración

### Variables de Entorno
* `ENV`: dev | prod (default: dev)
* `DBX_CATALOG`: Override del catálogo
* `PIPELINE_NAME`: Nombre del pipeline (default: extract_coto_precios)
* `SOURCE_NAME`: Fuente de datos (default: Coto)

### Argumentos CLI
```bash
python scrapper.py --env prod --catalog supermercadoetl_prod
```

## Uso

### Ejecución del Scraper

#### Modo Standalone
```python
python /Workspace/Users/.../01-Bronze/scrapper.py
```

#### Desde Notebook
```python
%run ./Databricks-Scrapping-Market/01-Bronze/scrapper.py
```

#### Como Módulo
```python
from scrapper import main
main()
```

## Flujo de Ejecución

1. **Inicialización** (main)
   * setup_logging()
   * Conexión a Spark
   * USE CATALOG {env}

2. **Preparación**
   * Generación de batch_id único
   * Timestamp de ingesta

3. **Scraping**
   * Iteración sobre URLS configuradas
   * coto_scraping() por cada URL
   * Captura de errores individuales

4. **Persistencia**
   * ensure_objects() (crear tablas si no existen)
   * write_event_log() STATUS=STARTED
   * write_bronze_prices()
   * write_scraping_errors()
   * write_event_log() STATUS=SUCCESS/FAILED

5. **Auditoría**
   * print_run_summary() (consola)
   * write_summary_log()
   * write_run_metrics()

## Monitoreo

### Consultar Última Ejecución
```sql
SELECT * 
FROM audit.run_metrics 
ORDER BY event_ts DESC 
LIMIT 1
```

### Ver Errores Recientes
```sql
SELECT corte, url, error_type, error_message
FROM audit.scraping_errors
WHERE DATE(event_ts) = CURRENT_DATE()
```

### Historial de Eventos
```sql
SELECT event_ts, status, batch_id, rows_read, rows_written
FROM audit.etl_logs
WHERE pipeline = 'extract_coto_precios'
ORDER BY event_ts DESC
```

## Manejo de Errores

El sistema maneja errores en tres niveles:

1. **URL-level**: Errores individuales no detienen la ejecución
2. **Batch-level**: Fallos críticos registrados en audit.etl_logs
3. **Validation**: Guardrail para evitar cargar DataFrames vacíos

## Extensibilidad

### Agregar Nuevo Supermercado

1. Agregar URLs en `products.py`
2. Crear función scraper en `etl_modules/scrappers.py`
3. Modificar `run_X_scraping()` para llamar a la nueva función
4. Actualizar SOURCE_NAME en config

### Agregar Nuevos Productos

Editar el diccionario URLS en `products.py`:
```python
"nuevo_producto": {
    "nombre": "Producto Nuevo",
    "unidad": "kg",
    "categoria": "Categoría",
    "urls": {
        "Coto": "https://..."
    }
}
```

## Diagramas

Para visualizar la arquitectura completa y el flujo de runtime, consultar:
`/Databricks-Scrapping-Market/01-Bronze/scrapper_documentation.drawio`

Contiene:
* Diagrama de estructura del scrapper
* Diagrama de runtime y secuencia
* Relaciones entre componentes
* Flujo de datos (Input → Processing → Output)

## Estructura de Catálogos

### Desarrollo (supermercadoetl_dev)
```
supermercadoetl_dev/
├── bronze/
│   └── precios_scraping
└── audit/
    ├── etl_logs
    ├── scraping_errors
    └── run_metrics
```

### Producción (supermercadoetl_prod)
```
supermercadoetl_prod/
├── bronze/
│   └── precios_scraping
└── audit/
    ├── etl_logs
    ├── scraping_errors
    └── run_metrics
```

## Mejores Prácticas

### Desarrollo
* Usar entorno `dev` para pruebas
* Validar nuevos productos en modo standalone
* Revisar logs antes de promover a producción

### Producción
* Configurar alertas sobre `success_rate_pct < 90`
* Monitorear tablas de auditoría diariamente
* Mantener historial de run_metrics para análisis de tendencias

### Mantenimiento
* Actualizar URLs cuando cambien layouts de sitios
* Documentar cambios en products.py
* Validar scraping después de actualizaciones de sitios

## Troubleshooting

### Problema: Sin datos en bronze.precios_scraping
**Solución**: Revisar audit.scraping_errors para identificar URLs problemáticas

### Problema: Timeouts frecuentes
**Solución**: Ajustar parámetros de timeout en config.py

### Problema: Cambio en estructura HTML
**Solución**: Actualizar selectores CSS en scrappers.py

### Problema: Certificados SSL
**Solución**: Verificar configuración de SSL en config.REQUEST_PARAMS

## Contacto y Soporte

Para dudas o soporte, revisar:
* Logs de auditoría en `audit.*` tables
* Documentación inline en cada módulo
* Diagramas en scrapper_documentation.drawio

---

**Última actualización**: Abril 2026 
**Versión**: 1.0  
**Autor**: Felipe Carballo
