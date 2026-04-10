# Capa Silver: Precios de Productos de Supermercados

## 📋 Descripción General

La **capa Silver** transforma los datos crudos de la capa Bronze en datos curados, validados y listos para análisis. Este proceso limpia, normaliza y estructura los datos de precios de productos de supermercados obtenidos mediante scraping.

---

## 🎯 Objetivo

Transformar datos crudos de scraping (Bronze) en datos de calidad analítica (Silver):

* **Bronze**: Datos tal cual vienen del scraper (strings, duplicados, campos técnicos)
* **Silver**: Datos limpios, normalizados, validados y deduplicados

---

## 📊 Arquitectura de Datos

### Tablas Involucradas

| Capa | Catálogo | Tabla | Descripción |
|------|----------|-------|-------------|
| **Bronze** (Source) | `supermercadoetl_[env]` | `bronze.precios_scraping` | Datos crudos del scraper |
| **Silver** (Target) | `supermercadoetl_[env]` | `silver.precios_productos` | Datos curados y normalizados |

**Nota**: `[env]` puede ser `dev` o `prod`

---

## 🔑 Modelo de Datos Silver

### Esquema de la Tabla

```sql
CREATE TABLE silver.precios_productos (
  producto_id STRING,              -- Surrogate key MD5(supermercado||nombre)
  supermercado STRING,              -- Nombre del supermercado
  categoria STRING,                 -- Categoría del producto
  nombre STRING,                    -- Nombre del producto
  descripcion STRING,               -- Descripción detallada
  url STRING,                       -- URL del producto
  precio_normalizado DECIMAL(18,2),-- Precio limpio en formato numérico
  fecha_extraccion DATE,            -- Fecha del scraping
  fecha_actualizacion TIMESTAMP     -- Timestamp de última actualización
)
```

### Clave Primaria Compuesta

```
(producto_id, fecha_extraccion)
```

* **producto_id**: Identifica de forma única al producto (sin fecha)
* **fecha_extraccion**: Dimensión temporal que permite histórico

**Ejemplo**:
```
producto_id='abc123' | fecha='2026-04-10' | precio=21999.00
producto_id='abc123' | fecha='2026-04-11' | precio=22499.00  ← Histórico
producto_id='abc123' | fecha='2026-04-12' | precio=21799.00
```

---

## 🔄 Transformaciones Aplicadas

### 1. Generación de Surrogate Key

```sql
MD5(CONCAT(supermercado, '|', nombre)) AS producto_id
```

**Propósito**: Identificador único y determinístico del producto
**Ventaja**: Mismo producto = mismo ID en todas las cargas (permite histórico)

### 2. Normalización de Precios

**Transformación**:
```sql
CAST(
  REGEXP_REPLACE(
    REGEXP_REPLACE(TRIM(precio), '[^0-9,.-]', ''),
    ',', '.'
  ) AS DECIMAL(18,2)
) AS precio_normalizado
```

**Ejemplo**:
| Input (Bronze) | Output (Silver) |
|----------------|-----------------|
| `"$ 21.999,00"` | `21999.00` |
| `"14799.0"` | `14799.00` |
| `"Precio: 13.599"` | `13599.00` |

**Pasos**:
1. `TRIM()`: Elimina espacios
2. `REGEXP_REPLACE(..., '[^0-9,.-]', '')`: Elimina caracteres no numéricos
3. `REGEXP_REPLACE(..., ',', '.')`: Reemplaza coma por punto
4. `CAST(...AS DECIMAL(18,2))`: Convierte a formato numérico

### 3. Eliminación de Columnas Técnicas

**Columnas eliminadas de Bronze**:

| Columna | Razón |
|---------|-------|
| `precio_raw` | Ya normalizado en `precio_normalizado` |
| `batch_id` | Control técnico de ingesta (no analítico) |
| `ingestion_timestamp` | Timestamp técnico interno |
| `load_date` | Metadata redundante |
| `validation_status` | Solo usado en transformación |

**Resultado**: De 12 columnas (Bronze) → 9 columnas (Silver)

---

## ✅ Validaciones de Calidad

### Filtros Aplicados

```sql
WHERE 
  -- 1. Precio válido y mayor a 0
  TRY_CAST(...precio_normalizado...) IS NOT NULL
  AND TRY_CAST(...precio_normalizado...) > 0
  
  -- 2. Campos críticos no nulos
  AND supermercado IS NOT NULL
  AND nombre IS NOT NULL
  AND categoria IS NOT NULL
```

### Reglas de Calidad

| Validación | Criterio | Acción |
|------------|----------|--------|
| **Precio válido** | Conversión a DECIMAL exitosa | Rechazar si falla |
| **Precio positivo** | `precio > 0` | Rechazar si ≤ 0 |
| **Supermercado** | No nulo | Rechazar si nulo |
| **Nombre** | No nulo | Rechazar si nulo |
| **Categoría** | No nulo | Rechazar si nulo |

---

## 🔀 Deduplicación

### Problema Resuelto

Cuando hay **múltiples scrapes del mismo producto el mismo día** en Bronze:

```
producto='Bife' | fecha='2026-04-10' | timestamp='10:00' | precio=21999
producto='Bife' | fecha='2026-04-10' | timestamp='10:30' | precio=22499 ← Más reciente
producto='Bife' | fecha='2026-04-10' | timestamp='10:15' | precio=21800
```

### Solución Implementada

```sql
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY MD5(CONCAT(supermercado, '|', nombre)), fecha_extraccion
  ORDER BY ingestion_timestamp DESC
) = 1
```

**Efecto**: Selecciona solo el registro más reciente (timestamp más alto)

**Resultado**:
```
producto='Bife' | fecha='2026-04-10' | timestamp='10:30' | precio=22499 ✓
```

---

## 📈 Estrategia de Carga

### Tipo: MERGE Incremental

```sql
MERGE INTO silver.precios_productos AS target
USING (...) AS source
ON target.producto_id = source.producto_id 
   AND target.fecha_extraccion = source.fecha_extraccion
```

### Comportamiento por Escenario

| Escenario | Condición | Acción | Resultado |
|-----------|-----------|--------|-----------|
| **Primera carga del día** | NOT MATCHED | INSERT | Nuevas filas |
| **Re-ejecución mismo día** | MATCHED | UPDATE | Actualiza precio |
| **Nuevo día de scraping** | NOT MATCHED | INSERT | Histórico crece |
| **Producto discontinuado** | N/A | N/A | Histórico preservado |

### Ejemplo de Crecimiento

```
Día 1 (10-04): 12 productos → 12 INSERT    (Total: 12 filas)
Día 1 (re-run): 12 productos → 12 UPDATE   (Total: 12 filas)
Día 2 (11-04): 12 productos → 12 INSERT    (Total: 24 filas)
Día 3 (12-04): 13 productos → 13 INSERT    (Total: 37 filas)
```

---

## ⚙️ Propiedades de Optimización

```sql
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
```

* **optimizeWrite**: Optimiza archivos pequeños durante escritura
* **autoCompact**: Compacta archivos automáticamente

---

## 📁 Archivos del Pipeline

| Archivo | Descripción |
|---------|-------------|
| `bronze_to_silver_precios_dev.sql` | Pipeline para ambiente DEV (`supermercadoetl_dev`) |
| `bronze_to_silver_precios_prod.sql` | Pipeline para ambiente PROD (`supermercadoetl_prod`) |
| `README.md` | Esta documentación |

---

## 🚀 Ejecución del Pipeline

### Manualmente (SQL Editor)

```sql
-- Ejecutar el archivo completo
%run ./bronze_to_silver_precios_dev.sql
```

### En Workflow/Job

1. Crear un Job de Databricks
2. Agregar una tarea SQL
3. Seleccionar el archivo `bronze_to_silver_precios_[env].sql`
4. Programar ejecución diaria

---

## 🔍 Validación Post-Ejecución

### Verificar registros cargados

```sql
SELECT COUNT(*) AS total_registros
FROM supermercadoetl_dev.silver.precios_productos;
```

### Verificar histórico

```sql
SELECT 
  COUNT(DISTINCT producto_id) AS productos_unicos,
  COUNT(DISTINCT fecha_extraccion) AS fechas_distintas,
  COUNT(*) AS total_filas
FROM supermercadoetl_dev.silver.precios_productos;
```

### Ver últimos precios

```sql
SELECT 
  nombre,
  supermercado,
  precio_normalizado,
  fecha_extraccion
FROM supermercadoetl_dev.silver.precios_productos
WHERE fecha_extraccion = CURRENT_DATE()
ORDER BY precio_normalizado DESC
LIMIT 10;
```

---

## 🧪 Tests de Calidad de Datos

### Test 1: Unicidad de Clave Primaria

**Objetivo**: Verificar que no hay duplicados en la clave compuesta `(producto_id, fecha_extraccion)`

```sql
-- Expected: 0 duplicados
SELECT 
  producto_id,
  fecha_extraccion,
  COUNT(*) AS num_duplicados
FROM supermercadoetl_dev.silver.precios_productos
GROUP BY producto_id, fecha_extraccion
HAVING COUNT(*) > 1;
```

**Resultado esperado**: 0 filas (sin duplicados)

### Test 2: Integridad de Campos Críticos

**Objetivo**: Verificar que no hay valores nulos en campos obligatorios

```sql
-- Expected: 0 registros con nulos
SELECT 
  'producto_id' AS campo,
  COUNT(*) AS registros_nulos
FROM supermercadoetl_dev.silver.precios_productos
WHERE producto_id IS NULL

UNION ALL

SELECT 
  'supermercado' AS campo,
  COUNT(*) AS registros_nulos
FROM supermercadoetl_dev.silver.precios_productos
WHERE supermercado IS NULL

UNION ALL

SELECT 
  'nombre' AS campo,
  COUNT(*) AS registros_nulos
FROM supermercadoetl_dev.silver.precios_productos
WHERE nombre IS NULL

UNION ALL

SELECT 
  'categoria' AS campo,
  COUNT(*) AS registros_nulos
FROM supermercadoetl_dev.silver.precios_productos
WHERE categoria IS NULL

UNION ALL

SELECT 
  'precio_normalizado' AS campo,
  COUNT(*) AS registros_nulos
FROM supermercadoetl_dev.silver.precios_productos
WHERE precio_normalizado IS NULL

UNION ALL

SELECT 
  'fecha_extraccion' AS campo,
  COUNT(*) AS registros_nulos
FROM supermercadoetl_dev.silver.precios_productos
WHERE fecha_extraccion IS NULL;
```

**Resultado esperado**: Todas las filas con `registros_nulos = 0`

### Test 3: Validación de Rangos de Precios

**Objetivo**: Verificar que los precios están en rangos razonables

```sql
-- Expected: 0 precios fuera de rango
SELECT 
  COUNT(*) AS precios_invalidos,
  MIN(precio_normalizado) AS precio_minimo,
  MAX(precio_normalizado) AS precio_maximo
FROM supermercadoetl_dev.silver.precios_productos
WHERE precio_normalizado <= 0 
   OR precio_normalizado > 1000000;  -- Ajustar límite según negocio
```

**Resultado esperado**: `precios_invalidos = 0`

### Test 4: Consistencia de producto_id

**Objetivo**: Verificar que el mismo nombre de producto genera el mismo producto_id

```sql
-- Expected: 0 inconsistencias
SELECT 
  nombre,
  supermercado,
  COUNT(DISTINCT producto_id) AS num_ids_distintos
FROM supermercadoetl_dev.silver.precios_productos
GROUP BY nombre, supermercado
HAVING COUNT(DISTINCT producto_id) > 1;
```

**Resultado esperado**: 0 filas (cada combinación nombre+supermercado → 1 producto_id)

### Test 5: Freshness de Datos

**Objetivo**: Verificar que hay datos recientes (últimos 2 días)

```sql
-- Expected: > 0 registros recientes
SELECT 
  fecha_extraccion,
  COUNT(*) AS registros,
  DATEDIFF(CURRENT_DATE(), fecha_extraccion) AS dias_antiguedad
FROM supermercadoetl_dev.silver.precios_productos
WHERE fecha_extraccion >= DATE_SUB(CURRENT_DATE(), 2)
GROUP BY fecha_extraccion
ORDER BY fecha_extraccion DESC;
```

**Resultado esperado**: Al menos 1 fila con `dias_antiguedad <= 1`

### Test 6: Distribución por Supermercado

**Objetivo**: Verificar que todos los supermercados esperados tienen datos

```sql
-- Expected: Todos los supermercados activos con registros
SELECT 
  supermercado,
  COUNT(*) AS total_productos,
  COUNT(DISTINCT producto_id) AS productos_unicos,
  MAX(fecha_extraccion) AS ultima_carga
FROM supermercadoetl_dev.silver.precios_productos
GROUP BY supermercado
ORDER BY total_productos DESC;
```

**Resultado esperado**: Todas las cadenas de supermercados en operación presentes

### Test 7: Outliers de Precios por Categoría

**Objetivo**: Detectar precios anómalos por categoría

```sql
-- Identificar outliers usando percentiles
WITH stats AS (
  SELECT 
    categoria,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY precio_normalizado) AS p25,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY precio_normalizado) AS p75,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY precio_normalizado) - 
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY precio_normalizado) AS iqr
  FROM supermercadoetl_dev.silver.precios_productos
  WHERE fecha_extraccion = CURRENT_DATE()
  GROUP BY categoria
)
SELECT 
  p.categoria,
  p.nombre,
  p.supermercado,
  p.precio_normalizado,
  s.p25,
  s.p75,
  CASE 
    WHEN p.precio_normalizado < (s.p25 - 1.5 * s.iqr) THEN 'OUTLIER_BAJO'
    WHEN p.precio_normalizado > (s.p75 + 1.5 * s.iqr) THEN 'OUTLIER_ALTO'
  END AS tipo_outlier
FROM supermercadoetl_dev.silver.precios_productos p
JOIN stats s ON p.categoria = s.categoria
WHERE fecha_extraccion = CURRENT_DATE()
  AND (p.precio_normalizado < (s.p25 - 1.5 * s.iqr) 
       OR p.precio_normalizado > (s.p75 + 1.5 * s.iqr))
ORDER BY p.categoria, tipo_outlier;
```

**Resultado esperado**: Pocos outliers (revisar manualmente si son errores)

### Test 8: Validación de URLs

**Objetivo**: Verificar que las URLs tienen formato válido

```sql
-- Expected: 0 URLs inválidas
SELECT 
  COUNT(*) AS urls_invalidas
FROM supermercadoetl_dev.silver.precios_productos
WHERE url IS NOT NULL
  AND url NOT LIKE 'http%';
```

**Resultado esperado**: `urls_invalidas = 0`

### Test Suite Completo

**Query para ejecutar todos los tests en un solo run**:

```sql
-- TEST SUITE: Silver Data Quality
-- Ejecutar diariamente después del pipeline

-- Test 1: Duplicados en PK
SELECT 
  'TEST_1_DUPLICADOS' AS test_name,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*) AS issues_found
FROM (
  SELECT producto_id, fecha_extraccion, COUNT(*) AS cnt
  FROM supermercadoetl_dev.silver.precios_productos
  GROUP BY producto_id, fecha_extraccion
  HAVING COUNT(*) > 1
)

UNION ALL

-- Test 2: Campos nulos
SELECT 
  'TEST_2_NULLS' AS test_name,
  CASE WHEN SUM(null_count) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  SUM(null_count) AS issues_found
FROM (
  SELECT COUNT(*) AS null_count
  FROM supermercadoetl_dev.silver.precios_productos
  WHERE producto_id IS NULL OR supermercado IS NULL OR nombre IS NULL 
        OR categoria IS NULL OR precio_normalizado IS NULL OR fecha_extraccion IS NULL
)

UNION ALL

-- Test 3: Precios inválidos
SELECT 
  'TEST_3_PRECIOS_INVALIDOS' AS test_name,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*) AS issues_found
FROM supermercadoetl_dev.silver.precios_productos
WHERE precio_normalizado <= 0 OR precio_normalizado > 1000000

UNION ALL

-- Test 4: Freshness
SELECT 
  'TEST_4_FRESHNESS' AS test_name,
  CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*) AS records_recent
FROM supermercadoetl_dev.silver.precios_productos
WHERE fecha_extraccion >= DATE_SUB(CURRENT_DATE(), 1);
```

**Interpretación de resultados**:
* **PASS**: Test exitoso, datos con calidad esperada
* **FAIL**: Test fallido, requiere investigación

---

## 📊 Métricas de Calidad

### Tasa de rechazo

```sql
WITH bronze_count AS (
  SELECT COUNT(*) AS total
  FROM supermercadoetl_dev.bronze.precios_scraping
),
silver_count AS (
  SELECT COUNT(*) AS total
  FROM supermercadoetl_dev.silver.precios_productos
  WHERE fecha_extraccion = CURRENT_DATE()
)
SELECT 
  b.total AS registros_bronze,
  s.total AS registros_silver,
  b.total - s.total AS registros_rechazados,
  ROUND(100.0 * (b.total - s.total) / b.total, 2) AS tasa_rechazo_pct
FROM bronze_count b, silver_count s;
```

---

## 🛠️ Troubleshooting

### Error: DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE

**Causa**: Duplicados en Bronze con mismo `producto_id` y `fecha_extraccion`

**Solución**: Ya implementada con cláusula `QUALIFY` (ver sección Deduplicación)

### Error: Precio no se convierte a DECIMAL

**Causa**: Formato de precio inesperado en Bronze

**Solución**: 
1. Revisar datos en Bronze: `SELECT DISTINCT precio FROM bronze.precios_scraping`
2. Ajustar regex de limpieza si es necesario

### Registros faltantes en Silver

**Posibles causas**:
1. Precio nulo o 0 en Bronze (validación los rechaza)
2. Campos críticos nulos (supermercado, nombre, categoría)
3. Formato de precio inválido

**Diagnóstico**:
```sql
-- Ver registros rechazados
SELECT *
FROM supermercadoetl_dev.bronze.precios_scraping
WHERE 
  TRY_CAST(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(precio), '[^0-9,.-]', ''), ',', '.') AS DECIMAL(18,2)) IS NULL
  OR supermercado IS NULL
  OR nombre IS NULL
  OR categoria IS NULL
LIMIT 100;
```

---

## 📚 Referencias

* [Databricks Delta Lake MERGE](https://docs.databricks.com/delta/merge.html)
* [QUALIFY Clause](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-qry-select-qualify.html)
* [Delta Table Properties](https://docs.databricks.com/delta/table-properties.html)

---

## 📝 Notas

* **Idempotencia**: El pipeline es idempotente - se puede re-ejecutar sin duplicar datos
* **Histórico**: La tabla mantiene histórico completo de precios por fecha
* **Performance**: Delta Auto-Optimize está habilitado para mejor rendimiento
* **Deduplicación**: Implementada en el source del MERGE para evitar conflictos