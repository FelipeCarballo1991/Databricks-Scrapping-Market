-- ========================================
-- PIPELINE BRONZE → SILVER: PRECIOS PRODUCTOS
-- ========================================
--
-- Autor: Data Engineering Team
-- Fecha: 2026-04-10
-- Descripción: Transformación diaria de datos de precios de supermercados
--              desde la capa Bronze (raw) a la capa Silver (curated)
--
-- Estrategia de Carga:
--   - Tipo: MERGE incremental
--   - Frecuencia: Diaria
--   - Deduplicación: Múltiples scrapes del mismo día → 1 registro (último precio)
--   - Histórico: 1 fila por producto por fecha
--   - Idempotente: Re-ejecutar el mismo día actualiza sin duplicar
--
-- Clave Primaria Compuesta:
--   (producto_id, fecha_extraccion)
--   - producto_id: MD5(supermercado || nombre) - identificador único del producto
--   - fecha_extraccion: Fecha del scraping - dimensión temporal
--
-- Transformaciones Aplicadas:
--   ✓ Normalización de precios (STRING → DECIMAL)
--   ✓ Limpieza de caracteres especiales
--   ✓ Validación de campos obligatorios
--   ✓ Filtrado de registros inválidos
--   ✓ Eliminación de columnas técnicas de Bronze
--
-- Validaciones de Calidad:
--   ✓ Precio > 0 y no nulo
--   ✓ Supermercado, nombre y categoría no nulos
--   ✓ Formato numérico válido
--
-- ========================================

-- Paso 1: Crear schema Silver si no existe
CREATE SCHEMA IF NOT EXISTS supermercadoetl_prod.silver
COMMENT 'Capa Silver - Datos curados y validados';

-- Paso 2: Crear tabla Silver con esquema completo
CREATE TABLE IF NOT EXISTS supermercadoetl_prod.silver.precios_productos (
  producto_id STRING 
    COMMENT 'Hash MD5(supermercado||nombre) - identificador único del producto',
  
  supermercado STRING 
    COMMENT 'Nombre del supermercado (ej: Coto, Carrefour, Dia)',
  
  categoria STRING 
    COMMENT 'Categoría del producto (ej: Carnes, Lácteos, Bebidas)',
  
  nombre STRING 
    COMMENT 'Nombre comercial del producto',
  
  descripcion STRING 
    COMMENT 'Descripción detallada del producto',
  
  url STRING 
    COMMENT 'URL del producto en el sitio web del supermercado',
  
  precio_normalizado DECIMAL(18,2) 
    COMMENT 'Precio en formato numérico decimal (pesos argentinos)',
  
  fecha_extraccion DATE 
    COMMENT 'Fecha de extracción del precio (parte de PK compuesta)',
  
  fecha_actualizacion TIMESTAMP 
    COMMENT 'Timestamp de última actualización del registro'
)
USING DELTA
COMMENT 'Tabla Silver con precios de productos de supermercados - histórico diario'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Paso 3: MERGE incremental - Cargar datos desde Bronze
MERGE INTO supermercadoetl_prod.silver.precios_productos AS target
USING (
  SELECT
    -- Surrogate key SIN fecha (mismo producto = mismo ID siempre)
    MD5(CONCAT(supermercado, '|', nombre)) AS producto_id,
    
    -- Dimensiones de negocio
    supermercado,
    categoria,
    
    -- Atributos funcionales del producto
    nombre,
    descripcion,
    url,
    
    -- Métrica de negocio: precio normalizado a DECIMAL
    -- Transformación: elimina caracteres no numéricos, reemplaza coma por punto
    CAST(
      REGEXP_REPLACE(
        REGEXP_REPLACE(TRIM(precio), '[^0-9,.-]', ''),
        ',',
        '.'
      ) AS DECIMAL(18,2)
    ) AS precio_normalizado,
    
    -- Dimensión temporal
    fecha_extraccion,
    
    -- Metadata de auditoría
    CURRENT_TIMESTAMP() AS fecha_actualizacion
    
  FROM supermercadoetl_prod.bronze.precios_scraping
  
  -- ========================================
  -- FILTROS DE CALIDAD (Data Quality Rules)
  -- ========================================
  WHERE 
    -- 1. Precio válido y mayor a 0
    TRY_CAST(
      REGEXP_REPLACE(
        REGEXP_REPLACE(TRIM(precio), '[^0-9,.-]', ''),
        ',',
        '.'
      ) AS DECIMAL(18,2)
    ) IS NOT NULL
    AND TRY_CAST(
      REGEXP_REPLACE(
        REGEXP_REPLACE(TRIM(precio), '[^0-9,.-]', ''),
        ',',
        '.'
      ) AS DECIMAL(18,2)
    ) > 0
    
    -- 2. Campos críticos no nulos
    AND supermercado IS NOT NULL
    AND nombre IS NOT NULL
    AND categoria IS NOT NULL
  
  -- ========================================
  -- 3. Deduplicación: Si hay múltiples registros del mismo producto en el mismo día, tomar el más reciente
  -- ========================================
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY MD5(CONCAT(supermercado, '|', nombre)), fecha_extraccion
    ORDER BY ingestion_timestamp DESC
  ) = 1
    
) AS source

-- Condición de MERGE: misma clave compuesta (producto_id + fecha_extraccion)
ON target.producto_id = source.producto_id 
   AND target.fecha_extraccion = source.fecha_extraccion

-- ========================================
-- WHEN MATCHED: Actualizar registro existente
-- ========================================
-- Escenario: Re-ejecución del pipeline el mismo día
-- Efecto: Actualiza con el último precio scrapeado (deduplicación)
WHEN MATCHED THEN UPDATE SET
  target.precio_normalizado = source.precio_normalizado,
  target.descripcion = source.descripcion,
  target.url = source.url,
  target.categoria = source.categoria,
  target.fecha_actualizacion = source.fecha_actualizacion

-- ========================================
-- WHEN NOT MATCHED: Insertar nuevo registro
-- ========================================
-- Escenarios:
--   1. Nuevo producto detectado
--   2. Nuevo día de scraping (histórico)
WHEN NOT MATCHED THEN INSERT (
  producto_id,
  supermercado,
  categoria,
  nombre,
  descripcion,
  url,
  precio_normalizado,
  fecha_extraccion,
  fecha_actualizacion
) VALUES (
  source.producto_id,
  source.supermercado,
  source.categoria,
  source.nombre,
  source.descripcion,
  source.url,
  source.precio_normalizado,
  source.fecha_extraccion,
  source.fecha_actualizacion
);

-- ========================================
-- RESULTADO ESPERADO POR DÍA:
-- ========================================
-- Día 1 (primera ejecución):     12 productos → 12 INSERT
-- Día 2 (nueva fecha):           12 productos → 12 INSERT    (total: 24 filas)
-- Día 2 (re-ejecución):          12 productos → 12 UPDATE   (total: 24 filas)
-- Día 3 (nuevo producto):        13 productos → 13 INSERT    (total: 37 filas)
-- Día N (producto discontinuado): histórico se mantiene intacto
-- ========================================
