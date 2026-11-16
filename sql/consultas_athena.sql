-- Consultas de validación y análisis en Amazon Athena
-- Base de datos: prueba_etl_db | Tabla: boston_housing

-- 1. Total de registros cargados
SELECT COUNT(*) AS total_casas
FROM prueba_etl_db.boston_housing;

-- 2. Casas y precio promedio por categoría de precio
SELECT precio_categoria,
       COUNT(*) AS casas,
       ROUND(AVG(medv), 2) AS precio_promedio
FROM prueba_etl_db.boston_housing
GROUP BY precio_categoria
ORDER BY precio_promedio;

-- 3. Precio promedio según cercanía al río Charles (chas = 1)
SELECT CASE WHEN chas = 1 THEN 'Junto al río' ELSE 'Lejos del río' END AS ubicacion,
       COUNT(*) AS casas,
       ROUND(AVG(medv), 2) AS precio_promedio
FROM prueba_etl_db.boston_housing
GROUP BY 1;

-- 4. Archivo de origen de los datos (verifica la última carga)
SELECT "$path" AS archivo, COUNT(*) AS filas
FROM prueba_etl_db.boston_housing
GROUP BY 1;
