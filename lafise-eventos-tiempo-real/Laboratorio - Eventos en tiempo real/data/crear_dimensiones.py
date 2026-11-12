# Databricks notebook source
# MAGIC %md
# MAGIC # Datos de referencia (dimensiones) — se llama desde `00_setup`
# MAGIC
# MAGIC Crea las tres tablas **estáticas** con las que enriqueceremos el stream en el notebook `04`:
# MAGIC
# MAGIC | Tabla | Filas | Contenido |
# MAGIC |---|---|---|
# MAGIC | `clientes` | 2,000 | segmento, país de residencia, nivel de riesgo y límite diario en USD |
# MAGIC | `comercios` | 500 | categoría (MCC), país y ciudad |
# MAGIC | `tipo_cambio` | 5 | moneda local → USD (tasas **didácticas**, no oficiales) |
# MAGIC
# MAGIC Todo se genera con expresiones de Spark (sin librerías externas) y es **idempotente**: se puede re-ejecutar.
# MAGIC Las reglas de país y moneda son las **mismas** que usa el generador del notebook `01`.

# COMMAND ----------

import re
dbutils.widgets.text("catalog", "", "Catálogo")
dbutils.widgets.text("schema", "eventos_rt", "Schema base")
user_prefix = re.sub(r"[^a-z0-9]", "_", spark.sql("SELECT current_user()").collect()[0][0].split("@")[0].lower())
CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = f"{(dbutils.widgets.get('schema').strip() or 'eventos_rt')}_{user_prefix}"
assert CATALOG, "Este notebook se llama desde 00_setup con el widget catalog."
FQ = f"`{CATALOG}`.{SCHEMA}"

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ}.clientes
COMMENT 'Clientes con tarjeta (sintéticos). Una tarjeta por cliente: tarjeta_id = TJ-<número de cliente>.'
AS
WITH base AS (SELECT id AS n FROM range(2000))
SELECT
  concat('CL-', lpad(CAST(n AS STRING), 5, '0'))                                   AS cliente_id,
  concat('TJ-', lpad(CAST(n AS STRING), 5, '0'))                                   AS tarjeta_id,
  element_at(array('NI','CR','HN','PA','DO'), CAST(pmod(n, 5) + 1 AS INT))         AS pais_residencia,
  CASE WHEN pmod(abs(hash(n, 'seg')), 100) < 60 THEN 'MASIVO'
       WHEN pmod(abs(hash(n, 'seg')), 100) < 85 THEN 'PREFERENTE'
       WHEN pmod(abs(hash(n, 'seg')), 100) < 97 THEN 'PYME'
       ELSE 'CORPORATIVO' END                                                      AS segmento,
  CASE WHEN pmod(abs(hash(n, 'rk')), 100) < 80 THEN 'BAJO'
       WHEN pmod(abs(hash(n, 'rk')), 100) < 95 THEN 'MEDIO'
       ELSE 'ALTO' END                                                             AS nivel_riesgo,
  date_sub(DATE'2026-10-31', CAST(pmod(abs(hash(n, 'alta')), 3650) AS INT))        AS fecha_alta
FROM base
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ}.limites_segmento
COMMENT 'Monto máximo por transacción (USD) por segmento, para la alerta de monto inusual.'
AS SELECT * FROM VALUES
  ('MASIVO', 500.0), ('PREFERENTE', 2000.0), ('PYME', 5000.0), ('CORPORATIVO', 20000.0)
  AS t(segmento, limite_usd_transaccion)
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ}.comercios
COMMENT 'Comercios afiliados (sintéticos) con su categoría MCC.'
AS
WITH base AS (SELECT id AS n FROM range(500))
SELECT
  concat('CM-', lpad(CAST(n AS STRING), 4, '0'))                                   AS comercio_id,
  element_at(array('Supermercado','Restaurante','Gasolinera','Farmacia','Tienda en línea',
                   'Viajes y hoteles','Electrónica','Servicios públicos'),
             CAST(pmod(abs(hash(n, 'mcc')), 8) + 1 AS INT))                         AS categoria,
  element_at(array('NI','CR','HN','PA','DO'), CAST(pmod(n, 5) + 1 AS INT))         AS pais_comercio,
  element_at(array('Managua','San José','Tegucigalpa','Ciudad de Panamá','Santo Domingo'),
             CAST(pmod(n, 5) + 1 AS INT))                                          AS ciudad
FROM base
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ}.tipo_cambio
COMMENT 'Unidades de moneda local por 1 USD. Tasas DIDÁCTICAS, no oficiales.'
AS SELECT * FROM VALUES
  ('NIO', 36.60), ('CRC', 505.00), ('HNL', 24.70), ('USD', 1.00), ('DOP', 59.00)
  AS t(moneda, unidades_por_usd)
""")

for t in ("clientes", "limites_segmento", "comercios", "tipo_cambio"):
    print(f"✅ {CATALOG}.{SCHEMA}.{t}: {spark.table(f'{FQ}.{t}').count():,} filas")
