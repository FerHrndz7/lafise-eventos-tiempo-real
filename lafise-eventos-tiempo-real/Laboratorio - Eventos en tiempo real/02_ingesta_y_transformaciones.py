# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Ingesta y transformaciones: Bronze y Silver en streaming
# MAGIC
# MAGIC **Tiempo estimado:** 15 minutos
# MAGIC
# MAGIC ## Objetivos
# MAGIC - Leer los archivos que llegan al volumen con **Auto Loader** y guardarlos tal cual en **Bronze**.
# MAGIC - Transformar Bronze → **Silver**: tipos correctos, validaciones y **deduplicación** por `event_id`.
# MAGIC - Separar los registros inválidos en una tabla de **cuarentena** con `foreachBatch`.
# MAGIC - Leer las **métricas de progreso** de cada consulta.
# MAGIC
# MAGIC ## Concepto — la arquitectura medallion en streaming
# MAGIC
# MAGIC | Capa | Qué guarda | Por qué |
# MAGIC |---|---|---|
# MAGIC | **Bronze** | El evento **tal como llegó** (todo como texto) + cuándo y de qué archivo | Auditoría y reproceso: nunca perdemos el original |
# MAGIC | **Silver** | Eventos **válidos, tipados y sin duplicados** | La base confiable para agregar y enriquecer |
# MAGIC | Cuarentena | Los inválidos, con el **motivo** del rechazo | Nada se pierde en silencio; alguien los revisa |
# MAGIC
# MAGIC Cada capa es una **consulta de streaming** que lee de la anterior **de forma incremental**: solo procesa lo
# MAGIC nuevo desde su último checkpoint.
# MAGIC
# MAGIC > **Deja este notebook abierto al terminar.** Sus dos consultas siguen corriendo y alimentan los
# MAGIC > notebooks `03` y `04`.

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "", "Catálogo asignado (REQUERIDO)")
dbutils.widgets.text("schema", "eventos_rt", "Schema base")
dbutils.widgets.text("mantener_min", "0", "Solo para jobs: minutos activo (0 = sesión en vivo)")

user_email = spark.sql("SELECT current_user()").collect()[0][0]
user_prefix = re.sub(r"[^a-z0-9]", "_", user_email.split("@")[0].lower())
CATALOG = dbutils.widgets.get("catalog").strip()
assert CATALOG, "Escribe en el widget 'catalog' el mismo catálogo del 00_setup y vuelve a ejecutar."
SCHEMA = f"{(dbutils.widgets.get('schema').strip() or 'eventos_rt')}_{user_prefix}"
FQ = f"`{CATALOG}`.{SCHEMA}"
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing/transacciones"
CHECKPOINTS = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"
BRONZE, SILVER, CUARENTENA = (f"{FQ}.bronze_transacciones", f"{FQ}.silver_transacciones",
                              f"{FQ}.cuarentena_transacciones")

# Pocos datos por micro-lote: 8 particiones de shuffle en lugar de 200 hacen cada lote mucho más rápido.
spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %run ./utils/streaming_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Bronze con Auto Loader
# MAGIC **Auto Loader** (`format("cloudFiles")`) detecta los archivos nuevos de una carpeta y procesa cada uno
# MAGIC **exactamente una vez**, aunque lleguen miles por hora. Recuerda cuáles ya leyó en su checkpoint.
# MAGIC
# MAGIC Decisiones de esta capa:
# MAGIC - **Esquema explícito y todo como texto:** Bronze no debe fallar porque un monto venga como `"N/A"`. Las
# MAGIC   conversiones se hacen en Silver.
# MAGIC - `rescuedDataColumn`: si un evento trae un campo inesperado, no se pierde; queda en `_rescued_data`.
# MAGIC - `pathGlobFilter = "*.json"`: solo archivos de datos.
# MAGIC - Agregamos `llegada_ts` (cuándo **aterrizó** el archivo en el volumen, `_metadata.file_modification_time`),
# MAGIC   `ingesta_ts` (cuándo lo procesó Bronze) y el archivo de origen (`_metadata.file_name`).

# COMMAND ----------

from pyspark.sql import functions as F

ESQUEMA_EVENTO = ("event_id STRING, event_time STRING, tarjeta_id STRING, cliente_id STRING, comercio_id STRING, "
                  "canal STRING, pais STRING, moneda STRING, monto STRING")

bronze_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("pathGlobFilter", "*.json")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(ESQUEMA_EVENTO)
    .load(LANDING)
    .withColumn("llegada_ts", F.col("_metadata.file_modification_time"))
    .withColumn("ingesta_ts", F.current_timestamp())
    .withColumn("archivo_origen", F.col("_metadata.file_name"))
)

detener_consultas("bronze")
q_bronze = (
    bronze_stream.writeStream
    .option("checkpointLocation", f"{CHECKPOINTS}/bronze")
    .trigger(processingTime="5 seconds")
    .queryName("bronze")
    .toTable(BRONZE)
)
esperar_progreso(q_bronze)
print(f"✅ Bronze corriendo → {BRONZE}")

# COMMAND ----------

display(spark.table(BRONZE).orderBy(F.col("ingesta_ts").desc()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — ¿Qué problemas trae Bronze?
# MAGIC Consultas **batch** normales sobre la tabla que el stream está llenando. En Databricks una tabla Delta
# MAGIC sirve a la vez como destino de streaming y como tabla para SQL.

# COMMAND ----------

display(spark.sql(f"""
SELECT
  count(*)                                                          AS eventos_bronze,
  count(*) - count(DISTINCT event_id)                               AS duplicados,
  count_if(try_cast(monto AS DECIMAL(18,2)) IS NULL)                AS monto_no_numerico,
  count_if(try_cast(monto AS DECIMAL(18,2)) <= 0)                   AS monto_no_positivo,
  count_if(moneda NOT IN ('NIO','CRC','HNL','USD','DOP'))           AS moneda_desconocida
FROM {BRONZE}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Silver: tipar, validar y deduplicar
# MAGIC Silver lee Bronze **como stream** (`readStream.table`) y aplica:
# MAGIC
# MAGIC 1. **Tipos:** `event_time` a timestamp y `monto` a decimal con `try_cast` (si no se puede, queda nulo en vez de
# MAGIC    fallar).
# MAGIC 2. **Validación:** una columna `motivo_rechazo` que dice por qué un evento no sirve (o nula si está bien).
# MAGIC 3. **Deduplicación:** `dropDuplicatesWithinWatermark(["event_id"])`. Para deduplicar, Spark recuerda los
# MAGIC    `event_id` que ya vio; el **watermark** de 10 minutos le dice **cuánto tiempo recordar**. Sin watermark,
# MAGIC    ese estado crecería para siempre.
# MAGIC
# MAGIC > ¿Por qué 10 minutos? Es más que el retraso máximo de nuestros eventos (8 min). En el `03` veremos qué pasa
# MAGIC > con un watermark más corto.

# COMMAND ----------

MONEDAS_VALIDAS = ["NIO", "CRC", "HNL", "USD", "DOP"]

silver_stream = (
    spark.readStream.table(BRONZE)
    .withColumn("event_time", F.to_timestamp("event_time"))
    .withColumn("monto_local", F.expr("try_cast(monto AS DECIMAL(18,2))"))
    .withColumn("motivo_rechazo",
        F.when(F.col("event_time").isNull(), "FECHA_INVALIDA")
         .when(F.col("monto_local").isNull(), "MONTO_NO_NUMERICO")
         .when(F.col("monto_local") <= 0, "MONTO_NO_POSITIVO")
         .when(~F.col("moneda").isin(MONEDAS_VALIDAS), "MONEDA_DESCONOCIDA"))
    .withColumn("retraso_llegada_s",
                F.col("llegada_ts").cast("long") - F.col("event_time").cast("long"))
    .withWatermark("event_time", "10 minutes")
    .dropDuplicatesWithinWatermark(["event_id"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Un stream, dos destinos: `foreachBatch`
# MAGIC Queremos que los válidos vayan a Silver y los inválidos a cuarentena. `foreachBatch` nos da cada
# MAGIC **micro-lote como un DataFrame normal**, y con él podemos escribir a varias tablas, hacer `MERGE`, llamar
# MAGIC APIs, etc.
# MAGIC
# MAGIC **Exactamente una vez:** si un micro-lote falla a la mitad y se reintenta, no queremos escribirlo dos veces.
# MAGIC Las opciones `txnAppId` + `txnVersion` (el id del lote) hacen que Delta **ignore** una escritura que ya hizo.

# COMMAND ----------

COLUMNAS_SILVER = ["event_id", "event_time", "tarjeta_id", "cliente_id", "comercio_id", "canal", "pais",
                   "moneda", "monto_local", "llegada_ts", "ingesta_ts", "retraso_llegada_s"]
COLUMNAS_CUARENTENA = ["event_id", "event_time", "tarjeta_id", "moneda", "monto", "motivo_rechazo",
                       "ingesta_ts", "archivo_origen"]


def escribir_silver_y_cuarentena(lote, lote_id):
    validos = lote.filter("motivo_rechazo IS NULL").select(*COLUMNAS_SILVER)
    invalidos = lote.filter("motivo_rechazo IS NOT NULL").select(*COLUMNAS_CUARENTENA)
    (validos.write.format("delta").mode("append")
        .option("txnAppId", "silver_validos").option("txnVersion", lote_id).saveAsTable(SILVER))
    (invalidos.write.format("delta").mode("append")
        .option("txnAppId", "silver_cuarentena").option("txnVersion", lote_id).saveAsTable(CUARENTENA))


detener_consultas("silver")
q_silver = (
    silver_stream.writeStream
    .foreachBatch(escribir_silver_y_cuarentena)
    .option("checkpointLocation", f"{CHECKPOINTS}/silver")
    .trigger(processingTime="5 seconds")
    .queryName("silver")
    .start()
)
esperar_progreso(q_silver)
esperar_filas(SILVER)
print(f"✅ Silver corriendo → {SILVER} (+ {CUARENTENA})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — Comprobar el resultado
# MAGIC Silver no debe tener duplicados ni inválidos; la cuarentena explica cada rechazo. Ejecuta las celdas un par de
# MAGIC veces: los números crecen porque las consultas siguen corriendo.

# COMMAND ----------

display(spark.sql(f"""
SELECT 'bronze' AS capa, count(*) AS eventos, count(DISTINCT event_id) AS event_ids_unicos FROM {BRONZE}
UNION ALL SELECT 'silver', count(*), count(DISTINCT event_id) FROM {SILVER}
UNION ALL SELECT 'cuarentena', count(*), count(DISTINCT event_id) FROM {CUARENTENA}
"""))
display(spark.sql(f"SELECT motivo_rechazo, count(*) AS eventos FROM {CUARENTENA} GROUP BY 1 ORDER BY 2 DESC"))

# COMMAND ----------

# Expected output: ✅ Silver sin duplicados ni registros inválidos
try:
    s = spark.sql(f"SELECT count(*) n, count(DISTINCT event_id) u, count_if(monto_local <= 0) neg FROM {SILVER}").first()
    assert s.n == s.u, f"Silver tiene {s.n - s.u} duplicados"
    assert s.neg == 0, "Silver tiene montos no positivos"
    print(f"✅ Silver sin duplicados ni registros inválidos ({s.n:,} eventos)")
except AssertionError as e:
    print("❌ Revisa el paso 3:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6 — ¿Con cuánto retraso llegan los eventos?
# MAGIC `retraso_llegada_s` = tiempo de llegada al volumen − tiempo de evento. La mayoría llega en segundos; una
# MAGIC minoría, minutos después. (Usamos la hora del archivo y no `ingesta_ts`: si Bronze arrancó tarde, todo lo
# MAGIC pendiente parecería tardío.) Esa cola es justo lo que tendremos que manejar al agregar por ventanas en el `03`.

# COMMAND ----------

display(spark.sql(f"""
SELECT CASE WHEN retraso_llegada_s < 15  THEN '1. menos de 15 s'
            WHEN retraso_llegada_s < 60  THEN '2. 15 s a 1 min'
            WHEN retraso_llegada_s < 240 THEN '3. 1 a 4 min (tardío)'
            ELSE '4. más de 4 min (muy tardío)' END AS retraso,
       count(*) AS eventos,
       round(100 * count(*) / sum(count(*)) OVER (), 1) AS porcentaje
FROM {SILVER} GROUP BY 1 ORDER BY 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 7 — Métricas de progreso
# MAGIC Fíjate en dos columnas:
# MAGIC - `filas_por_seg_llegada` vs. `filas_por_seg_procesadas`: si la segunda es **menor** que la primera de forma
# MAGIC   sostenida, el stream se está **atrasando** (necesita más cómputo o un trigger más espaciado).
# MAGIC - `watermark` de Silver: avanza con cada lote, siempre ~10 minutos detrás del evento más reciente. Los
# MAGIC   `event_id` más viejos que el watermark se **olvidan**: ya no pueden repetirse. Así el estado no crece sin fin.
# MAGIC
# MAGIC > En DBR 17.3, el operador de deduplicación (`dedupeWithinWatermark`) reporta `filas_en_estado = 0` aunque sí
# MAGIC > guarda estado; las ventanas del notebook `03` sí lo muestran.

# COMMAND ----------

print("Bronze:"); resumen_progreso(q_bronze)
print("Silver:"); resumen_progreso(q_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC - **Auto Loader** convirtió una carpeta con archivos nuevos en un stream, con checkpoint y sin duplicar archivos.
# MAGIC - **Silver** tipó, validó y deduplicó con un **watermark** que limita cuánto estado se guarda.
# MAGIC - **foreachBatch** envió cada micro-lote a dos tablas, de forma idempotente (`txnAppId` + `txnVersion`).
# MAGIC
# MAGIC **Deja este notebook abierto** y sigue con `03_ventanas_y_eventos_tardios`.

# COMMAND ----------

mantener_activo(int(dbutils.widgets.get("mantener_min") or 0), [q_bronze, q_silver])
