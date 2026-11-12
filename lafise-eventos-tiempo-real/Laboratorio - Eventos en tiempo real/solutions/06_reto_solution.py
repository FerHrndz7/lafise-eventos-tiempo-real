# Databricks notebook source
# MAGIC %md
# MAGIC # SOLUCIÓN · 06 · Reto (tarea) — El "viaje imposible"
# MAGIC
# MAGIC **Tiempo estimado:** 20–30 minutos, después de la sesión
# MAGIC
# MAGIC ## El problema
# MAGIC Una tarjeta física no puede estar en **Managua** y en **Santo Domingo** con minutos de diferencia. Si ocurre,
# MAGIC probablemente la tarjeta fue **clonada** o sus datos se usan en línea desde otro país. Monitoreo de Fraude
# MAGIC pide una alerta en tiempo real:
# MAGIC
# MAGIC > **Alerta `VIAJE_IMPOSIBLE`:** la misma tarjeta tiene transacciones en **2 o más países distintos** dentro de
# MAGIC > una ventana de **5 minutos** (evaluada cada minuto).
# MAGIC
# MAGIC El generador tiene 5 tarjetas "viajeras" (`TJ-00010` a `TJ-00014`) que compran en países al azar. Tu alerta
# MAGIC debe encontrarlas **a ellas y a ninguna otra**.
# MAGIC
# MAGIC ## Lo que vas a completar
# MAGIC Tres `<FILL_IN>` en una sola consulta: el **watermark**, la **ventana deslizante** y la **agregación** que
# MAGIC cuenta países. Todo lo demás está listo. Necesitas que sigan corriendo el **generador (01)** y el **02**.
# MAGIC
# MAGIC Pistas:
# MAGIC - Repasa el Paso 3 del notebook `04` (alerta de velocidad): es casi la misma forma.
# MAGIC - `F.window(columna, duración, deslizamiento)` crea ventanas deslizantes.
# MAGIC - En streaming no existe `countDistinct`; usa `F.collect_set("pais")` (la lista sin repetidos) y su tamaño con
# MAGIC   `F.size(...)`.

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "", "Catálogo asignado (REQUERIDO)")
dbutils.widgets.text("schema", "eventos_rt", "Schema base")

user_email = spark.sql("SELECT current_user()").collect()[0][0]
user_prefix = re.sub(r"[^a-z0-9]", "_", user_email.split("@")[0].lower())
CATALOG = dbutils.widgets.get("catalog").strip()
assert CATALOG, "Escribe en el widget 'catalog' el mismo catálogo del 00_setup y vuelve a ejecutar."
SCHEMA = f"{(dbutils.widgets.get('schema').strip() or 'eventos_rt')}_{user_prefix}"
FQ = f"`{CATALOG}`.{SCHEMA}"
CHECKPOINTS = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"
SILVER = f"{FQ}.silver_transacciones"
ALERTAS = f"{FQ}.alertas"
spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %run ../utils/streaming_utils

# COMMAND ----------

esperar_filas(SILVER)
_ = spark.sql(f"""CREATE TABLE IF NOT EXISTS {ALERTAS} (
  tipo_alerta STRING, tarjeta_id STRING, cliente_id STRING, detalle STRING, monto_usd DOUBLE,
  desde TIMESTAMP, hasta TIMESTAMP, generada_ts TIMESTAMP)""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — La consulta de la alerta
# MAGIC **TODO:** completa los tres `<FILL_IN>`. *(En esta solución ya están completos.)*
# MAGIC 1. Un watermark de **1 minuto** sobre `event_time`.
# MAGIC 2. Ventanas de **5 minutos** que avanzan cada **1 minuto**.
# MAGIC 3. El conjunto de países distintos de cada tarjeta en la ventana, con el alias `paises`.

# COMMAND ----------

from pyspark.sql import functions as F

# SOLUCIÓN
# 1. withWatermark("event_time", "1 minute"): cuánto esperar a los tardíos antes de cerrar una ventana.
# 2. F.window("event_time", "5 minutes", "1 minute"): ventanas de 5 min que avanzan cada minuto.
# 3. F.collect_set("pais"): países sin repetidos; en streaming no existe countDistinct, así que contamos con size().
viaje_imposible = (
    spark.readStream.table(SILVER)
    .withWatermark("event_time", "1 minute")
    .groupBy(F.window("event_time", "5 minutes", "1 minute"), "tarjeta_id", "cliente_id")
    .agg(F.collect_set("pais").alias("paises"),
         F.count("*").alias("n"))
    .filter(F.size("paises") >= 2)
    .select(
        F.lit("VIAJE_IMPOSIBLE").alias("tipo_alerta"), "tarjeta_id", "cliente_id",
        F.format_string("%d transacciones en %s en 5 minutos", "n",
                        F.array_join(F.array_sort("paises"), ", ")).alias("detalle"),
        F.lit(None).cast("double").alias("monto_usd"),
        F.col("window.start").alias("desde"), F.col("window.end").alias("hasta"),
        F.current_timestamp().alias("generada_ts"),
    )
)

detener_consultas("viaje_imposible")
q_viaje = (
    viaje_imposible.writeStream
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINTS}/viaje_imposible")
    .trigger(processingTime="10 seconds")
    .queryName("viaje_imposible")
    .toTable(ALERTAS)
)
esperar_progreso(q_viaje)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Ver las alertas
# MAGIC El stream empieza leyendo **todo el historial** de Silver, así que las ventanas antiguas se cierran en los
# MAGIC primeros lotes. Si no ves nada, espera un minuto y vuelve a ejecutar.

# COMMAND ----------

inicio = time.time()
while (spark.sql(f"SELECT count(*) FROM {ALERTAS} WHERE tipo_alerta = 'VIAJE_IMPOSIBLE'").first()[0] == 0
       and time.time() - inicio < 240):
    time.sleep(10)

display(spark.sql(f"""
SELECT tarjeta_id, count(*) AS ventanas_con_alerta, max(detalle) AS ejemplo, max(hasta) AS ultima
FROM {ALERTAS} WHERE tipo_alerta = 'VIAJE_IMPOSIBLE' GROUP BY 1 ORDER BY 1
"""))

# COMMAND ----------

# Expected output: ✅ VIAJE_IMPOSIBLE encontró las tarjetas viajeras y ninguna otra
try:
    tarjetas = {r[0] for r in spark.sql(
        f"SELECT DISTINCT tarjeta_id FROM {ALERTAS} WHERE tipo_alerta = 'VIAJE_IMPOSIBLE'").collect()}
    viajeras = {f"TJ-{n:05d}" for n in range(10, 15)}
    assert tarjetas, "No hay alertas todavía. ¿Siguen corriendo el 01 y el 02? Espera un minuto y reintenta."
    assert tarjetas <= viajeras, f"Falsos positivos: {sorted(tarjetas - viajeras)[:5]} (revisa el filtro de países)"
    print(f"✅ VIAJE_IMPOSIBLE encontró {len(tarjetas)} de las 5 tarjetas viajeras y ninguna otra")
except AssertionError as e:
    print("❌ Casi:", e)

# COMMAND ----------

detener_consultas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Para pensar
# MAGIC - Una compra **en línea** (`ECOMMERCE`) desde otro país no implica que la tarjeta viajó. ¿Cómo refinarías la
# MAGIC   regla? (Pista: filtra por `canal` antes de agrupar, o exige que al menos una transacción sea `POS` o `ATM`.)
# MAGIC - Con un watermark de 1 minuto, una transacción que llega 4 minutos tarde **no** entra en la alerta. ¿Qué
# MAGIC   preferiría Fraude: alertas más rápidas o más completas?
# MAGIC - ¿Cómo evitarías mandar la misma alerta en 5 ventanas seguidas? (Pista: una tabla de alertas vigentes y un
# MAGIC   `MERGE` en `foreachBatch`, como en el notebook `03`.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enfoques alternativos
# MAGIC - **Join stream-stream** de la tabla consigo misma: `a.tarjeta_id = b.tarjeta_id AND a.pais <> b.pais AND
# MAGIC   b.event_time BETWEEN a.event_time AND a.event_time + INTERVAL 5 MINUTES`. Da el **par exacto** de
# MAGIC   transacciones (útil para el analista), pero guarda más estado.
# MAGIC - **`transformWithState`** (DBR 16.2+): guardar por tarjeta el último país y hora, y alertar en el momento en
# MAGIC   que llega la transacción del otro país, sin esperar a que cierre una ventana. Es la opción de menor latencia.
# MAGIC - **Regla refinada:** `.filter("canal IN ('POS','ATM')")` antes de agrupar, para no confundir compras en línea
# MAGIC   con presencia física.
