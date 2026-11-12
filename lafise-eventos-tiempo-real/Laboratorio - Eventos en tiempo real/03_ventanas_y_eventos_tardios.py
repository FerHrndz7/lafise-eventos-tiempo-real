# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Ventanas de tiempo y eventos tardíos
# MAGIC
# MAGIC **Tiempo estimado:** 22 minutos
# MAGIC
# MAGIC ## Objetivos
# MAGIC - Agregar transacciones por **ventanas de tiempo de evento**: fijas (*tumbling*) y deslizantes (*sliding*).
# MAGIC - Usar un **watermark** para decidir cuándo una ventana está "cerrada".
# MAGIC - Comparar los modos de salida **append** (solo resultados finales) y **update** (resultados que se corrigen).
# MAGIC - **Medir** cuántos eventos tardíos se descartaron y elegir el watermark adecuado.
# MAGIC
# MAGIC ## Concepto — tiempo de evento, ventanas y watermark
# MAGIC Queremos saber **cuánto se vendió por minuto y país**. Pero "por minuto" ¿según qué reloj?
# MAGIC - Si agrupamos por **tiempo de llegada**, una compra hecha a las 10:00 que llega a las 10:03 cuenta en el
# MAGIC   minuto equivocado.
# MAGIC - Agrupamos por **tiempo de evento** (`event_time`): cada compra cae en el minuto en que **ocurrió**.
# MAGIC
# MAGIC El problema: ¿cuándo podemos dar por terminado el minuto 10:00, si todavía puede llegar una compra de ese
# MAGIC minuto? El **watermark** responde eso:
# MAGIC
# MAGIC     watermark = (el event_time más reciente que hemos visto) − (el retraso que toleramos)
# MAGIC
# MAGIC Con `withWatermark("event_time", "1 minute")`, si el evento más reciente es de las 10:05:00, el watermark es
# MAGIC 10:04:00: **toda ventana que termina antes de 10:04 se cierra**, se emite su resultado final y se libera su
# MAGIC estado. Un evento que llegue después para una ventana cerrada **se descarta**.
# MAGIC
# MAGIC | Watermark | Latencia del resultado | Completitud | Memoria (estado) |
# MAGIC |---|---|---|---|
# MAGIC | Corto (segundos) | Baja: resultados rápidos | Se pierden más tardíos | Poca |
# MAGIC | Largo (horas) | Alta: hay que esperar | Casi todo cuenta | Mucha |

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
spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %run ./utils/streaming_utils

# COMMAND ----------

esperar_filas(SILVER)
print(f"✅ {SILVER} tiene datos. (Si esperas mucho aquí, revisa que los notebooks 01 y 02 sigan corriendo.)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Ventanas fijas de 1 minuto, modo append
# MAGIC `window(event_time, "1 minute")` asigna cada evento a un intervalo fijo: 10:00–10:01, 10:01–10:02…
# MAGIC En **modo append** cada ventana se escribe **una sola vez**, cuando el watermark la cierra. Es lo que quieres
# MAGIC para un reporte o un sistema que no admite correcciones.
# MAGIC
# MAGIC > Monto en USD: en este paso convertimos con un mapa fijo para no adelantarnos; en el `04` lo hacemos bien,
# MAGIC > uniendo con la tabla `tipo_cambio`.

# COMMAND ----------

from pyspark.sql import functions as F

A_USD = F.expr("monto_local / element_at(map('NIO',36.60D,'CRC',505.00D,'HNL',24.70D,'USD',1.00D,'DOP',59.00D), moneda)")


def ventas_por_minuto(watermark):
    return (
        spark.readStream.table(SILVER)
        .withWatermark("event_time", watermark)
        .groupBy(F.window("event_time", "1 minute"), "pais")
        .agg(F.count("*").alias("transacciones"), F.round(F.sum(A_USD), 2).alias("monto_usd"))
        .select(F.col("window.start").alias("ventana_inicio"), F.col("window.end").alias("ventana_fin"),
                "pais", "transacciones", "monto_usd")
    )


FINAL = f"{FQ}.ventas_por_minuto_final"
detener_consultas("ventas_final")
q_final = (
    ventas_por_minuto("1 minute").writeStream
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINTS}/ventas_final")
    .trigger(processingTime="10 seconds")
    .queryName("ventas_final")
    .toTable(FINAL)
)
print("⏳ La primera ventana se emite cuando el watermark la cierra: ~1 min de ventana + 1 min de watermark.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — La misma agregación en modo update (con MERGE)
# MAGIC En **modo update**, cada micro-lote emite las ventanas que **cambiaron**, aunque no estén cerradas. Así un
# MAGIC tablero ve el minuto en curso **en vivo**, y cuando llega un evento tardío, la ventana se **corrige**.
# MAGIC
# MAGIC Una tabla Delta acepta streaming en modo append o complete, no update. Para "actualizar" filas usamos
# MAGIC `foreachBatch` + **MERGE**: insertar la ventana si es nueva, actualizarla si ya existe. La columna
# MAGIC `revisiones` cuenta cuántas veces cambió cada ventana.

# COMMAND ----------

VIVO = f"{FQ}.ventas_por_minuto_vivo"
spark.sql(f"""CREATE TABLE IF NOT EXISTS {VIVO} (ventana_inicio TIMESTAMP, ventana_fin TIMESTAMP, pais STRING,
              transacciones BIGINT, monto_usd DOUBLE, revisiones INT, actualizado_ts TIMESTAMP)""")


def merge_ventanas(lote, lote_id):
    lote.createOrReplaceTempView("lote_ventanas")
    lote.sparkSession.sql(f"""
        MERGE INTO {VIVO} t
        USING lote_ventanas s
        ON t.ventana_inicio = s.ventana_inicio AND t.pais = s.pais
        WHEN MATCHED THEN UPDATE SET t.transacciones = s.transacciones, t.monto_usd = s.monto_usd,
                                     t.revisiones = t.revisiones + 1, t.actualizado_ts = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (ventana_inicio, ventana_fin, pais, transacciones, monto_usd, revisiones, actualizado_ts)
                              VALUES (s.ventana_inicio, s.ventana_fin, s.pais, s.transacciones, s.monto_usd, 1, current_timestamp())
    """)


detener_consultas("ventas_vivo")
q_vivo = (
    ventas_por_minuto("1 minute").writeStream
    .outputMode("update")
    .foreachBatch(merge_ventanas)
    .option("checkpointLocation", f"{CHECKPOINTS}/ventas_vivo")
    .trigger(processingTime="10 seconds")
    .queryName("ventas_vivo")
    .start()
)
esperar_progreso(q_vivo)
esperar_filas(VIVO)

# COMMAND ----------

# MAGIC %md
# MAGIC Ejecuta esta celda varias veces. El minuto **en curso** cambia en cada lote (`revisiones` sube) y aparece
# MAGIC enseguida, mientras que en la tabla final todavía no existe.

# COMMAND ----------

display(spark.sql(f"""
SELECT v.ventana_inicio, v.pais, v.transacciones AS en_vivo, v.revisiones,
       f.transacciones AS final, f.transacciones IS NOT NULL AS ventana_cerrada
FROM {VIVO} v LEFT JOIN {FINAL} f USING (ventana_inicio, pais)
WHERE v.pais = 'NI'
ORDER BY v.ventana_inicio DESC LIMIT 8
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Las ventanas cerradas
# MAGIC Esperamos a que el watermark cierre las primeras ventanas y miramos la tabla final.

# COMMAND ----------

esperar_filas(FINAL, minimo=5, timeout_s=300)
display(spark.table(FINAL).orderBy(F.col("ventana_inicio").desc(), "pais").limit(10))
resumen_progreso(q_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — ¿Cuántos eventos tardíos perdimos?
# MAGIC Silver deduplica con un watermark de **10 minutos**, así que conserva incluso los muy tardíos. Si recontamos
# MAGIC en **batch** sobre Silver las mismas ventanas que ya cerró la consulta de 1 minuto, la diferencia son los
# MAGIC eventos que llegaron **después** de que su ventana se cerró.

# COMMAND ----------

display(spark.sql(f"""
WITH real AS (
  SELECT window(event_time, '1 minute').start AS ventana_inicio, pais, count(*) AS transacciones_reales
  FROM {SILVER} GROUP BY ALL
)
SELECT f.ventana_inicio, f.pais, f.transacciones AS contadas_por_el_stream, r.transacciones_reales,
       r.transacciones_reales - f.transacciones AS llegaron_tarde_y_se_descartaron
FROM {FINAL} f JOIN real r USING (ventana_inicio, pais)
ORDER BY f.ventana_inicio DESC, f.pais LIMIT 15
"""))

print(f"Filas descartadas por el watermark de 1 min (métrica de la consulta): {descartadas_por_watermark(q_final):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Cómo leer esto:** los eventos con 30 s–3 min de retraso a veces entran y a veces no (depende de qué tan
# MAGIC avanzada estaba la ventana); los de 4–8 min **siempre** se pierden con un watermark de 1 minuto.
# MAGIC
# MAGIC Elegir el watermark es una **decisión de negocio**, no técnica:
# MAGIC - ¿Un tablero operativo de ventas? 1–2 min está bien; la precisión de ±2% no cambia una decisión.
# MAGIC - ¿Una conciliación o un reporte regulatorio? Watermark largo, o recalcular en batch al cierre del día.
# MAGIC - ¿Ambos? Lo común: streaming con watermark corto para lo operativo + un proceso batch que recalcula y corrige.

# COMMAND ----------

# Expected output: ✅ El stream descartó eventos tardíos y la tabla en vivo se corrigió varias veces
try:
    perdidos = spark.sql(f"""
        WITH real AS (SELECT window(event_time,'1 minute').start AS ventana_inicio, pais, count(*) n
                      FROM {SILVER} GROUP BY ALL)
        SELECT coalesce(sum(r.n - f.transacciones), 0) FROM {FINAL} f JOIN real r USING (ventana_inicio, pais)
    """).first()[0]
    revisadas = spark.sql(f"SELECT count_if(revisiones > 1) FROM {VIVO}").first()[0]
    assert perdidos > 0, "Aún no hay ventanas cerradas con tardíos; espera un minuto y vuelve a ejecutar"
    assert revisadas > 0, "Ninguna ventana se revisó todavía; espera un par de lotes"
    print(f"✅ El stream descartó {perdidos} eventos tardíos y {revisadas} ventanas en vivo se corrigieron")
except AssertionError as e:
    print("⏳", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — Ventanas deslizantes: "los últimos 5 minutos, cada minuto"
# MAGIC `window(event_time, "5 minutes", "1 minute")` crea ventanas de 5 minutos que **avanzan** de minuto en minuto,
# MAGIC así que **un evento cae en 5 ventanas a la vez**. Sirven para métricas móviles: transacciones por canal en los
# MAGIC últimos 5 minutos, actualizadas cada minuto.
# MAGIC
# MAGIC Esta vez escribimos a la **memoria** del clúster (`format("memory")`), útil para explorar en un notebook, nunca
# MAGIC para producción.

# COMMAND ----------

detener_consultas("movil_canal")
q_movil = (
    spark.readStream.table(SILVER)
    .withWatermark("event_time", "1 minute")
    .groupBy(F.window("event_time", "5 minutes", "1 minute"), "canal")
    .agg(F.count("*").alias("transacciones"), F.round(F.avg(A_USD), 2).alias("ticket_promedio_usd"))
    .writeStream.format("memory").outputMode("complete")
    .queryName("movil_canal")
    .trigger(processingTime="10 seconds")
    .start()
)
esperar_progreso(q_movil)
display(spark.sql("""
SELECT window.start AS desde, window.end AS hasta, canal, transacciones, ticket_promedio_usd
FROM movil_canal ORDER BY window.end DESC, canal LIMIT 12
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6 — Detener las consultas de este notebook
# MAGIC Cada consulta ocupa recursos del clúster. Las de este notebook ya cumplieron su función.
# MAGIC (El generador y las consultas del `02` siguen corriendo en sus propios notebooks.)

# COMMAND ----------

detener_consultas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC - Agregamos por **tiempo de evento** con ventanas fijas (`window(t, "1 minute")`) y deslizantes
# MAGIC   (`window(t, "5 minutes", "1 minute")`).
# MAGIC - El **watermark** decide cuándo cerrar una ventana; lo que llega después se descarta y se puede **medir**.
# MAGIC - **Append** = cada resultado una vez, final. **Update + MERGE** = resultados en vivo que se corrigen.
# MAGIC - El watermark es un balance entre **latencia, completitud y memoria**, y lo decide el negocio.
# MAGIC
# MAGIC Sigue con `04_enriquecimiento`.
