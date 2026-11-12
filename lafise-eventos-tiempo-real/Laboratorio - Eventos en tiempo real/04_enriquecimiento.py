# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Enriquecimiento y alertas en tiempo real
# MAGIC
# MAGIC **Tiempo estimado:** 20 minutos
# MAGIC
# MAGIC ## Objetivos
# MAGIC - **Enriquecer** cada transacción, mientras llega, con datos del cliente, del comercio y del tipo de cambio.
# MAGIC - Generar **alertas** de dos tipos: por **regla** sobre un solo evento y por **comportamiento** en una ventana.
# MAGIC - Comprobar que un cambio en una tabla de referencia se refleja **en el siguiente micro-lote**, sin reiniciar.
# MAGIC
# MAGIC ## Concepto — joins stream-static
# MAGIC Una transacción sola dice poco: `CL-00042` compró `15,000` en `CRC`. Unida a sus tablas de referencia dice
# MAGIC mucho más: *cliente del segmento MASIVO, residente en Nicaragua, compró USD 29.70 en una farmacia de San José*.
# MAGIC
# MAGIC En un **join stream-static**, el lado stream (las transacciones) avanza y el lado estático (una tabla Delta) se
# MAGIC **vuelve a leer en su versión más reciente en cada micro-lote**. Si alguien actualiza el tipo de cambio, los
# MAGIC eventos siguientes ya usan el valor nuevo. No hace falta guardar estado: cada evento se une y sigue.
# MAGIC
# MAGIC > Usamos **left join**: si falta un cliente o un comercio en la referencia, la transacción **no se pierde**;
# MAGIC > queda con columnas nulas y se puede revisar. Con un inner join desaparecería en silencio.

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
ENRIQUECIDAS = f"{FQ}.transacciones_enriquecidas"
ALERTAS = f"{FQ}.alertas"
spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %run ./utils/streaming_utils

# COMMAND ----------

esperar_filas(SILVER)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — El stream enriquecido
# MAGIC Cuatro joins con tablas pequeñas. `F.broadcast(...)` le dice a Spark que copie cada tabla de referencia a
# MAGIC todos los nodos: así el join no necesita mover el stream por la red.
# MAGIC
# MAGIC Columnas que **calculamos** con lo que trae la referencia:
# MAGIC - `monto_usd` = monto en moneda local ÷ unidades por USD.
# MAGIC - `es_internacional` = la compra ocurrió en un país distinto al de residencia del cliente.
# MAGIC - `excede_limite` = el monto supera el límite por transacción de su segmento.

# COMMAND ----------

from pyspark.sql import functions as F

clientes = spark.table(f"{FQ}.clientes").select("cliente_id", "segmento", "pais_residencia", "nivel_riesgo")
comercios = spark.table(f"{FQ}.comercios").select("comercio_id", "categoria", "ciudad")
tipo_cambio = spark.table(f"{FQ}.tipo_cambio")
limites = spark.table(f"{FQ}.limites_segmento")

enriquecidas = (
    spark.readStream.table(SILVER)
    .join(F.broadcast(clientes), "cliente_id", "left")
    .join(F.broadcast(comercios), "comercio_id", "left")
    .join(F.broadcast(tipo_cambio), "moneda", "left")
    .join(F.broadcast(limites), "segmento", "left")
    .withColumn("monto_usd", F.round(F.col("monto_local") / F.col("unidades_por_usd"), 2))
    .withColumn("es_internacional", F.col("pais") != F.col("pais_residencia"))
    .withColumn("excede_limite", F.col("monto_usd") > F.col("limite_usd_transaccion"))
    .select("event_id", "event_time", "tarjeta_id", "cliente_id", "segmento", "nivel_riesgo", "pais_residencia",
            "pais", "canal", "comercio_id", "categoria", "ciudad", "moneda", "monto_local", "unidades_por_usd",
            "monto_usd", "es_internacional", "excede_limite", "ingesta_ts")
)

detener_consultas("enriquecidas")
q_enr = (
    enriquecidas.writeStream
    .option("checkpointLocation", f"{CHECKPOINTS}/enriquecidas")
    .trigger(processingTime="5 seconds")
    .queryName("enriquecidas")
    .toTable(ENRIQUECIDAS)
)
esperar_progreso(q_enr)
esperar_filas(ENRIQUECIDAS)
display(spark.table(ENRIQUECIDAS).orderBy(F.col("ingesta_ts").desc()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Alerta por regla: monto inusual
# MAGIC Una regla **sin estado**: se evalúa con un solo evento. Alertamos si la compra **excede el límite** del segmento,
# MAGIC o si un cliente de riesgo **ALTO** compra **fuera** de su país de residencia.
# MAGIC
# MAGIC Las dos alertas de este notebook van a la **misma tabla** `alertas`, con una columna `tipo_alerta`. Dos
# MAGIC consultas pueden escribir a la vez en una tabla Delta en modo append.

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {ALERTAS} (
  tipo_alerta STRING, tarjeta_id STRING, cliente_id STRING, detalle STRING, monto_usd DOUBLE,
  desde TIMESTAMP, hasta TIMESTAMP, generada_ts TIMESTAMP)
  COMMENT 'Alertas de tarjetas generadas en streaming (datos sintéticos)'""")

alertas_monto = (
    spark.readStream.table(ENRIQUECIDAS)
    .filter("excede_limite OR (nivel_riesgo = 'ALTO' AND es_internacional)")
    .select(
        F.when(F.col("excede_limite"), "MONTO_SOBRE_LIMITE").otherwise("RIESGO_ALTO_INTERNACIONAL").alias("tipo_alerta"),
        "tarjeta_id", "cliente_id",
        F.format_string("%s · %s en %s (%s), segmento %s", "canal", "categoria", "ciudad", "pais", "segmento").alias("detalle"),
        F.col("monto_usd").cast("double").alias("monto_usd"),
        F.col("event_time").alias("desde"), F.col("event_time").alias("hasta"),
        F.current_timestamp().alias("generada_ts"),
    )
)
detener_consultas("alertas_monto")
q_monto = (
    alertas_monto.writeStream
    .option("checkpointLocation", f"{CHECKPOINTS}/alertas_monto")
    .trigger(processingTime="5 seconds")
    .queryName("alertas_monto")
    .toTable(ALERTAS)
)
esperar_progreso(q_monto)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Alerta por comportamiento: velocidad de uso
# MAGIC Una regla **con estado**: "10 o más transacciones de la misma tarjeta en 2 minutos". Necesita recordar eventos
# MAGIC anteriores, así que usa una **ventana deslizante** (2 min, avanza cada 30 s) y un **watermark** que limita
# MAGIC cuánto recordar. Filtrar **después** de agregar (`count >= 10`) es válido en modo append: cada ventana cerrada
# MAGIC se evalúa una vez.
# MAGIC
# MAGIC El generador tiene 5 tarjetas "en ráfaga" (`TJ-00000` a `TJ-00004`): deberían aparecer aquí, y ninguna otra.

# COMMAND ----------

alertas_velocidad = (
    spark.readStream.table(ENRIQUECIDAS)
    .withWatermark("event_time", "1 minute")
    .groupBy(F.window("event_time", "2 minutes", "30 seconds"), "tarjeta_id", "cliente_id")
    .agg(F.count("*").alias("n"), F.sum("monto_usd").alias("monto_usd"))
    .filter("n >= 10")
    .select(
        F.lit("VELOCIDAD").alias("tipo_alerta"), "tarjeta_id", "cliente_id",
        F.format_string("%d transacciones en 2 minutos", "n").alias("detalle"),
        F.round(F.col("monto_usd"), 2).cast("double").alias("monto_usd"),
        F.col("window.start").alias("desde"), F.col("window.end").alias("hasta"),
        F.current_timestamp().alias("generada_ts"),
    )
)
detener_consultas("alertas_velocidad")
q_vel = (
    alertas_velocidad.writeStream
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINTS}/alertas_velocidad")
    .trigger(processingTime="10 seconds")
    .queryName("alertas_velocidad")
    .toTable(ALERTAS)
)
print("⏳ Las alertas de velocidad aparecen cuando el watermark cierra las primeras ventanas (~3 min).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Las alertas
# MAGIC Ejecuta la celda un par de veces mientras las ventanas se cierran.

# COMMAND ----------

def alertas_de(tipo):
    return spark.sql(f"SELECT count(*) FROM {ALERTAS} WHERE tipo_alerta = '{tipo}'").first()[0]


inicio = time.time()
while alertas_de("VELOCIDAD") == 0 and time.time() - inicio < 300:
    time.sleep(10)

display(spark.sql(f"""
SELECT tipo_alerta, count(*) AS alertas, count(DISTINCT tarjeta_id) AS tarjetas, round(sum(monto_usd), 2) AS monto_usd
FROM {ALERTAS} GROUP BY 1 ORDER BY 2 DESC
"""))
display(spark.sql(f"""
SELECT tarjeta_id, max(detalle) AS peor_ventana, count(*) AS ventanas_con_alerta, max(hasta) AS ultima
FROM {ALERTAS} WHERE tipo_alerta = 'VELOCIDAD' GROUP BY 1 ORDER BY 3 DESC LIMIT 10
"""))

# COMMAND ----------

# Expected output: ✅ Alertas de velocidad solo para las tarjetas en ráfaga, y alertas de monto
try:
    tarjetas = {r[0] for r in spark.sql(f"SELECT DISTINCT tarjeta_id FROM {ALERTAS} WHERE tipo_alerta = 'VELOCIDAD'").collect()}
    rafaga = {f"TJ-{n:05d}" for n in range(5)}
    assert tarjetas, "Todavía no hay alertas de velocidad: espera un minuto y vuelve a ejecutar"
    assert tarjetas <= rafaga, f"Alertas para tarjetas que no están en ráfaga: {sorted(tarjetas - rafaga)[:5]}"
    assert alertas_de("MONTO_SOBRE_LIMITE") > 0, "Todavía no hay alertas de monto"
    print(f"✅ Alertas de velocidad para {len(tarjetas)} de las 5 tarjetas en ráfaga, y alertas de monto")
except AssertionError as e:
    print("⏳", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — Cambia la referencia, sin detener nada
# MAGIC Supongamos que Tesorería publica un nuevo tipo de cambio del córdoba (valor **didáctico**). Actualizamos la
# MAGIC tabla `tipo_cambio` con un `UPDATE` normal y esperamos un par de micro-lotes: las transacciones en `NIO` que
# MAGIC lleguen **después** usan la tasa nueva. Las anteriores conservan la que tenían (no se recalcula el pasado).
# MAGIC
# MAGIC > Puede que unas pocas filas con `despues_del_cambio = false` ya tengan la tasa nueva: el micro-lote que estaba
# MAGIC > en curso leyó la referencia **después** del `UPDATE`. El límite exacto es el micro-lote, no el segundo.

# COMMAND ----------

_ = spark.sql(f"UPDATE {FQ}.tipo_cambio SET unidades_por_usd = 40.00 WHERE moneda = 'NIO'")
marca = spark.sql("SELECT current_timestamp()").first()[0]
time.sleep(20)
esperar_progreso(q_enr, lotes=len(q_enr.recentProgress) + 2)

display(spark.sql(f"""
SELECT ingesta_ts > '{marca}' AS despues_del_cambio, unidades_por_usd, count(*) AS transacciones_nio
FROM {ENRIQUECIDAS} WHERE moneda = 'NIO' AND ingesta_ts > timestampadd(MINUTE, -3, '{marca}')
GROUP BY ALL ORDER BY 1, 2
"""))

# COMMAND ----------

# Expected output: ✅ Las transacciones nuevas en NIO ya usan la tasa 40.00 (sin reiniciar el stream)
try:
    nuevas = spark.sql(f"""SELECT count_if(unidades_por_usd = 40.00) FROM {ENRIQUECIDAS}
                           WHERE moneda = 'NIO' AND ingesta_ts > '{marca}'""").first()[0]
    assert nuevas > 0, "Aún no llegan transacciones NIO con la tasa nueva; espera unos segundos y reintenta"
    print(f"✅ {nuevas} transacciones nuevas en NIO ya usan la tasa 40.00 (sin reiniciar el stream)")
except AssertionError as e:
    print("⏳", e)

# Volvemos a la tasa original para no afectar el resto del laboratorio
_ = spark.sql(f"UPDATE {FQ}.tipo_cambio SET unidades_por_usd = 36.60 WHERE moneda = 'NIO'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6 — Detener las consultas de este notebook

# COMMAND ----------

detener_consultas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC - El **join stream-static** enriqueció cada evento con tablas de referencia, que se releen en cada micro-lote.
# MAGIC - Una alerta **sin estado** (regla sobre un evento) y una **con estado** (comportamiento en una ventana con
# MAGIC   watermark) escribieron en la misma tabla.
# MAGIC - Un `UPDATE` en la referencia se aplicó a los eventos siguientes **sin reiniciar** el stream.
# MAGIC
# MAGIC > **¿Y si la referencia también es un stream?** Por ejemplo, unir cada autorización con su confirmación, que
# MAGIC > llega después. Eso es un **join stream-stream**: ambos lados guardan estado y necesitan watermark y una
# MAGIC > condición de tiempo (`b.t BETWEEN a.t AND a.t + INTERVAL 5 MINUTES`). Lo mencionamos en el `05`.
# MAGIC
# MAGIC Sigue con `05_operacion`.
