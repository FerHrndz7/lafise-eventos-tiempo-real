# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · El generador de transacciones en tiempo real
# MAGIC
# MAGIC **Tiempo estimado:** 7 minutos
# MAGIC
# MAGIC ## Objetivos
# MAGIC - Arrancar un **generador** que escribe transacciones con tarjeta nuevas cada **2 segundos** en tu volumen.
# MAGIC - Entender la **forma** de cada evento y los "problemas" que trae a propósito.
# MAGIC - Dejarlo **corriendo** mientras trabajas en los demás notebooks.
# MAGIC
# MAGIC ## Concepto — ¿de dónde vienen los eventos?
# MAGIC En producción, las transacciones llegarían de **Kafka**, **Event Hubs**, **Kinesis**, de una base de datos vía
# MAGIC **CDC** o de archivos que deja otro sistema. Aquí simulamos esa llegada con el **rate source** de Spark: una
# MAGIC fuente de streaming que emite N filas por segundo. Convertimos cada fila en una transacción y la escribimos
# MAGIC como **archivo JSON** en el volumen `landing`. Desde el notebook `02`, ese volumen es nuestra "fuente".
# MAGIC
# MAGIC Lo importante del laboratorio **no** es el generador, sino lo que hacemos con sus eventos. Pero conviene saber
# MAGIC qué trae, porque los datos reales también llegan imperfectos:
# MAGIC
# MAGIC | Problema inyectado | Proporción | Lo resolvemos en |
# MAGIC |---|---|---|
# MAGIC | **Eventos tardíos** (ocurrieron hace 30 s–3 min) | ~8% | `03` · watermarks |
# MAGIC | **Eventos muy tardíos** (hace 4–8 min) | ~2% | `03` · se descartan |
# MAGIC | **Duplicados** (mismo `event_id` dos veces) | ~3% | `02` · deduplicación |
# MAGIC | **Inválidos** (monto `N/A`, monto negativo, moneda `XXX`) | ~3% | `02` · cuarentena |
# MAGIC | **Ráfagas** (5 tarjetas con muchas compras seguidas) | ~3% | `04` · alerta de velocidad |
# MAGIC | **Tarjetas viajeras** (5 tarjetas que compran en varios países) | ~0.5% | `06` · reto |
# MAGIC
# MAGIC > **Deja este notebook abierto y conectado.** El generador corre en segundo plano mientras el notebook siga
# MAGIC > conectado al clúster. Si lo cierras o lo desconectas, dejan de llegar eventos.

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "", "Catálogo asignado (REQUERIDO)")
dbutils.widgets.text("schema", "eventos_rt", "Schema base")
dbutils.widgets.text("eventos_por_segundo", "20", "Eventos por segundo")
dbutils.widgets.dropdown("detener", "no", ["no", "si"], "¿Detener el generador?")
dbutils.widgets.text("mantener_min", "0", "Solo para jobs: minutos activo (0 = sesión en vivo)")

user_email = spark.sql("SELECT current_user()").collect()[0][0]
user_prefix = re.sub(r"[^a-z0-9]", "_", user_email.split("@")[0].lower())
CATALOG = dbutils.widgets.get("catalog").strip()
assert CATALOG, "Escribe en el widget 'catalog' el mismo catálogo del 00_setup y vuelve a ejecutar."
SCHEMA = f"{(dbutils.widgets.get('schema').strip() or 'eventos_rt')}_{user_prefix}"
FQ = f"`{CATALOG}`.{SCHEMA}"
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing/transacciones"
CHECKPOINTS = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"
EVENTOS_POR_SEGUNDO = int(dbutils.widgets.get("eventos_por_segundo"))

# COMMAND ----------

# MAGIC %run ./utils/streaming_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Convertir cada fila del rate source en una transacción
# MAGIC El rate source entrega dos columnas: `timestamp` (cuándo se emitió la fila) y `value` (un contador 0, 1, 2…).
# MAGIC Con funciones de Spark (`hash`, `pmod`, `element_at`) derivamos de `value` una transacción **reproducible**:
# MAGIC el mismo `value` siempre produce la misma tarjeta, comercio y monto.
# MAGIC
# MAGIC Dos tiempos distintos, que son la base de todo el laboratorio:
# MAGIC - **Tiempo de evento** (`event_time`): cuándo **ocurrió** la compra en el POS o en la app.
# MAGIC - **Tiempo de llegada**: cuándo el evento **llega** a nuestro sistema. Un evento tardío ocurrió hace rato pero
# MAGIC   llega ahora (un POS sin conexión, un reintento, una cola atrasada).

# COMMAND ----------

from pyspark.sql import functions as F

PAISES = "array('NI','CR','HN','PA','DO')"
MONEDA_PAIS = "map('NI','NIO','CR','CRC','HN','HNL','PA','USD','DO','DOP')"
UNIDADES_POR_USD = "map('NIO',36.60D,'CRC',505.00D,'HNL',24.70D,'USD',1.00D,'DOP',59.00D)"
CORRIDA = spark.sql("SELECT date_format(current_timestamp(), 'MMddHHmmss')").first()[0]

transacciones = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", EVENTOS_POR_SEGUNDO)
    .option("numPartitions", 1)
    .load()
    .selectExpr("timestamp AS llegada", "value AS v")
    # Perfil de la tarjeta: normal, ráfaga (5 tarjetas) o viajera (5 tarjetas)
    .withColumn("perfil", F.expr("""CASE WHEN pmod(abs(hash(v,'hot')),100) < 3 THEN 'rafaga'
                                         WHEN pmod(abs(hash(v,'trv')),1000) < 5 THEN 'viajero'
                                         ELSE 'normal' END"""))
    .withColumn("n_cliente", F.expr("""CASE perfil WHEN 'rafaga'  THEN pmod(abs(hash(v,'hc')),5)
                                                   WHEN 'viajero' THEN 10 + pmod(abs(hash(v,'tc')),5)
                                                   ELSE 15 + pmod(abs(hash(v,'c')),1985) END"""))
    # País de la compra: el de residencia del cliente, salvo las tarjetas viajeras (país al azar)
    .withColumn("i_pais", F.expr("IF(perfil = 'viajero', pmod(abs(hash(v,'tp')),5), pmod(n_cliente,5))"))
    .withColumn("pais", F.expr(f"element_at({PAISES}, CAST(i_pais + 1 AS INT))"))
    .withColumn("moneda", F.expr(f"""CASE WHEN pmod(abs(hash(v,'usd')),100) < 20 THEN 'USD'
                                          ELSE element_at({MONEDA_PAIS}, pais) END"""))
    .withColumn("monto_usd", F.expr("""round((pmod(abs(hash(v,'m')),20000) / 100.0 + 1)
                                             * CASE WHEN pmod(abs(hash(v,'big')),100) = 0 THEN 40 ELSE 1 END, 2)"""))
    .withColumn("monto_local", F.expr(f"round(monto_usd * element_at({UNIDADES_POR_USD}, moneda), 2)"))
    # Retraso: ~8% tardíos (30 s–3 min), ~2% muy tardíos (4–8 min), el resto 0–2 s
    .withColumn("retraso_s", F.expr("""CASE WHEN pmod(abs(hash(v,'late')),100) < 8 THEN 30 + pmod(abs(hash(v,'lt')),150)
                                            WHEN pmod(abs(hash(v,'late')),100) < 10 THEN 240 + pmod(abs(hash(v,'vlt')),240)
                                            ELSE pmod(abs(hash(v,'j')),3) END"""))
    # Registros inválidos (~3%) y duplicados (~3%)
    .withColumn("defecto", F.expr("pmod(abs(hash(v,'bad')),100)"))
    .withColumn("copia", F.expr("explode(IF(pmod(abs(hash(v,'dup')),100) < 3, array(1,2), array(1)))"))
    .select(
        F.expr(f"concat('EV-', '{CORRIDA}', '-', CAST(v AS STRING))").alias("event_id"),
        F.expr("timestampadd(SECOND, -retraso_s, llegada)").alias("event_time"),
        F.expr("concat('TJ-', lpad(CAST(n_cliente AS STRING), 5, '0'))").alias("tarjeta_id"),
        F.expr("concat('CL-', lpad(CAST(n_cliente AS STRING), 5, '0'))").alias("cliente_id"),
        # Un comercio del mismo país (en `comercios`, el país del comercio n es n mod 5)
        F.expr("concat('CM-', lpad(CAST(5 * pmod(abs(hash(v,'cm')),100) + i_pais AS STRING), 4, '0'))").alias("comercio_id"),
        F.expr("element_at(array('POS','ECOMMERCE','ATM','APP'), CAST(pmod(abs(hash(v,'cn')),4) + 1 AS INT))").alias("canal"),
        F.col("pais"),
        F.expr("IF(defecto = 2, 'XXX', moneda)").alias("moneda"),
        F.expr("""CASE WHEN defecto = 0 THEN 'N/A'
                       WHEN defecto = 1 THEN CAST(-monto_local AS STRING)
                       ELSE CAST(monto_local AS STRING) END""").alias("monto"),
    )
)
transacciones.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Escribir los eventos como archivos JSON, cada 2 segundos
# MAGIC Esta es nuestra primera **consulta de streaming**: `readStream` (fuente) → transformaciones → `writeStream`
# MAGIC (destino). Tres opciones que verás en todas las consultas del laboratorio:
# MAGIC - `checkpointLocation`: dónde Spark guarda su avance. Si la consulta se reinicia, continúa donde quedó.
# MAGIC - `trigger(processingTime="2 seconds")`: cada cuánto se ejecuta un **micro-lote**.
# MAGIC - `queryName`: un nombre para encontrarla en `spark.streams.active` y en la interfaz.

# COMMAND ----------

detener_consultas("generador")

if dbutils.widgets.get("detener") == "si":
    print("⏹  Generador detenido (widget detener = si). Cámbialo a 'no' y vuelve a ejecutar para reanudarlo.")
    dbutils.notebook.exit("detenido")

generador = (
    transacciones.coalesce(1)
    .writeStream.format("json")
    .option("checkpointLocation", f"{CHECKPOINTS}/generador")
    .option("path", LANDING)
    .trigger(processingTime="2 seconds")
    .queryName("generador")
    .start()
)
esperar_progreso(generador, lotes=2)
print(f"✅ Generador corriendo: ~{EVENTOS_POR_SEGUNDO} eventos/s → {LANDING}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Mirar lo que está llegando
# MAGIC Cada micro-lote deja un archivo nuevo. Ejecuta la celda varias veces: el número de archivos crece.
# MAGIC (La carpeta `_spark_metadata` es del propio generador; las carpetas que empiezan con `_` se ignoran al leer.)

# COMMAND ----------

archivos = [f for f in dbutils.fs.ls(LANDING) if f.name.endswith(".json")]
print(f"Archivos JSON en el volumen: {len(archivos)}")
display(spark.read.json(LANDING).orderBy(F.col("event_time").desc()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — El progreso de la consulta
# MAGIC Toda consulta de streaming reporta sus **micro-lotes**: cuántas filas leyó, a qué velocidad y cuánto tardó.
# MAGIC También lo ves en la interfaz, bajo la celda que la arrancó (el panel con el gráfico de *Input vs. Processing
# MAGIC Rate*).

# COMMAND ----------

resumen_progreso(generador)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Listo: deja este notebook abierto 👋
# MAGIC Sigue con **`02_ingesta_y_transformaciones`** en otra pestaña. El generador sigue escribiendo mientras tanto.
# MAGIC
# MAGIC **Para detenerlo al final del taller:** cambia el widget **detener** a `si` y ejecuta la celda del Paso 2.
# MAGIC (Si algún notebook dice que no llegan datos, revisa que este siga activo: la celda del Paso 2 muestra el
# MAGIC panel de la consulta corriendo.)
# MAGIC
# MAGIC > **¿Más carga?** Cambia **eventos_por_segundo** (por ejemplo a `100`) y vuelve a ejecutar el Paso 2. El
# MAGIC > generador continúa desde su checkpoint con la nueva velocidad, y en el `02` verás subir la tasa de entrada.
# MAGIC
# MAGIC > **Ojo al pausar y reanudar:** el rate source "se pone al día" con los segundos que estuvo detenido. El primer
# MAGIC > lote tras una pausa trae todas esas filas de golpe, con su hora original, así que río abajo se ven como
# MAGIC > **eventos tardíos**. Es un buen ejemplo en vivo de lo que pasa cuando un sistema origen se cae y se recupera.

# COMMAND ----------

mantener_activo(int(dbutils.widgets.get("mantener_min") or 0), [generador])
