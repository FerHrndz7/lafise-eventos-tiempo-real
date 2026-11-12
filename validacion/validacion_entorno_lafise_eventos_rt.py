# Databricks notebook source
# MAGIC %md
# MAGIC # Validación de entorno — Taller de Procesamiento de Eventos en Tiempo Real
# MAGIC
# MAGIC Ejecuta este notebook en el **workspace donde se dará el taller**, con el **clúster clásico del curso**
# MAGIC adjunto (DBR **17.3 LTS**, modo *Standard* o *Dedicated*), idealmente con un usuario **sin permisos de admin**
# MAGIC (como los asistentes).
# MAGIC
# MAGIC Prueba con datos mínimos cada capacidad que usa el laboratorio (rate source, Auto Loader sobre un volumen,
# MAGIC triggers de micro-lote, `foreachBatch` + MERGE, watermarks y ventanas, deduplicación, joins stream-static,
# MAGIC sink en memoria, `availableNow` y métricas de progreso) y reporta **PASA / FALLA / ALTERNATIVA / REVISAR** por
# MAGIC requisito. Al final **borra** lo que creó.
# MAGIC
# MAGIC ## Instrucciones
# MAGIC 1. *Workspace → Import → File* y elige este archivo `.py`.
# MAGIC 2. Adjunta el **clúster clásico** del curso.
# MAGIC 3. En el widget **catalog** escribe el catálogo de los asistentes.
# MAGIC 4. **Run all** (~5 min). Comparte la **tabla resumen final** con el facilitador.
# MAGIC
# MAGIC Si algo sale **FALLA**, la columna *Detalle* dice qué pedirle al administrador.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catálogo (requerido)")
dbutils.widgets.text("schema", "validacion_eventos_rt", "Schema de prueba")

import time
from pyspark.sql import functions as F

resultados = {}


def marcar(req, estado, detalle=""):
    resultados[req] = (estado, str(detalle).replace("\n", " ")[:300])
    print(f"[{estado}] {req} — {detalle}")


CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip() or "validacion_eventos_rt"
assert CATALOG, "Escribe el catálogo en el widget 'catalog' antes de ejecutar."
FQ = f"`{CATALOG}`.{SCHEMA}"
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/val"
spark.conf.set("spark.sql.shuffle.partitions", "4")


def metrica(obj, nombre, defecto=None):
    # En DBR 17.3 numInputRows y otros son atributos calculados: p.get(...) devuelve None, p.numInputRows no.
    v = getattr(obj, nombre, None)
    if v is None and hasattr(obj, "get"):
        v = obj.get(nombre)
    return defecto if v is None else v


def esperar(q, lotes=2, timeout_s=120):
    inicio = time.time()
    while time.time() - inicio < timeout_s:
        if q.exception():
            raise RuntimeError(q.exception())
        if len([p for p in q.recentProgress if metrica(p, "numInputRows", 0) > 0]) >= lotes:
            return
        time.sleep(3)
    raise TimeoutError(f"{q.name}: sin {lotes} micro-lotes con datos en {timeout_s}s")

# COMMAND ----------

try:
    dbr = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
    marcar("1. Clúster clásico y runtime", "PASA" if dbr.startswith("17.") else "ALTERNATIVA",
           f"{dbr} (el taller se probó en 17.3 LTS)")
except Exception as e:
    marcar("1. Clúster clásico y runtime", "FALLA",
           f"No es un clúster clásico (¿serverless?). El taller usa triggers processingTime, que serverless no admite. {e}")

try:
    from databricks.sdk import WorkspaceClient
    cid = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
    modo = str(WorkspaceClient().clusters.get(cid).data_security_mode).split(".")[-1]
    marcar("2. Modo de acceso del clúster", "PASA" if modo in ("USER_ISOLATION", "SINGLE_USER", "DATA_SECURITY_MODE_STANDARD",
                                                               "DATA_SECURITY_MODE_DEDICATED", "DATA_SECURITY_MODE_AUTO")
           else "REVISAR", f"{modo} (Standard = USER_ISOLATION, Dedicated = SINGLE_USER)")
except Exception as e:
    marcar("2. Modo de acceso del clúster", "REVISAR", f"No pude leerlo; confirmen que sea Standard o Dedicated. {e}")

# COMMAND ----------

try:
    assert "." not in CATALOG, "el nombre del catálogo tiene puntos"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQ}.val")
    dbutils.fs.mkdirs(f"{VOL}/landing")
    spark.sql(f"CREATE OR REPLACE TABLE {FQ}.dim AS SELECT * FROM VALUES ('NI', 36.6D), ('CR', 505.0D) AS t(pais, tasa)")
    marcar("3. Crear schema, volumen y tabla en el catálogo", "PASA", f"{CATALOG}.{SCHEMA}")
except Exception as e:
    marcar("3. Crear schema, volumen y tabla en el catálogo", "FALLA",
           f"Pedir GRANT USE CATALOG, CREATE SCHEMA ON CATALOG {CATALOG} TO <grupo>. {e}")

# COMMAND ----------

q_gen = None
try:
    q_gen = (spark.readStream.format("rate").option("rowsPerSecond", 10).load()
             .selectExpr("concat('EV-', CAST(value AS STRING)) AS event_id", "timestamp AS event_time",
                         "element_at(array('NI','CR'), CAST(pmod(value, 2) + 1 AS INT)) AS pais",
                         "CAST(value % 100 AS STRING) AS monto")
             .coalesce(1).writeStream.format("json")
             .option("checkpointLocation", f"{VOL}/chk/gen").option("path", f"{VOL}/landing")
             .trigger(processingTime="2 seconds").queryName("val_gen").start())
    esperar(q_gen)
    marcar("4. Rate source → JSON en un volumen (trigger processingTime)", "PASA", "archivos escritos cada 2 s")
except Exception as e:
    marcar("4. Rate source → JSON en un volumen (trigger processingTime)", "FALLA", e)

# COMMAND ----------

q_bronze = None
try:
    q_bronze = (spark.readStream.format("cloudFiles").option("cloudFiles.format", "json")
                .option("pathGlobFilter", "*.json").option("rescuedDataColumn", "_rescued_data")
                .schema("event_id STRING, event_time STRING, pais STRING, monto STRING").load(f"{VOL}/landing")
                .withColumn("archivo", F.col("_metadata.file_name"))
                .writeStream.option("checkpointLocation", f"{VOL}/chk/bronze")
                .trigger(processingTime="5 seconds").queryName("val_bronze").toTable(f"{FQ}.bronze"))
    esperar(q_bronze)
    marcar("5. Auto Loader (cloudFiles) desde el volumen → tabla Delta", "PASA",
           f"{spark.table(f'{FQ}.bronze').count()} filas")
except Exception as e:
    marcar("5. Auto Loader (cloudFiles) desde el volumen → tabla Delta", "FALLA", e)

# COMMAND ----------

q_fb = None
try:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {FQ}.silver (event_id STRING, event_time TIMESTAMP, pais STRING, monto INT)")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {FQ}.agregado (pais STRING, n BIGINT)")
    SILVER_T, AGG_T = f"{FQ}.silver", f"{FQ}.agregado"

    def lote_fn(lote, lote_id):
        (lote.select("event_id", "event_time", "pais", F.col("monto").cast("int").alias("monto"))
             .write.format("delta").mode("append").option("txnAppId", "val_silver").option("txnVersion", lote_id)
             .saveAsTable(SILVER_T))
        lote.groupBy("pais").count().withColumnRenamed("count", "n").createOrReplaceTempView("val_lote")
        lote.sparkSession.sql(f"""MERGE INTO {AGG_T} t USING val_lote s ON t.pais = s.pais
                                  WHEN MATCHED THEN UPDATE SET t.n = t.n + s.n WHEN NOT MATCHED THEN INSERT *""")

    q_fb = (spark.readStream.table(f"{FQ}.bronze")
            .withColumn("event_time", F.to_timestamp("event_time"))
            .withWatermark("event_time", "1 minute").dropDuplicatesWithinWatermark(["event_id"])
            .writeStream.foreachBatch(lote_fn).option("checkpointLocation", f"{VOL}/chk/silver")
            .trigger(processingTime="5 seconds").queryName("val_silver").start())
    esperar(q_fb)
    n = spark.table(SILVER_T).count()
    m = spark.table(AGG_T).count()
    assert n > 0 and m > 0, f"silver={n} agregado={m}"
    marcar("6. foreachBatch + MERGE + dropDuplicatesWithinWatermark", "PASA", f"silver={n} filas, MERGE ok")
except Exception as e:
    marcar("6. foreachBatch + MERGE + dropDuplicatesWithinWatermark", "FALLA", e)

# COMMAND ----------

q_win = None
try:
    q_win = (spark.readStream.table(f"{FQ}.silver")
             .join(F.broadcast(spark.table(f"{FQ}.dim")), "pais", "left")
             .withWatermark("event_time", "10 seconds")
             .groupBy(F.window("event_time", "10 seconds"), "pais").agg(F.count("*").alias("n"), F.max("tasa").alias("tasa"))
             .writeStream.outputMode("append").option("checkpointLocation", f"{VOL}/chk/win")
             .trigger(processingTime="5 seconds").queryName("val_win").toTable(f"{FQ}.ventanas"))
    inicio = time.time()
    while time.time() - inicio < 120 and (not spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.ventanas")
                                          or spark.table(f"{FQ}.ventanas").count() == 0):
        if q_win.exception():
            raise RuntimeError(q_win.exception())
        time.sleep(5)
    n = spark.table(f"{FQ}.ventanas").count()
    p = q_win.lastProgress
    marcar("7. Join stream-static + ventana con watermark (append)", "PASA" if n > 0 else "REVISAR",
           f"{n} ventanas cerradas · watermark={(metrica(p, 'eventTime', {}) or {}).get('watermark')}")
except Exception as e:
    marcar("7. Join stream-static + ventana con watermark (append)", "FALLA", e)

# COMMAND ----------

q_mem = None
try:
    q_mem = (spark.readStream.table(f"{FQ}.silver").groupBy("pais").count()
             .writeStream.format("memory").outputMode("complete").queryName("val_memoria")
             .trigger(processingTime="5 seconds").start())
    esperar(q_mem, lotes=1)
    n = spark.sql("SELECT count(*) FROM val_memoria").first()[0]
    marcar("8. Sink en memoria (modo complete)", "PASA", f"{n} filas")
except Exception as e:
    marcar("8. Sink en memoria (modo complete)", "ALTERNATIVA",
           f"Solo afecta el paso 5 del notebook 03 (se puede saltar). {e}")

# COMMAND ----------

try:
    q_an = (spark.readStream.table(f"{FQ}.silver").writeStream
            .option("checkpointLocation", f"{VOL}/chk/an").trigger(availableNow=True)
            .queryName("val_available_now").toTable(f"{FQ}.copia"))
    q_an.awaitTermination(180)
    assert not q_an.isActive, "no terminó en 3 min"
    marcar("9. Trigger availableNow (incremental que se detiene solo)", "PASA",
           f"{spark.table(f'{FQ}.copia').count()} filas copiadas")
except Exception as e:
    marcar("9. Trigger availableNow (incremental que se detiene solo)", "FALLA", e)

try:
    p = q_fb.lastProgress if q_fb else None
    assert p is not None and metrica(p, "durationMs"), "sin progreso"
    marcar("10. Métricas de progreso (lastProgress / recentProgress)", "PASA",
           f"lote {metrica(p, 'batchId')} · {metrica(p, 'numInputRows', 0)} filas · "
           f"estado={len(metrica(p, 'stateOperators', []) or [])} op.")
except Exception as e:
    marcar("10. Métricas de progreso (lastProgress / recentProgress)", "REVISAR", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpieza y resumen

# COMMAND ----------

for q in spark.streams.active:
    q.stop()
try:
    spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")
    print("🧹 Limpieza completa")
except Exception as e:
    print("⚠️ Limpieza parcial:", e)

print(f"\n{'Requisito':62s} {'Estado':12s} Detalle")
for req, (estado, det) in resultados.items():
    print(f"{req:62s} {estado:12s} {det[:110]}")
fallas = [r for r, (e, _) in resultados.items() if e == "FALLA"]
print("\n" + ("✅ El entorno está listo para el taller." if not fallas else
              f"❌ Hay {len(fallas)} requisito(s) en FALLA: compartir esta tabla con el facilitador."))
