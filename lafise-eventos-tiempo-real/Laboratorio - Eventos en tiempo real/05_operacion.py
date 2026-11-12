# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Operar el flujo en producción
# MAGIC
# MAGIC **Tiempo estimado:** 10 minutos
# MAGIC
# MAGIC ## Objetivos
# MAGIC - Elegir el **trigger** según la latencia que el negocio necesita (y lo que cuesta).
# MAGIC - Entender qué guarda un **checkpoint** y comprobar que un reinicio **no duplica** datos.
# MAGIC - Conocer el checklist mínimo para poner un stream en producción, y sus alternativas declarativas.
# MAGIC
# MAGIC ## Concepto — ¿qué tan "tiempo real" necesitas?
# MAGIC
# MAGIC | Trigger | Latencia típica | Cómputo | Cuándo usarlo |
# MAGIC |---|---|---|---|
# MAGIC | `processingTime="5 seconds"` | segundos | Clúster **siempre encendido** | Alertas y tableros operativos (lo que hicimos hoy) |
# MAGIC | `availableNow=True` | minutos a horas (la frecuencia del job) | Se enciende, procesa lo pendiente **y se apaga** | Cargas incrementales programadas: el 80% de los casos |
# MAGIC | **Real-Time Mode** | milisegundos | Clúster clásico siempre encendido (DBR 16.4+) | Fraude en la autorización, sub-segundo. Fuentes Kafka / Event Hubs / Kinesis |
# MAGIC
# MAGIC El mismo código sirve con cualquier trigger: cambiar la latencia es cambiar **una línea**.

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
ECOMMERCE = f"{FQ}.silver_ecommerce"
spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %run ./utils/streaming_utils

# COMMAND ----------

esperar_filas(SILVER)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — `availableNow`: streaming que se apaga solo
# MAGIC Copiamos las transacciones de comercio electrónico de Silver a otra tabla con `trigger(availableNow=True)`.
# MAGIC La consulta procesa **todo lo pendiente** desde su último checkpoint y **termina**. `awaitTermination()`
# MAGIC espera a que acabe, como un batch.

# COMMAND ----------

from pyspark.sql import functions as F


def copiar_ecommerce():
    q = (
        spark.readStream.table(SILVER)
        .filter("canal = 'ECOMMERCE'")
        .writeStream
        .option("checkpointLocation", f"{CHECKPOINTS}/silver_ecommerce")
        .trigger(availableNow=True)
        .queryName("ecommerce_incremental")
        .toTable(ECOMMERCE)
    )
    q.awaitTermination()
    filas = sum(metrica(p, "numInputRows", 0) for p in q.recentProgress)
    print(f"Corrida terminada · filas procesadas: {filas:,}")
    return spark.table(ECOMMERCE).count()


n1 = copiar_ecommerce()
print(f"Tabla destino tras la 1.ª corrida: {n1:,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Segunda corrida: solo lo nuevo
# MAGIC Esperamos 20 segundos (llegan eventos nuevos a Silver) y corremos **la misma función**. Gracias al checkpoint,
# MAGIC solo procesa lo que llegó desde la corrida anterior. Así funciona un job incremental programado cada hora:
# MAGIC paga cómputo solo mientras corre.

# COMMAND ----------

time.sleep(20)
n2 = copiar_ecommerce()
print(f"Tabla destino tras la 2.ª corrida: {n2:,} filas (+{n2 - n1:,} nuevas)")

# COMMAND ----------

# Expected output: ✅ La 2.ª corrida agregó solo filas nuevas, sin duplicar ninguna
try:
    r = spark.sql(f"SELECT count(*) n, count(DISTINCT event_id) u FROM {ECOMMERCE}").first()
    assert n2 > n1, "La 2.ª corrida no encontró filas nuevas: ¿siguen corriendo el generador (01) y el 02?"
    assert r.n == r.u, f"Hay {r.n - r.u} filas duplicadas"
    print(f"✅ La 2.ª corrida agregó solo {n2 - n1:,} filas nuevas, sin duplicar ninguna")
except AssertionError as e:
    print("❌", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — ¿Qué hay en un checkpoint?
# MAGIC El checkpoint es una carpeta con el "diario" de la consulta:
# MAGIC
# MAGIC | Carpeta | Qué guarda |
# MAGIC |---|---|
# MAGIC | `offsets/` | Para cada micro-lote, **hasta dónde** va a leer de la fuente (se escribe **antes** de procesar) |
# MAGIC | `commits/` | Qué micro-lotes **terminaron** (se escribe **después** de escribir el destino) |
# MAGIC | `state/` | El estado de ventanas, deduplicación y joins (solo consultas con estado) |
# MAGIC | `sources/` | Datos propios de la fuente (Auto Loader recuerda aquí qué archivos ya leyó) |
# MAGIC
# MAGIC Si el clúster se cae entre `offsets` y `commits`, al reiniciar Spark **repite ese lote**; el destino Delta
# MAGIC (o `txnVersion` en `foreachBatch`) evita escribirlo dos veces. Eso es **exactamente una vez** de punta a punta.

# COMMAND ----------

for c in ("silver", "silver_ecommerce"):
    print(f"\n{CHECKPOINTS}/{c}/")
    for f in dbutils.fs.ls(f"{CHECKPOINTS}/{c}"):
        print("   ", f.name)

ultimo = sorted(int(f.name) for f in dbutils.fs.ls(f"{CHECKPOINTS}/silver_ecommerce/offsets") if f.name.isdigit())[-1]
print(f"\nÚltimo offset de silver_ecommerce (lote {ultimo}):")
print(dbutils.fs.head(f"{CHECKPOINTS}/silver_ecommerce/offsets/{ultimo}"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Reglas de oro de los checkpoints:**
# MAGIC 1. **Un checkpoint por consulta.** Nunca compartirlo entre dos consultas.
# MAGIC 2. Guárdalo en un **volumen de Unity Catalog** (como hoy), junto a los datos que protege.
# MAGIC 3. Si cambias la **lógica con estado** (otra ventana, otro watermark, otra clave), usa un checkpoint **nuevo**;
# MAGIC    un checkpoint viejo no es compatible con un estado distinto.
# MAGIC 4. Borrar un checkpoint = **reprocesar todo desde el principio**. Hazlo a propósito, nunca por accidente.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Checklist para producción
# MAGIC
# MAGIC | Tema | Recomendación |
# MAGIC |---|---|
# MAGIC | **Dónde corre** | Un **job** de Lakeflow Jobs (no un notebook abierto), con **reintentos** ilimitados y alertas por correo o Slack |
# MAGIC | **Cómputo** | Clúster de job de **tamaño fijo** (sin autoscaling) para streams continuos; serverless + `availableNow` para incrementales |
# MAGIC | **Estado grande** | RocksDB como state store y watermarks acordes al negocio |
# MAGIC | **Monitoreo** | Retraso de la fuente (*backlog*), duración de los lotes vs. el trigger, filas descartadas por watermark |
# MAGIC | **Calidad** | Cuarentena con motivo (como hicimos) y alertas si crece |
# MAGIC | **Gobierno** | Tablas y volúmenes en Unity Catalog: permisos, linaje de punta a punta y auditoría |
# MAGIC | **Costo** | ¿De verdad necesita segundos? Si basta con cada 15 min, `availableNow` en un job programado cuesta una fracción |
# MAGIC
# MAGIC ### El mismo flujo, declarativo: Lakeflow Spark Declarative Pipelines
# MAGIC Lo que hicimos a mano (checkpoints, reinicios, dependencias entre capas) lo gestiona Databricks si declaras
# MAGIC **qué** tablas quieres. Así se verían Silver y las ventanas como pipeline en SQL:
# MAGIC
# MAGIC     CREATE OR REFRESH STREAMING TABLE silver_transacciones (
# MAGIC       CONSTRAINT monto_valido EXPECT (monto_local > 0) ON VIOLATION DROP ROW
# MAGIC     ) AS SELECT event_id, to_timestamp(event_time) AS event_time, tarjeta_id, pais, canal, moneda,
# MAGIC               try_cast(monto AS DECIMAL(18,2)) AS monto_local
# MAGIC        FROM STREAM read_files('/Volumes/.../landing/transacciones', format => 'json');
# MAGIC
# MAGIC     CREATE OR REFRESH STREAMING TABLE ventas_por_minuto AS
# MAGIC     SELECT window.start AS ventana_inicio, pais, count(*) AS transacciones
# MAGIC     FROM STREAM(silver_transacciones) WATERMARK event_time DELAY OF INTERVAL 1 MINUTE
# MAGIC     GROUP BY window(event_time, '1 minute'), pais;
# MAGIC
# MAGIC Las **expectations** reemplazan la cuarentena manual y sus métricas quedan en el event log del pipeline.
# MAGIC Structured Streaming (lo de hoy) da **control total**; los pipelines declarativos dan **menos código y menos
# MAGIC operación**. Ambos corren sobre el mismo motor.
# MAGIC
# MAGIC ### ¿Y Kafka o Event Hubs?
# MAGIC Solo cambia la **fuente**: `spark.readStream.format("kafka").option("kafka.bootstrap.servers", ...)` en lugar
# MAGIC de `cloudFiles`. Ventanas, watermarks, joins y checkpoints funcionan **igual**. Con Kafka o Event Hubs también
# MAGIC está disponible **Real-Time Mode** para latencias de milisegundos. Para enviar eventos directo a una tabla
# MAGIC sin un broker existe **Zerobus Ingest** (Lakeflow Connect).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — Apagar todo
# MAGIC Al terminar el taller:
# MAGIC 1. En el notebook **`02`**: ejecuta `detener_consultas()` (o el botón *Stop* de cada consulta).
# MAGIC 2. En el notebook **`01`**: widget **detener** = `si` y ejecuta el Paso 2.
# MAGIC 3. Si nadie más usa el clúster, termínalo.
# MAGIC
# MAGIC Esta celda detiene las consultas de **este** notebook (cada notebook tiene su propia sesión) y muestra cuáles
# MAGIC siguen activas aquí.

# COMMAND ----------

detener_consultas()
print("Consultas activas en este notebook:", [q.name for q in spark.streams.active] or "ninguna")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC - El **trigger** fija la latencia y el costo; el código es el mismo.
# MAGIC - El **checkpoint** (offsets + commits + estado) da reinicios **sin pérdida ni duplicados**.
# MAGIC - En producción: un **job** con reintentos, monitoreo de backlog y, si conviene, **pipelines declarativos**.
# MAGIC
# MAGIC **Tarea:** `06_reto`, detectar el "viaje imposible" de una tarjeta.
