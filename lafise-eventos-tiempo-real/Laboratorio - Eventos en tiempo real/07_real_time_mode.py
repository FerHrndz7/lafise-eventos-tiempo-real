# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · (Adicional) Real-Time Mode: alertas en milisegundos
# MAGIC
# MAGIC **Tiempo estimado:** 15–20 minutos · **Opcional**, después del taller
# MAGIC
# MAGIC ## Objetivos
# MAGIC - Ejecutar la **misma lógica de alertas** del notebook `04` con **Real-Time Mode** (RTM) en lugar de micro-lotes.
# MAGIC - Ver la diferencia: la alerta de velocidad sale **en la transacción número 10**, sin esperar a que cierre la ventana.
# MAGIC - Medir la latencia de punta a punta en **milisegundos** con las métricas propias de RTM.
# MAGIC - Conocer las reglas de RTM: clúster dedicado, fuentes y destinos soportados, y cómo conectarlo a **Event Hubs**.
# MAGIC
# MAGIC ## Concepto — micro-lotes vs. Real-Time Mode
# MAGIC
# MAGIC | | Micro-lotes (`processingTime`, notebooks 01–06) | **Real-Time Mode** (este notebook) |
# MAGIC |---|---|---|
# MAGIC | Cómo procesa | Junta eventos N segundos y los procesa juntos | Procesa **cada evento al llegar**, de forma continua |
# MAGIC | Latencia típica | Segundos | **Milisegundos** (puede bajar de 100 ms) |
# MAGIC | Fuentes | Archivos (Auto Loader), Delta, Kafka, Event Hubs… | Kafka, **Event Hubs**, Kinesis y el rate source (demos). **No** Delta ni Auto Loader |
# MAGIC | Destinos | Delta, `foreachBatch`, Kafka… | Kafka / Event Hubs o un escritor `foreach`. **No** Delta ni `foreachBatch` |
# MAGIC | Modo de salida | append, update, complete | Solo **update** |
# MAGIC | Cómputo | Clúster clásico (autoscaling posible) | Clúster clásico **de tamaño fijo, sin Photon ni spot** y con RTM activado |
# MAGIC | Cuándo usarlo | Tableros, alertas operativas, cargas incrementales | Fraude en la **autorización**, decisiones que no pueden esperar segundos |
# MAGIC
# MAGIC El código es casi el mismo: cambia el **trigger** (`.trigger(realTime=...)`), el **modo de salida** (`update`) y el
# MAGIC **destino**. RTM tiene un costo: el clúster queda dedicado y encendido todo el tiempo. Úsalo solo cuando el
# MAGIC negocio de verdad necesita milisegundos.
# MAGIC
# MAGIC > **Por qué no leemos de Silver:** RTM no admite tablas Delta como fuente. Por eso este notebook genera sus
# MAGIC > propios eventos con el **rate source** (la misma lógica del `01`), como si llegaran de Event Hubs. Al final
# MAGIC > está el código para conectarlo a un Event Hubs real.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Antes de empezar: un clúster para Real-Time Mode
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <div style="color:#333;">
# MAGIC
# MAGIC Este notebook **no** corre en el clúster del taller. Necesita uno propio con esta configuración:
# MAGIC
# MAGIC | Opción | Valor |
# MAGIC |---|---|
# MAGIC | Runtime | **DBR 17.3 LTS** (mínimo 16.4 LTS) |
# MAGIC | Modo de acceso | Standard o Dedicated (en Standard, RTM es solo para Python) |
# MAGIC | Photon | **Desactivado** |
# MAGIC | Autoscaling | **Desactivado**: tamaño fijo, **2 workers de 4 núcleos** para este notebook |
# MAGIC | Instancias spot | **Desactivadas** (en Azure: *On-demand*) |
# MAGIC | Spark config (Advanced options) | `spark.databricks.streaming.realTimeMode.enabled true` |
# MAGIC
# MAGIC **¿Por qué 8 núcleos?** En RTM todas las etapas corren **al mismo tiempo**: cada partición de cada etapa
# MAGIC ocupa un núcleo mientras la consulta vive. Este notebook usa pocas particiones, pero 8 núcleos dan holgura para
# MAGIC las dos consultas y para el arranque.
# MAGIC
# MAGIC También necesitas haber corrido el `00_setup` (usamos las tablas de referencia `clientes`, `limites_segmento`
# MAGIC y `tipo_cambio`). **No** hace falta que el generador del `01` esté corriendo.
# MAGIC
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "", "Catálogo asignado (REQUERIDO)")
dbutils.widgets.text("schema", "eventos_rt", "Schema base")
dbutils.widgets.text("eventos_por_segundo", "50", "Eventos por segundo")
dbutils.widgets.text("event_hubs_namespace", "", "Opcional: namespace de Event Hubs (Parte D)")

user_email = spark.sql("SELECT current_user()").collect()[0][0]
user_prefix = re.sub(r"[^a-z0-9]", "_", user_email.split("@")[0].lower())
CATALOG = dbutils.widgets.get("catalog").strip()
assert CATALOG, "Escribe en el widget 'catalog' el mismo catálogo del 00_setup y vuelve a ejecutar."
SCHEMA = f"{(dbutils.widgets.get('schema').strip() or 'eventos_rt')}_{user_prefix}"
FQ = f"`{CATALOG}`.{SCHEMA}"
CHECKPOINTS = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"
SALIDA_RTM = f"/Volumes/{CATALOG}/{SCHEMA}/landing/rtm_alertas"
EVENTOS_POR_SEGUNDO = int(dbutils.widgets.get("eventos_por_segundo"))

# COMMAND ----------

# MAGIC %run ./utils/streaming_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 0 — ¿El clúster tiene Real-Time Mode?

# COMMAND ----------

rtm_activo = spark.conf.get("spark.databricks.streaming.realTimeMode.enabled", "false").lower() == "true"
try:
    photon = "photon" in spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "").lower()
except Exception:
    photon = False
print(f"Real-Time Mode activado: {rtm_activo}")
print(f"Photon:                  {'sí (debe estar apagado)' if photon else 'no'}")
if not rtm_activo or photon:
    dbutils.notebook.exit("Este clúster no está configurado para Real-Time Mode: revisa la tabla de arriba.")
assert spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.clientes"), "Corre primero el 00_setup (tablas de referencia)."

# Pocas particiones: en RTM cada partición de cada etapa ocupa un núcleo todo el tiempo.
spark.conf.set("spark.sql.shuffle.partitions", "1")

# Cada corrida de este notebook empieza de cero: el rate source retomaría sus offsets (y sus horas) de una corrida
# anterior desde el checkpoint, y las latencias medirían ese atraso en lugar de la del pipeline.
for c in ("rtm_alertas_monto", "rtm_alertas_velocidad"):
    dbutils.fs.rm(f"{CHECKPOINTS}/{c}", True)
dbutils.fs.rm(SALIDA_RTM, True)
dbutils.fs.mkdirs(SALIDA_RTM)
print("✅ Clúster listo para Real-Time Mode")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Los eventos: el mismo generador, sin pasar por archivos
# MAGIC En el `01` los eventos se escribían como JSON y Auto Loader los leía. Aquí los leemos **directo** del rate
# MAGIC source, como llegarían de Event Hubs. Usamos las mismas reglas: las tarjetas `TJ-00000` a `TJ-00004` hacen
# MAGIC ráfagas de compras. Omitimos los inválidos y los tardíos: el foco de este notebook es la **latencia**.

# COMMAND ----------

from pyspark.sql import functions as F

PAISES = "array('NI','CR','HN','PA','DO')"
MONEDA_PAIS = "map('NI','NIO','CR','CRC','HN','HNL','PA','USD','DO','DOP')"
UNIDADES_POR_USD = "map('NIO',36.60D,'CRC',505.00D,'HNL',24.70D,'USD',1.00D,'DOP',59.00D)"

eventos = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", EVENTOS_POR_SEGUNDO)
    .option("numPartitions", 1)
    .load()
    .selectExpr("timestamp AS event_time", "value AS v")
    .withColumn("perfil", F.expr("IF(pmod(abs(hash(v,'hot')),100) < 3, 'rafaga', 'normal')"))
    .withColumn("n_cliente", F.expr("IF(perfil = 'rafaga', pmod(abs(hash(v,'hc')),5), 15 + pmod(abs(hash(v,'c')),1985))"))
    .withColumn("pais", F.expr(f"element_at({PAISES}, CAST(pmod(n_cliente,5) + 1 AS INT))"))
    .withColumn("moneda", F.expr(f"IF(pmod(abs(hash(v,'usd')),100) < 20, 'USD', element_at({MONEDA_PAIS}, pais))"))
    .withColumn("monto_usd_gen", F.expr("""round((pmod(abs(hash(v,'m')),20000) / 100.0 + 1)
                                                 * IF(pmod(abs(hash(v,'big')),100) = 0, 40, 1), 2)"""))
    .select(
        F.expr("concat('EV-RTM-', CAST(v AS STRING))").alias("event_id"),
        "event_time",
        F.expr("concat('TJ-', lpad(CAST(n_cliente AS STRING), 5, '0'))").alias("tarjeta_id"),
        F.expr("concat('CL-', lpad(CAST(n_cliente AS STRING), 5, '0'))").alias("cliente_id"),
        F.expr("element_at(array('POS','ECOMMERCE','ATM','APP'), CAST(pmod(abs(hash(v,'cn')),4) + 1 AS INT))").alias("canal"),
        "pais", "moneda",
        F.expr(f"round(monto_usd_gen * element_at({UNIDADES_POR_USD}, moneda), 2)").alias("monto_local"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — El destino: un escritor `foreach`
# MAGIC RTM no escribe en Delta. En producción el destino sería un tópico de **Event Hubs o Kafka** (ver Parte D) que
# MAGIC lee el sistema de autorizaciones o de casos. Para el laboratorio usamos un escritor `foreach`: una clase
# MAGIC Python con tres métodos que Spark llama **en los workers**:
# MAGIC - `open(partition_id, epoch_id)`: abre el destino (aquí, un archivo en el volumen).
# MAGIC - `process(row)`: recibe **cada alerta en cuanto se produce**; anotamos la hora exacta (`recibida_ms`).
# MAGIC - `close(error)`: cierra el destino.
# MAGIC
# MAGIC > En RTM los destinos `foreach` y Kafka garantizan **al menos una vez**: tras un reinicio, una alerta podría
# MAGIC > llegar dos veces. El sistema que las recibe debe tolerarlo (por ejemplo, usando `alerta_id` como clave).

# COMMAND ----------

class EscritorAlertas:
    """Escribe cada alerta como una línea JSON en el volumen, con su latencia en milisegundos."""

    def __init__(self, carpeta, tipo):
        self.carpeta, self.tipo = carpeta, tipo

    def open(self, partition_id, epoch_id):
        import uuid
        self.ruta = f"{self.carpeta}/{self.tipo}_p{partition_id}_e{epoch_id}_{uuid.uuid4().hex[:8]}.jsonl"
        self.archivo = open(self.ruta, "w")
        return True

    def process(self, row):
        import json, time
        d = row.asDict()
        d["recibida_ms"] = int(time.time() * 1000)          # hora de llegada al destino, en epoch ms
        d["latencia_ms"] = d["recibida_ms"] - d["evento_ms"]  # desde la transacción que disparó la alerta
        d["tipo_alerta"] = self.tipo
        self.archivo.write(json.dumps(d, default=str) + "\n")
        self.archivo.flush()

    def close(self, error):
        self.archivo.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Alerta sin estado: monto sobre el límite del segmento
# MAGIC El mismo enriquecimiento del `04`, con una diferencia de forma: en lugar de un join, convertimos cada tabla de
# MAGIC referencia en un **mapa en memoria** (`clave → valor`) que viaja con la consulta. Son tablas pequeñas (2,000
# MAGIC clientes, 4 segmentos, 5 monedas), y así cada evento se enriquece sin ninguna etapa adicional.
# MAGIC
# MAGIC > **Por qué no un join:** RTM admite joins stream-static con `broadcast`, pero en la prueba de este laboratorio
# MAGIC > (DBR 17.3, modo Standard) la consulta no arrancó por falta de slots al preparar el broadcast. Para tablas de
# MAGIC > referencia pequeñas, el mapa es más simple y no tiene ese problema.
# MAGIC >
# MAGIC > **La diferencia con el `04`:** allí el join releía `tipo_cambio` en cada micro-lote. Aquí el mapa se arma al
# MAGIC > **iniciar** la consulta: si cambia una tasa, hay que reiniciarla. Para referencias que cambian seguido, la
# MAGIC > alternativa en producción es publicar los cambios como otro stream o consultar un almacén de baja latencia
# MAGIC > (por ejemplo Lakebase) desde el escritor.
# MAGIC
# MAGIC Dos cambios respecto al `04`:
# MAGIC - `.trigger(realTime="30 seconds")`: **no** significa "cada 30 segundos". RTM procesa sin parar; ese tiempo es la
# MAGIC   duración de cada **lote largo**, al final del cual guarda su checkpoint y reporta métricas. Aquí usamos 30 s para
# MAGIC   ver resultados rápido; en producción se recomiendan 5 minutos o más.
# MAGIC - `.outputMode("update")`: el único modo que admite RTM.

# COMMAND ----------

def como_mapa(tabla, clave, valor):
    """Convierte una tabla de referencia pequeña en un mapa literal clave → valor (se arma al iniciar la consulta)."""
    filas = spark.table(f"{FQ}.{tabla}").select(clave, valor).collect()
    return F.create_map(*[F.lit(x) for f in filas for x in (f[0], f[1])])


segmento_de = como_mapa("clientes", "cliente_id", "segmento")
limite_de = como_mapa("limites_segmento", "segmento", "limite_usd_transaccion")
tasa_de = como_mapa("tipo_cambio", "moneda", "unidades_por_usd")


def enriquecer(df):
    return (df
            .withColumn("segmento", segmento_de[F.col("cliente_id")])
            .withColumn("limite_usd_transaccion", limite_de[F.col("segmento")])
            .withColumn("unidades_por_usd", tasa_de[F.col("moneda")])
            .withColumn("monto_usd", F.round(F.col("monto_local") / F.col("unidades_por_usd"), 2)))


alertas_monto = (
    enriquecer(eventos)
    .filter(F.col("monto_usd") > F.col("limite_usd_transaccion"))
    .select(F.col("event_id").alias("alerta_id"), "event_time", F.unix_millis("event_time").alias("evento_ms"),
            "tarjeta_id", "segmento", "pais", "canal", "monto_usd", "limite_usd_transaccion")
)

detener_consultas()
q_monto = (
    alertas_monto.writeStream
    .foreach(EscritorAlertas(SALIDA_RTM, "MONTO_SOBRE_LIMITE"))
    .outputMode("update")
    .trigger(realTime="30 seconds")
    .option("checkpointLocation", f"{CHECKPOINTS}/rtm_alertas_monto")
    .queryName("rtm_alertas_monto")
    .start()
)
print("⏳ Corriendo en Real-Time Mode. Las métricas aparecen al cerrar cada lote largo (cada ~30 s).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — La latencia, en milisegundos
# MAGIC Cada lote largo de RTM reporta tres latencias (percentiles P50, P90, P99):
# MAGIC
# MAGIC | Métrica | Qué mide |
# MAGIC |---|---|
# MAGIC | `processingLatencyMs` | Desde que la consulta lee el evento hasta que lo escribe |
# MAGIC | `sourceQueuingLatencyMs` | Desde que el evento llega a la fuente hasta que la consulta lo lee |
# MAGIC | `e2eLatencyMs` | De punta a punta (no incluye el tiempo de escritura del destino) |
# MAGIC
# MAGIC Mostramos el **segundo** lote largo: el primero incluye el arranque de la consulta y sale más alto.
# MAGIC Compáralo con el `04`: allí la consulta procesaba un micro-lote cada 5 segundos.

# COMMAND ----------

def latencias_rtm(q, lotes=2, timeout_s=300):
    """Espera `lotes` lotes largos de RTM y muestra las latencias (en ms) del último."""
    inicio = time.time()
    while time.time() - inicio < timeout_s:
        if q.exception():
            raise RuntimeError(f"La consulta {q.name} falló: {q.exception()}")
        # En DBR 17.3 las latencias vienen en "rtmMetrics": {"e2eLatencyMs": {"obj": [["P50", {"num": 0}], ...]}, ...}
        con_lat = [p for p in q.recentProgress if json.loads(p.json).get("rtmMetrics")]
        if len(con_lat) >= lotes:
            lat = json.loads(con_lat[-1].json)["rtmMetrics"]
            filas = []
            for m in ("e2eLatencyMs", "processingLatencyMs", "sourceQueuingLatencyMs"):
                pct = {k: float(v.get("num", 0)) for k, v in (lat.get(m) or {}).get("obj", [])}
                filas.append((m, pct.get("P50", 0.0), pct.get("P90", 0.0), pct.get("P99", 0.0)))
            display(spark.createDataFrame(filas, "metrica STRING, p50_ms DOUBLE, p90_ms DOUBLE, p99_ms DOUBLE"))
            return lat
        time.sleep(5)
    raise TimeoutError(f"{q.name}: sin métricas de latencia en {timeout_s}s")


lat_monto = latencias_rtm(q_monto)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — Alerta con estado: velocidad, en la transacción número 10
# MAGIC En el `04` la alerta de velocidad usaba modo **append**: Spark esperaba a que el watermark **cerrara** cada
# MAGIC ventana de 2 minutos para emitirla (unos 3 minutos de espera). En RTM el modo es **update**: cada vez que el
# MAGIC conteo de una tarjeta cambia, la fila se emite **al instante**. Con el filtro `n >= 10`, la alerta sale en el
# MAGIC momento en que llega la **décima** transacción, mientras la ventana sigue abierta.
# MAGIC
# MAGIC Detenemos la consulta anterior para liberar núcleos (cada consulta RTM ocupa los suyos todo el tiempo). La
# MAGIC dejamos correr **3 lotes largos (~1.5 min)**: una tarjeta en ráfaga necesita algo más de un minuto para llegar a
# MAGIC 10 compras dentro de su ventana de 2 minutos.

# COMMAND ----------

q_monto.stop()

alertas_velocidad = (
    eventos
    .withWatermark("event_time", "1 minute")
    .groupBy(F.window("event_time", "2 minutes"), "tarjeta_id")
    .agg(F.count("*").alias("n"), F.max("event_time").alias("ultimo_evento"),
         F.round(F.sum("monto_local"), 2).alias("monto_local_acumulado"))
    .filter("n >= 10")
    .select(F.concat_ws("-", "tarjeta_id", F.col("window.start").cast("string"), "n").alias("alerta_id"),
            "tarjeta_id", F.col("window.start").alias("ventana_inicio"), F.col("window.end").alias("ventana_fin"),
            F.unix_millis("window.end").alias("ventana_fin_ms"), "n",
            F.unix_millis("ultimo_evento").alias("evento_ms"), "monto_local_acumulado")
)

q_vel = (
    alertas_velocidad.writeStream
    .foreach(EscritorAlertas(SALIDA_RTM, "VELOCIDAD"))
    .outputMode("update")
    .trigger(realTime="30 seconds")
    .option("checkpointLocation", f"{CHECKPOINTS}/rtm_alertas_velocidad")
    .queryName("rtm_alertas_velocidad")
    .start()
)
lat_vel = latencias_rtm(q_vel, lotes=3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6 — Las alertas que escribió el destino
# MAGIC Leemos en batch los archivos del volumen:
# MAGIC - `latencia_ms`: desde la transacción que disparó la alerta (la décima, en las de velocidad) hasta que el
# MAGIC   escritor la recibió.
# MAGIC - `s_antes_del_cierre`: cuántos segundos **antes de que terminara la ventana** salió la alerta de velocidad. En
# MAGIC   el `04` (modo append) esa misma alerta salía **después** del cierre más 1 minuto de watermark.
# MAGIC
# MAGIC Los archivos de un lote se ven completos cuando ese lote largo termina.
# MAGIC
# MAGIC > **¿Por qué `latencia_ms` sale más alta que `e2eLatencyMs`?** La métrica de RTM no incluye el tiempo de
# MAGIC > escritura del destino, y aquí el destino es un archivo en el volumen (almacenamiento en la nube). En la prueba
# MAGIC > del laboratorio: `e2eLatencyMs` P50 de 0 a 27 ms y `latencia_ms` P50 de ~0.5 a 1.6 s. Con Event Hubs o Kafka
# MAGIC > como destino (Parte D), esa escritura es mucho más rápida.

# COMMAND ----------

q_vel.stop()
alertas = spark.read.json(SALIDA_RTM)
display(alertas.groupBy("tipo_alerta").agg(
    F.count("*").alias("alertas"),
    F.round(F.expr("percentile(latencia_ms, 0.5)"), 1).alias("latencia_p50_ms"),
    F.round(F.expr("percentile(latencia_ms, 0.9)"), 1).alias("latencia_p90_ms")))
display(alertas.filter("tipo_alerta = 'VELOCIDAD' AND n = 10")
        .withColumn("s_antes_del_cierre", F.round((F.col("ventana_fin_ms") - F.col("recibida_ms")) / 1000, 1))
        .select("tarjeta_id", "ventana_inicio", "ventana_fin", "n", "latencia_ms", "s_antes_del_cierre")
        .orderBy("ventana_inicio", "tarjeta_id"))

# COMMAND ----------

# Expected output: ✅ Alertas de velocidad en la transacción 10, solo para las tarjetas en ráfaga, en milisegundos
try:
    assert "n" in alertas.columns, "Aún no hay alertas de velocidad en el volumen; vuelve a correr el Paso 5 un poco más"
    vel = alertas.filter("tipo_alerta = 'VELOCIDAD'")
    tarjetas = {r[0] for r in vel.select("tarjeta_id").distinct().collect()}
    rafaga = {f"TJ-{n:05d}" for n in range(5)}
    assert tarjetas, "Aún no hay alertas de velocidad en el volumen; espera un minuto y vuelve a ejecutar la celda anterior"
    assert tarjetas <= rafaga, f"Alertas para tarjetas que no están en ráfaga: {sorted(tarjetas - rafaga)[:5]}"
    assert vel.filter("n = 10").count() > 0, "No hay alertas emitidas exactamente en la transacción 10"
    p50 = vel.selectExpr("percentile(latencia_ms, 0.5)").first()[0]
    print(f"✅ Alertas de velocidad para {len(tarjetas)} de las 5 tarjetas en ráfaga, emitidas en la transacción 10, "
          f"con latencia mediana de {p50:,.0f} ms")
except AssertionError as e:
    print("⏳", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte D — Conectarlo a Event Hubs (producción)
# MAGIC En LAFISE (Azure) la fuente real serían las transacciones publicadas en **Event Hubs**, que ofrece un endpoint
# MAGIC compatible con Kafka. Solo cambian la **fuente** y el **destino**; las transformaciones y alertas de arriba
# MAGIC quedan igual.
# MAGIC
# MAGIC Requisitos:
# MAGIC - Un namespace de Event Hubs (plan **Standard** o superior, que incluye Kafka) con dos event hubs:
# MAGIC   `transacciones` (entrada) y `alertas` (salida).
# MAGIC - La cadena de conexión guardada en un **secret scope** de Databricks (nunca en el notebook).
# MAGIC - Salida de red desde el clúster al puerto **9093** del namespace.
# MAGIC
# MAGIC Escribe el namespace en el widget **event_hubs_namespace** para ejecutar la celda; si está vacío, se omite.

# COMMAND ----------

NAMESPACE = dbutils.widgets.get("event_hubs_namespace").strip()
if not NAMESPACE:
    print("Parte D omitida: el widget event_hubs_namespace está vacío.")
else:
    conexion = dbutils.secrets.get(scope="event_hubs", key="cadena_conexion")
    kafka_opciones = {
        "kafka.bootstrap.servers": f"{NAMESPACE}.servicebus.windows.net:9093",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": ("kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
                                   f'username="$ConnectionString" password="{conexion}";'),
    }
    esquema = ("event_id STRING, event_time TIMESTAMP, tarjeta_id STRING, cliente_id STRING, canal STRING, "
               "pais STRING, moneda STRING, monto_local DOUBLE")
    desde_event_hubs = (
        spark.readStream.format("kafka").options(**kafka_opciones)
        .option("subscribe", "transacciones")
        .option("kafka.fetch.max.wait.ms", "50")     # menor espera en la fuente = menor latencia
        .load()
        .select(F.from_json(F.col("value").cast("string"), esquema).alias("e")).select("e.*")
    )
    q_eh = (
        enriquecer(desde_event_hubs)
        .filter(F.col("monto_usd") > F.col("limite_usd_transaccion"))
        .select(F.col("tarjeta_id").alias("key"), F.to_json(F.struct("*")).alias("value"))
        .writeStream.format("kafka").options(**kafka_opciones)
        .option("topic", "alertas")
        .outputMode("update")
        .trigger(realTime="5 minutes")
        .option("checkpointLocation", f"{CHECKPOINTS}/rtm_event_hubs")
        .queryName("rtm_event_hubs")
        .start()
    )
    print("✅ Leyendo de Event Hubs 'transacciones' y escribiendo alertas en 'alertas' en Real-Time Mode")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 7 — Apagar
# MAGIC Detén las consultas y **termina el clúster de RTM** si no lo vas a seguir usando: RTM no escala a cero.

# COMMAND ----------

detener_consultas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC - Con **Real-Time Mode** la misma lógica corre de forma continua y la latencia baja de segundos a
# MAGIC   **milisegundos**.
# MAGIC - Con modo **update**, la alerta de velocidad sale en la transacción 10, sin esperar a que cierre la ventana.
# MAGIC - Reglas clave: clúster dedicado (sin Photon, autoscaling ni spot), fuentes y destinos tipo Kafka / Event Hubs,
# MAGIC   referencias pequeñas como mapas en memoria, un solo agrupamiento por consulta y entrega **al menos una vez**
# MAGIC   en el destino.
# MAGIC - Elegir RTM es una decisión de **costo vs. latencia**: para la mayoría de los tableros y alertas operativas, los
# MAGIC   micro-lotes del taller alcanzan.
