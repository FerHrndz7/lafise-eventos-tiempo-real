# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Preparación — Procesamiento de eventos en tiempo real
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Duración** | ~95 min de práctica (esta preparación: ~8 min) |
# MAGIC | **Nivel** | Intermedio |
# MAGIC | **Lenguaje** | Python (PySpark Structured Streaming) |
# MAGIC | **Cómputo** | Clúster **clásico** (DBR **17.3 LTS**, modo de acceso *Standard* o *Dedicated*) |
# MAGIC | **Catalog.Schema** | `<catálogo_asignado>.eventos_rt_<tu_usuario>` |
# MAGIC
# MAGIC ## Qué construirás hoy 🧱
# MAGIC Las transacciones con tarjeta de LAFISE llegan **una por una, todo el día**, desde 5 países, por POS, comercio
# MAGIC electrónico, cajeros y la app. Hoy construyes el flujo que las procesa **mientras llegan**:
# MAGIC
# MAGIC 1. `00_setup` — tu schema, los volúmenes y las tablas de referencia (**estás aquí**).
# MAGIC 2. `01_generador_tiempo_real` — arranca un generador que escribe transacciones nuevas **cada 2 segundos**,
# MAGIC    con eventos tardíos, duplicados y registros inválidos a propósito.
# MAGIC 3. `02_ingesta_y_transformaciones` — Auto Loader → **Bronze**; limpieza, deduplicación y cuarentena → **Silver**.
# MAGIC 4. `03_ventanas_y_eventos_tardios` — **ventanas** de tiempo, **watermarks** y qué pasa con los eventos tardíos.
# MAGIC 5. `04_enriquecimiento` — une el stream con clientes, comercios y tipo de cambio, y genera **alertas**.
# MAGIC 6. `05_operacion` — triggers, checkpoints, reinicios y monitoreo para producción.
# MAGIC 7. `06_reto` — **tarea**: detectar el "viaje imposible" de una tarjeta.
# MAGIC
# MAGIC ## Objetivos de aprendizaje
# MAGIC - Leer un flujo continuo de archivos con **Auto Loader** y escribirlo en Delta con **checkpoints**.
# MAGIC - Transformar, validar y **deduplicar** eventos en streaming.
# MAGIC - Agregar por **ventanas** de tiempo de evento y controlar los **eventos tardíos** con watermarks.
# MAGIC - **Enriquecer** el stream con tablas de referencia y generar alertas.
# MAGIC - Operar el flujo: triggers, reinicios y métricas de progreso.
# MAGIC
# MAGIC > ⚠️ **Datos sintéticos.** Clientes, tarjetas, comercios y montos son inventados. Las tasas de cambio y los
# MAGIC > límites son **didácticos**. Nunca uses datos reales de clientes en un workspace de laboratorio.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 1 — Configuración (aislamiento por participante)
# MAGIC Escribe en el widget **catalog** el catálogo que te asignó el facilitador. Tu schema será
# MAGIC `eventos_rt_<tu_usuario>`, así que nadie pisa los datos de otro. El widget **reiniciar** borra todo tu schema
# MAGIC (tablas, volúmenes y checkpoints) para empezar de cero: úsalo solo si algo quedó en mal estado.

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "", "Catálogo asignado (REQUERIDO)")
dbutils.widgets.text("schema", "eventos_rt", "Schema base")
dbutils.widgets.dropdown("reiniciar", "no", ["no", "si"], "¿Borrar todo y empezar de cero?")

user_email = spark.sql("SELECT current_user()").collect()[0][0]
user_prefix = re.sub(r"[^a-z0-9]", "_", user_email.split("@")[0].lower())
CATALOG = dbutils.widgets.get("catalog").strip()
assert CATALOG, "Escribe en el widget 'catalog' el catálogo que te asignó el facilitador y vuelve a ejecutar."
assert "." not in CATALOG, "El nombre del catálogo no puede tener puntos."
SCHEMA = f"{(dbutils.widgets.get('schema').strip() or 'eventos_rt')}_{user_prefix}"
FQ = f"`{CATALOG}`.{SCHEMA}"
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing/transacciones"
CHECKPOINTS = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

print(f"Usuario:      {user_email}")
print(f"Destino:      {CATALOG}.{SCHEMA}")
print(f"Aterrizaje:   {LANDING}")
print(f"Checkpoints:  {CHECKPOINTS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 2 — Verificación del entorno
# MAGIC Structured Streaming con `processingTime` (micro-lotes cada N segundos) necesita **cómputo clásico**: en
# MAGIC serverless solo existe el trigger `availableNow` (procesar lo pendiente y detenerse). Por eso este taller
# MAGIC usa un clúster clásico.

# COMMAND ----------

import sys

print(f"Python: {sys.version.split()[0]}")
assert sys.version_info >= (3, 10), "Se requiere Python 3.10+"
try:
    dbr = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
    print(f"DBR del clúster: {dbr}")
    if not dbr.startswith(("17.", "18.")):
        print("⚠️  El laboratorio se probó en DBR 17.3 LTS. Otras versiones pueden comportarse distinto.")
except Exception:
    print("⚠️  No pude leer la versión del runtime. ¿Estás en serverless? Conecta el clúster clásico del curso.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 3 — Permisos, schema y volúmenes
# MAGIC Creamos tu schema y dos **volúmenes** de Unity Catalog:
# MAGIC - `landing`: donde el generador deja los archivos JSON (simula la llegada de eventos).
# MAGIC - `checkpoints`: donde cada consulta de streaming guarda **qué ya procesó**. Es lo que permite reiniciar
# MAGIC   sin perder ni duplicar datos.

# COMMAND ----------

if dbutils.widgets.get("reiniciar") == "si":
    for q in spark.streams.active:
        q.stop()
    spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")
    print(f"🧹 Borré {CATALOG}.{SCHEMA}. Detén también el generador (notebook 01) si estaba corriendo.")

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQ}.landing COMMENT 'Archivos JSON que llegan del generador'")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQ}.checkpoints COMMENT 'Checkpoints de las consultas de streaming'")
    dbutils.fs.mkdirs(LANDING)
    dbutils.fs.put(f"{CHECKPOINTS}/_prueba_escritura.txt", "ok", True)
    dbutils.fs.rm(f"{CHECKPOINTS}/_prueba_escritura.txt")
    print(f"✅ Schema y volúmenes listos en {CATALOG}.{SCHEMA}")
except Exception as e:
    raise PermissionError(
        f"No pude crear el schema o los volúmenes en {CATALOG}. Pide al administrador: USE CATALOG y CREATE SCHEMA "
        f"sobre el catálogo (o CREATE TABLE + CREATE VOLUME sobre un schema existente). Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 4 — Tablas de referencia
# MAGIC Clientes, comercios, límites por segmento y tipo de cambio. Son **tablas Delta normales** (estáticas): en el
# MAGIC notebook `04` las uniremos con el stream.

# COMMAND ----------

# MAGIC %run ./data/crear_dimensiones $catalog=$catalog $schema=$schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 5 — Resumen

# COMMAND ----------

print("=" * 64)
print(f"Catálogo.schema : {CATALOG}.{SCHEMA}")
print(f"Aterrizaje      : {LANDING}")
print(f"Checkpoints     : {CHECKPOINTS}")
print("Tablas          : clientes, comercios, limites_segmento, tipo_cambio")
print("=" * 64)
display(spark.sql(f"SELECT segmento, pais_residencia, count(*) AS clientes FROM {FQ}.clientes "
                  "GROUP BY ALL ORDER BY 1, 2"))
print("✅ Preparación completa. Sigue con 01_generador_tiempo_real.")
