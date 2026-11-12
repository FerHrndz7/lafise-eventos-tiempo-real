# Databricks notebook source
# MAGIC %md
# MAGIC # Utilidades del laboratorio (se cargan con `%run ./utils/streaming_utils`)
# MAGIC
# MAGIC Funciones de apoyo para **esperar** a que lleguen datos, **ver el progreso** de una consulta de streaming y
# MAGIC **detener** consultas. El código de streaming que se enseña (`readStream`, `writeStream`, ventanas, joins)
# MAGIC está escrito completo en cada notebook; aquí solo hay ayudas para no repetir código de monitoreo.

# COMMAND ----------

import json, time


def metrica(obj, nombre, defecto=None):
    """Lee un campo de un objeto de progreso. En DBR 17.3 algunos campos (numInputRows, inputRowsPerSecond…)
    son atributos calculados: `p.get("numInputRows")` devuelve None, pero `p.numInputRows` tiene el valor."""
    v = getattr(obj, nombre, None)
    if v is None and hasattr(obj, "get"):
        v = obj.get(nombre)
    return defecto if v is None else v


def consultas_activas(prefijo=None):
    """Consultas de streaming activas en ESTE notebook (cada notebook tiene su propia sesión)."""
    return [q for q in spark.streams.active if prefijo is None or (q.name or "").startswith(prefijo)]


def detener_consultas(prefijo=None):
    """Detiene las consultas activas de este notebook (todas, o las que empiezan con `prefijo`)."""
    for q in consultas_activas(prefijo):
        print(f"⏹  Deteniendo {q.name}")
        q.stop()


def esperar_progreso(q, lotes=1, timeout_s=180, con_datos=True):
    """Espera a que la consulta `q` procese `lotes` micro-lotes (con filas, si `con_datos`)."""
    inicio = time.time()
    while time.time() - inicio < timeout_s:
        if q.exception():
            raise RuntimeError(f"La consulta {q.name} falló: {q.exception()}")
        hechos = [p for p in q.recentProgress if not con_datos or metrica(p, "numInputRows", 0) > 0]
        if len(hechos) >= lotes:
            print(f"   {q.name}: {len(hechos)} micro-lote(s) procesados · último con "
                  f"{metrica(hechos[-1], 'numInputRows', 0):,} filas")
            return
        time.sleep(3)
    raise TimeoutError(f"{q.name}: no completó {lotes} micro-lote(s) con datos en {timeout_s}s. "
                       "¿Está corriendo el generador (notebook 01)?")


def esperar_filas(tabla, minimo=1, timeout_s=240):
    """Espera a que la tabla `tabla` tenga al menos `minimo` filas."""
    inicio = time.time()
    n = 0
    while time.time() - inicio < timeout_s:
        try:
            n = spark.table(tabla).count()
            if n >= minimo:
                print(f"   {tabla}: {n:,} filas")
                return
        except Exception:
            pass
        time.sleep(5)
    raise TimeoutError(f"{tabla}: {n} filas tras {timeout_s}s (esperaba ≥ {minimo}).")


def resumen_progreso(q, ultimos=5):
    """Tabla con los últimos micro-lotes de una consulta: filas, velocidad, duración y watermark."""
    filas = []
    for p in q.recentProgress[-ultimos:]:
        estado = metrica(p, "stateOperators", []) or []
        filas.append((
            metrica(p, "batchId"),
            metrica(p, "numInputRows", 0),
            round(float(metrica(p, "inputRowsPerSecond", 0)), 1),
            round(float(metrica(p, "processedRowsPerSecond", 0)), 1),
            (metrica(p, "durationMs", {}) or {}).get("triggerExecution"),
            (metrica(p, "eventTime", {}) or {}).get("watermark"),
            sum(metrica(s, "numRowsTotal", 0) for s in estado),
            sum(metrica(s, "numRowsDroppedByWatermark", 0) for s in estado),
        ))
    cols = "lote INT, filas_entrada LONG, filas_por_seg_llegada DOUBLE, filas_por_seg_procesadas DOUBLE, " \
           "duracion_ms LONG, watermark STRING, filas_en_estado LONG, descartadas_por_watermark LONG"
    display(spark.createDataFrame(filas, cols))


def descartadas_por_watermark(q):
    """Total de filas que esta consulta descartó por llegar después del watermark."""
    return sum(metrica(s, "numRowsDroppedByWatermark", 0)
               for p in q.recentProgress for s in (metrica(p, "stateOperators", []) or []))


def mantener_activo(minutos, consultas):
    """Para ejecuciones como job (pruebas automatizadas): mantiene vivas las consultas `minutos` y luego las detiene.
    En la sesión en vivo el widget vale 0 y esta función no hace nada: las consultas siguen en segundo plano."""
    if minutos <= 0:
        return
    print(f"Modo job: manteniendo {len(consultas)} consulta(s) activas {minutos} min")
    fin = time.time() + minutos * 60
    while time.time() < fin:
        for q in consultas:
            if q.exception():
                raise RuntimeError(f"La consulta {q.name} falló: {q.exception()}")
        time.sleep(15)
    for q in consultas:
        q.stop()
    print("Consultas detenidas")
