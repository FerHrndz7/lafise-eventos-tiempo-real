# RUNBOOK — LAFISE · Taller de Procesamiento de Eventos en Tiempo Real

**Audiencia:** equipos de datos, analítica e ingeniería de LAFISE. **Duración:** 120 min = 15 min de presentación +
~95 min de práctica + 10 min de cierre. **Cómputo:** clúster clásico DBR 17.3 LTS (Standard o Dedicated).

> **Nombres y capacidades verificados con la documentación (11 de noviembre de 2026):**
> - En **serverless solo existe el trigger `availableNow`**; `processingTime` (micro-lotes cada N s) requiere
>   cómputo clásico. Por eso el taller es en clásico.
> - En DBR 17.3 Standard funcionan: rate source, Auto Loader, `foreachBatch`, `dropDuplicatesWithinWatermark`,
>   joins stream-static y stream-stream, sink en memoria, `lastProgress`/`recentProgress`.
> - **Lakeflow Spark Declarative Pipelines** = nombre actual de Delta Live Tables.
> - **Real-Time Mode** de Structured Streaming: GA en clásico (DBR 16.4+), fuentes Kafka / Event Hubs / Kinesis (no Delta).
> - **Zerobus Ingest** (Lakeflow Connect): GA. **`transformWithState`**: DBR 16.2+.

## 1. Requisitos del workspace (validar con `validacion/validacion_entorno_lafise_eventos_rt.py`)

Enviar el `.py` a los organizadores **antes** del taller. Lo importan (*Workspace → Import → File*), lo adjuntan al
clúster clásico del curso, escriben el catálogo en el widget y hacen **Run all** (~5 min), idealmente con un
usuario **sin permisos de admin**.

| # | Requisito | Si falla |
|---|---|---|
| 1 | Clúster clásico DBR 17.3 LTS | Bloqueante: en serverless no hay `processingTime` |
| 2 | Modo de acceso Standard o Dedicated | REVISAR si no se puede leer; confirmar en la configuración del clúster |
| 3 | `USE CATALOG` + `CREATE SCHEMA` en el catálogo (crea schema, volumen y tabla) | `GRANT USE CATALOG, CREATE SCHEMA ON CATALOG <cat> TO <grupo>` |
| 4 | Rate source → archivos JSON en un volumen con `processingTime` | Bloqueante (notebook `01`) |
| 5 | Auto Loader desde un volumen a una tabla Delta | Bloqueante (notebook `02`) |
| 6 | `foreachBatch` + MERGE + `dropDuplicatesWithinWatermark` | Bloqueante (`02` y `03`) |
| 7 | Join stream-static + ventana con watermark (append) | Bloqueante (`03` y `04`) |
| 8 | Sink en memoria | ALTERNATIVA: solo el Paso 5 del `03` |
| 9 | Trigger `availableNow` | Bloqueante solo para el `05` |
| 10 | Métricas de progreso | REVISAR: los notebooks las usan para esperar y mostrar avance |

**Permisos que pide el taller:** `USE CATALOG` + `CREATE SCHEMA` en el catálogo. Cada participante es dueño de su
schema `eventos_rt_<usuario>` (tablas + volúmenes `landing` y `checkpoints`). **No instala librerías.**

**Tamaño del clúster:** cada participante mantiene ~3 consultas activas (generador, Bronze, Silver) más 2–3 por
notebook. Recomendación: clúster Standard de **tamaño fijo** (sin autoscaling para streaming), ~2 núcleos por
participante (p. ej. 10 workers de 4 núcleos para 20 personas), o un clúster por cada 5–8 personas. La prueba de punta
a punta se hizo con 1 participante en 2–8 workers D4ds_v5.

## 2. Importación

`build/lafise-eventos-tiempo-real.dbc` (un clic: *Workspace → Home → Import → File*) o
`databricks workspace import-dir lafise-eventos-tiempo-real <ruta>`. Reconstruir el `.dbc` tras cualquier cambio:
`python3 build/build_dbc.py`.

## 3. Día de la sesión

Ver `es/guia-rapida.md`. Puntos clave:
- El `01` (generador) y el `02` (Bronze/Silver) **quedan corriendo** en sus pestañas; los demás leen de Silver.
- La tabla final del `03` tarda ~2 min en tener ventanas; las alertas de velocidad del `04`, ~3 min.
- Al final: `detener_consultas()` en el `02` y widget `detener = si` en el `01`.

## 4. Costos y limpieza

Todo corre en el clúster clásico del curso; no hay endpoints ni warehouses. Mientras las consultas con
`processingTime` estén activas, el clúster no se apaga por inactividad: **detenerlas al terminar** y terminar el
clúster si nadie más lo usa. Los datos por participante son pequeños (decenas de miles de filas por hora).
Para borrar todo de un participante: `00_setup` con `reiniciar = si` (borra su schema completo).

## 5. Prueba de punta a punta — Azure Databricks, DBR 17.3 LTS clásico Standard (Photon), 2026-11-11

Ejecución de todos los notebooks como jobs sobre un clúster existente (2–8 × D4ds_v5, modo Standard), con el `01` y
el `02` en `mantener_min` > 0 para que siguieran vivos mientras corrían los demás. **Corrida limpia final** (schema
regenerado desde cero con `reiniciar = si`):

| Notebook | Resultado |
|---|---|
| `00_setup` | ✅ Schema, volúmenes, `clientes` (2,000), `comercios` (500), `limites_segmento`, `tipo_cambio` |
| `01_generador` | ✅ ~20 eventos/s, un archivo JSON cada 2 s, duplicados visibles |
| `02_ingesta` | ✅ Silver sin duplicados ni inválidos; cuarentena con 3 motivos; retraso: 91% < 15 s, ~7% tardíos, ~1% muy tardíos |
| `03_ventanas` | ✅ 18 eventos tardíos descartados (17 según la métrica de la consulta); 23 ventanas en vivo corregidas por MERGE; ventanas deslizantes en memoria |
| `04_enriquecimiento` | ✅ VELOCIDAD para las **5/5 tarjetas en ráfaga y ninguna otra**; 53 MONTO_SOBRE_LIMITE; UPDATE de NIO tomado en vivo (72 filas nuevas con 40.00); 100% de comercios del mismo país |
| `05_operacion` | ✅ `availableNow`: 2.ª corrida +129 filas, 0 duplicados; checkpoint inspeccionado |
| `06_reto` (solución) | ✅ VIAJE_IMPOSIBLE para las **5/5 tarjetas viajeras y ninguna otra** |
| Validación de organizadores | ✅ 9 PASA + 1 REVISAR (modo de acceso) → corregido para leerlo con el SDK (USER_ISOLATION → PASA) |
| `.dbc` | ✅ 11 notebooks, mismo árbol que el fuente |

**Hallazgos (incorporados al paquete):**
1. **`StreamingQueryProgress` en DBR 17.3:** hereda de `dict`, pero `p.get("numInputRows")` devuelve `None`; el valor
   está en el **atributo** `p.numInputRows`. Lo mismo con `inputRowsPerSecond` y los campos de `stateOperators`.
   Los helpers usan `metrica(obj, nombre)` (atributo primero, `get` de respaldo).
2. **`dedupeWithinWatermark` reporta `numRowsTotal = 0`** en las métricas aunque sí guarda estado y deduplica; las
   agregaciones con ventana sí lo reportan. El texto del `02` lo explica.
3. **Retraso medido con la hora del archivo** (`_metadata.file_modification_time`), no con `ingesta_ts`: si Bronze
   arranca minutos después del generador, todo el backlog parecía tardío (66% en el primer pase).
4. **Rate source y reinicios:** se puede cambiar `rowsPerSecond` y reanudar desde el mismo checkpoint. Tras una pausa,
   el primer lote trae de golpe todas las filas del tiempo detenido (2,911 tras 20 s a 100/s), con su hora original:
   río abajo se ven como tardías. Documentado en el `01` y en la guía.
5. **Umbral de la alerta de velocidad:** con ≥5 transacciones en 2 min habría ~25 falsos positivos por minuto entre
   ~2,000 tarjetas normales (λ ≈ 1.15). Se usan 5 tarjetas en ráfaga (~14 por ventana) y umbral ≥10; las viajeras
   bajaron a 0.5% del tráfico para no cruzar ese umbral.
6. El sink en **memoria no admite recuperar desde checkpoint** (por eso el `03` lo usa sin checkpoint y solo para
   explorar).
7. RocksDB ya es el state store por defecto en DBR 17.3 (se ve en el `offsets` del checkpoint, notebook `05`).
