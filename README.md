# LAFISE · Taller de Procesamiento de Eventos en Tiempo Real

Contenido del taller práctico (2 h, en español, cómputo clásico) sobre **procesamiento de eventos en tiempo real:
transformaciones, ventanas, enriquecimiento y manejo de eventos tardíos** con Spark Structured Streaming. Un
generador escribe transacciones con tarjeta **sintéticas** cada 2 segundos (5 países, 4 canales, con eventos
tardíos, duplicados e inválidos a propósito) y los participantes construyen el flujo que las procesa mientras
llegan: Auto Loader → Bronze → Silver, ventanas con watermark, joins con tablas de referencia y alertas.

## Contenido

- `lafise-eventos-tiempo-real/`
  - `00 - Course Overview`
  - `Laboratorio - Eventos en tiempo real/`: `00_setup` → `06_reto`, `07_real_time_mode` (adicional), `solutions/`, `data/`, `utils/`
- `build/lafise-eventos-tiempo-real.dbc`: archivo DBC para importar todo el árbol en un clic (`python3 build/build_dbc.py`).
- `validacion/validacion_entorno_lafise_eventos_rt.py`: validación del entorno para los **organizadores**
  (PASA / FALLA / ALTERNATIVA / REVISAR por requisito; borra lo que crea). Enviar antes del taller.
- `es/`: `invitacion.md`, `guia-rapida.md` (facilitador), `glosario.md`, `cheat-sheet-fill-in.md` (respuestas del reto).
- `facilitador/deck-intro/`: presentación de 15 min (abrir `lafise-intro-eventos-tiempo-real.html`; `assets/` y
  `fonts/` viajan con ella; reconstruir con `python3 build.py`).
- `RUNBOOK.md`: requisitos, importación, día de la sesión, costos y resultados de la prueba de punta a punta.

## Requisitos

Clúster clásico DBR 17.3 LTS (Standard o Dedicated) · Unity Catalog · en el catálogo: `USE CATALOG` + `CREATE SCHEMA`
(o un schema propio con `CREATE TABLE` + `CREATE VOLUME`). **Sin librerías externas** (no necesita salida a PyPI).

El notebook adicional `07_real_time_mode` necesita su propio clúster clásico con Real-Time Mode activado
(`spark.databricks.streaming.realTimeMode.enabled true`, sin Photon, sin autoscaling y sin spot; ver el notebook).

Todos los datos son sintéticos.
