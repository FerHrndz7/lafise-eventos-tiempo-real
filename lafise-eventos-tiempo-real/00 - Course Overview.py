# Databricks notebook source
# MAGIC %md
# MAGIC # LAFISE — Taller de Procesamiento de Eventos en Tiempo Real
# MAGIC ## Transformaciones, ventanas, enriquecimiento y eventos tardíos con Structured Streaming
# MAGIC
# MAGIC Las transacciones con tarjeta llegan **una por una, todo el día**, desde Nicaragua, Costa Rica, Honduras,
# MAGIC Panamá y República Dominicana. En este taller construyes, sobre datos **sintéticos** que se generan en vivo,
# MAGIC el flujo que las procesa **mientras llegan**: las limpia y deduplica, las agrega por minuto y país, maneja las
# MAGIC que llegan tarde, las enriquece con datos de clientes, comercios y tipo de cambio, y dispara alertas.
# MAGIC
# MAGIC > **Tecnología:** Spark **Structured Streaming** + **Auto Loader** + tablas **Delta** en **Unity Catalog**,
# MAGIC > sobre un **clúster clásico**. Al final vemos cómo se expresa lo mismo con **Lakeflow Spark Declarative
# MAGIC > Pipelines** y cuándo conviene **Real-Time Mode**.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Aviso
# MAGIC
# MAGIC <div style="border-left: 4px solid #f44336; background: #ffebee; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Solo laboratorio · datos sintéticos</strong>
# MAGIC <div style="color:#333;">
# MAGIC <ul>
# MAGIC <li>Ejecuta esta práctica únicamente en el <strong>workspace de laboratorio</strong> proporcionado.</li>
# MAGIC <li>Clientes, tarjetas, comercios y montos son <strong>sintéticos</strong>. <strong>Nunca</strong> uses datos reales de clientes de LAFISE.</li>
# MAGIC <li>Las tasas de cambio, los límites y las reglas de alerta son <strong>didácticos</strong>, no reglas de negocio ni de fraude reales.</li>
# MAGIC <li>Las consultas de streaming consumen el clúster mientras corren: <strong>detenlas al terminar</strong> (notebook <code>05</code>, paso 5).</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Recorrido de la sesión (2 horas)
# MAGIC
# MAGIC <div style="max-width: 1200px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 20px 24px; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC <style>.agenda-table td, .agenda-table th { font-size: 14pt !important; }</style>
# MAGIC <table class="agenda-table" style="width: 100%; border-collapse: collapse; line-height: 1.5;">
# MAGIC   <thead>
# MAGIC      <tr style="background: #1B5162; color: white;">
# MAGIC       <th style="padding: 10px 14px; text-align: center; border: 1px solid #EEEDE9; width: 60px;">#</th>
# MAGIC       <th style="padding: 10px 14px; text-align: center; border: 1px solid #EEEDE9; width: 90px;">Tiempo</th>
# MAGIC       <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">Notebook</th>
# MAGIC      </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC      <tr style="background: white;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">—</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">15 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Introducción del facilitador: qué es "tiempo real", tiempo de evento y watermarks</td></tr>
# MAGIC      <tr style="background: #FBFAF8;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">00</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">8 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Preparación: tu schema, volúmenes y tablas de referencia</td></tr>
# MAGIC      <tr style="background: white;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">01</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">7 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Generador de transacciones en tiempo real (queda corriendo)</td></tr>
# MAGIC      <tr style="background: #FBFAF8;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">02</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">15 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Ingesta y transformaciones: Auto Loader → Bronze → Silver, deduplicación y cuarentena</td></tr>
# MAGIC      <tr style="background: white;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">03</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">22 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Ventanas de tiempo y eventos tardíos: watermarks, append vs. update</td></tr>
# MAGIC      <tr style="background: #FBFAF8;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">04</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">20 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Enriquecimiento con clientes, comercios y tipo de cambio; alertas de monto y de velocidad</td></tr>
# MAGIC      <tr style="background: white;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">05</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">10 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Operación: triggers, checkpoints, reinicios y el camino a producción</td></tr>
# MAGIC      <tr style="background: #FBFAF8;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">06</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">Tarea</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Reto: alerta de "viaje imposible" (su solución está en <code>solutions/</code>)</td></tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC Empieza en `Laboratorio - Eventos en tiempo real/00_setup`. Los notebooks `00`–`05` traen el **código
# MAGIC completo**: lo ejecutas, lo lees y lo modificas. El `06_reto` es la tarea para después de la sesión.
# MAGIC
# MAGIC **Importante:** el `01` (generador) y el `02` (Bronze y Silver) **se quedan corriendo** en sus pestañas
# MAGIC mientras trabajas en el `03`, `04` y `05`. No los cierres hasta el final.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Requisitos
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <div style="color:#333;">
# MAGIC
# MAGIC - Conecta un **clúster clásico** (DBR **17.3 LTS**, modo de acceso *Standard* o *Dedicated*) a cada notebook.
# MAGIC   Usamos micro-lotes cada pocos segundos (`processingTime`), que en serverless no están disponibles.
# MAGIC - En el widget **catalog** escribe el catálogo que te asignó el facilitador.
# MAGIC - Necesitas `USE CATALOG` + `CREATE SCHEMA` en ese catálogo (o un schema propio con `CREATE TABLE` y `CREATE VOLUME`).
# MAGIC - No se instala ninguna librería: todo usa lo que ya trae el runtime.
# MAGIC - Usa **Chrome o Firefox** (no Safari), sin modo Incógnito.
# MAGIC - Todo se valida antes de la sesión con el notebook de validación de entorno.
# MAGIC
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. Todos los derechos reservados.
