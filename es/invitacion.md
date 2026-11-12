# Invitación — Taller de Procesamiento de Eventos en Tiempo Real

**Asunto:** Taller práctico: procesamiento de eventos en tiempo real con Databricks

Hola equipo,

Los invitamos a un taller práctico de **2 horas**, en español, sobre cómo procesar eventos **mientras ocurren**.
Trabajaremos con transacciones con tarjeta **sintéticas** de 5 países que se generan en vivo durante la sesión, y
construiremos el flujo que las limpia, las agrega por minuto, maneja las que llegan tarde, las enriquece con datos
de clientes y comercios, y dispara alertas.

**Qué veremos**
- Qué significa "tiempo real" y cuándo de verdad se necesita.
- Ingesta incremental de archivos con Auto Loader y la arquitectura Bronze / Silver en streaming.
- Transformaciones, validación, deduplicación y cuarentena de registros inválidos.
- Ventanas de tiempo, watermarks y qué hacer con los eventos tardíos.
- Enriquecimiento con tablas de referencia y alertas de comportamiento.
- Cómo operarlo en producción: triggers, checkpoints, costos y alternativas declarativas.

**Antes de la sesión**
- Tener acceso al workspace del taller y al **clúster clásico** del curso.
- Saber qué **catálogo** les asignaron (lo escribirán en el widget `catalog`).
- Navegador Chrome o Firefox (no Safari), sin modo Incógnito.
- Conocimientos básicos de Python o SQL ayudan, pero el código viene completo.

Todo el contenido usa datos sintéticos. Nos vemos en la sesión.
