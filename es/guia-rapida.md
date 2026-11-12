# Guía rápida del facilitador — Taller de Procesamiento de Eventos en Tiempo Real

**Duración:** 2 h (15 min de presentación + ~95 min de práctica + 10 min de cierre y preguntas) · **Idioma:** español ·
**Audiencia:** equipos de datos, analítica e ingeniería de LAFISE · **Cómputo:** clúster clásico DBR 17.3 LTS.

## Agenda (120 min)

| Bloque | Tiempo | Qué pasa |
|---|---|---|
| Presentación (tú) | 15 min | `facilitador/deck-intro/lafise-intro-eventos-tiempo-real.html` (9 diapositivas) |
| `00_setup` | 8 min | Schema, volúmenes y dimensiones. **Pre-warm:** que lo corran mientras presentas |
| `01_generador_tiempo_real` | 7 min | Arranca el generador. **Queda corriendo** en su pestaña |
| `02_ingesta_y_transformaciones` | 15 min | Bronze y Silver. **Queda corriendo** en su pestaña |
| `03_ventanas_y_eventos_tardios` | 22 min | El corazón del tema: ventanas, watermark, append vs. update, tardíos descartados |
| Pausa | 5 min | Buen momento: las ventanas del `03` se van cerrando mientras tanto |
| `04_enriquecimiento` | 20 min | Joins stream-static, alertas de monto y de velocidad, cambio del tipo de cambio en vivo |
| `05_operacion` | 10 min | `availableNow`, checkpoints, checklist de producción, pipelines declarativos, Real-Time Mode |
| Cierre | 10 min | Preguntas. Presentar el `06_reto` como **tarea**. Apagar todo (paso 5 del `05`) |

**Formato:** el código viene completo; cada quien ejecuta, lee y modifica. El único notebook con `<FILL_IN>` es el
`06_reto` (respuestas en `cheat-sheet-fill-in.md`).

## Guion de la presentación (15 min)

1. **Portada y manifiesto (2 min).** Una alerta vale lo que vale el momento en que llega. Preguntar al grupo: ¿qué
   procesos suyos hoy corren en batch nocturno y "deberían" ser en tiempo real? Anotar dos o tres para el cierre.
2. **El escenario (2 min).** ~20 transacciones/s, 5 países, 4 canales. El 10% llega tarde: POS sin conexión,
   reintentos, colas atrasadas. Los datos reales también llegan duplicados e inválidos.
3. **Tiempo de evento y watermark (4 min).** Es el concepto que más cuesta. Dibujarlo: una línea de tiempo con el
   evento más reciente, el watermark 1 min atrás, y una ventana que se cierra cuando el watermark la pasa. Recalcar:
   el watermark es una **decisión de negocio** (latencia vs. completitud vs. memoria).
4. **Lo que trae el generador (2 min).** Cada problema se resuelve en un notebook distinto: así se ve el mapa.
5. **¿Cuánto tiempo real? (3 min).** processingTime (hoy) vs. availableNow vs. Real-Time Mode vs. pipelines
   declarativos. Mensaje: el código es el mismo; cambiar la latencia es cambiar el trigger. La mayoría de los casos
   "en tiempo real" se resuelven con `availableNow` cada pocos minutos, a una fracción del costo.
6. **Agenda, flujo y logística (2 min).** Importar el `.dbc`, conectar el clúster clásico, widget `catalog`, y la
   regla de oro de hoy: **el `01` y el `02` se quedan abiertos** en sus pestañas.

## Secuencia recomendada en vivo

- **Durante la presentación:** que todos corran el `00_setup` (no necesita nada más).
- **Al terminar la presentación:** `01` y luego `02`, cada uno en su pestaña. Verificar que en el `01` el panel de la
  consulta diga *Running* y que el `02` llegue al Paso 5 con datos.
- **En el `03`:** la tabla final tarda ~2 min en tener ventanas (1 min de ventana + 1 min de watermark). Usar ese
  tiempo para explicar append vs. update con la tabla en vivo. El Paso 4 (tardíos descartados) es el momento clave.
- **Demostración opcional en el `01`:** subir `eventos_por_segundo` a `100` y re-ejecutar el Paso 2; en el `02`
  (Paso 7) se ve subir la tasa de entrada.
- **En el `04`:** las alertas de velocidad tardan ~3 min. El Paso 5 (UPDATE del tipo de cambio) siempre sorprende:
  el stream toma la tasa nueva sin reiniciarse.

## Watch-outs

- **"Esperando datos" en el `02`–`06`:** casi siempre el generador (`01`) se detuvo o la pestaña se desconectó.
  Revisar que su consulta esté activa; si no, re-ejecutar su Paso 2.
- **Pausar y reanudar el generador** produce una ráfaga: el rate source se pone al día con los segundos detenidos y
  esas filas llegan con su hora original, así que río abajo se ven como **tardías**. Es un buen ejemplo en vivo de
  la recuperación de un sistema origen, pero conviene avisarlo.
- **Cada notebook tiene su propia sesión:** `detener_consultas()` en un notebook no detiene las de otro. Para apagar
  todo: el `02` (`detener_consultas()`) y el `01` (widget `detener = si`).
- **Si alguien cambia la lógica con estado** (otra ventana o watermark) y re-ejecuta, puede fallar por un checkpoint
  incompatible. Solución rápida: borrar el checkpoint de esa consulta
  (`dbutils.fs.rm(f"{CHECKPOINTS}/<nombre>", True)`) y la tabla destino, o `00_setup` con `reiniciar = si`.
- **`filas_en_estado = 0` en Silver:** en DBR 17.3 el operador de deduplicación no reporta su estado en las
  métricas, aunque sí deduplica (la validación del `02` lo comprueba). Las ventanas del `03` sí lo muestran.
- **Clúster compartido:** cada participante corre ~3 consultas a la vez (generador, Bronze, Silver) más las de cada
  notebook. Con 20 personas en un mismo clúster Standard, usar uno de **tamaño fijo** con suficientes núcleos (ver
  RUNBOOK) o un clúster por grupo.
- **Al final, apagar todo.** Las consultas con `processingTime` mantienen el clúster ocupado mientras corran.
