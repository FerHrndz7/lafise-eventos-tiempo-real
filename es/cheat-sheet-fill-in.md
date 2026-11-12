# Cheat sheet — respuestas de los `<FILL_IN>` (Taller de Eventos en Tiempo Real)

Los notebooks `00`–`05` traen el **código completo**. El único notebook con `<FILL_IN>` es **`06_reto`** (tarea).
Esta es su clave de respuestas (la misma de `solutions/06_reto_solution`).

> Recordatorio: widget `catalog` = el del taller; `schema` = dejar `eventos_rt`. El `06` necesita que sigan
> corriendo el generador (`01`) y el `02`.

| # | Qué se completa | Dificultad |
|---|---|---|
| 1 | Duración del watermark | Baja |
| 2 | Ventana deslizante de 5 min cada 1 min | Media |
| 3 | Conjunto de países distintos (`collect_set`) | Media |

## Respuestas

```python
viaje_imposible = (
    spark.readStream.table(SILVER)
    .withWatermark("event_time", "1 minute")                                    # 1
    .groupBy(F.window("event_time", "5 minutes", "1 minute"),                   # 2
             "tarjeta_id", "cliente_id")
    .agg(F.collect_set("pais").alias("paises"),                                 # 3
         F.count("*").alias("n"))
    .filter(F.size("paises") >= 2)
    ...
)
```

Por qué:
1. El watermark de 1 minuto cierra cada ventana poco después de que termina; los eventos que lleguen más tarde no
   cuentan para la alerta.
2. `F.window(col, "5 minutes", "1 minute")` evalúa "los últimos 5 minutos" cada minuto.
3. En streaming no existe `countDistinct`; `collect_set` devuelve los países sin repetir y `size()` los cuenta.

Lo que se valida: que haya alertas `VIAJE_IMPOSIBLE` y que **todas** sean de las tarjetas viajeras `TJ-00010` a
`TJ-00014` (ningún falso positivo). Errores típicos:
- Usar `F.collect_list` en lugar de `collect_set`: cuenta países repetidos y alerta a todas las tarjetas activas.
- Olvidar el watermark: Spark rechaza el modo append con una agregación sin watermark.
- Ventana fija (`F.window("event_time", "5 minutes")`): funciona, pero detecta menos casos (los que cruzan el
  borde de la ventana).
