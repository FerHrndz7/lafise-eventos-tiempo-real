# Glosario — Taller de Procesamiento de Eventos en Tiempo Real

| Término | Definición |
|---|---|
| **Evento** | Un hecho que ocurre en un momento: una compra con tarjeta, un retiro en cajero, un login en la app. |
| **Stream (flujo)** | Una secuencia de eventos que no termina: siempre pueden llegar más. |
| **Structured Streaming** | El motor de Spark para procesar streams con la misma API de DataFrames y SQL que el batch. |
| **Micro-lote** | Un grupo pequeño de eventos nuevos que Spark procesa de una vez, cada N segundos. |
| **Trigger** | Cada cuánto se ejecuta un micro-lote: `processingTime="5 seconds"`, o `availableNow` (procesar lo pendiente y detenerse). |
| **Real-Time Mode** | Modo de Structured Streaming (GA) con latencia de milisegundos, sin micro-lotes; fuentes Kafka, Event Hubs y Kinesis. |
| **Fuente (source)** | De dónde lee el stream: archivos (Auto Loader), Kafka, Event Hubs, Kinesis, una tabla Delta, el rate source. |
| **Destino (sink)** | Dónde escribe el stream: una tabla Delta, archivos, Kafka, `foreachBatch`, la memoria (solo para pruebas). |
| **Rate source** | Fuente de Spark que emite N filas por segundo; la usamos para simular la llegada de transacciones. |
| **Auto Loader** | `format("cloudFiles")`: detecta e ingiere archivos nuevos de una carpeta, cada uno exactamente una vez. |
| **Volumen (UC)** | Carpeta gobernada por Unity Catalog para archivos (aquí: `landing` y `checkpoints`). |
| **Checkpoint** | Carpeta donde una consulta guarda su avance (offsets, commits y estado). Permite reiniciar sin perder ni duplicar. |
| **Exactamente una vez** | Garantía de que cada evento afecta el resultado una sola vez, aunque haya fallas y reintentos. |
| **Medallion (Bronze / Silver / Gold)** | Capas de datos: crudo tal como llegó / limpio y validado / agregado para el negocio. |
| **Cuarentena** | Tabla donde van los registros inválidos con el motivo del rechazo, para revisarlos. |
| **`rescuedDataColumn`** | Columna donde Auto Loader guarda los campos inesperados en lugar de perderlos. |
| **Tiempo de evento** | Cuándo ocurrió el hecho (`event_time`), según el sistema origen. |
| **Tiempo de llegada / procesamiento** | Cuándo el evento llegó o se procesó en nuestro sistema. |
| **Evento tardío** | Un evento que llega mucho después de haber ocurrido (POS sin conexión, reintentos, colas atrasadas). |
| **Ventana fija (tumbling)** | Intervalos de tiempo sin traslape: 10:00–10:01, 10:01–10:02… Cada evento cae en una. |
| **Ventana deslizante (sliding)** | Ventanas que se traslapan ("últimos 5 min, cada 1 min"); un evento cae en varias. |
| **Watermark** | Umbral de cuánto esperar a los eventos tardíos: `max(event_time) − retraso tolerado`. Cierra ventanas y libera estado. |
| **Estado (state)** | Lo que el stream recuerda entre micro-lotes (conteos de ventanas, `event_id` vistos, lado de un join). |
| **RocksDB** | Almacén de estado en disco que usa Databricks por defecto para streams con estado grande. |
| **Modo append** | Solo escribe resultados finales (una ventana cuando se cierra). |
| **Modo update** | Escribe las filas que cambiaron en cada micro-lote; se combina con MERGE para corregir resultados. |
| **Modo complete** | Reescribe todo el resultado en cada micro-lote; solo para resultados pequeños. |
| **`foreachBatch`** | Permite tratar cada micro-lote como un DataFrame normal: escribir a varias tablas, hacer MERGE, etc. |
| **`txnAppId` / `txnVersion`** | Opciones de escritura Delta que hacen idempotente un `foreachBatch` (ignora un lote repetido). |
| **MERGE** | Sentencia que inserta o actualiza filas según una condición (upsert). |
| **`dropDuplicatesWithinWatermark`** | Deduplicación en streaming que recuerda las claves solo mientras el watermark lo permite. |
| **Join stream-static** | Unir el stream con una tabla de referencia; la tabla Delta se relee en cada micro-lote. |
| **Join stream-stream** | Unir dos streams; ambos lados guardan estado y requieren watermark y una condición de tiempo. |
| **`broadcast`** | Copiar una tabla pequeña a todos los nodos para unir sin mover el stream por la red. |
| **`transformWithState`** | API para lógica con estado personalizada (por clave, con temporizadores); DBR 16.2+. |
| **Backlog** | Datos que llegaron a la fuente y el stream aún no procesa; si crece, el stream se atrasa. |
| **Lakeflow Spark Declarative Pipelines** | Pipelines declarativos de Databricks (antes Delta Live Tables): defines las tablas y Databricks gestiona checkpoints, dependencias y calidad. |
| **Expectations** | Reglas de calidad de un pipeline declarativo (`EXPECT ... ON VIOLATION DROP ROW`). |
| **Lakeflow Jobs** | Orquestador de Databricks para correr notebooks y pipelines programados o continuos, con reintentos. |
| **Zerobus Ingest** | Servicio de Lakeflow Connect para enviar eventos directo a una tabla, sin un broker de mensajes. |
| **Kafka / Event Hubs / Kinesis** | Brokers de mensajes donde los sistemas publican eventos; fuentes típicas de streaming en producción. |
| **CDC** | *Change Data Capture*: capturar los cambios (inserciones, actualizaciones, borrados) de una base de datos como eventos. |
| **MCC** | *Merchant Category Code*: código que clasifica el giro de un comercio (supermercado, gasolinera…). |
| **Viaje imposible** | Patrón de fraude: la misma tarjeta usada en países distintos en un lapso en que no es posible viajar. |
