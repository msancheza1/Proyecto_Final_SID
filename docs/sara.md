# Procesamiento de Sara con Spark Structured Streaming

Este componente ejecuta transformaciones sobre los mensajes recibidos en Kafka.
No genera envíos ni devuelve alertas prefabricadas. El productor de Mariana sigue
siendo la fuente disponible; esos datos son ficticios. Una fuente empresarial
puede publicar el mismo contrato para procesar registros de operación real.

## Ejecutar

Con Docker Desktop iniciado, desde la carpeta del repositorio:

```powershell
docker compose --profile streaming up -d --build
docker compose logs -f spark
```

La construcción descarga Python, Java 17 y Spark 3.5.6. Al primer arranque se
descargan los conectores Kafka y Delta desde Maven; se requiere internet. El
servicio `spark-topics` crea los cuatro tópicos y termina con código 0.
Spark arranca por sí solo y espera datos. No ejecuta el simulador.

Si no hay un productor empresarial conectado, se pueden usar los mensajes ya
retenidos en Kafka o ejecutar el productor de Mariana para una prueba controlada:

```powershell
.\.venv\Scripts\python.exe -m ingestion simulate --scenario delay --steps 12
```

En otra terminal consulta las decisiones calculadas por Spark:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic operational_updates --from-beginning --timeout-ms 30000
```

Al agotar 30 segundos sin mensajes este consumidor puede mostrar TimeoutException.
Los mensajes previos siguen siendo válidos. Spark publica cuando cambia el evento
de envío, el GPS o la clasificación de riesgo; no publica repetidamente el mismo
estado sin cambios.

## Procesamiento

1. Lee `shipment_events` y `vehicle_gps`, conservando clave, tópico, partición,
   offset, JSON original y timestamp Kafka.
2. Valida contra el contrato de Mariana, incluida la coincidencia de clave y
   entidad. Rechaza fechas más de cinco minutos posteriores al timestamp Kafka.
   Normaliza espacios en identificadores y ciudades, UUID a minúsculas y ruta a
   mayúsculas. El contrato permite `source` empresarial y `scenario` es opcional;
   los eventos existentes del simulador siguen siendo compatibles.
3. Guarda entradas inválidas en cuarentena y mensajes válidos en Delta.
4. Deduplica por `event_id` dentro del lote y mediante MERGE persistente entre
   lotes y reinicios. El primer contenido aceptado para un UUID prevalece.
5. Selecciona el último evento por guía y por vehículo según tiempo del evento;
   eventos tardíos permanecen almacenados y no hacen retroceder el estado actual.
   En empate de fechas se usa event_id como desempate determinista.
6. Une guía con GPS por `vehicle_id` y `route_id`, y con el catálogo de rutas por
   `route_id`. Calcula distancia al destino, tiempo estimado y minutos sin movimiento.
7. Evalúa riesgo y publica actualizaciones en `operational_updates`, con clave
   `shipment_id`. Cada salida incluye UUID, eventos de origen y métricas trazables.

Se utiliza un join de estados actuales persistidos en cada microbatch, no un join
stream-stream que descarte coincidencias por watermark. La deduplicación global
persistida acepta eventos tardíos sin imponer una ventana de descarte. El coste es
que se vuelve a leer Silver para obtener estados actuales; es una implementación
académica que aún necesita optimización y pruebas de carga para 5.000 eventos/s.

## Reglas de riesgo

| Condición | Nivel |
| --- | --- |
| Entrega confirmada | Bajo y alerta resuelta |
| Promesa vencida y aún no entregado | Alto |
| Tiempo restante menor que tiempo estimado al destino | Alto |
| Vehículo con velocidad menor a 1 km/h y envío sin movimiento durante 15 minutos | Alto |
| GPS ausente o con más de 15 minutos de antigüedad | Medio |
| Ruta sin configuración | Medio |
| Envío sin movimientos durante 15 minutos | Medio |
| Ninguna condición anterior | Bajo |

La promesa vencida prevalece sobre las demás causas y la entrega confirmada resuelve
la alerta. El cálculo usa distancia Haversine y velocidad esperada del catálogo:
es una estimación geográfica, no una ruta vial ni un modelo predictivo. El catálogo
`config/routes.jsonl` incluye MED-BOG y debe ampliarse con las rutas de la fuente.
No se utiliza `scenario` para decidir el riesgo.

Un reloj técnico de Spark genera ticks para evaluar el paso del tiempo cuando Kafka
queda en silencio. Los ticks nunca se guardan ni publican como eventos logísticos.
Esto permite detectar una promesa vencida aunque no lleguen nuevos movimientos.

## Persistencia y recuperación

El volumen `spark_data` conserva `/data/sara`:

- `raw`: registros Kafka originales únicos por tópico/partición/offset.
- `rejected`: cuarentena con motivo de rechazo y datos originales.
- `silver_events`: eventos válidos únicos por event_id.
- `current_shipments`: estado enriquecido y evaluación más reciente que cambió.
- `checkpoints/main`: offsets y progreso del streaming y del reloj.

Los checkpoints recuperan el progreso tras reiniciar. Las escrituras Delta son
idempotentes mediante MERGE. No existe una transacción distribuida entre Kafka y
Delta: si cae después de publicar pero antes de actualizar el estado, puede reenviar
una salida. Conserva su UUID para deduplicación por el consumidor de Juan José.
Las métricas de `current_shipments` corresponden a la última actualización publicada.
Los ticks no provocan nuevas salidas solo por variar minutos o distancia sin cambios
en eventos o clasificación. Usa un único proceso Spark por checkpoint y volumen.

Comprueba la recuperación:

```powershell
docker compose stop spark
# Publica otro lote desde el productor de Mariana o tu fuente conectada.
docker compose --profile streaming start spark
docker compose logs --tail 100 spark
```

No borres volúmenes ni checkpoints para reiniciar. `failOnDataLoss=true` detiene
el job si Kafka perdió offsets por retención, en lugar de omitir datos silenciosamente.
Modificar el plan de fuentes requiere un checkpoint nuevo y un plan de reprocesamiento.

## Pruebas

Las pruebas rápidas de reglas se ejecutan sin Java ni Docker:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Prueba de integración con Spark y Delta reales, aislada en un directorio temporal:

```powershell
docker compose --profile streaming run --rm --no-deps spark tests/spark_integration.py
```

Verifica joins, deduplicación persistente, reejecución del mismo lote, JSON inválido,
eventos tardíos, contrato de salida y alertas cuando avanza el reloj sin movimientos.
Usa fixtures únicamente en la prueba; el servicio normal consume Kafka.
La prueba no publica mensajes Kafka ni altera las tablas operacionales.

## Alcance

Incluye la parte de Sara: validación, deduplicación, joins, reglas y checkpoints.
Las tablas Delta son persistencia técnica del procesamiento y punto de integración
con Emmanuel; falta su modelo dimensional Gold. MongoDB, Streamlit y Airflow quedan
para la siguiente fase de Juan José. No se implementaron en esta versión.

Referencias: [Spark Structured Streaming](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html),
[conector Kafka](https://spark.apache.org/docs/3.5.6/structured-streaming-kafka-integration.html)
y [MERGE en Delta Lake](https://docs.delta.io/delta-update/).
