# Proyecto Final SID

Esta guía explica cómo ejecutar la ingesta de envíos y GPS en Kafka y cómo procesar esos eventos con Spark Structured Streaming. Los comandos son para PowerShell en Windows. Ejecútalos desde la carpeta `Proyecto_Final_SID`, donde está `docker-compose.yml`.

## 1. Preparar el equipo

Abre Docker Desktop y espera a que esté iniciado. Para ejecutar el productor local, necesitas Python 3.11 o superior. Crea el entorno una sola vez; si ya existe `.venv`, puedes saltar la primera línea:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Spark y Java se ejecutan dentro de Docker. La primera construcción necesita internet para descargar la imagen, las dependencias y los conectores de Spark.

## 2. Ejecutar Kafka y Spark

```powershell
docker compose --profile streaming up -d --build
docker compose --profile streaming ps -a
docker compose logs -f spark
```

`kafka` y `spark` deben quedar en ejecución. `spark-topics` crea los tópicos y termina con código `0`. Espera a que Spark termine de iniciar; el primer lote puede tardar más que los siguientes. Usa `Ctrl+C` para salir de los logs sin detener el servicio.

Si solo quieres probar la ingesta, inicia Kafka y crea los tópicos sin Spark:

```powershell
docker compose up -d --wait kafka
.\.venv\Scripts\python.exe -m ingestion topics
```

## 3. Enviar y leer eventos

Spark puede procesar los eventos que ya estén retenidos en Kafka. Si necesitas datos nuevos para una prueba, el simulador genera guías y posiciones GPS ficticias. El procesamiento de Spark usa los mensajes publicados; no fabrica alertas.

```powershell
.\.venv\Scripts\python.exe -m ingestion simulate --shipments 3 --steps 12 --scenario normal
.\.venv\Scripts\python.exe -m ingestion consume --limit 20
```

Una corrida de tres guías y doce pasos genera 36 eventos de envío y 36 posiciones GPS. El consumidor lee desde el inicio de los datos retenidos: las primeras 20 filas pueden ser de un solo tópico o de corridas anteriores. Para generar un caso con promesa vencida, usa `--scenario delay`. Para publicar continuamente, usa `--steps 0` y detén el simulador con `Ctrl+C`.

| Tópico | Contenido | Clave |
| --- | --- | --- |
| `shipment_events` | Cambios de estado de las guías | `shipment_id` |
| `vehicle_gps` | Coordenadas y velocidad de los vehículos | `vehicle_id` |
| `operational_updates` | Estado, riesgo y alertas calculados por Spark | `shipment_id` |
| `external_conditions` | Reservado para fuentes externas | Por definir |

## 4. Ver resultados de Spark

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic operational_updates --from-beginning --max-messages 5 --timeout-ms 30000
```

Busca `shipment_id`, `status`, `risk_level`, `active_alert` y `possible_delay_reason` en el JSON. `shipment_event_id` y `gps_event_id`, cuando está disponible, permiten rastrear los mensajes que dieron origen al resultado. Si el consumidor muestra `TimeoutException` después de imprimir mensajes, simplemente agotó el tiempo esperando más. Si no imprime ninguno, revisa los logs de Spark y comprueba que haya mensajes de entrada.

Spark guarda datos originales, eventos rechazados, eventos válidos y estados actuales en Delta Lake. Sus checkpoints quedan en `/data/streaming/checkpoints/main` dentro del volumen de Docker.

## 5. Probar recuperación

**Reenvío de ingesta:** inicia el simulador con `--steps 0`. En otra terminal ejecuta `docker compose stop kafka`. Espera a que el simulador agote sus reintentos y se detenga. Consulta qué evento quedó pendiente:

```powershell
.\.venv\Scripts\python.exe -c "import sqlite3; c=sqlite3.connect('outbox.sqlite3'); print(c.execute('SELECT event_id,topic,key FROM pending').fetchall())"
docker compose up -d --wait kafka
.\.venv\Scripts\python.exe -m ingestion replay
```

La salida esperada es `Pendientes: 0`. Para comprobar que llegó a Kafka, busca el mismo `event_id` en el tópico que indicó SQLite. Por ejemplo, para un pendiente de GPS:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic vehicle_gps --from-beginning --timeout-ms 10000 | Select-String 'UUID_COPIADO'
```

**Recuperación de Spark:** ejecuta `docker compose restart spark` y revisa `docker compose logs --tail 50 spark`. El servicio debe retomar el procesamiento desde el checkpoint guardado. Para validar el avance, compara el número de lote antes y después del reinicio.

## 6. Ejecutar las pruebas y apagar

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
docker compose --profile streaming run --rm --no-deps --build spark tests/spark_integration.py
```

Las primeras pruebas se ejecutan sin Kafka ni Spark. La segunda usa Spark y Delta dentro de Docker con datos de prueba aislados; no publica en Kafka.

Para apagar los servicios conservando los datos y checkpoints:

```powershell
docker compose --profile streaming down
```

Para conservar los datos, ejecuta ese comando sin `-v`.
