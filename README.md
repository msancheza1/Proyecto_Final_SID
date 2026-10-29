# Componente de Mariana

Base para Proyecto Final SID: simulación de envíos y GPS e ingesta en Apache Kafka.
Datos completamente ficticios. Alcance: Python, contrato JSON, tópicos, reintentos
y evidencia de reenvío del hito S13. Spark, Delta, MongoDB y Airflow corresponden
a los demás integrantes.

## Ejecución en Windows PowerShell

Abre una terminal en esta carpeta. Requiere Python 3.10 o superior y Docker Desktop
con contenedores Linux iniciado.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
docker compose up -d --wait
.\.venv\Scripts\python.exe -m ingestion topics
.\.venv\Scripts\python.exe -m ingestion simulate --steps 0 --scenario delay
```

El último comando genera ciclos continuos hasta Ctrl+C. En otra terminal:

```powershell
.\.venv\Scripts\python.exe -m ingestion consume --limit 20
```

Para una demostración finita: `python -m ingestion simulate --shipments 3 --steps 12`.
Para probar sin Docker: `python -m ingestion simulate --dry-run --interval 0`.
Usa siempre el Python del entorno si las dependencias solo están instaladas allí.

## Contrato para Sara

| Tópico | Clave Kafka UTF-8 | Contenido |
| --- | --- | --- |
| shipment_events | shipment_id | Estado, vehículo, ruta, centro y promesa de entrega |
| vehicle_gps | vehicle_id | Latitud, longitud y velocidad |
| external_conditions | Por definir con la fuente | Reservado; no se publican datos externos |

Los dos primeros tópicos usan el contrato `schemas/event.schema.json`. Fechas UTC
ISO 8601 terminadas en Z, coordenadas en grados y velocidad en km/h. Cada evento
tiene UUID `event_id`, versión `1.0`, `event_time` y `source`. El consumidor debe
agregar su propio tiempo de ingesta. Spark debe interpretar fechas como timestamp,
deduplicar por `event_id` y relacionar GPS y envíos por `vehicle_id` y `route_id`
con una condición temporal. El escenario `delay` detiene al primer vehículo y
genera promesas vencidas para probar reglas de riesgo. No implementa esas reglas.

Se simula una ruta Medellín–Bogotá mediante interpolación de coordenadas, no una
trayectoria vial real. Un ciclo recorre estados hasta delivered; delay permanece
en tránsito. Los ciclos continuos crean nuevas guías y vehículos. Los estados
pueden repetirse entre rondas con eventos distintos. Los tiempos están comprimidos
para la demostración; no representan un viaje real.

## Persistencia y reintentos

Cada evento válido se guarda primero en `outbox.sqlite3`, con SQLite WAL y
synchronous FULL. Se envía con productor idempotente y `acks=all`, y se elimina de
pendientes únicamente tras recibir confirmación. Hay tres intentos de aplicación
con espera exponencial, además de los reintentos del cliente Kafka. Si Kafka sigue
caído, el simulador se detiene y conserva el evento. Para recuperarlo:

```powershell
python -m ingestion replay
```

El reenvío preserva el JSON, su UUID y la clave. Una confirmación perdida puede
generar duplicados después de reiniciar el productor: la garantía es at-least-once,
y Sara debe deduplicar. Solo se garantizan eventos ya persistidos; no se generan
datos nuevos mientras el simulador está detenido. Ejecutar un solo simulador o
replay por archivo SQLite. El envío es secuencial para preservar orden y facilitar
la demostración; no acredita el objetivo de 5.000 eventos/s.

## Evidencia de S13 y prueba de falla real

1. Inicia Kafka, crea los tópicos y ejecuta el simulador continuo.
2. En otra terminal consume eventos y registra tópico, partición, offset y UUID.
3. Ejecuta `docker compose stop kafka`. Espera a que el simulador reporte el fallo
   y se detenga dejando un evento pendiente. Si estaba confirmado justo al detener
   Kafka, la siguiente ronda producirá el pendiente.
4. Consulta el pendiente sin modificarlo:

```powershell
python -c "import sqlite3; c=sqlite3.connect('outbox.sqlite3'); print(c.execute('SELECT event_id,topic,key FROM pending').fetchall())"
docker compose start kafka
python -m ingestion replay
python -m ingestion consume --limit 100 --seconds 30
```

El consumidor lee desde el inicio con un grupo nuevo. Ajusta `--limit` al número
de eventos disponible; devuelve error si no alcanza ese número en el plazo.
Busca el UUID pendiente entre los registros consumidos y verifica que SQLite
quede vacío. Conserva capturas o salidas de terminal de los pasos 2–4 para la entrega.
Esta es una prueba manual pendiente, no evidencia de integración ya ejecutada.

Pruebas automáticas sin Kafka:

```powershell
python -m unittest discover -s tests -v
```

Cubren contrato, claves, coordenadas inválidas, retraso, falla simulada, reapertura
de SQLite, orden de reenvío y duplicados por confirmación perdida.

## Configuración y límites

Tres particiones por tópico, siete días de retención y un broker para desarrollo.
Clientes locales: localhost:9092; otros contenedores de la misma red: kafka:29092.
Para un cliente remoto ajusta los listeners anunciados. Se puede cambiar el destino
del cliente con `$env:KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'`.
No aumentar particiones durante una prueba que dependa del orden por clave.

Un broker no tolera pérdida de nodo y `acks=all` solo confirma esa réplica.
Para validar RNF1/RNF5 hace falta un clúster de al menos tres brokers, factor de
replicación 3, min.insync.replicas 2 y pruebas de carga/fallas. La latencia menor
a cinco segundos es un objetivo pendiente de medición; durante fallas se excede.
No se incluye autenticación ni cifrado en la configuración local.

Para apagar sin borrar datos: `docker compose down`. El volumen y SQLite conservan
el estado para la siguiente ejecución.

Referencias de implementación: [cliente oficial Confluent Python](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
y [imagen oficial Apache Kafka](https://hub.docker.com/r/apache/kafka).
