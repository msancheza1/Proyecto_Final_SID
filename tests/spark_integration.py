"""Prueba de Spark y Delta real con fixtures; ejecutar con Dockerfile.spark."""
import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

from jsonschema import Draft202012Validator, FormatChecker
from pyspark.sql import SparkSession
from streaming.job import process, kafka_rows

spark = (SparkSession.builder.appName('Streaming-integration-test').config('spark.sql.session.timeZone', 'UTC')
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    .config('spark.databricks.delta.snapshotPartitions', '2')
    .config('spark.sql.shuffle.partitions', '2').getOrCreate())
spark.sparkContext.setLogLevel('WARN')

try:
    with tempfile.TemporaryDirectory() as directory:
        now = datetime(2026, 11, 3, 12)
        base = dict(schema_version='1.0', event_id=str(uuid4()), event_time='2026-11-03T12:00:00Z',
                    source='python_simulator', scenario='normal', vehicle_id='V1', route_id='MED-BOG')
        shipment = dict(base, event_type='shipment_event', shipment_id='S1', status='in_transit',
                        origin_city='Medellín', destination_city='Bogotá', center_id='C1',
                        promised_delivery='2026-11-03T20:00:00Z')
        gps = dict(base, event_id=str(uuid4()), event_type='vehicle_gps', latitude=6.2442,
                   longitude=-75.5812, speed_kmh=65)
        schema = 'key string, raw_json string, topic string, partition int, offset long, kafka_timestamp timestamp, clock_tick boolean'

        def row(event, offset):
            topic = 'vehicle_gps' if event['event_type'] == 'vehicle_gps' else 'shipment_events'
            return (event.get('shipment_id', event['vehicle_id']), json.dumps(event), topic, 0, offset, now, False)

        # UUID duplicado en offsets diferentes y JSON inválido.
        records = [row(shipment, 0), row(gps, 0), row(shipment, 1), ('S2', '{', 'shipment_events', 0, 2, now, False)]
        batch = spark.createDataFrame(records, schema)
        process(spark, batch, 0, directory, '', '/app/config/routes.jsonl', publish=False)
        process(spark, batch, 0, directory, '', '/app/config/routes.jsonl', publish=False)
        assert spark.read.format('delta').load(f'{directory}/raw').count() == 4
        assert spark.read.format('delta').load(f'{directory}/silver_events').count() == 2
        assert spark.read.format('delta').load(f'{directory}/rejected').count() == 1
        current = spark.read.format('delta').load(f'{directory}/current_shipments')
        assert current.first()['risk_level'] == 'low'
        validator = Draft202012Validator(json.loads(Path('/app/schemas/operational.schema.json').read_text()), format_checker=FormatChecker())
        validator.validate(json.loads(kafka_rows(current, now).first()['value']))

        # Evento tardío no debe hacer retroceder el estado.
        late = dict(shipment, event_id=str(uuid4()), status='received', event_time='2026-11-03T11:00:00Z')
        process(spark, spark.createDataFrame([row(late, 3)], schema), 1, directory, '', '/app/config/routes.jsonl', publish=False)
        assert spark.read.format('delta').load(f'{directory}/current_shipments').first()['status'] == 'in_transit'

        # Reloj técnico sin eventos nuevos: la promesa vencida debe generar riesgo alto.
        tick = spark.createDataFrame([(None, None, None, None, None, now+timedelta(hours=10), True)], schema)
        process(spark, tick, 2, directory, '', '/app/config/routes.jsonl', publish=False)
        assert spark.read.format('delta').load(f'{directory}/current_shipments').first()['risk_level'] == 'high'
        print('OK: Spark joins, Delta MERGE, deduplicación, cuarentena, contrato, evento tardío y reloj')
finally:
    spark.stop()
