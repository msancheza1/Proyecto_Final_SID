# Verificación de Sara el 3 de noviembre de 2026

Se restauró el código al commit inicial `d806870` antes de implementar Sara. No se
incluyen los cambios descartados de MongoDB, Streamlit ni Airflow.

Resultados ejecutados:

- 15 pruebas unitarias pasaron, incluidas las cinco de ingesta de Mariana.
- La imagen Docker se construyó con Python 3.11, Java 17, Spark 3.5.6 y Delta 3.2.0.
- La prueba `tests/spark_integration.py` terminó con código 0. Verificó joins,
  deduplicación entre lotes, reejecución del mismo lote, cuarentena, contrato de
  salida, eventos tardíos y evaluación por reloj sin nuevos movimientos.
- El servicio Spark consumió los mensajes que ya estaban retenidos en el Kafka
  del proyecto. No se inició un generador nuevo para esta comprobación.
- Se leyó una salida real en `operational_updates` para `GUIA-b2322e35-0000`, con
  riesgo `high` y motivo `Promesa de entrega vencida`. Incluía el evento GPS
  `b0bf8ae2-f885-4e2d-befb-75ed8d257b70`, el mismo que había sido recuperado en la
  prueba de reenvío de Mariana. Se conserva así su trazabilidad.
- El checkpoint creó commits de los microbatches 0, 1 y 2 antes del reinicio.

Los mensajes de origen provienen del simulador de Mariana ejecutado anteriormente;
esto verifica el procesamiento real con Spark y Kafka, no una conexión a Servientrega.
El primer microbatch tardó aproximadamente 41 segundos con el entorno en frío.
No se ejecutó una prueba de carga ni se acredita capacidad de 5.000 eventos/s.
