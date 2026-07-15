from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, \
    KafkaRecordSerializationSchema, DeliveryGuarantee, KafkaOffsetsInitializer
from pyflink.datastream.functions import FlatMapFunction
import json
import os

from postgres_writer import PostgresWriter

ANOMALY_MAP = {
    "missing_field": 1,
    "negative_price": 2,
    "quantity_zero": 3,
    "corrupted_timestamp": 4,
}

ANOMALY_ALERT_SQL = (
    "INSERT INTO anomaly_alerts (sale_id, anomaly_type, anomaly_code, event_timestamp) "
    "VALUES %s"
)


def _anomaly_alert_row(value):
    e = json.loads(value)
    return (e.get("sale_id", "N/A"),
            e.get("anomaly_type", "unknown"),
            e.get("anomaly_code", 0),
            e.get("event_timestamp", "N/A"))


class AnomalyClassifier(FlatMapFunction):
    def flat_map(self, value):
        event = json.loads(value)
        error_reason = event.get("error_reason")
        anomaly_code = ANOMALY_MAP.get(error_reason, 0)
        if anomaly_code == 0:
            return
        yield json.dumps({
            "sale_id": event.get("sale_id", "N/A"),
            "anomaly_type": error_reason,
            "anomaly_code": anomaly_code,
            "event_timestamp": event.get("event_timestamp", "N/A"),
        })


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    group_id = os.environ.get("GROUP_ID", "anomaly-group")

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_topics("invalid-sales-events") \
        .set_group_id(group_id) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env \
        .from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source") \
        .flat_map(AnomalyClassifier(),
                  output_type=Types.STRING())

    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("anomaly-alerts")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    ds \
        .map(PostgresWriter(ANOMALY_ALERT_SQL, _anomaly_alert_row),
             output_type=Types.STRING()) \
        .sink_to(kafka_sink)

    env.execute("Anomaly Detection Job")


if __name__ == "__main__":
    main()
