from decimal import Decimal

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, \
    KafkaRecordSerializationSchema, DeliveryGuarantee, KafkaOffsetsInitializer
from pyflink.datastream.functions import FlatMapFunction, MapFunction
import json
import os

from postgres_writer import PostgresWriter
from sales_splitter import split_event

VALID = 0
INVALID = 1


def _fact_sale_row(value):
    e = json.loads(value)
    return (e["sale_id"], e["user_id"], e["product_id"],
            e["date_id"], e["quantity"],
            Decimal(str(e["unit_price"])),
            Decimal(str(e["total_amount"])),
            e["value_band"], e["sale_hour"], e["sale_minute"],
            e["day_of_week"], e["event_timestamp"])


FACT_SALE_SQL = (
    "INSERT INTO fact_sales (sale_id, user_id, product_id, date_id, "
    "quantity, unit_price, total_amount, value_band, sale_hour, "
    "sale_minute, day_of_week, event_timestamp) "
    "VALUES %s ON CONFLICT (sale_id) DO NOTHING"
)


def _anomaly_row(value):
    e = json.loads(value)
    return (e.get("sale_id", "N/A"),
            e.get("error_reason", "unknown"),
            e.get("event_timestamp", "N/A"))


ANOMALY_SQL = (
    "INSERT INTO anomaly_alerts (sale_id, anomaly_type, event_timestamp) "
    "VALUES %s"
)


class SalesSplitter(FlatMapFunction):
    def flat_map(self, value):
        event = json.loads(value)
        for tag_name, data in split_event(event):
            routing = VALID if tag_name == "valid" else INVALID
            yield (routing, json.dumps(data))


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    group_id = os.environ.get("GROUP_ID", "etl-group")

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_topics("sales-events") \
        .set_group_id(group_id) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    tagged = env \
        .from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source") \
        .flat_map(SalesSplitter(), output_type=Types.TUPLE([Types.INT(), Types.STRING()]))

    kafka_sink_valid = KafkaSink.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("valid-sales")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    kafka_sink_invalid = KafkaSink.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("invalid-sales-events")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    valid_stream = tagged \
        .filter(lambda x: x[0] == VALID) \
        .map(lambda x: x[1], output_type=Types.STRING())

    invalid_stream = tagged \
        .filter(lambda x: x[0] != VALID) \
        .map(lambda x: x[1], output_type=Types.STRING())

    valid_stream \
        .map(PostgresWriter(FACT_SALE_SQL, _fact_sale_row),
             output_type=Types.STRING()) \
        .sink_to(kafka_sink_valid)

    invalid_stream \
        .map(PostgresWriter(ANOMALY_SQL, _anomaly_row),
             output_type=Types.STRING()) \
        .sink_to(kafka_sink_invalid)

    env.execute("ETL Sales Job")


if __name__ == "__main__":
    main()
