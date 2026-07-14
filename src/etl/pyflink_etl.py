from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, \
    KafkaRecordSerializationSchema, DeliveryGuarantee, KafkaOffsetsInitializer
from pyflink.datastream.functions import FlatMapFunction
import json
import os

from sales_splitter import split_event

VALID = 0
INVALID = 1


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

    tagged \
        .filter(lambda x: x[0] == VALID) \
        .map(lambda x: x[1], output_type=Types.STRING()) \
        .sink_to(kafka_sink_valid)

    tagged \
        .filter(lambda x: x[0] != VALID) \
        .map(lambda x: x[1], output_type=Types.STRING()) \
        .sink_to(kafka_sink_invalid)

    env.execute("ETL Sales Job")


if __name__ == "__main__":
    main()