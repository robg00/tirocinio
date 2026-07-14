from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, \
    KafkaRecordSerializationSchema, DeliveryGuarantee, KafkaOffsetsInitializer
from pyflink.datastream.functions import AggregateFunction, MapFunction
from pyflink.common.time import Time
from pyflink.datastream.window import SlidingProcessingTimeWindows
import json
import os


class SalesStatsAggregator(AggregateFunction):

    def create_accumulator(self):
        return [0, 0.0]

    def add(self, value, acc):
        event = json.loads(value)
        acc[0] += 1
        acc[1] += event.get("total_amount", 0.0)
        return acc

    def get_result(self, acc):
        if acc[0] == 0:
            return json.dumps({"sale_count": 0, "total_revenue": 0.0, "avg_order_value": 0.0})
        avg = round(acc[1] / acc[0], 2)
        return json.dumps({
            "sale_count": acc[0],
            "total_revenue": round(acc[1], 2),
            "avg_order_value": avg,
        })

    def merge(self, acc1, acc2):
        acc1[0] += acc2[0]
        acc1[1] += acc2[1]
        return acc1


class PassthroughMapper(MapFunction):
    def map(self, value):
        return value


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    group_id = os.environ.get("GROUP_ID", "agg-group")

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_topics("valid-sales") \
        .set_group_id(group_id) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env \
        .from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source") \
        .map(PassthroughMapper(), output_type=Types.STRING()) \
        .window_all(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10))) \
        .aggregate(SalesStatsAggregator(), output_type=Types.STRING())

    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("agg-sales-windowed")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    ds.sink_to(kafka_sink)
    env.execute("Aggregation Windowed Job")


if __name__ == "__main__":
    main()