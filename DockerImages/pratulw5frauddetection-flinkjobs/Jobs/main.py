from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common import Types
import json
import sys
sys.path.append('/opt/Jobs')
from UnifiedScoring import UnifiedScoring
from RuleCheck import RuleCheck


# Kafka Config

KAFKA_PRODUCER_TOPIC = "NeuralNetworkFeed"
kafka_props = {
    'bootstrap.servers': 'kafka:9092',
}
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    consumer = FlinkKafkaConsumer(
        topics='raw-transactions',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    source_stream = env.add_source(consumer)

    processed = (
        source_stream
        .key_by(lambda x: json.loads(x)["nameOrig"])
        .process(RuleCheck(), output_type=Types.STRING())
    )

    scoring_function = UnifiedScoring()
    keyed = processed.key_by(lambda x: json.loads(x)["nameOrig"])
    side_output = processed.get_side_output(scoring_function.output_tag)

    scored = keyed.process(scoring_function).name("Unified Scoring")

    if side_output is not None:
        producer = FlinkKafkaProducer(
            topic=KAFKA_PRODUCER_TOPIC,
            serialization_schema=SimpleStringSchema(),
            producer_config=kafka_props
        )
        side_output.add_sink(producer).name("Kafka Rule Score Sink")

    env.execute("Flink Incremental Anomaly Detection")

if __name__ == "__main__":
    main()
