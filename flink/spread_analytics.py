import os
import json
import time
import math
import logging

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('spread-analytics')

KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092')
INPUT_TOPICS = os.getenv(
    'FLINK_INPUT_TOPICS', 'binance-futures-ticker,bybit-futures-ticker'
).split(',')
OUTPUT_TOPIC = os.getenv('FLINK_OUTPUT_TOPIC', 'spread-analytics')
WINDOW_SECONDS = int(os.getenv('SPREAD_WINDOW_SECONDS', '300'))
KAFKA_JAR = os.getenv(
    'FLINK_KAFKA_JAR', '/app/jars/flink-sql-connector-kafka.jar'
)


class SpreadAnalyticsFunction(KeyedProcessFunction):
    """
    Keyed by symbol. Tracks latest price per exchange and maintains a
    rolling buffer of spread samples over WINDOW_SECONDS. On each price
    update that produces a valid spread, computes mean/stddev/z-score
    and emits a JSON analytics record.
    """

    def open(self, runtime_context: RuntimeContext):
        self.binance_state = runtime_context.get_state(
            ValueStateDescriptor("binance", Types.STRING())
        )
        self.bybit_state = runtime_context.get_state(
            ValueStateDescriptor("bybit", Types.STRING())
        )
        self.buffer_state = runtime_context.get_state(
            ValueStateDescriptor("spread_buffer", Types.STRING())
        )

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        try:
            msg = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return

        exchange = msg.get('exchange', '')
        fields = msg.get('changed_fields') or msg.get('data', {})
        price = fields.get('price')
        if price is None:
            return
        price = float(price)

        now_ms = int(time.time() * 1000)

        if 'binance' in exchange:
            self.binance_state.update(json.dumps({'price': price, 'ts': now_ms}))
        elif 'bybit' in exchange:
            self.bybit_state.update(json.dumps({'price': price, 'ts': now_ms}))
        else:
            return

        binance_raw = self.binance_state.value()
        bybit_raw = self.bybit_state.value()
        if not binance_raw or not bybit_raw:
            return

        binance = json.loads(binance_raw)
        bybit = json.loads(bybit_raw)

        bp = binance['price']
        yp = bybit['price']
        min_price = min(bp, yp)
        if min_price <= 0:
            return

        spread_pct = (yp - bp) / min_price * 100

        buffer = json.loads(self.buffer_state.value() or '[]')
        buffer.append({'ts': now_ms, 'spread': spread_pct})

        cutoff = now_ms - (WINDOW_SECONDS * 1000)
        buffer = [s for s in buffer if s['ts'] >= cutoff]
        self.buffer_state.update(json.dumps(buffer))

        ctx.timer_service().register_processing_time_timer(
            now_ms + WINDOW_SECONDS * 1000 + 1000
        )

        spreads = [s['spread'] for s in buffer]
        n = len(spreads)
        mean = sum(spreads) / n

        if n >= 2:
            variance = sum((s - mean) ** 2 for s in spreads) / (n - 1)
            stddev = math.sqrt(variance)
            zscore = (spread_pct - mean) / stddev if stddev > 0 else 0.0
        else:
            stddev = 0.0
            zscore = 0.0

        result = json.dumps({
            'symbol': ctx.get_current_key(),
            'binance_price': bp,
            'bybit_price': yp,
            'spread_pct': round(spread_pct, 6),
            'spread_mean_5m': round(mean, 6),
            'spread_stddev_5m': round(stddev, 6),
            'spread_zscore': round(zscore, 4),
            'event_timestamp': max(binance['ts'], bybit['ts']),
        })
        yield result

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        buffer_raw = self.buffer_state.value()
        if not buffer_raw:
            return

        buffer = json.loads(buffer_raw)
        cutoff = int(time.time() * 1000) - (WINDOW_SECONDS * 1000)
        buffer = [s for s in buffer if s['ts'] >= cutoff]

        if buffer:
            self.buffer_state.update(json.dumps(buffer))
        else:
            self.buffer_state.clear()


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.getenv('FLINK_PARALLELISM', '1')))

    jar_uri = f"file://{os.path.abspath(KAFKA_JAR)}"
    env.add_jars(jar_uri)
    logger.info(f"Loaded Kafka connector JAR: {jar_uri}")

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics(*INPUT_TOPICS)
        .set_group_id('flink-spread-analytics')
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(OUTPUT_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-source")

    analytics = (
        ds
        .map(lambda v: v, output_type=Types.STRING())
        .key_by(lambda v: json.loads(v).get('token', ''))
        .process(SpreadAnalyticsFunction(), output_type=Types.STRING())
    )

    analytics.sink_to(sink)

    logger.info(
        f"Starting spread-analytics | sources={INPUT_TOPICS} "
        f"sink={OUTPUT_TOPIC} window={WINDOW_SECONDS}s"
    )
    env.execute('spread-analytics')


if __name__ == '__main__':
    main()
