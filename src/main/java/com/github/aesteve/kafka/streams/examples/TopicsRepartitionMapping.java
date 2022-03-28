package com.github.aesteve.kafka.streams.examples;

import com.github.aesteve.kafka.streams.examples.conf.ConfLoader;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

public class TopicsRepartitionMapping {

    private final static Logger LOG = LoggerFactory.getLogger(TopicsRepartitionMapping.class);

    public static final String APPLICATION_ID = "5-repartition-topics"; // change to re-consume from start

    public final static String INPUT_TOPIC_PREFIX = "incoming-topic";
    public final static Pattern INPUT_TOPICS_PATTERN = Pattern.compile(inputTopicFor("[0-5]"));
    public final static String OUTPUT_TOPIC_PREFIX = "output-topic";
    public final static String TENANT_ID_HEADER = "tenant-id";
    public final static Integer NB_THREADS = 1; // 5 input topics w/ 50 partitions each => 250

    private static Optional<String> getTenantId(Headers headers) {
        return Optional
                .ofNullable(headers.headers(TENANT_ID_HEADER).iterator().next())
                .map(h -> new String(h.value(), StandardCharsets.UTF_8));
    }

    public static String outputTopicFor(String tenantId) {
        return String.format("%s-%s", OUTPUT_TOPIC_PREFIX, tenantId);
    }

    public static String inputTopicFor(String tenantId) {
        return String.format("%s-%s", INPUT_TOPIC_PREFIX, tenantId);
    }

    private static class TenantIdTransformer implements  Transformer<String, String, KeyValue<String, String>> {

        ProcessorContext context;
        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }

        @Override
        public KeyValue<String, String> transform(String key, String value) {
            return new KeyValue<>(String.format("%s:%s", key, getTenantId(context.headers()).orElse("")), value); // careful with "oldKey:" here <-- check if ok
        }

        @Override
        public void close() {}

    }

    public static Properties streamProps() throws Exception {
        var props = ConfLoader.fromResources("ccloud.properties");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        // props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2); // <-- may be useful !! FIXME (seems bugged, facing this: https://stackoverflow.com/questions/70138589/kafka-streams-with-exactly-once-v2-invalidproducerepochexception-producer-atte)

        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NB_THREADS.toString()); // <-- parallelism! (50 partitions for the 30 input topics). Could also use N app instances
        // FIXME: using too many threads, or subscribing to too many topics at once leads to weird timeout issues in fetch requests? Why?

//        props.put(StreamsConfig.POLL_MS_CONFIG, "1");
//        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

//        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "10000000");
//        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "1000000000");
//        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "1000");
//        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "50000");

        props.put(ProducerConfig.LINGER_MS_CONFIG, "10"); // allow for batching
        props.put(ProducerConfig.ACKS_CONFIG, "all");
         props.put(ProducerConfig.BATCH_SIZE_CONFIG, "200000");
        return props;
    }

    public static KafkaStreams createStream(StreamsBuilder builder) throws Exception {
        // Topology part
        var consumedWith = Consumed.with(Serdes.String(), Serdes.String()); // <-- use your own here
        var producedWith = Produced.with(Serdes.String(), Serdes.String()); // <-- use your own here
        builder.stream(INPUT_TOPICS_PATTERN, consumedWith)
                .transform(TenantIdTransformer::new)
                .to((key, value, context) -> outputTopicFor(getTenantId(context.headers()).orElse(null)), producedWith);
        return new KafkaStreams(builder.build(), streamProps());
    }


    public static void main(String... args) throws Exception {
        var builder = new StreamsBuilder();
        var stream = createStream(builder);
        startPublishingMetrics(stream);
        runStreamingApp(stream);
    }

    static void startPublishingMetrics(KafkaStreams streams) {
        var isRunning = new AtomicBoolean(false);
        var metricsThread = new Thread(() -> {
            isRunning.set(true);
            var hasSent = false;
            var start = System.currentTimeMillis();
            while (isRunning.get()) {
                var metrics = streams.metrics();
                var sent = totalSentRecords(metrics);
                if (sent > 0 && !hasSent) {
                    start = System.currentTimeMillis();
                    hasSent = true;
                }
                var elapsedSeconds = (System.currentTimeMillis() - start) / 1000;
                var ratePerSec = sent / elapsedSeconds;
                var avgRateMetric = sumProduceRates(metrics);
                LOG.info("Records sent: {} msgs. Avg rate: {} msg/s. (avg metric rate: {} msg/s)", sent, ratePerSec, avgRateMetric);
                try {
                    Thread.sleep(1_000);
                } catch(Exception e) {}
            }
        });
        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> isRunning.set(false)));
        metricsThread.start();
    }

    static double sumProduceRates(Map<MetricName, ? extends Metric> metrics) {
        return producerMetric(metrics, "record-send-rate");
    }

    static double producerMetric(Map<MetricName, ? extends Metric> metrics, String metric) {
        return metrics
                .entrySet()
                .stream()
                .filter(e -> e.getKey().name().equals(metric) && e.getKey().group().equals("producer-metrics"))
                .mapToDouble(e -> (double) e.getValue().metricValue())
                .sum();
    }

    static double totalSentRecords(Map<MetricName, ? extends Metric> metrics) {
        return metrics
                .entrySet()
                .stream()
                .filter(e -> e.getKey().name().equals("record-send-total") && e.getKey().group().equals("producer-metrics"))
                .mapToDouble(e -> (double) e.getValue().metricValue())
                .sum();
    }

    private static void runStreamingApp(KafkaStreams stream) {
        var latch = new CountDownLatch(1);
        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> {
                    LOG.info("Gracefully shutting down");
                    stream.close(); // Graceful shutdown
                    latch.countDown();
                }));
        try {
            stream.start();
            latch.await();
        } catch (Exception e) {
            LOG.error("An exception occurred while running the stream processing application", e);
            System.exit(1);
        }
        System.exit(0);
    }


}
