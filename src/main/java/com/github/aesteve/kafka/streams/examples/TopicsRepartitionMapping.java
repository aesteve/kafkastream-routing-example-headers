package com.github.aesteve.kafka.streams.examples;

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
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class TopicsRepartitionMapping {

    private final static Logger LOG = LoggerFactory.getLogger(TopicsRepartitionMapping.class);

    public final static String INPUT_TOPIC = "incoming-topic";
    public final static String OUTPUT_TOPIC_PREFIX = "output-topic";
    public final static String TENANT_ID_HEADER = "tenant-id";

    private static Optional<String> getTenantId(Headers headers) {
        return Optional
                .ofNullable(headers.headers(TENANT_ID_HEADER).iterator().next())
                .map(h -> new String(h.value(), StandardCharsets.UTF_8));
    }

    public static String topicFor(String tenantId) {
        return String.format("%s-%s", OUTPUT_TOPIC_PREFIX, tenantId);
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

    public static Properties streamProps() {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "repartition-topics");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2); // <-- may be useful !!
        return props;
    }

    public static KafkaStreams createStream(StreamsBuilder builder) {
        // Topology part
        var consumedWith = Consumed.with(Serdes.String(), Serdes.String()); // <-- use your own here
        var producedWith = Produced.with(Serdes.String(), Serdes.String()); // <-- use your own here
        builder.stream(INPUT_TOPIC, consumedWith)
                .transform(TenantIdTransformer::new)
                .to((key, value, context) -> topicFor(getTenantId(context.headers()).orElse(null)), producedWith);
        return new KafkaStreams(builder.build(), streamProps());
    }


    public static void main(String... args) {
        var builder = new StreamsBuilder();
        var stream = createStream(builder);
        runStreamingApp(stream);
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
