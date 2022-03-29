package com.github.aesteve.kafka.streams.examples;

import com.github.aesteve.kafka.streams.examples.conf.ConfLoader;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.stream.IntStream;

import static com.github.aesteve.kafka.streams.examples.TestDataProducer.produceTestMessages;

public class AllTopicsDataProducer {
    public static final int NB_OUTPUT_TENANTS = 100; // == NB_OUTPUT_TOPICS
    public static final int NB_INPUT_TENANTS = 100;
    public static final int BULK_SIZE = 25_000;
    public static final int TOTAL_RECORDS_TO_SEND = 1_250_000;
    public static final int PAYLOAD_SIZE = 200;

    public static void main(String... args) throws Exception {
        var props = ConfLoader.fromResources("ccloud.properties");
        // Mandatory, fixed, no perf. impact.
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Configured, fixed.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "25000");
//        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "20000");
//        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
//        props.put(ProducerConfig.RETRIES_CONFIG, "10000");
//        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1000000");
//        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "64000000");

        var tenantIds = IntStream.range(0, NB_OUTPUT_TENANTS).mapToObj(i -> String.format("tenant-%s", i)).toList();
        // prepareTestEnv(props, NB_INPUT_TENANTS, NB_OUTPUT_TENANTS);

        produceTestMessages(TOTAL_RECORDS_TO_SEND, BULK_SIZE, PAYLOAD_SIZE, tenantIds, props);
    }


}
