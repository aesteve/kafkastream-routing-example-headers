package com.github.aesteve.kafka.streams.examples;

import com.github.aesteve.kafka.streams.examples.conf.ConfLoader;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static com.github.aesteve.kafka.streams.examples.TestDataProducer.produceTestMessages;
import static com.github.aesteve.kafka.streams.examples.utils.CollectionUtils.chunked;

public class ParallelTestDataProducer {
    public static final int NB_OUTPUT_TENANTS = 100; // == NB_OUTPUT_TOPICS
    public static final int NB_INPUTS = 100;
    public static final int BULK_SIZE = 25_000;
    public static final int TOTAL_RECORDS_TO_SEND = 1_000_000;
    public static final int PAYLOAD_SIZE = 200;
    public static final int NB_THREADS = 5;
    public static final ProduceStrategy PRODUCE_STRATEGY = ProduceStrategy.ROUND_ROBIN;

    public static void main(String... args) throws Exception {
        var props = ConfLoader.fromResources("ccloud.properties");
        // Mandatory, fixed, no impact.
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

        var executorService = Executors.newFixedThreadPool(NB_THREADS);
        var chunkSize = NB_OUTPUT_TENANTS / NB_THREADS;
        var tasks = chunked(tenantIds, chunkSize)
                .stream()
                .map(tenantIdChunk ->
                        (Callable<TestOutcome>)
                                () -> produceTestMessages(TOTAL_RECORDS_TO_SEND, BULK_SIZE, PAYLOAD_SIZE, tenantIdChunk, props, PRODUCE_STRATEGY)
                )
                .toList();
        executorService
                .invokeAll(tasks)
                .stream()
                .map(fut -> {
                    try {
                        return fut.get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .forEach(res -> System.out.println(res.jmxSendRate() + ";" + res.avgBatchDuration())); //CSV paste
        executorService.shutdownNow();
    }

}
