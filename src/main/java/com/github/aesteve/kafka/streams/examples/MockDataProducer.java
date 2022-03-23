package com.github.aesteve.kafka.streams.examples;

import com.github.aesteve.kafka.streams.examples.conf.ConfLoader;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.github.aesteve.kafka.streams.examples.TopicsRepartitionMapping.*;

public class MockDataProducer {
    public static final int NB_TENANTS = 100; // == NB_OUTPUT_TOPICS
    public static final int NB_INPUTS = 30;
    public static final int BATCH_SIZE = 25_000;
    public static final int NB_TO_SEND = 1_000_000_000;
    public static final int PAYLOAD_SIZE = 200;
    private static final Random RAND = new Random();
    private static final char[] SYMBOLS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();
    private static final Logger LOG = LoggerFactory.getLogger(MockDataProducer.class.getName());

    public static void main(String... args) throws Exception {

        var props = ConfLoader.fromResources("ccloud.properties");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "100");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "20000");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        props.put(ProducerConfig.RETRIES_CONFIG, "10000");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        var producer = new KafkaProducer<String, String>(props);
        var tenantIds = IntStream.range(0, NB_TENANTS).mapToObj(i -> String.format("tenant-%s", i)).toList();
        var adminClient = AdminClient.create(props);
//        var toDelete = Stream.concat(
//                tenantIds.stream().map(TopicsRepartitionMapping::topicFor),
//                Stream.of(INPUT_TOPIC)
//        ).toList();
//        LOG.info("Deleting existing topics {}", toDelete);
//        adminClient.deleteTopics(toDelete).all().get();
//        LOG.info("Creating topics");
//        var inputs = IntStream.range(0, NB_INPUTS).mapToObj(Integer::toString).toList();
//        var toCreate = Stream.concat(
//                tenantIds.stream().map(id -> new NewTopic(outputTopicFor(id), 6, (short) 3)),
//                inputs.stream().map(id -> new NewTopic(inputTopicFor(id), 50, (short) 3)) // 20 consumers atm => topic is configured w/ at least 20 partitions
//        ).toList();
//        adminClient.createTopics(toCreate).all().get();
        LOG.info("Running producer benchmark");
        var totalMsgs = 0;
        var totalMs = 0;
        var nbBatchesSent = 0;
        var topics = new HashSet<String>();
        while (totalMsgs <= NB_TO_SEND) {
            var tenantId = tenantIds.get(totalMsgs % NB_TENANTS);
            int inputTopic = totalMsgs % NB_INPUTS;
            var k = UUID.randomUUID().toString();
            var value = randomPayload();
            var record = new ProducerRecord<>(inputTopicFor(Integer.toString(inputTopic)), k, value);
            topics.add(record.topic());
            record.headers().add(new RecordHeader(TENANT_ID_HEADER, tenantId.getBytes(StandardCharsets.UTF_8)));
            producer.send(record);
            totalMsgs++;
            if (totalMsgs % BATCH_SIZE == 0) {
//                LOG.info("Sent {} messages, flushing", totalMsgs);
                var before = System.currentTimeMillis();
                producer.flush();
                var after = System.currentTimeMillis();
                var batchDuration = after - before;
//                LOG.info("Flushed in {}ms", after - before);
                totalMs += after - before;
                var avgSendRate = ((float)(totalMsgs)) / totalMs * 1000;
                nbBatchesSent += 1;
                LOG.info(
                        "Sent {} msg to {} topics in {}ms. Avg Send rate: {} msg/s. Batch duration {}ms. (Avg batch duration: {}ms)",
                        totalMsgs,
                        topics.size(),
                        totalMs,
                        avgSendRate,
                        batchDuration,
                        totalMs / nbBatchesSent
                );
                topics.clear();
            }
        }
//        Thread.sleep(360_000);
    }


    private static String randomPayload() {
        var buf = new char[PAYLOAD_SIZE];
//        for (int idx = 0; idx < buf.length; ++idx)
//            // buf[idx] = SYMBOLS[RAND.nextInt(SYMBOLS.length)];
//            buf[idx] = 'j';
        Arrays.fill(buf, 'j');
        return new String(buf);
    }

}
