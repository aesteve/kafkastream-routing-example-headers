package com.github.aesteve.kafka.streams.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static com.github.aesteve.kafka.streams.examples.TestEnv.*;
import static com.github.aesteve.kafka.streams.examples.utils.MetricUtils.producerMetric;

public interface TestDataProducer {
//    private static final Random RAND = new Random();
//    private static final char[] SYMBOLS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();

    Logger LOG = LoggerFactory.getLogger(TestDataProducer.class);

    static TestOutcome produceTestMessages(
            int totalRecordsToSend,
            int bulkSize,
            int payloadSize,
            List<String> tenantIds,
            Properties props
    ) {
        var producer = new KafkaProducer<String, String>(props);
        LOG.info("Running producer benchmark");
        var totalMsgs = 0;
        var beforeStart = System.currentTimeMillis();
        var nbBatchesSent = 0;
        var nbTenants = tenantIds.size();
        var outcome = new TestOutcome(0.0, 0);
        while (totalMsgs <= totalRecordsToSend) {
            var tenantId = tenantIds.get(totalMsgs % nbTenants);
//            int inputTopic = totalMsgs % NB_INPUTS;
            var key = UUID.randomUUID().toString();
            var value = randomPayload(payloadSize);
            var record = new ProducerRecord<>(outputTopicFor(tenantId), key, value);
            record.headers().add(new RecordHeader(TENANT_ID_HEADER, tenantId.getBytes(StandardCharsets.UTF_8)));
            producer.send(record);
            totalMsgs++;
            if (totalMsgs % bulkSize == 0) {
                producer.flush();
                nbBatchesSent++;
                var after = System.currentTimeMillis();
                var totalMs = after - beforeStart;
                var metrics = producer.metrics();
                var totalMsgsMetric = producerMetric(metrics, "record-send-total");
                var avgSendRate = producerMetric(metrics, "record-send-rate");
                var queueTimeAvg = producerMetric(metrics, "record-queue-time-avg");
                var batchSizeAvg = producerMetric(metrics, "batch-size-avg");
                var batchAvg = totalMs / nbBatchesSent;
                LOG.debug("Avg duration spent in queue {}ms. Batch size avg = {}", queueTimeAvg, batchSizeAvg);
                LOG.info(
                        "Sent {} msg to in {}ms. Avg Send rate: {} msg/s. Avg batch duration: {}ms",
                        totalMsgsMetric,
                        totalMs,
                        avgSendRate,
                        batchAvg
                );
                outcome = new TestOutcome(avgSendRate, batchAvg);
            }
        }
        return outcome;
    }

    private static String randomPayload(int size) {
        var buf = new char[size];
        Arrays.fill(buf, 'j');
        return new String(buf);
    }

}
