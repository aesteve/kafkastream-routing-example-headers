package com.github.aesteve.kafka.streams.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.github.aesteve.kafka.streams.examples.TestEnv.*;
import static com.github.aesteve.kafka.streams.examples.utils.MetricUtils.producerMetric;
import static com.github.aesteve.kafka.streams.examples.utils.RngUtils.gaussianIdxIn;

public interface TestDataProducer {
//    private static final Random RAND = new Random();
//    private static final char[] SYMBOLS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();

    Logger LOG = LoggerFactory.getLogger(TestDataProducer.class);

    static TestOutcome produceTestMessages(
            int totalRecordsToSend,
            int bulkSize,
            int payloadSize,
            List<String> tenantIds,
            Properties props,
            ProduceStrategy produceStrategy
    ) {
        var producer = new KafkaProducer<String, String>(props);
        var random = new Random();
        var gaussianTenants = new ArrayList<Integer>();
        var nbTenants = tenantIds.size();
        for (int i = 0; i <= totalRecordsToSend; i++) {
            gaussianTenants.add(gaussianIdxIn(random, nbTenants));
        }
        LOG.info("Running producer benchmark");
        var totalMsgs = 0;
        var beforeStart = System.currentTimeMillis();
        var nbBatchesSent = 0;
        var outcome = new TestOutcome(0, 0, 0, 0.0, 0);
        var toSend = new HashMap<String, List<ProducerRecord<String, String>>>(bulkSize);
        while (totalMsgs <= totalRecordsToSend) {
            var tenantIdx = switch (produceStrategy) {
                case RANDOM_UNIFORM ->
                        random.nextInt(nbTenants);
                case RANDOM_GAUSSIAN ->
                        gaussianTenants.get(totalMsgs);
                default ->
                        totalMsgs % nbTenants;
            };
            var tenantId = tenantIds.get(tenantIdx);
//            int inputTopic = totalMsgs % NB_INPUTS;
            var key = UUID.randomUUID().toString();
            var value = randomPayload(payloadSize);
            var record = new ProducerRecord<>(outputTopicFor(tenantId), key, value);
            record.headers().add(new RecordHeader(TENANT_ID_HEADER, tenantId.getBytes(StandardCharsets.UTF_8)));
            if (produceStrategy == ProduceStrategy.GROUP_BY_TENANT) {
                toSend.computeIfAbsent(tenantId, k -> new ArrayList<>())
                        .add(record);
            } else {
                producer.send(record);
            }
            totalMsgs++;
            if (totalMsgs % bulkSize == 0) {
                if (produceStrategy == ProduceStrategy.GROUP_BY_TENANT) {
                    toSend.values().forEach(perTenant -> perTenant.forEach(producer::send));
                    toSend.clear(); // should be after clear() to be correct
                }
                producer.flush();
                nbBatchesSent++;
                var after = System.currentTimeMillis();
                var totalMs = after - beforeStart;
                var metrics = producer.metrics();
                var totalMsgsMetric = producerMetric(metrics, "record-send-total");
                var avgSendRate = producerMetric(metrics, "record-send-rate");
                var queueTimeAvg = producerMetric(metrics, "record-queue-time-avg");
                var batchSizeAvg = producerMetric(metrics, "batch-size-avg");
                var requestRate = producerMetric(metrics, "request-rate");
                var requestLatencyAvg = producerMetric(metrics, "request-latency-avg");
                var recordsPerRequestAvg = producerMetric(metrics, "records-per-request-avg");
                var batchAvg = totalMs / nbBatchesSent;
                LOG.info("Avg duration spent in queue {}ms. Batch size avg = {}. Request Rate = {}, Request Latency avg = {}. Records per Request avg = {}", queueTimeAvg, batchSizeAvg, requestRate,requestLatencyAvg, recordsPerRequestAvg);
                LOG.info(
                        "Sent {} msg to in {}ms. Avg Send rate: {} msg/s. Avg batch duration: {}ms",
                        totalMsgsMetric,
                        totalMs,
                        avgSendRate,
                        batchAvg
                );
                outcome = new TestOutcome(totalMs, totalMsgs, ((double)totalMsgs) / (((double)totalMs) / 1000), avgSendRate, batchAvg);
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
