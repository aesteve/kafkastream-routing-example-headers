package com.github.aesteve.kafka.streams.examples;

import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.aesteve.kafka.streams.examples.utils.MetricUtils.totalSentRecords;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMetrics extends BaseRepartitionTest{

    @Test
    void checkProcessRateIsAccurate() {
        // Publish a ton of messages in input topics
        var nbMsg = 10_000;
        for (int i = 0; i < nbMsg; i++) {
            var tenantId = tenantIds.get(0 % NB_TENANTS);
            var key = UUID.randomUUID().toString();
            var value = String.format("%s %d", "Just a simple string message sent at iteration: ", i);
            publishRecord(tenantId, key, value);
        }
        var received = new AtomicInteger();
        outputTopicMap.values().forEach(topic -> {
            while(!topic.isEmpty()) {
                topic.readRecord(); // trigger the actual processing? to get proper metrics
                received.getAndIncrement();
            }
        });
        testDriver.metrics().forEach((name, value) -> {
            System.out.println(name.name() + " is: " + value.metricValue());
        });
        // Metrics are OK
        var processRate = totalSentRecords(testDriver.metrics());
        assertTrue(processRate > 0);
    }

}
