package com.github.aesteve.kafka.streams.examples;

import static com.github.aesteve.kafka.streams.examples.TopicsRepartitionMapping.*;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class TopicsRepartitionTest {

    private static TopologyTestDriver testDriver;
    private static TestInputTopic<String, String> inputTopic;
    private static final int NB_TENANTS = 100;
    private static final List<String> tenantIds = new ArrayList<>(NB_TENANTS);
    private static final Map<String, TestOutputTopic<String, String>> outputTopicMap = new HashMap<>(NB_TENANTS);



    @BeforeAll
    static void setup() {
        final StreamsBuilder builder = new StreamsBuilder();
        //Create Actual Stream Processing pipeline
        createStream(builder);
        testDriver = new TopologyTestDriver(builder.build(), streamProps());
        inputTopic = testDriver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());
        for (int i = 0;  i < NB_TENANTS; i++) {
            var tenantId = UUID.randomUUID().toString();
            var topic =  testDriver.createOutputTopic(topicFor(tenantId), new StringDeserializer(), new StringDeserializer());
            tenantIds.add(tenantId);
            outputTopicMap.put(tenantId, topic);
        }
    }

    @Test
    public void aSingleMsgMustBeRoutedProperly() {
        // Prepare the input record
        var tenantId = tenantIds.get(0);
        var originalKey = "original-key";
        var originalValue = "ShouldBeSomeBytesOfData";
        var tenantIdHeader = new RecordHeader(TENANT_ID_HEADER, tenantId.getBytes(StandardCharsets.UTF_8));
        var originalHeaders = new RecordHeaders(Collections.singleton(tenantIdHeader));
        // Send it
        inputTopic.pipeInput(new TestRecord<>(originalKey, originalValue, originalHeaders));

        // Fetch the appropriate topic
        var desiredOutputTopic = outputTopicMap.get(tenantId);
        var output = desiredOutputTopic.readRecord();
        assertNotNull(output); // i.e. the record has been routed to the appropriate topic

        // Key must be mapped properly
        assertTrue(output.key().contains(tenantId));
        assertTrue(output.key().contains(originalKey));
        // Value must be untouched
        assertEquals(originalValue, output.value());
        // Headers must be untouched
        assertEquals(output.headers(), originalHeaders);
    }

    @AfterAll
    static void tearDown() {
        testDriver.close();
    }


}
