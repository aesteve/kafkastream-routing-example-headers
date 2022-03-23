package com.github.aesteve.kafka.streams.examples;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.github.aesteve.kafka.streams.examples.TopicsRepartitionMapping.*;
import static com.github.aesteve.kafka.streams.examples.TopicsRepartitionMapping.outputTopicFor;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class BaseRepartitionTest {

    protected TopologyTestDriver testDriver;
    protected TestInputTopic<String, String> inputTopic;
    protected KafkaStreams stream;
    protected final int NB_TENANTS = 100;
    protected final List<String> tenantIds = new ArrayList<>(NB_TENANTS);
    protected final Map<String, TestOutputTopic<String, String>> outputTopicMap = new HashMap<>(NB_TENANTS);


    @BeforeAll
    void setup() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        //Create Actual Stream Processing pipeline
        stream = createStream(builder);
        testDriver = new TopologyTestDriver(builder.build(), streamProps());
        inputTopic = testDriver.createInputTopic(inputTopicFor("simple"), new StringSerializer(), new StringSerializer());
        for (int i = 0;  i < NB_TENANTS; i++) {
            var tenantId = UUID.randomUUID().toString();
            var topic =  testDriver.createOutputTopic(outputTopicFor(tenantId), new StringDeserializer(), new StringDeserializer());
            tenantIds.add(tenantId);
            outputTopicMap.put(tenantId, topic);
        }
    }

    @AfterAll
    void tearDown() {
        testDriver.close();
    }

    /**
     * Prepare a record and send it to the input topic
     * @return the record that has been sent
     */
    TestRecord<String, String> publishRecord(String tenantId, String key, String value) {
        var tenantIdHeader = new RecordHeader(TENANT_ID_HEADER, tenantId.getBytes(StandardCharsets.UTF_8));
        var headers = new RecordHeaders(Collections.singleton(tenantIdHeader));
        // Send it
        var record = new TestRecord<>(key, value, headers);
        inputTopic.pipeInput(record);
        return record;
    }

}
