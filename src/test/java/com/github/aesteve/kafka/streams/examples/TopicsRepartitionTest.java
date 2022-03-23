package com.github.aesteve.kafka.streams.examples;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class TopicsRepartitionTest extends BaseRepartitionTest {

    @Test
    public void aSingleMsgMustBeRoutedProperly() {
        // Prepare the input record
        var tenantId = tenantIds.get(0);
        var originalKey = UUID.randomUUID().toString();
        var originalValue = "ShouldBeSomeBytesOfData";
        var sent = publishRecord(tenantId, originalKey, originalValue);

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
        assertEquals(output.headers(), sent.getHeaders());
    }


}
