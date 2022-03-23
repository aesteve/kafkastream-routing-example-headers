package com.github.aesteve.kafka.streams.examples;

import org.junit.jupiter.api.Test;

import static com.github.aesteve.kafka.streams.examples.TopicsRepartitionMapping.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SubscribePatternTest {

    @Test
    void shouldMatchAnyTenant() {
        var tenantId = "4";
        assertTrue(INPUT_TOPICS_PATTERN.matcher(inputTopicFor(tenantId)).matches());
    }

}
