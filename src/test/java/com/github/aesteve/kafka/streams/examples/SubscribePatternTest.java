package com.github.aesteve.kafka.streams.examples;

import org.junit.jupiter.api.Test;

import static com.github.aesteve.kafka.streams.examples.TestEnv.inputTopicFor;
import static com.github.aesteve.kafka.streams.examples.TopicsRepartitionMapping.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SubscribePatternTest {

    @Test
    void shouldMatchAnyTenant() {
        var tenantId = "11";
        assertTrue(INPUT_TOPICS_PATTERN.matcher(inputTopicFor(tenantId)).matches());
    }

    @Test
    void shouldNotMatchOtherTenants() {
        var tenantId = "19";
        assertFalse(INPUT_TOPICS_PATTERN.matcher(inputTopicFor(tenantId)).matches());
    }
}
