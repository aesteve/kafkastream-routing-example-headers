package com.github.aesteve.kafka.streams.examples;

public enum ProduceStrategy {
    ROUND_ROBIN,
    RANDOM_UNIFORM,
    RANDOM_GAUSSIAN,
    GROUP_BY_TENANT,
}
