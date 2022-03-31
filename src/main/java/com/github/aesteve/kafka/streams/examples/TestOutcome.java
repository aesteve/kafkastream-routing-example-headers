package com.github.aesteve.kafka.streams.examples;

public record TestOutcome(
        long totalDuration,
        long totalSentMsgs,
        double computedAvgSendRate,
        double jmxSendRate,
        long avgBatchDuration
) {}
