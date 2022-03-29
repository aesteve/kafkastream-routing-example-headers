package com.github.aesteve.kafka.streams.examples.utils;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.Map;

public interface MetricUtils {

    static double sumProduceRates(Map<MetricName, ? extends Metric> metrics) {
        return producerMetric(metrics, "record-send-rate");
    }

    static double producerMetric(Map<MetricName, ? extends Metric> metrics, String metric) {
        return metrics
                .entrySet()
                .stream()
                .filter(e -> e.getKey().name().equals(metric) && e.getKey().group().equals("producer-metrics"))
                .mapToDouble(e -> (double) e.getValue().metricValue())
                .sum();
    }

    static double totalSentRecords(Map<MetricName, ? extends Metric> metrics) {
        return metrics
                .entrySet()
                .stream()
                .filter(e -> e.getKey().name().equals("record-send-total") && e.getKey().group().equals("producer-metrics"))
                .mapToDouble(e -> (double) e.getValue().metricValue())
                .sum();
    }


}
