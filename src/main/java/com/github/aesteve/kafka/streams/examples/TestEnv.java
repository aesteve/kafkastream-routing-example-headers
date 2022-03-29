package com.github.aesteve.kafka.streams.examples;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public interface TestEnv {

    Logger LOG = LoggerFactory.getLogger(TestEnv.class);

    String INPUT_TOPIC_PREFIX = "incoming-topic";
    String OUTPUT_TOPIC_PREFIX = "output-topic";
    String TENANT_ID_HEADER = "tenant-id";

    static String outputTopicFor(String tenantId) {
        return String.format("%s-%s", OUTPUT_TOPIC_PREFIX, tenantId);
    }

    static String inputTopicFor(String tenantId) {
        return String.format("%s-%s", INPUT_TOPIC_PREFIX, tenantId);
    }

    static String tenantId(int idx) {
        return String.format("tenant-%s", idx);
    }

    static List<String> tenantIds(int size) {
        return IntStream.range(0, size).mapToObj(TestEnv::tenantId).toList();
    }

    static void prepareTestEnv(Properties config, int nbInputTenants, int nbOutputTenants) throws ExecutionException, InterruptedException {
        var adminClient = AdminClient.create(config);
        var tenantIds = tenantIds(nbOutputTenants);
        var outputTopicNames = tenantIds.stream().map(TestEnv::outputTopicFor).toList();
        var inputTopicNames = IntStream.range(0, nbInputTenants).mapToObj(Integer::toString).toList();
        var toDelete = Stream.concat(
                inputTopicNames.stream(),
                outputTopicNames.stream()
        ).toList();
        LOG.info("Deleting existing topics {}", toDelete);
        adminClient.deleteTopics(toDelete).all().get();
        LOG.info("Creating topics");
        var toCreate = Stream.concat(
                outputTopicNames.stream().map(name -> new NewTopic(outputTopicFor(name), 6, (short) 3)),
                inputTopicNames.stream().map(name -> new NewTopic(inputTopicFor(name), 50, (short) 3))
        ).toList();
        adminClient.createTopics(toCreate).all().get();
    }

}
