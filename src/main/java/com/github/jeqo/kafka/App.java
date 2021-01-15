package com.github.jeqo.kafka;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class App {
    final Config config;

    final Properties adminConfig;

    public App(Config config) {
        this.config = config;

        adminConfig = new Properties();
        adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafka.bootstrapServers);
    }

    public Map<String, Map<TopicPartition, Long>> lags() {
        var lags = new LinkedHashMap<String, Map<TopicPartition, Long>>();
        try (var adminClient = KafkaAdminClient.create(adminConfig)) {
            var cgs =
                    adminClient.listConsumerGroups().all().get()
                            .stream()
                            .map(ConsumerGroupListing::groupId)
                            .filter(s -> s.startsWith(config.connectGroupPrefix))
                            .collect(Collectors.toList());

            var cgTpLags = new LinkedHashMap<String, Map<TopicPartition, Long>>();

            cgs.forEach(s -> {
                var tpLags = new LinkedHashMap<TopicPartition, Long>();
                try {
                    var cgo =
                            adminClient.listConsumerGroupOffsets(s).partitionsToOffsetAndMetadata().get();
                    cgo.forEach((topicPartition, offsetAndMetadata) -> {
                        var groupOffset = offsetAndMetadata.offset();
                        try {
                            var offsets =
                                    adminClient.listOffsets(Collections.singletonMap(topicPartition, OffsetSpec.latest())).all().get();
                            var offsetInfo = offsets.get(topicPartition);
                            var latestOffset = offsetInfo.offset();

                            var lag = latestOffset - groupOffset;
                            if (lag >= config.maxLag) {
                                tpLags.put(topicPartition, lag);
                            }
                        } catch (InterruptedException | ExecutionException e) {
                            e.printStackTrace();
                        }
                    });
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                cgTpLags.put(s, tpLags);
            });

            adminClient.describeConsumerGroups(cgs).all().get().forEach((s, description) -> {
                var tpLags = cgTpLags.get(s);
                for (var member : description.members()) {
                    var l = new LinkedHashMap<TopicPartition, Long>();
                    member.assignment().topicPartitions().stream()
                            .filter(tpLags::containsKey)
                            .forEach(tp -> l.put(tp, tpLags.get(tp)));
                    if (!l.isEmpty()) {
                        lags.put(member.consumerId(), l);
                    }
                }
            });
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return lags;
    }

    public static void main(String[] args) {
        var bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        var connectGroupPrefix = System.getenv("CONNECT_GROUP_PREFIX");
        var config = new Config(
                new Config.Kafka(bootstrapServers),
                connectGroupPrefix,
                10_000L,
                Duration.ofMinutes(1));


        var app = new App(config);
        var lags = app.lags();
        lags.forEach((s, topicPartitionLongMap) -> {
            System.out.println(s + " -> " );
            topicPartitionLongMap.forEach((topicPartition, aLong) ->
                    System.out.println("  "+topicPartition+" : "+aLong));
        });
    }
}
