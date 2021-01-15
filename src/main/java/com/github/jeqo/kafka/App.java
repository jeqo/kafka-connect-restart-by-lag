package com.github.jeqo.kafka;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class App {
    static final Logger LOG = LoggerFactory.getLogger(App.class);

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

    static Pattern pattern = Pattern.compile("connector-consumer-(\\w+)-(\\d)-");

    static HttpClient httpClient = HttpClient.newBuilder().build();

    void restartTask(String connectorName, int task) throws IOException, InterruptedException {
        var url = URI.create(config.kafkaConnect.url + "/connectors/" + connectorName + "/tasks/" + task);
        LOG.info("Restarting connector task {}-{}", connectorName, task);
        var httpRequest = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.noBody())
                .uri(url)
                .build();
        var response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
        LOG.info("Result restart {}-{}", response.statusCode(), response.body());
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        var bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        var connectGroupPrefix = System.getenv("CONNECT_GROUP_PREFIX");
        var url = System.getenv("KAFKA_CONNECT_URL");
        var maxLagDefault = 100_000L;
        var maxLagEnv = System.getenv("MAX_LAG");
        long maxLag;
        if (maxLagEnv != null) {
            try {
                maxLag = Long.parseLong(maxLagEnv);
            } catch (NumberFormatException e) {
                LOG.warn("Using default lag {}", maxLagDefault, e);
                maxLag = maxLagDefault;
            }
        } else maxLag = maxLagDefault;
        var config = new Config(
                new Config.Kafka(bootstrapServers),
                connectGroupPrefix,
                maxLag,
                Duration.ofMinutes(1),
                new Config.KafkaConnect(url));

        var app = new App(config);

        //Sample output:
        // connector-consumer-SplunkSink_group_santander-1-07838100-36fa-40dc-bd4b-d5a556c3b7e8 ->
        //   GROUP_SANTANDER_SPLUNK_SINK_JSON-6 : 68403
        //   GROUP_SANTANDER_SPLUNK_SINK_JSON-5 : 68949
        //   GROUP_SANTANDER_SPLUNK_SINK_JSON-7 : 64913
        //   GROUP_SANTANDER_SPLUNK_SINK_JSON-4 : 62396
        var lags = app.lags();
        lags.forEach((s, topicPartitionLongMap) -> {
            System.out.println(s + " -> ");
            topicPartitionLongMap.forEach((topicPartition, aLong) ->
                    System.out.println("  " + topicPartition + " : " + aLong));
        });

        for (String instance : lags.keySet()) {
            Matcher matcher = pattern.matcher(instance);
            var connectorName = matcher.group(1);
            var task = Integer.parseInt(matcher.group(2));
            app.restartTask(connectorName, task);
        }
    }
}
