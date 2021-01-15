package com.github.jeqo.kafka;

import java.time.Duration;

public class Config {
    final Kafka kafka;
    final String connectGroupPrefix;
    final long maxLag;
    final Duration frequency;

    public Config(Kafka kafka, String connectGroupPrefix, long maxLag, Duration frequency) {
        this.kafka = kafka;
        this.connectGroupPrefix = connectGroupPrefix;
        this.maxLag = maxLag;
        this.frequency = frequency;
    }

    static class Kafka {
        final String bootstrapServers;

        Kafka(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }
    }
}
