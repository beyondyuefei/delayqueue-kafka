package com.ch.delayqueue.starter;

import com.ch.delayqueue.core.DelayQueueService;
import com.ch.delayqueue.core.internal.InternalKafkaConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.Objects;
import java.util.Optional;

@Slf4j
@Configuration
@ComponentScan(basePackages = {"com.ch.delayqueue.starter"})
public class DelayQueueConfiguration {
    private final KafkaConfig kafkaConfig;

    @Autowired
    public DelayQueueConfiguration(final KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    @Bean
    public DelayQueueService delayQueueService() {
        final String bootstrapServers = Objects.requireNonNullElse(kafkaConfig.getBootstrapServers(), "localhost:9092");
        final String lingerMs = Optional.ofNullable(kafkaConfig.getLingerMs()).map(String::valueOf).orElse("0");
        final String appId = Objects.requireNonNull(kafkaConfig.getAppId());
        final InternalKafkaConfig internalKafkaConfig = new InternalKafkaConfig(bootstrapServers, lingerMs, appId);
        final DelayQueueService delayQueueService = DelayQueueService.getInstance(internalKafkaConfig);
        delayQueueService.start();
        log.info("init bean DelayQueueService success, kafkaConfig:{}", internalKafkaConfig);
        return delayQueueService;
    }
}
