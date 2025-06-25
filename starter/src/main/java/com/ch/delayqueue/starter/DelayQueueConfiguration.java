package com.ch.delayqueue.starter;

import com.ch.delayqueue.core.DelayQueueService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import scala.jdk.CollectionConverters;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

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
        final Map<String, String> kafkaConfigMap = Map.of("bootstrap.servers", Objects.requireNonNullElse(kafkaConfig.getBootstrapServers(), "localhost:9092"),
                "linger.ms", Optional.ofNullable(kafkaConfig.getLingerMs()).map(String::valueOf).orElse("0"),
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.GROUP_ID_CONFIG, "delayqueue-consumer-group:" +
                        Optional.ofNullable(kafkaConfig.getAppId())
                                .orElseGet(() -> UUID.randomUUID().toString().replace("-", "")));

        final scala.collection.immutable.Map<String, String> scalaKafkaConfig = scala.collection.immutable.Map.from(CollectionConverters.MapHasAsScala((kafkaConfigMap)).asScala());
        final DelayQueueService delayQueueService = DelayQueueService.getInstance(scalaKafkaConfig);
        delayQueueService.start();
        log.info("init bean DelayQueueService success, kafkaConfig:{}", kafkaConfigMap);
        return delayQueueService;
    }
}
