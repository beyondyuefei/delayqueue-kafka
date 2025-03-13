package com.ch.delayqueue.starter;

import com.ch.delayqueue.core.Message;
import com.ch.delayqueue.core.DelayQueueService;
import com.ch.delayqueue.core.internal.DelayedMessageOutputTopicConsumer;
import com.ch.delayqueue.core.internal.StreamMessageDispatcher;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import scala.jdk.CollectionConverters;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@SpringBootApplication
@Slf4j
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
        log.info("ch-delayqueue start success!");
        final Map<String, String> kafkaConfig = Map.of("bootstrap.servers", "localhost:9092", "linger.ms", "1",
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                ConsumerConfig.GROUP_ID_CONFIG, "delayqueue-consumer-group");

        final scala.collection.immutable.Map<String, String> scalaKafkaConfig = scala.collection.immutable.Map.from(CollectionConverters.MapHasAsScala((kafkaConfig)).asScala());
        StreamMessageDispatcher.dispatch();
        DelayQueueService.getInstance(scalaKafkaConfig).executeWithFixedDelay(new Message("test", "3", "def" + ThreadLocalRandom.current().nextLong(9999)), 3);
        DelayQueueService.getInstance(scalaKafkaConfig).executeWithFixedDelay(new Message("test", "4", "def" + ThreadLocalRandom.current().nextLong(9999)), 3);
        log.info("send message success");
        final DelayedMessageOutputTopicConsumer delayedMessageOutputTopicConsumer = new DelayedMessageOutputTopicConsumer(scalaKafkaConfig);
        try (ExecutorService executorService = Executors.newSingleThreadExecutor()) {
            executorService.execute(delayedMessageOutputTopicConsumer::consume);
            log.info("consume message success");
        } catch (Exception e) {
            log.error("consume message error:{}", e.getMessage());
        }
    }
}
