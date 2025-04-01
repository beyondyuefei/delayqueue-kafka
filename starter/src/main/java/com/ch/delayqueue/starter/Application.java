package com.ch.delayqueue.starter;

import com.ch.delayqueue.core.Message;
import com.ch.delayqueue.core.DelayQueueService;
import com.ch.delayqueue.core.internal.StreamMessageDispatcher;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import scala.jdk.CollectionConverters;

import java.util.Map;
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
        final DelayQueueService delayQueueService = DelayQueueService.getInstance(scalaKafkaConfig);
        delayQueueService.start();
        final String orderNamespace = "order_pay_timeout";
        delayQueueService.registerCallback(orderNamespace, message -> {log.info("order_pay_timeout namespace callback value:{}", message.value()); return null;});
        delayQueueService.executeWithFixedDelay(new Message(orderNamespace, "1", "def123_" + ThreadLocalRandom.current().nextLong(9999)), 3);
        delayQueueService.executeWithFixedDelay(new Message(orderNamespace, "2", "def456_" + ThreadLocalRandom.current().nextLong(9999)), 10);
        log.info("send message success");
    }
}
