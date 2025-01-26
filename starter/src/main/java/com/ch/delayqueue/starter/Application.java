package com.ch.delayqueue.starter;

import com.ch.delayqueue.core.Message;
import com.ch.delayqueue.core.DelayQueueService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import scala.jdk.CollectionConverters;

import java.util.Map;

@SpringBootApplication
@Slf4j
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
        log.info("ch-delayqueue start success!");
        Map<String, String> kafkaConfig = Map.of("bootstrap.servers","localhost:9092", "linger.ms" ,"1",
                "key.serializer" ,"org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        DelayQueueService.get(CollectionConverters.MapHasAsScala((kafkaConfig)).asScala()).executeWithFixedDelay(new Message("test", "4", "def"), 10);
        log.info("send message success");
    }
}
