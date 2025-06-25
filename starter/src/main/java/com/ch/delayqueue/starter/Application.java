package com.ch.delayqueue.starter;

import com.ch.delayqueue.core.Message;
import com.ch.delayqueue.core.DelayQueueService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.concurrent.ThreadLocalRandom;

@SpringBootApplication
@Slf4j
public class Application {
    public static void main(String[] args) {
        final ConfigurableApplicationContext context = SpringApplication.run(Application.class, args);
        log.info("ch-delayqueue start success!");
        final DelayQueueService delayQueueService = context.getBean(DelayQueueService.class);
        final String orderNamespace = "order_pay_timeout";
        delayQueueService.registerCallback(orderNamespace, message -> {log.info("order_pay_timeout namespace callback value:{}", message.value()); return null;});
        delayQueueService.executeWithFixedDelay(new Message(orderNamespace, "1", "def123_" + ThreadLocalRandom.current().nextLong(9999)), 3);
        delayQueueService.executeWithFixedDelay(new Message(orderNamespace, "2", "def456_" + ThreadLocalRandom.current().nextLong(9999)), 3);
        delayQueueService.executeWithFixedDelay(new Message(orderNamespace, "3", "def789_" + ThreadLocalRandom.current().nextLong(9999)), 10);
        log.info("send message success");
    }
}
