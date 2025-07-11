### 主要特性
- 直接支持秒级精度自定义消息的延迟时间，区别于传统的RocketMQ等只能按照内置预先定义好的延迟等级
- 在kafka stream之上构建延迟消息队列，天然支持了消息的持久化、高可用、partition水平扩展
- K-V数据库RocksDB作为kafka stream local消息缓存层，基于LSM实现延迟消息按过期时间优先的高效范围查询
- 提供 SpringBoot starter，开箱即用

### 环境要求
- Scala 2.13
- JDK 21+
- SpringBoot 6.x
- kafka 3.9+

### use demo
- 以订单延迟消息为例，首先在需要使用延迟消息的地方注入bean: DelayQueueService
- 通过 delayQueueService.executeWithFixedDelay 方法发送延迟消息，并指定namespace
```
import com.ch.delayqueue.core.DelayQueueService;
import com.ch.delayqueue.core.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Service
public class OrderService {
    private final DelayQueueService delayQueueService;

    public OrderService(DelayQueueService delayQueueService) {
        this.delayQueueService = delayQueueService;
    }

    @PostConstruct
    public void  init() {
        final String orderNamespace = NamespaceConstants.ORDER_PAY_TIMEOUT;
        delayQueueService.executeWithFixedDelay(new Message(orderNamespace, "5", "fde123_" + ThreadLocalRandom.current().nextLong(9999)), 5);
        delayQueueService.executeWithFixedDelay(new Message(orderNamespace, "6", "fde456_" + ThreadLocalRandom.current().nextLong(9999)), 9);
        log.info("send message success");
    }
}
```

- 定义延迟消息回调处理的实现类，必须加上类注解@DelayQueueConsumer并提供与发送延迟消息一致的namespace
```
import com.ch.delayqueue.core.Message;
import com.ch.delayqueue.starter.DelayQueueCallback;
import com.ch.delayqueue.starter.DelayQueueConsumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@DelayQueueConsumer(namespace = NamespaceConstants.ORDER_PAY_TIMEOUT)
public class OrderDelayQueueCallback implements DelayQueueCallback {
    @Override
    public void OnDelayMessageTriggered(Message message) {
        log.info("order-pay-timeout namespace OnDelayMessageTriggered, message:{}", message);
    }
}
```
- namespace定义的常量类，不同的namespace代表不同业务域的延迟消息、彼此之间互相隔离
```
public class NamespaceConstants {
    public static final String ORDER_PAY_TIMEOUT = "order-pay-timeout";
}
```
- 应用启动类
```
@SpringBootApplication
@Slf4j
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
        log.info("ch-delayqueue-consumer app start success!");
    }
}
```
- application.yml
```
kafka:
  app-id: order-service
```
