package com.ch.delayqueue.core

import com.ch.delayqueue.core.internal.InternalKafkaConfig
import org.junit.jupiter.api.{Assertions, Test}

class DelayQueueServiceTest {
  @Test
  def sendMessageTest(): Unit = {
    @volatile var value: Option[String] = Option.empty
    val kafkaConfig = InternalKafkaConfig(
      bootstrapServers = "localhost:9092",
      lingerMs = "10",
      appId = "test"
    )

    val delayQueueService = DelayQueueService.getInstance(kafkaConfig)
    val orderNamespace = "order_pay_timeout"
    delayQueueService.registerCallback(orderNamespace, msg => {
      println(s"in callback ${msg.value}")
      value = Some(msg.value)
    })

    delayQueueService.start()

    val record = delayQueueService.executeWithFixedDelay(Message(orderNamespace, "14", "def"), 3)
    Assertions.assertNotNull(record)
    Assertions.assertNotNull(record.partition())
    Assertions.assertNotNull(record.offset())
    println(s"topic:${record.topic()}, partition:${record.partition()}, offset:${record.offset()}")
    Assertions.assertNotNull(record.offset())
    // 如果是重启jvm后马上触发此单测的场景，这里的sleep时间需要改为45s+ ,(@see https://cwiki.apache.org/confluence/display/KAFKA/KIP-735%3A+Increase+default+consumer+session+timeout)，因为启动时需要等待kafka的rebalance完成
    Thread.sleep(10000)
    delayQueueService.stop()
    Assertions.assertTrue(value.nonEmpty)
  }
}
