package com.ch.delayqueue.core

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.junit.jupiter.api.{Assertions, Test}

class DelayQueueServiceTest {
  @Test
  def sendMessageTest(): Unit = {
    val kafkaConfig = Map("bootstrap.servers" -> "localhost:9092", "linger.ms" -> "1",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> "delayqueue-consumer-group",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")

    val delayQueueService = DelayQueueService.getInstance(kafkaConfig)
    val orderNamespace = "order_pay_timeout"
    delayQueueService.registerCallback(orderNamespace, msg => {
      println(s"in callback ${msg.value}")
      Assertions.assertNotNull(msg.value)
    })

    delayQueueService.start()

    val record = delayQueueService.executeWithFixedDelay(Message(orderNamespace, "14", "def"), 3)
    Assertions.assertNotNull(record)
    Assertions.assertNotNull(record.partition())
    Assertions.assertNotNull(record.offset())
    println(s"topic:${record.topic()}, partition:${record.partition()}, offset:${record.offset()}")
    Assertions.assertNotNull(record.offset())

    Thread.sleep(10000)
    delayQueueService.stop()
  }
}
