package com.ch.delayqueue.core

import com.ch.delayqueue.core.internal.{DelayedMessageOutputTopicConsumer, StreamMessageDispatcher}
import org.junit.jupiter.api.{Assertions, Test}

import scala.collection.mutable

class DelayQueueServiceTest {
  @Test
  def sendMessageTest(): Unit = {
    val kafkaConfig = Map("bootstrap.servers" -> "localhost:9092", "linger.ms" -> "1",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")

    StreamMessageDispatcher.dispatch(kafkaConfig)

    val record = DelayQueueService.get(kafkaConfig).executeWithFixedDelay(Message("test", "11", "def"), 10)
    Assertions.assertNotNull(record)
    Assertions.assertNotNull(record.partition())
    Assertions.assertNotNull(record.offset())
    println(s"topic:${record.topic()}, partition:${record.partition()}, offset:${record.offset()}")

    val sinkProcessor = new DelayedMessageOutputTopicConsumer(kafkaConfig)
    sinkProcessor.consume()
  }
}
