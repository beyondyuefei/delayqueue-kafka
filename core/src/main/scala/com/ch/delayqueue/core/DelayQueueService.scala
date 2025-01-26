package com.ch.delayqueue.core

import com.alibaba.fastjson2.JSON
import com.ch.delayqueue.core.internal.StreamMessage
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.collection.mutable

// todo: mutable -> immutable
class DelayQueueService private(kafkaConfig: mutable.Map[String, String]) {
  private val delayQueueTopic = "Delay_Queue_Topic"
  private val props = new Properties()
  kafkaConfig.foreach { case (k, v) => props.setProperty(k, v) }
  private val kafkaProducer = new KafkaProducer[String, String](props)
  private val logger = LoggerFactory.getLogger(DelayQueueService.getClass)

  def executeWithFixedDelay(message: Message, delaySeconds: Long): RecordMetadata = {
    val producerRecord = new ProducerRecord[String, String](delayQueueTopic, message.id, JSON.toJSONString(StreamMessage(delaySeconds, message.id, message.value)))
    val recordMetadata = kafkaProducer.send(producerRecord).get(1, TimeUnit.SECONDS)
    if (logger.isDebugEnabled()) logger.debug(s"topic:${recordMetadata.topic()}, partition:${recordMetadata.partition()}, offset:${recordMetadata.offset()}")
    recordMetadata
  }

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = {
      kafkaProducer.close(Duration.ofSeconds(3))
    }
  })
}

object DelayQueueService {
  @volatile private var instance: Option[DelayQueueService] = None

  def get(kafkaConfig: mutable.Map[String, String]): DelayQueueService = {
    if (instance.isEmpty) {
      synchronized {
        if (instance.isEmpty) {
          instance = Some(new DelayQueueService(kafkaConfig))
        }
      }
    }
    instance.get
  }
}
