package com.ch.delayqueue.core

import com.alibaba.fastjson2.JSON
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit

class DelayQueueService private(kafkaConfig: Map[String, String]) {
  private val delayQueueTopic = "Delay_Queue_Topic"
  private val props = new Properties()
  kafkaConfig.foreach { case (k, v) => props.setProperty(k, v) }
  private val kafkaProducer = new KafkaProducer[String, String](props)

  def executeWithFixedDelay(message: Message, delaySeconds: Long): Unit = {
    val producerRecord = new ProducerRecord[String, String](delayQueueTopic, message.id, JSON.toJSONString(new StreamMessage(delaySeconds, message.id, message.value)))
    val recordMetadata = kafkaProducer.send(producerRecord).get(1, TimeUnit.SECONDS)
    println(s"${recordMetadata.topic()}, ${recordMetadata.partition()}, ${recordMetadata.offset()}")
  }

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = {
      kafkaProducer.close(Duration.ofSeconds(3))
    }
  })
}

object DelayQueueService {
  @volatile private var instance: Option[DelayQueueService] = None

  def get(kafkaConfig: Map[String, String]): DelayQueueService = {
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
