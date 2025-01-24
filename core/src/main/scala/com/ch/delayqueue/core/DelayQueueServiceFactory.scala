package com.ch.delayqueue.core

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit

object DelayQueueServiceFactory {
  private val delayQueueTopic = "Delay_Queue_Topic"
  @volatile private var instance: Option[DelayQueueService] = None

  def getServiceInstance(kafkaConfig: Map[String, String]): DelayQueueService = {
    if (instance.isEmpty) {
      synchronized {
        if (instance.isEmpty) {
          instance = Some(new DelayQueueService(kafkaConfig))
        }
      }
    }
    instance.get
  }

  private class DelayQueueService(kafkaConfig: Map[String, String]) {
    private val props = new Properties()
    kafkaConfig.foreach { case (k, v) => props.setProperty(k, v) }
    private val kafkaProducer = new KafkaProducer[String, String](props)

    def executeWithFixedDelay(message: Message, delaySeconds: Long): Unit = {
      val producerRecord = new ProducerRecord[String, String](DelayQueueServiceFactory.delayQueueTopic, message.id, message.value)
      kafkaProducer.send(producerRecord).get(1, TimeUnit.SECONDS)
    }

    Runtime.getRuntime().addShutdownHook(new Thread {
      override def run(): Unit = {
        kafkaProducer.close(Duration.ofSeconds(3))
      }
    })
  }
}