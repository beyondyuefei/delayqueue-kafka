package com.ch.delayqueue.core

import com.ch.delayqueue.core.common.Constants.delayQueueInputTopic
import com.ch.delayqueue.core.internal.StreamMessage
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.collection.{immutable, mutable}

class DelayQueueService private(kafkaConfig: Map[String, String]) {
  private val props = {
    val p = new Properties()
    kafkaConfig.foreach { case (k, v) => p.setProperty(k, v) }
    p
  }
  private val kafkaProducer = new KafkaProducer[String, String](props)
  private type Callback = Message => Unit
  private val callbacks = mutable.Map[String, Callback]()
  private val logger = LoggerFactory.getLogger(DelayQueueService.getClass)

  def executeWithFixedDelay(message: Message, delaySeconds: Long): RecordMetadata = {
    val jsonStrVal = StreamMessage(delaySeconds, System.currentTimeMillis(), message).asJson.noSpaces
    val producerRecord = new ProducerRecord[String, String](delayQueueInputTopic, message.id, jsonStrVal)
    val recordMetadata = kafkaProducer.send(producerRecord).get(1, TimeUnit.SECONDS)
    if (logger.isDebugEnabled()) logger.debug(s"topic:${recordMetadata.topic()}, partition:${recordMetadata.partition()}, offset:${recordMetadata.offset()}")
    recordMetadata
  }

  def registerCallback(namespace: String, callback: Callback): Unit = {
    callbacks += (namespace -> callback)
  }

  private def getCallback: immutable.Map[String, Callback] = callbacks.toMap

  sys.addShutdownHook({
    kafkaProducer.close(Duration.ofSeconds(3))
  })
}

object DelayQueueService {
  @volatile private var instance: Option[DelayQueueService] = None

  def getInstance(kafkaConfig: Map[String, String]): DelayQueueService = {
    if (instance.isEmpty) {
      synchronized {
        if (instance.isEmpty) {
          instance = Some(new DelayQueueService(kafkaConfig))
        }
      }
    }
    instance.get
  }

  def getCallbacks: immutable.Map[String, Message => Unit] = instance.get.getCallback
}
