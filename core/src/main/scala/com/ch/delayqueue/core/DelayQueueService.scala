package com.ch.delayqueue.core

import com.ch.delayqueue.core.common.Constants.delayQueueInputTopic
import com.ch.delayqueue.core.internal.{CallbackThreadPool, Component, DelayedMessageOutputTopicConsumer, Lifecycle, StreamMessage, StreamMessageProcessTopologyConfigurator}
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.collection.immutable

class DelayQueueService private(kafkaConfig: Map[String, String]) extends Lifecycle {
  private val props = {
    val p = new Properties()
    kafkaConfig.foreach { case (k, v) => p.setProperty(k, v) }
    p
  }
  private val kafkaProducer = new KafkaProducer[String, String](props)
  private type Callback = Message => Unit
  private var callbacks = Map[String, Callback]()
  private var childComponents: List[Lifecycle] = List.empty
  private val logger = LoggerFactory.getLogger(DelayQueueService.getClass)

  override def start(): Unit = {
    childComponents :+= CallbackThreadPool
    childComponents :+= StreamMessageProcessTopologyConfigurator
    childComponents :+= new DelayedMessageOutputTopicConsumer(kafkaConfig)
    childComponents.foreach(_.start())
  }

  override def stop(): Unit = {
    childComponents.foreach(_.stop())
    kafkaProducer.close(Duration.ofSeconds(3))
  }

  def executeWithFixedDelay(message: Message, delaySeconds: Long): RecordMetadata = {
    val jsonStrVal = StreamMessage(delaySeconds, System.currentTimeMillis(), message).asJson.noSpaces
    val producerRecord = new ProducerRecord[String, String](delayQueueInputTopic, message.id, jsonStrVal)
    val recordMetadata = kafkaProducer.send(producerRecord).get(1, TimeUnit.SECONDS)
    logger.debug(s"topic:${recordMetadata.topic()}, partition:${recordMetadata.partition()}, offset:${recordMetadata.offset()}")
    recordMetadata
  }

  def registerCallback(namespace: String, callback: Callback): Unit = {
    callbacks += (namespace -> callback)
  }

  private def getCallback: Map[String, Callback] = callbacks
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
