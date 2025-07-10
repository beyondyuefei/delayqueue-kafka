package com.ch.delayqueue.core

import com.ch.delayqueue.core.common.DelayQueueResourceNames
import com.ch.delayqueue.core.internal._
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.immutable

class DelayQueueService private(kafkaConfig: InternalKafkaConfig) extends Lifecycle {
  private val props = {
    val p = new Properties()
    p.setProperty("bootstrap.servers", kafkaConfig.bootstrapServers)
    p.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    p.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    p.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    p.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    p
  }
  private val kafkaProducer = new KafkaProducer[String, String](props)
  private type Callback = Message => Unit
  private var callbacks = Map[String, Callback]()
  private var childComponents: List[Lifecycle] = List.empty
  private val initialized: AtomicBoolean = new AtomicBoolean(false)
  private val logger = LoggerFactory.getLogger(DelayQueueService.getClass)

  override def start(): Unit = {
    if (!initialized.compareAndSet(false, true)) {
      logger.info("DelayQueueService already started")
      return
    }
    DelayQueueResourceNames.initialize(kafkaConfig.appId)
    childComponents :+= CallbackThreadPool
    childComponents :+= new StreamMessageProcessTopologyConfigurator(kafkaConfig)
    childComponents :+= new DelayedMessageOutputTopicConsumer(kafkaConfig)
    childComponents.foreach(_.start())
  }

  override def stop(): Unit = {
    if (!initialized.compareAndSet(true, false)) {
      logger.info("DelayQueueService already stopped")
      return
    }
    childComponents.foreach(_.stop())
    kafkaProducer.close(Duration.ofSeconds(3))
  }

  def executeWithFixedDelay(message: Message, delaySeconds: Long): RecordMetadata = {
    val jsonStrVal = StreamMessage(delaySeconds, message).asJson.noSpaces
    val producerRecord = new ProducerRecord[String, String](DelayQueueResourceNames.appDelayQueueInputTopic, message.id, jsonStrVal)
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

  def getInstance(kafkaConfig: InternalKafkaConfig): DelayQueueService = {
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
