package com.ch.delayqueue.core.internal

import com.ch.delayqueue.core.{DelayQueueService, Message}
import com.ch.delayqueue.core.common.Constants
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.Properties
import scala.jdk.javaapi.CollectionConverters

class DelayedMessageOutputTopicConsumer(kafkaConfig: Map[String, String]) {
  private val props = new Properties()
  kafkaConfig.foreach { case (k, v) => props.setProperty(k, v) }
  private val kafkaConsumer = new KafkaConsumer[String, String](props)
  private val logger = LoggerFactory.getLogger(this.getClass)

  def consume(): Unit = {
    while (true) {
      kafkaConsumer.subscribe(CollectionConverters.asJavaCollection(List(Constants.delayQueueOutputTopic)))
      val records = kafkaConsumer.poll(Duration.ofSeconds(1))
      logger.debug(s"consume ${records.count()} records")
      records.forEach(record => {
        val message = Message("", "", "")
        DelayQueueService.getCallbacks.get("asdsa") match {
          case Some(callback) => callback(message)
          case None => logger.error(s"no callback for message: ${record.value()}")
        }
        logger.info(s"consume message: ${record.value()}, ${record.key()}")
      })
    }
  }

  sys.addShutdownHook(() => kafkaConsumer.close())
}
