package com.ch.delayqueue.core.internal

import com.ch.delayqueue.core.DelayQueueService
import com.ch.delayqueue.core.common.Constants
import io.circe.generic.auto._
import io.circe.parser.decode
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.Properties
import scala.jdk.javaapi.CollectionConverters

class DelayedMessageOutputTopicConsumer(kafkaConfig: Map[String, String]) {
  private val props = {
    val p = new Properties()
    kafkaConfig.foreach { case (k, v) => p.setProperty(k, v) }
    p
  }
  private val kafkaConsumer = new KafkaConsumer[String, String](props)
  private val logger = LoggerFactory.getLogger(this.getClass)

  def consume(): Unit = {
    while (true) {
      kafkaConsumer.subscribe(CollectionConverters.asJavaCollection(List(Constants.delayQueueOutputTopic)))
      val records = kafkaConsumer.poll(Duration.ofSeconds(1))
      logger.debug(s"consume ${records.count()} records")
      records.forEach(record => {
        val streamMessageResult = decode[StreamMessage](record.value())
        streamMessageResult match {
          case Right(streamMessage) =>
            DelayQueueService.getCallbacks.get(streamMessage.message.namespace) match {
              case Some(callback) => callback(streamMessage.message)
              case None => logger.error(s"no callback for message: ${record.value()}")
            }
            logger.info(s"consume message: ${record.value()}, ${record.key()}")
          case Left(error) => logger.error(s"decode streamMessage error, error:$error")
        }
      })
    }
  }

  sys.addShutdownHook(() => kafkaConsumer.close())
}
