package com.ch.delayqueue.core.internal

import com.ch.delayqueue.core.common.Constants
import com.ch.delayqueue.core.common.Constants.delayQueueInputTopic
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.streams.processor.api
import org.apache.kafka.streams.processor.api.{Processor, ProcessorContext}
import org.slf4j.LoggerFactory

import java.util.Properties
import java.util.concurrent.TimeUnit

class DelayedMessageOutTopicProducerProcessor(kafkaConfig: Map[String, String]) extends Processor[String, String, String, String] {
  private val props = new Properties()
  kafkaConfig.foreach { case (k, v) => props.setProperty(k, v) }
  private val kafkaProducer = new KafkaProducer[String, String](props)
  private val logger = LoggerFactory.getLogger(classOf[DelayedMessageOutTopicProducerProcessor])

  override def init(context: ProcessorContext[String, String]): Unit = {
  }

  override def close(): Unit = {
     kafkaProducer.close()
  }

  override def process(record: api.Record[String, String]): Unit = {
    val producerRecord = new ProducerRecord[String, String](Constants.delayQueueOutputTopic, record.key(), record.value())
    val recordMetadata = kafkaProducer.send(producerRecord).get(1, TimeUnit.SECONDS)
    if (logger.isDebugEnabled()) logger.debug(s"topic:${recordMetadata.topic()}, partition:${recordMetadata.partition()}, offset:${recordMetadata.offset()}")
  }
}
