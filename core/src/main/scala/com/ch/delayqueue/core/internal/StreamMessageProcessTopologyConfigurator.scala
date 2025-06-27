package com.ch.delayqueue.core.internal

import com.ch.delayqueue.core.common.Constants.{delayQueueInputTopic, delayQueueOutputTopic, storeName}
import com.ch.delayqueue.core.internal.exception.LifecycleException
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.{Objects, Properties}


private[core] class StreamMessageProcessTopologyConfigurator(kafkaConfig: InternalKafkaConfig) extends Component {
  private var streams: KafkaStreams = _
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def start(): Unit = {
    try {
      val streamsBuilder = new StreamsBuilder()
      // 定义状态存储
      val storeSupplier = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(storeName),
        org.apache.kafka.common.serialization.Serdes.String(),
        org.apache.kafka.common.serialization.Serdes.String()
      )

      streamsBuilder.addStateStore(storeSupplier)
      val delayedMessageSchedulerProcessor: ProcessorSupplier[String, String, String, String] = () => new DelayedMessageSchedulerProcessor()
      // 创建 KStream
      streamsBuilder.stream[String, String](delayQueueInputTopic)
        .process(delayedMessageSchedulerProcessor, storeName)
        .to(delayQueueOutputTopic)

      // 配置 Kafka Streams
      val props = new Properties()
      // 会作为kafka stream内置的topic name, see: org.apache.kafka.streams.processor.internals.StreamThread.runLoop
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "delayed-message-stream-" + Objects.requireNonNull(kafkaConfig.appId))
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Objects.requireNonNull(kafkaConfig.bootstrapServers))
      props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.String().getClass)
      props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.String().getClass)
      // 构建 Kafka Streams 实例
      streams = new KafkaStreams(streamsBuilder.build(), props)
      // 启动 Kafka Streams
      streams.start()
      logger.info("Kafka Streams component started")
    } catch {
      case e: Exception =>
        logger.error("Failed to start Kafka Streams", e)
        throw new LifecycleException("Failed to start Kafka Streams")
    }
  }

  override def stop(): Unit = {
    try {
      streams.close(Duration.ofSeconds(3))
    } catch {
      case e: Exception =>
        logger.error("Failed to stop Kafka Streams", e)
        throw new LifecycleException("Failed to stop Kafka Streams")
    }
  }
}
