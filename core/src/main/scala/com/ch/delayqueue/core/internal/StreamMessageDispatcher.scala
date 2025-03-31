package com.ch.delayqueue.core.internal

import com.ch.delayqueue.core.common.Constants.{delayQueueInputTopic, delayQueueOutputTopic, storeName}
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import java.time.Duration
import java.util.Properties


object StreamMessageDispatcher extends Lifecycle {
  override def start(): Unit = {

  }

  override def stop(): Unit = {

  }

  def dispatch(): Unit = {
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

    println(streamsBuilder.build().describe())

    // 配置 Kafka Streams
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "delayed-message-stream")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.String().getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.String().getClass)
    // 构建 Kafka Streams 实例
    val streams = new KafkaStreams(streamsBuilder.build(), props)
    // 启动 Kafka Streams
    streams.start()


    // 注册关闭钩子，确保程序优雅关闭
    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(3))
    }

  }
}
