package com.ch.delayqueue.core.internal

import org.apache.kafka.streams.processor.api.{Processor, ProcessorContext, ProcessorSupplier, Record}
import org.apache.kafka.streams.processor.api
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}

import java.time.Duration
import java.util.Properties

// 自定义处理器类
class DelayedMessageProcessor extends Processor[String, String, String, String] {
  private var context: ProcessorContext[String, String] = _
  private var store: KeyValueStore[String, String] = _
  private val DELAY_TIME_MS: Long = 5000 // 延迟时间，单位为毫秒

  override def init(context: api.ProcessorContext[String, String]): Unit = {
    this.context = context
    // 初始化状态存储
    store = context.getStateStore("delayed-messages-store").asInstanceOf[KeyValueStore[String, String]]
    // 调度定时任务，每隔一段时间检查是否有消息需要处理
    context.schedule(Duration.ofMillis(1000), org.apache.kafka.streams.processor.PunctuationType.WALL_CLOCK_TIME, _ => {
      val iterator = store.all()
      while (iterator.hasNext) {
        val entry = iterator.next()
        if (context.currentSystemTimeMs() - entry.key.toLong >= DELAY_TIME_MS) {
          // 延迟时间到达，处理消息
          context.forward(new Record[String, String](entry.key, entry.value, entry.key.toLong))
          store.delete(entry.key)
        }
      }
      iterator.close()
    })
  }

  override def process(record: api.Record[String, String]): Unit = {
    // 存储消息，并记录当前时间戳作为键
    store.put(context.currentSystemTimeMs().toString, record.toString)
  }

  override def close(): Unit = {
    // 关闭处理器时关闭状态存储
    store.close()
  }
}