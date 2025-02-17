package com.ch.delayqueue.core.internal

import com.ch.delayqueue.core.common.Constants
import io.circe.generic.auto._
import io.circe.parser._
import org.apache.kafka.streams.processor.api
import org.apache.kafka.streams.processor.api.{Processor, ProcessorContext}
import org.apache.kafka.streams.state.KeyValueStore
import org.slf4j.LoggerFactory

import java.time.Duration

// 自定义处理器类
class DelayedMessageSchedulerProcessor extends Processor[String, String, String, String] {
  private var context: ProcessorContext[String, String] = _
  private var store: KeyValueStore[String, String] = _
  private val DELAY_TIME_MS: Long = 5000 // 延迟时间，单位为毫秒
  private val logger = LoggerFactory.getLogger(classOf[DelayedMessageSchedulerProcessor])

  override def init(context: api.ProcessorContext[String, String]): Unit = {
    this.context = context
    // 初始化状态存储
    store = context.getStateStore(Constants.storeName).asInstanceOf[KeyValueStore[String, String]]
    // 调度定时任务，每隔一段时间检查是否有消息需要处理
    context.schedule(Duration.ofMillis(1000), org.apache.kafka.streams.processor.PunctuationType.WALL_CLOCK_TIME, _ => {
      val iterator = store.all()
      while (iterator.hasNext) {
        val entry = iterator.next()
        try {
          val streamMessageResult = decode[StreamMessage](entry.value)
          streamMessageResult match {
            case Right(streamMessage) =>
              if ((context.currentSystemTimeMs() - streamMessage.bizTimeInMs) >= (streamMessage.delaySeconds * 1000)) {
                logger.debug(s"message time happened..., value:${streamMessage.value}")
                // 延迟时间到达，处理消息
                context.forward(new api.Record[String, String](entry.key, entry.value, context.currentSystemTimeMs()))
                store.delete(entry.key)
              } else {
                logger.debug(s"message time not~~ happened..., value:${streamMessage.value}")
              }

            case Left(error) =>
              logger.error(s"decode streamMessage error, error:$error")
              store.delete(entry.key)
          }

        } catch {
          case e: Exception => logger.error(s"process error, error:${e.getMessage}")
        }
      }
      iterator.close()
    })
  }

  override def process(record: api.Record[String, String]): Unit = {
    // 存储消息
    store.put(record.key(), record.value())
  }

  override def close(): Unit = {
    // 关闭处理器时关闭状态存储
    store.close()
  }
}