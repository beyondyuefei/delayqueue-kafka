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
  private val logger = LoggerFactory.getLogger(classOf[DelayedMessageSchedulerProcessor])
  private val startTime = String.format("%013d", 0)

  override def init(context: api.ProcessorContext[String, String]): Unit = {
    this.context = context
    // 初始化状态存储
    store = context.getStateStore(Constants.storeName).asInstanceOf[KeyValueStore[String, String]]
    // 调度定时任务，每隔一段时间检查是否有消息需要处理
    context.schedule(Duration.ofMillis(300), org.apache.kafka.streams.processor.PunctuationType.WALL_CLOCK_TIME, _ => {
      val endTime = String.format("%013d", System.currentTimeMillis()) + "\uffff"
      val iterator = store.range(startTime, endTime)
      if (!iterator.hasNext) {
        logger.debug("store not have message...")
      }
      while (iterator.hasNext) {
        val entry = iterator.next()
        try {
          val streamMessageResult = decode[StreamMessage](entry.value)
          streamMessageResult match {
            case Right(streamMessage) =>
              logger.debug(s"message time happened..., value:${streamMessage.message}")
              // 延迟时间到达，处理消息
              context.forward(new api.Record[String, String](entry.key, entry.value, context.currentSystemTimeMs()))
              store.delete(entry.key)
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
    val streamMessageResult = decode[StreamMessage](record.value)
    streamMessageResult match {
      case Right(streamMessage) =>
        // 固定13位长度补全，按storeKey有序存储和后续范围查询
        val storeKey = String.format("%013d", System.currentTimeMillis() + streamMessage.delaySeconds * 1000) + "_" + record.key()
        val storeValue = record.value()
        // 保存消息到持久化k-v存储系统(RocksDB)
        store.put(storeKey, storeValue)
        logger.debug(s"put record to store, storeKey:$storeKey, storeValue:$storeValue")
      case Left(error) =>
        logger.error(s"decode streamMessage error, error:$error")
    }
  }
}