package com.ch.delayqueue.core.internal

import com.ch.delayqueue.core.DelayQueueService
import com.ch.delayqueue.core.common.Constants
import com.ch.delayqueue.core.internal.CallbackThreadPool.executionContext
import com.ch.delayqueue.core.internal.exception.LifecycleException
import io.circe.generic.auto._
import io.circe.parser.decode
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.errors.{RetriableException, WakeupException}
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import scala.concurrent.{Await, Future, TimeoutException}
import scala.jdk.javaapi.CollectionConverters
import scala.util.{Failure, Success}

class DelayedMessageOutputTopicConsumer(kafkaConfig: Map[String, String], futureTimeoutInSeconds: Long = 1) extends Component {
  private val props = {
    val p = new Properties()
    kafkaConfig.foreach { case (k, v) => p.setProperty(k, v) }
    p
  }
  private val kafkaConsumer = new KafkaConsumer[String, String](props)
  private val executorService: ExecutorService = Executors.newSingleThreadExecutor
  @volatile private var isRunning = true
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def start(): Unit = {
    try {
      kafkaConsumer.subscribe(CollectionConverters.asJavaCollection(List(Constants.delayQueueOutputTopic)))
      logger.info(s"kafka consumer component started, subscribe kafka topic: ${Constants.delayQueueOutputTopic}")
    } catch {
      case e: Exception =>
        logger.error(s"subscribe kafka topic error, error:${e.getMessage}")
        throw new LifecycleException(s"subscribe kafka topic error, message:${e.getMessage}", e)
    }
    executorService.execute(() => {
      while (isRunning) {
        // 1. poll
        val records: ConsumerRecords[String, String] = try {
          kafkaConsumer.poll(Duration.ofSeconds(1))
        } catch {
          case e: Exception =>
            e match {
              case _: RetriableException =>
                logger.error(s"Kafka poll error, error: ${e.getMessage}, retry again", e)
              case _: WakeupException =>
                logger.info("Wakeup consumer")
              case _ =>
                logger.error(s"Kafka poll error, error: ${e.getMessage}, return from while loop", e)
                isRunning = false
            }
            ConsumerRecords.empty()
        }

        if (!records.isEmpty) {
          // 2. consume，execute Callback
          val futures = try {
            consumeRecordsWithCallback(records)
          } catch {
            case e: Exception =>
              // 在 consumeRecordsWithCallback() 中 不应该 抛出异常，因为：
              //   1、decode的错误也是封装在Either中
              //   2、业务Callback的执行是在Future包装的异步线程中执行的
              // 所以如果走到这里，则说明是框架层面代码出了bug，涉及这批的消息都不会被认为是 已消费，会做重试处理
              logger.error(s"unexpected error here, message:${e.getMessage}", e)
              List.empty[Future[Unit]]
          }

          logger.debug(s"futures.nonEmpty: ${futures.nonEmpty}")

          // 3. wait Callback Futures result
          if (futures.nonEmpty) {
            val isCallbacksExecuted = try {
              Await.result(Future.sequence(futures), scala.concurrent.duration.Duration(futureTimeoutInSeconds, TimeUnit.SECONDS))
              true
            } catch {
              case e: TimeoutException =>
                logger.error(s"wait for futures Callback execute timeout, message:${e.getMessage}", e)
                // 超时时间是用户自己可配置的，因此这里即便业务逻辑执行等待超时，我们仍然认为这个消息的回调在用户侧已经执行完成
                true
              case e: Exception =>
                logger.error(s"wait for futures Callback execute error, message:${e.getMessage}", e)
                // 非预期的错误，则后续会触发消息的poll重试&回调
                false
            }


            if (isCallbacksExecuted) {
              // 4. commit
              try {
                logger.debug("ready to commit kafka offset")
                kafkaConsumer.commitSync()
              } catch {
                case e: Exception =>
                  logger.error(s"commit error, error:${e.getMessage}", e)
              }
            }
          }
        }
      }
      kafkaConsumer.close()
      logger.info("Kafka consumer closed gracefully")
    })
  }

  override def stop(): Unit = {
    try {
      isRunning = false
      kafkaConsumer.wakeup()
      executorService.close()
    } catch {
      case e: Exception =>
        val errorMsg = s"stop kafka consumer component error, message:${e.getMessage}"
        logger.error(errorMsg, e)
        throw new LifecycleException(errorMsg, e)
    }
  }

  private def consumeRecordsWithCallback(records: ConsumerRecords[String, String]): List[Future[Unit]] = {
    logger.debug(s"consume ${records.count()} records")
    CollectionConverters.asScala(records).foldLeft(List.empty[Future[Unit]]) { (acc, record) => {
      decode[StreamMessage](record.value()) match {
        case Right(streamMessage) =>
          val message = streamMessage.message
          DelayQueueService.getCallbacks.get(message.namespace) match {
            case Some(callback) =>
              val future = Future {
                logger.debug(s"ready to execute callback for message: ${record.value()}, ${record.key()}")
                callback(message)
              }
              future.onComplete {
                case Success(_) => logger.info(s"execute callback for message success: $message")
                case Failure(ex) => logger.error(s"execute callback for message failed: $message", ex)
              }
              acc :+ future
            case None =>
              logger.error(s"no callback found for message: $message")
              acc
          }
        case Left(error) =>
          logger.error(s"decode streamMessage error, error:$error")
          acc
      }
    }
    }
  }
}
