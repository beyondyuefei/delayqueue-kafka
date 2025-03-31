package com.ch.delayqueue.core.internal

import com.ch.delayqueue.core.DelayQueueService
import com.ch.delayqueue.core.common.Constants
import com.ch.delayqueue.core.internal.CallbackThreadPool.executionContext
import com.ch.delayqueue.core.internal.exception.LifecycleException
import io.circe.generic.auto._
import io.circe.parser.decode
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.RetriableException
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.Future
import scala.jdk.javaapi.CollectionConverters
import scala.util.control.Breaks.break
import scala.util.{Failure, Success}

class DelayedMessageOutputTopicConsumer(kafkaConfig: Map[String, String]) extends Component {
  private val props = {
    val p = new Properties()
    kafkaConfig.foreach { case (k, v) => p.setProperty(k, v) }
    p
  }
  private val kafkaConsumer = new KafkaConsumer[String, String](props)
  private val executorService: ExecutorService = Executors.newSingleThreadExecutor
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def start(): Unit = {
    try {
      kafkaConsumer.subscribe(CollectionConverters.asJavaCollection(List(Constants.delayQueueOutputTopic)))
    } catch {
      case e: Exception =>
        logger.error(s"subscribe kafka topic error, error:${e.getMessage}")
        throw new LifecycleException(s"subscribe kafka topic error, message:${e.getMessage}", e)
    }
    executorService.execute(() => {
      consume()
    })
  }

  override def stop(): Unit = {
    // todo: 这里是否需要加一个 try-catch ？ 也抛出LifeCycleException
    kafkaConsumer.close()
    executorService.close()
  }

  private def consume(): Unit = {
    while (true) {
      try {
        val records = kafkaConsumer.poll(Duration.ofSeconds(1))
        logger.debug(s"consume ${records.count()} records")
        records.forEach(record => {
          val streamMessageResult = decode[StreamMessage](record.value())
          streamMessageResult match {
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
                case None => logger.error(s"no callback found for message: $message")
              }
            case Left(error) => logger.error(s"decode streamMessage error, error:$error")
          }
        })
      } catch {
        case e: RetriableException =>
          logger.error(s"consume error, error:${e.getMessage}", e)
        case e: Exception =>
          logger.error(s"consume error, break poll(), error:${e.getMessage}", e)
          break
      }
    }
  }
}
