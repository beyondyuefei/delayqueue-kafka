package com.ch.delayqueue.core.internal

import com.ch.delayqueue.core.DelayQueueService
import com.ch.delayqueue.core.common.Constants
import io.circe.generic.auto._
import io.circe.parser.decode
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, LinkedBlockingQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.javaapi.CollectionConverters
import scala.util.{Failure, Success}

class DelayedMessageOutputTopicConsumer(kafkaConfig: Map[String, String]) {
  private val props = {
    val p = new Properties()
    kafkaConfig.foreach { case (k, v) => p.setProperty(k, v) }
    p
  }
  private val threadNum = new AtomicLong(1)
  private val callbackThreadPool = new ThreadPoolExecutor(100, 100, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue[Runnable](1000), new ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val thread = new Thread(r)
      thread.setName(s"delayQueue-callback-thread-${threadNum.getAndIncrement()}")
      thread.setDaemon(false)
      thread
    }
  })
  implicit private val ec: ExecutionContext = ExecutionContext.fromExecutor(callbackThreadPool)
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
              case None => logger.error(s"no callback for message: ${record.value()}")
            }
          case Left(error) => logger.error(s"decode streamMessage error, error:$error")
        }
      })
    }
  }

  sys.addShutdownHook(() => kafkaConsumer.close())
}
