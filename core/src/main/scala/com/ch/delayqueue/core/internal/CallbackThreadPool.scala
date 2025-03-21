package com.ch.delayqueue.core.internal

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.ExecutionContext

private[internal] object CallbackThreadPool {
  private val threadNum = new AtomicLong(1)

  // 线程池配置
  private val callbackThreadPool: ThreadPoolExecutor = new ThreadPoolExecutor(
    100, // 核心线程数
    100, // 最大线程数
    0L, TimeUnit.MILLISECONDS, // 闲置线程存活时间
    new LinkedBlockingQueue[Runnable](1000), // 队列大小
    new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val thread = new Thread(r)
        thread.setName(s"delayQueue-callback-thread-${threadNum.getAndIncrement()}")
        thread.setDaemon(false)
        thread
      }
    }
  )

  // 提供ExecutionContext
  implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(callbackThreadPool)

  sys.addShutdownHook(() => {
    callbackThreadPool.close()
  })
}