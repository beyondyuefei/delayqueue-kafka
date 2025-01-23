package com.ch.delayqueue.core

object DelayQueueService {
   def executeWithFixedDelay[T] (message: Message[T], delaySeconds: Long):Boolean = {
     // todo: send to delayqueue kafka topic
      true
   }
}
