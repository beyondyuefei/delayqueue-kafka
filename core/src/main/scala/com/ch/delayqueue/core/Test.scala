package com.ch.delayqueue.core

import scala.collection.mutable

object Test {
   def main(args:Array[String]):Unit = {
     val kafkaConfig = mutable.Map("bootstrap.servers" -> "localhost:9092", "linger.ms" -> "1",
       "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
       "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer")
     DelayQueueService.get(kafkaConfig).executeWithFixedDelay(new Message("test", "4", "def"), 10)
     println("hello")
   }
}