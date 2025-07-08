package com.ch.delayqueue.core.common

import com.ch.delayqueue.core.DelayQueueService

private[core] object DelayQueueResourceNames {
  private val delayQueuePrefix = "Delay-Queue-"
  private val delayQueueInputTopic = delayQueuePrefix + "Input-Topic"
  private val storeName = delayQueuePrefix + "Message-Store"
  private val delayQueueOutputTopic = delayQueuePrefix + "Output-Topic"
  private val delayQueueConsumerGroup = delayQueuePrefix + "Consumer-Group"
  private val delayQueueKafkaStreamBuildInTopicName = delayQueuePrefix + "Message-Stream-Topic"

  val appDelayQueueInputTopic = s"$delayQueueInputTopic-${DelayQueueService.getInternalKafkaConfig.appId}"

  val appDelayQueueOutputTopic = s"$delayQueueOutputTopic-${DelayQueueService.getInternalKafkaConfig.appId}"

  val appDelayQueueStoreName = s"$storeName-${DelayQueueService.getInternalKafkaConfig.appId}"

  val appDelayQueueConsumerGroup = s"$delayQueueConsumerGroup-${DelayQueueService.getInternalKafkaConfig.appId}"

  val appDelayQueueKafkaStreamBuildInTopicName = s"$delayQueueKafkaStreamBuildInTopicName-${DelayQueueService.getInternalKafkaConfig.appId}"

}

