package com.ch.delayqueue.core.common

object DelayQueueResourceNames {
  private val delayQueuePrefix = "Delay-Queue-"
  private val delayQueueInputTopic = delayQueuePrefix + "Input-Topic"
  private val storeName = delayQueuePrefix + "Message-Store"
  private val delayQueueOutputTopic = delayQueuePrefix + "Output-Topic"
  private val delayQueueConsumerGroup = delayQueuePrefix + "Consumer-Group"
  private val delayQueueKafkaStreamBuildInTopicName = delayQueuePrefix + "Message-Stream-Topic"

  def getAppDelayQueueInputTopic(appId: String): String = {
    s"$delayQueueInputTopic-$appId"
  }

  def getAppDelayQueueOutputTopic(appId: String): String = {
    s"$delayQueueOutputTopic-$appId"
  }

  def getAppDelayQueueStoreName(appId: String): String = {
    s"$storeName-$appId"
  }

  def getAppDelayQueueConsumerGroup(appId: String): String = {
    s"$delayQueueConsumerGroup-$appId"
  }

  def getAppDelayQueueKafkaStreamBuildInTopicName(appId: String): String = {
    s"$delayQueueKafkaStreamBuildInTopicName-$appId"
  }
}
