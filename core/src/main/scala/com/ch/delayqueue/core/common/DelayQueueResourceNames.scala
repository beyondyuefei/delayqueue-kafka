package com.ch.delayqueue.core.common

object DelayQueueResourceNames {
  private val delayQueueInputTopic = "Delay-Queue-Input-Topic"
  private val storeName = "delayed-messages-store"
  private val delayQueueOutputTopic = "Delay-Queue-Output-Topic"
  private val delayQueueConsumerGroup = "delayqueue-consumer-group"
  private val delayQueueKafkaStreamBuildInTopicName = "delayed-message-stream-topic"

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
