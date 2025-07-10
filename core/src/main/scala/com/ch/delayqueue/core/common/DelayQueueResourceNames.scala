package com.ch.delayqueue.core.common

private[core] class DelayQueueResourceNames private(appId: String) {
  private val delayQueuePrefix = "Delay-Queue-"
  private val delayQueueInputTopic = delayQueuePrefix + "Input-Topic"
  private val storeName = delayQueuePrefix + "Message-Store"
  private val delayQueueOutputTopic = delayQueuePrefix + "Output-Topic"
  private val delayQueueConsumerGroup = delayQueuePrefix + "Consumer-Group"
  private val delayQueueKafkaStreamBuildInTopicName = delayQueuePrefix + "Message-Stream-Topic"

  val appDelayQueueInputTopic = s"$delayQueueInputTopic-$appId"

  val appDelayQueueOutputTopic = s"$delayQueueOutputTopic-$appId"

  val appDelayQueueStoreName = s"$storeName-$appId"

  val appDelayQueueConsumerGroup = s"$delayQueueConsumerGroup-$appId"

  val appDelayQueueKafkaStreamBuildInTopicName = s"$delayQueueKafkaStreamBuildInTopicName-$appId"

}

object DelayQueueResourceNames {
  @volatile private var instance: DelayQueueResourceNames = _

  def initialize(appId: String): Unit = synchronized {
    if (instance == null) {
      instance = new DelayQueueResourceNames(appId)
    }
  }

  def appDelayQueueInputTopic: String = instance.appDelayQueueInputTopic

  def appDelayQueueOutputTopic: String = instance.appDelayQueueOutputTopic

  def appDelayQueueStoreName: String = instance.appDelayQueueStoreName

  def appDelayQueueConsumerGroup: String = instance.appDelayQueueConsumerGroup

  def appDelayQueueKafkaStreamBuildInTopicName: String = instance.appDelayQueueKafkaStreamBuildInTopicName
}

