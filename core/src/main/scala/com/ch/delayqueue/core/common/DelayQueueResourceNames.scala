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

  def appDelayQueueInputTopic: String = {
    require(instance != null, "DelayQueueResourceNames is not initialized, Call initialize(appId) method first")
    instance.appDelayQueueInputTopic
  }

  def appDelayQueueOutputTopic: String = {
    require(instance != null, "DelayQueueResourceNames is not initialized, Call initialize(appId) method first")
    instance.appDelayQueueOutputTopic
  }

  def appDelayQueueStoreName: String = {
    require(instance != null, "DelayQueueResourceNames is not initialized, Call initialize(appId) method first")
    instance.appDelayQueueStoreName
  }

  def appDelayQueueConsumerGroup: String = {
    require(instance != null, "DelayQueueResourceNames is not initialized, Call initialize(appId) method first")
    instance.appDelayQueueConsumerGroup
  }

  def appDelayQueueKafkaStreamBuildInTopicName: String = {
    require(instance != null, "DelayQueueResourceNames is not initialized, Call initialize(appId) method first")
    instance.appDelayQueueKafkaStreamBuildInTopicName
  }
}

