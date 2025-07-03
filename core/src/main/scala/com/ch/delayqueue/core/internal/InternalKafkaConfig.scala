package com.ch.delayqueue.core.internal

private[delayqueue] case class InternalKafkaConfig(bootstrapServers: String, lingerMs: String, appId: String) {
  require(bootstrapServers != null && bootstrapServers.nonEmpty, "bootstrapServers must not be null or empty")
  require(lingerMs != null && lingerMs.nonEmpty, "lingerMs must not be null or empty")
  require(appId != null && appId.nonEmpty, "appId must not be null or empty")
}
