package com.ch.delayqueue.core.internal

case class StreamMessage(delaySeconds: Long, id: String, value: String)
