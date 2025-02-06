package com.ch.delayqueue.core.internal

case class StreamMessage(delaySeconds: Long, bizTimeInMs: Long, value: String, namespace: String)
