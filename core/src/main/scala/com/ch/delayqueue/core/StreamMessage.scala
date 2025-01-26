package com.ch.delayqueue.core

case class StreamMessage(delaySeconds: Long, id: String, value: String)
