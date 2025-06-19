package com.ch.delayqueue.core.internal

import com.ch.delayqueue.core.Message

case class StreamMessage(delaySeconds: Long, message: Message)
