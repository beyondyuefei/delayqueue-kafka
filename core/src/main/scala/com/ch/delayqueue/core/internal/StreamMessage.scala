package com.ch.delayqueue.core.internal

import com.ch.delayqueue.core.Message

private[core] case class StreamMessage(delaySeconds: Long, message: Message)
