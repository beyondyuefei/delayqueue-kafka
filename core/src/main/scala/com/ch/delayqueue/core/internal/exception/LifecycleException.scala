package com.ch.delayqueue.core.internal.exception

class LifecycleException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this(message: String) {
    this(message, null)
  }
}
