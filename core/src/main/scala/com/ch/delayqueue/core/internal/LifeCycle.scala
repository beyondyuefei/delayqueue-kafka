package com.ch.delayqueue.core.internal

trait LifeCycle {
  def start(): Unit = {}

  def stop(): Unit = {}
}
