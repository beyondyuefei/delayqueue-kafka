package com.ch.delayqueue.core.internal

import com.ch.delayqueue.core.internal.exception.LifecycleException

trait Lifecycle {
  /**
   * @throws LifecycleException 在启动app过程中发生异常时抛出，底层异常需要转化为 LifeCycleException
   */
  def start(): Unit = {}

  /**
   * @throws LifecycleException 在停止app过程中发生异常时抛出, 底层异常需要转化为 LifeCycleException
   */
  def stop(): Unit = {}
}
