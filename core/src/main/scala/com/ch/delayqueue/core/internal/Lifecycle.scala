package com.ch.delayqueue.core.internal

import com.ch.delayqueue.core.internal.exception.LifecycleException

/**
 * 任何需要明确生命周期管理的对象都需要实现这个trait，然后会在 [[com.ch.delayqueue.core.DelayQueueService]] 中拉起
 */

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
