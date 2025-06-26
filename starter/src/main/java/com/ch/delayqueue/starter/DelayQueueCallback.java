package com.ch.delayqueue.starter;

import com.ch.delayqueue.core.Message;

@FunctionalInterface
public interface DelayQueueCallback {
    void OnDelayMessageTriggered(final Message message);
}
