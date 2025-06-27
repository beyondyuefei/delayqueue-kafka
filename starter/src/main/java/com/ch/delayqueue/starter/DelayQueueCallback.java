package com.ch.delayqueue.starter;

import com.ch.delayqueue.core.Message;

public interface DelayQueueCallback {
    void OnDelayMessageTriggered(final Message message);
}
