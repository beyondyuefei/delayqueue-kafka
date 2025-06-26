package com.ch.delayqueue.starter;

import org.springframework.stereotype.Component;

import java.lang.annotation.*;

@Component
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface DelayQueueConsumer {
    /**
     * 声明对应的 namespace
     */
    String namespace();
}
