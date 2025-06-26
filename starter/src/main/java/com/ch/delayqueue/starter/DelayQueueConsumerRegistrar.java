package com.ch.delayqueue.starter;

import com.ch.delayqueue.core.DelayQueueService;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;

@Component
public class DelayQueueConsumerRegistrar implements BeanPostProcessor {
    private final DelayQueueService delayQueueService;

    public DelayQueueConsumerRegistrar(DelayQueueService delayQueueService) {
        this.delayQueueService = delayQueueService;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (bean.getClass().isAnnotationPresent(DelayQueueConsumer.class)) {
            final DelayQueueConsumer annotation = bean.getClass().getAnnotation(DelayQueueConsumer.class);
            final String namespace = annotation.namespace();
            final DelayQueueCallback delayQueueCallback = (DelayQueueCallback) bean;
            delayQueueService.registerCallback(namespace, message -> {
                delayQueueCallback.OnDelayMessageTriggered(message);
                return null;
            });
        }
        return bean;
    }
}
