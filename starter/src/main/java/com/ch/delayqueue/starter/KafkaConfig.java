package com.ch.delayqueue.starter;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "kafka")
@Data
public class KafkaConfig {
    private String bootstrapServers;
    private Integer lingerMs;
    /**
     * 作为消费者组GroupId命名使用，appId需要唯一
     */
    private String appId;
}
