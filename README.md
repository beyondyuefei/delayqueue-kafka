### 主要特性
- 直接支持秒级精度自定义消息的延迟时间，区别于传统的RocketMQ等只能按照内置预先定义好的延迟等级
- 在kafka stream之上构建延迟消息队列，天然支持了消息的持久化、高可用、partition水平扩展
- 提供 SpringBoot starter，开箱即用

### 环境要求**
- JDK 21+
- SpringBoot 6.x
