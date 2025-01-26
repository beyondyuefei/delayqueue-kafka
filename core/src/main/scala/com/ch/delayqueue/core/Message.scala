package com.ch.delayqueue.core

/**
 *
 * @param namespace 不同的延迟类型使用不同的namespace做数据隔离，会对应到kafka stream 的topic
 * @param id 消息唯一标识
 * @param value 消息内容
 */
case class Message(namespace: String, id: String, value: String)
