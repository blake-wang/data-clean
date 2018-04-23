package com.ijunhai.common.rocketmq;


import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * Created by Admin on 2017-08-02.
 */
public class Consumer {
    public static void main(String[] args){
        DefaultMQPushConsumer consumer =
                new DefaultMQPushConsumer("Default");
//        consumer.setNamesrvAddr("123.59.62.55:9876");
        consumer.setNamesrvAddr("bjc-apilog.dalan.net:19876");
        consumer.setConsumeMessageBatchMaxSize(10);
//        consumer.setPullBatchSize(50);
//        consumer.setConsumerGroup("testOrder");
        try {
            //订阅PushTopic下Tag为push的消息
            consumer.subscribe("test","");
            //程序第一次启动从消息队列头取数据
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.registerMessageListener(
                new MessageListenerConcurrently() {
                    public ConsumeConcurrentlyStatus consumeMessage(
                            List<MessageExt> list,
                            ConsumeConcurrentlyContext Context) {
                        Message msg = list.get(0);
                        System.out.println(msg.toString());
                        System.out.println(list.size());
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                }
            );
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
