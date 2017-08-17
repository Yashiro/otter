package com.alibaba.otter.node.etl.nosql.rocketmq;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author Andy Zhou
 * @date 2017/8/17
 */
public class RocketmqConsumer {

    @Override
    public String toString() {
        return super.toString();
    }

    public static void main(String[] args) throws MQClientException {
        String namestr = "10.9.20.106:9876;10.9.20.107:9876";

        final DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumergroup_ljg");
        consumer.setNamesrvAddr(namestr);
        consumer.subscribe("kafka_user", "");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setConsumeThreadMin(3);
        consumer.setConsumeThreadMax(20);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {

                System.out.println(msgs.size());
                String message = new String(msgs.get(0).getBody());
                System.out.println(message);


                //消费者向mq服务器返回消费成功的消息
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;


            }
        });
        consumer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                consumer.shutdown();
            }
        }));
    }
}