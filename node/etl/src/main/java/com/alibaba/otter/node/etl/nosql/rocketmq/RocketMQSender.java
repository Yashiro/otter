package com.alibaba.otter.node.etl.nosql.rocketmq;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.node.etl.load.loader.db.DbLoadData;
import com.alibaba.otter.node.etl.nosql.common.QueueContent;
import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author Andy Zhou
 * @date 2017/8/17
 */
@Component
public class RocketMQSender implements InitializingBean {

    private static Logger LOG = LoggerFactory.getLogger(RocketMQSender.class);

    @Value("${rocketmq.list}")
    private String rocketMQList;

    @Value("${producerGroup}")
    private String producerGroup;

    private DefaultMQProducer producer;

    @Override
    public void afterPropertiesSet() throws Exception {
        LOG.info("## rocketMQ List = {}", rocketMQList);
        LOG.info("## producer Group = {}", producerGroup);

        producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(rocketMQList);
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    public void sourceDataTransforQueue(DbLoadData dbLoadData) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        for (DbLoadData.TableLoadData tableLoadData : dbLoadData.getTables()) {
            List<EventData> deleteDatas  = tableLoadData.getDeleteDatas();
            List<EventData> insertDatas  = tableLoadData.getInsertDatas();
            List<EventData> upadateDatas = tableLoadData.getUpadateDatas();

            if (deleteDatas != null && deleteDatas.size() > 0) {
                QueueContent queueContent = null;
                for (EventData delEventData : deleteDatas) {
                    queueContent = new QueueContent();
                    queueContent.setType("delete");
                    queueContent.setId(delEventData.getKeys().get(0).getColumnValue());
                    Map<String, String> columns = Maps.newConcurrentMap();
                    for (EventColumn delEventColumn : delEventData.getColumns()) {
                        columns.put(delEventColumn.getColumnName(), delEventColumn.getColumnValue() != null ? delEventColumn.getColumnValue() : "");
                    }
                    queueContent.setColumns(columns);
                    queueContent.setCurrentTime(new Date());

                    Message record = new Message(delEventData.getTableName(), JSON.toJSONString(queueContent).getBytes());
                    producer.send(record);
                }
            }

            if (insertDatas != null && insertDatas.size() >0) {
                QueueContent queueContent = null;
                for (EventData addEventData : insertDatas) {
                    queueContent = new QueueContent();
                    queueContent.setType("insert");
                    queueContent.setId(addEventData.getKeys().get(0).getColumnValue());
                    Map<String, String> columns = Maps.newConcurrentMap();
                    for (EventColumn addEventColumn : addEventData.getColumns()) {
                        columns.put(addEventColumn.getColumnName(), addEventColumn.getColumnValue() != null ? addEventColumn.getColumnValue() : "");
                    }
                    queueContent.setColumns(columns);
                    queueContent.setCurrentTime(new Date());

                    Message record = new Message(addEventData.getTableName(), JSON.toJSONString(queueContent).getBytes());
                    send(producer, record);
                }
            }

            if (upadateDatas != null && upadateDatas.size() >0) {
                QueueContent queueContent = null;
                for (EventData updEventData : upadateDatas) {
                    queueContent = new QueueContent();
                    queueContent.setType("update");
                    queueContent.setId(updEventData.getKeys().get(0).getColumnValue());
                    Map<String, String> columns = Maps.newConcurrentMap();
                    for (EventColumn updEventColumn : updEventData.getColumns()) {
                        columns.put(updEventColumn.getColumnName(), updEventColumn.getColumnValue() != null ? updEventColumn.getColumnValue() : "");
                    }
                    queueContent.setColumns(columns);
                    queueContent.setCurrentTime(new Date());

                    Message record = new Message(updEventData.getTableName(), JSON.toJSONString(queueContent).getBytes());
                    send(producer, record);
                }
            }
        }
    }

    private void send(DefaultMQProducer producer, Message message){
        try {
            producer.send(message, new SendCallback() {

                @Override
                public void onSuccess(SendResult sendResult) {
                    LOG.info("The rocketMQ queue offset = {}", sendResult.getQueueOffset());
                    LOG.info("The rocketMQ queue broker name = {}", sendResult.getMessageQueue().getBrokerName());
                }

                @Override
                public void onException(Throwable e) {
                    if (e != null) LOG.error("The rocketMQ err = {}", e.getMessage());
                }
            });
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String namesrvAddr = "10.9.20.106:9876;10.9.20.107:9876";

        final DefaultMQProducer producer = new DefaultMQProducer("33");
        producer.setNamesrvAddr(namesrvAddr);
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        try {
            while (true) {
                Message msg = new Message("topic_ljg",// topic
                        "TagB",// tag
                        "OrderID002",// key
                        ("Hello MetaQ2").getBytes());// body
                SendResult sendResult = producer.send(msg);
                System.out.println(sendResult);
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("end");
            }
        });
        producer.shutdown();
    }


}

