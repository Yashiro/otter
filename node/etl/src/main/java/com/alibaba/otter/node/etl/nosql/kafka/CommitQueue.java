package com.alibaba.otter.node.etl.nosql.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.node.etl.load.loader.db.DbLoadData;
import com.alibaba.otter.node.etl.nosql.common.QueueContent;
import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;
import com.google.common.collect.Maps;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * @author Andy Zhou
 * @date 2017/8/15
 */
@Component
public class CommitQueue {

    private static Logger LOG = LoggerFactory.getLogger(CommitQueue.class);

    @Value("${broker.list}")
    private String brokerList;

    public void sourceDataTransforQueue(DbLoadData dbLoadData) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);

        props.put("retries", 0);
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

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

                    ProducerRecord<String, String> record = new ProducerRecord<String, String>(delEventData.getTableName(), null, JSON.toJSONString(queueContent));
                    send(producer, record);
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

                    ProducerRecord<String, String> record = new ProducerRecord<String, String>(addEventData.getTableName(), null, JSON.toJSONString(queueContent));
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

                    ProducerRecord<String, String> record = new ProducerRecord<String, String>(updEventData.getTableName(), null, JSON.toJSONString(queueContent));
                    send(producer, record);
                }
            }
        }

    }

    private void send(KafkaProducer producer, ProducerRecord record){

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e != null)
                    LOG.error("the producer has a error:" + e.getMessage());
                else {
                    LOG.info("The partition of the record we just sent is: " + metadata.partition());
                    LOG.info("The offset of the record we just sent is: " + metadata.offset());
                }
            }
        });
    }
}
