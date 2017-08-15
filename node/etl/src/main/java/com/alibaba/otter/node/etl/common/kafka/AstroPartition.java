package com.alibaba.otter.node.etl.common.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author Andy Zhou
 * @date 2017/8/15
 */
public class AstroPartition implements Partitioner {
    private static Logger LOG = LoggerFactory.getLogger(AstroPartition.class);

    public AstroPartition() {
        // TODO Auto-generated constructor stub
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // TODO Auto-generated method stub
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        int partitionNum = 0;
        try {
            partitionNum = Integer.parseInt((String) key);
        } catch (Exception e) {
            partitionNum = key.hashCode() ;
        }
        LOG.info("the message sendTo topic:"+ topic+" and the partitionNum:"+ partitionNum);
        return Math.abs(partitionNum  % numPartitions);
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
    }

}
