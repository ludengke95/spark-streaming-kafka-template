package com.opensharing.bigdata.template.streamingkafka;

import cn.hutool.log.StaticLog;
import com.opensharing.bigdata.toolfactory.ZookeeperFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author ludengke
 * @date 2019/12/13
 **/
public class KafkaToSparkStreamingOffsetInZookeeperTemplate extends SparkStreamingKafkaFactory {
    /**
     * 从指定位置获取offset
     *
     * @return offset集
     */
    @Override
    protected Map<TopicPartition, Long> getOffset() throws Exception{
        Map<TopicPartition, Long> fromOffsets = new HashMap<>(16);
        if (!ZookeeperFactory.getZkClient().exists(OFFSET_DIR)) {
            ZookeeperFactory.getZkClient().createPersistent(OFFSET_DIR, true);
        }
        List<String> children = ZookeeperFactory.getZkClient().getChildren(OFFSET_DIR);
        if (children != null && !children.isEmpty()) {
            // 可以读取到存在Zookeeper中的偏移量 使用读取到的偏移量创建Streaming来读取Kafka
            for (String child : children) {
                long offset = Long.valueOf(ZookeeperFactory.getZkClient().readData(String.join("/", OFFSET_DIR, child)));
                fromOffsets.put(new TopicPartition(CONSUMER_TOPIC_NAME, Integer.valueOf(child)), offset);
                StaticLog.info("FOUND OFFSET IN ZOOKEEPER, USE [topic : {} ,partition : {} ,offset : {} ]",
                        CONSUMER_TOPIC_NAME,child,offset);
            }
        }
        return fromOffsets;
    }

    /**
     * 更新offset
     *
     * @param stream kafka流
     */
    @Override
    protected void updateOffset(JavaInputDStream<ConsumerRecord<String, String>> stream) throws Exception{
        stream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            for (OffsetRange o : offsetRanges) {
                ZookeeperFactory.getZkUtils().updatePersistentPath(
                        String.join("/", OFFSET_DIR, String.valueOf(o.partition())),
                        String.valueOf(o.untilOffset()),
                        ZookeeperFactory.getZkUtils().defaultAcls(String.join("/", OFFSET_DIR, String.valueOf(o.partition())))
                );
                StaticLog.info("UPDATE OFFSET TO ZOOKEEPER WITH [ topic : {} ,partition : {} ,offset: {} ~ {} ]",
                        o.topic(),o.partition(),o.fromOffset(),o.untilOffset());
            }
        });
    }
}
