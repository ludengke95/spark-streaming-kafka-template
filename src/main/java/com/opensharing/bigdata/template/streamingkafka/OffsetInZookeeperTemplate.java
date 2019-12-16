package com.opensharing.bigdata.template.streamingkafka;

import cn.hutool.log.StaticLog;
import com.opensharing.bigdata.Conf.ZkConf;
import com.opensharing.bigdata.toolfactory.ZookeeperFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
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
public class OffsetInZookeeperTemplate implements OffsetTemplate {

    /**
     * 路径位置
     * 到group_id的上一级
     * eg: /kafka/consumer/group_id/topic/partition 仅需要填写/kafka/consumer
     */
    private String offsetDir = "";

    /**
     * topic名称
     */
    private String topicName = "";

    /**
     * 消费者组ID
     */
    private String groupId = "";

    public OffsetInZookeeperTemplate(Map<ZkConf,Object> map, String offsetDir, String topicName, String groupId) {
        this.offsetDir = offsetDir;
        this.topicName = topicName;
        this.groupId = groupId;
        ZookeeperFactory.init(map);
    }

    /**
     * 从指定位置获取offset
     *
     * @return offset集
     */
    @Override
    public Map<TopicPartition, Long> getOffset() throws Exception{
        String path = String.join("/",offsetDir,groupId,topicName);
        Map<TopicPartition, Long> fromOffsets = new HashMap<>(16);
        if (!ZookeeperFactory.getZkClient().exists(path)) {
            ZookeeperFactory.getZkClient().createPersistent(path, true);
        }
        List<String> children = ZookeeperFactory.getZkClient().getChildren(path);
        if (children != null && !children.isEmpty()) {
            // 可以读取到存在Zookeeper中的偏移量 使用读取到的偏移量创建Streaming来读取Kafka
            for (String child : children) {
                long offset = Long.valueOf(ZookeeperFactory.getZkClient().readData(String.join("/", path, child)));
                fromOffsets.put(new TopicPartition(topicName, Integer.valueOf(child)), offset);
                StaticLog.info("FOUND OFFSET IN ZOOKEEPER, USE [topic : {} ,partition : {} ,offset : {} ]",
                        topicName,child,offset);
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
    public void updateOffset(JavaInputDStream<ConsumerRecord<String, String>> stream) throws Exception{
        String path = String.join("/",offsetDir,groupId,topicName);
        stream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            for (OffsetRange o : offsetRanges) {
                ZookeeperFactory.getZkUtils().updatePersistentPath(
                        String.join("/", path, String.valueOf(o.partition())),
                        String.valueOf(o.untilOffset()),
                        ZookeeperFactory.getZkUtils().defaultAcls(String.join("/", offsetDir, String.valueOf(o.partition())))
                );
                StaticLog.info("UPDATE OFFSET TO ZOOKEEPER WITH [ topic : {} ,partition : {} ,offset: {} ~ {} ]",
                        o.topic(),o.partition(),o.fromOffset(),o.untilOffset());
            }
        });
    }
}
