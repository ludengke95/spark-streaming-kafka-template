package com.opensharing.bigdata.template.streamingkafka;

import cn.hutool.log.StaticLog;
import com.opensharing.bigdata.toolfactory.ZookeeperFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ludengke
 * @date 2019/12/13
 **/
public class OffsetInKafkaTemplate implements OffsetTemplate{

    /**
     * 存储kafka配置
     */
    private Map<String,Object> kafkaConfMap;

    public OffsetInKafkaTemplate(Map<String, Object> kafkaConfMap) {
        this.kafkaConfMap = kafkaConfMap;
    }

    /**
     * 从指定位置获取offset
     *
     * @return offset集
     */
    @Override
    public Map<TopicPartition, Long> getOffset(String topicName,String groupId) throws Exception{
        Map<TopicPartition, Long> fromOffsets = new HashMap<>(16);
        AdminClient adminClient = AdminClient.create(kafkaConfMap);
        try{
            adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get().forEach((key,value)->{
                if(key.topic().equals(topicName)){
                    StaticLog.info("FOUND OFFSET IN KAFKA, USE [ topic : {} ,partition : {} ,offset : {} ]",
                            key.topic(),key.partition(),value.offset());
                    fromOffsets.put(key,value.offset());
                }
            });
        }catch (NullPointerException e){
            StaticLog.info("NOT FOUND OFFSET IN KAFKA");
        }
        return fromOffsets;
    }

    /**
     * 更新offset
     *
     * @param stream kafka流
     */
    @Override
    public void updateOffset(JavaInputDStream<ConsumerRecord<String, String>> stream,String topicName,String groupId) throws Exception{
        stream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
        });
    }
}
