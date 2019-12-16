package com.opensharing.bigdata.template.streamingkafka;

import cn.hutool.log.StaticLog;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.api.java.JavaInputDStream;

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

    /**
     * topic的名称
     */
    private String topicName;

    /**
     * 消费者组ID
     */
    private String groupId;

    public OffsetInKafkaTemplate(Map<String, Object> kafkaConfMap, String topicName, String groupId) {
        this.kafkaConfMap = kafkaConfMap;
        this.topicName = topicName;
        this.groupId = groupId;
    }

    /**
     * 从指定位置获取offset
     *
     * @return offset集
     */
    @Override
    public Map<TopicPartition, Long> getOffset() throws Exception{
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
    public void updateOffset(JavaInputDStream<ConsumerRecord<String, String>> stream) throws Exception{

    }
}
