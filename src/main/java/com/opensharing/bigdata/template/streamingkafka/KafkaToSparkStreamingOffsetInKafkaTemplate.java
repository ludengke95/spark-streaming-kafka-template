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
public class KafkaToSparkStreamingOffsetInKafkaTemplate extends SparkStreamingKafkaFactory {
    /**
     * 从指定位置获取offset
     *
     * @return offset集
     */
    @Override
    protected Map<TopicPartition, Long> getOffset() throws Exception{
        Map<TopicPartition, Long> fromOffsets = new HashMap<>(16);
        AdminClient adminClient = AdminClient.create(kafkaConfMap);
        adminClient.listConsumerGroupOffsets(CONSUMER_GROUP_ID).partitionsToOffsetAndMetadata().get().forEach((key,value)->{
            if(key.topic().equals(CONSUMER_TOPIC_NAME)){
                StaticLog.info("FOUND OFFSET IN KAFKA, USE [ topic : {} ,partition : {} ,offset : {} ]",
                        key.topic(),key.partition(),value.offset());
                fromOffsets.put(key,value.offset());
            }
        });
        return fromOffsets;
    }

    /**
     * 更新offset
     *
     * @param stream kafka流
     */
    @Override
    protected void updateOffset(JavaInputDStream<ConsumerRecord<String, String>> stream) throws Exception{

    }
}
