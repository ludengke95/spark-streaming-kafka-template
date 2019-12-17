package com.opensharing.bigdata.template.streamingkafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.api.java.JavaInputDStream;

import java.util.Map;

public interface OffsetTemplate {
    /**
     * 从指定位置获取offset
     *
     * @return offset集
     */
    public Map<TopicPartition, Long> getOffset(String topicName,String groupId) throws Exception;

    /**
     * 更新offset
     *
     * @param stream kafka流
     */
    public void updateOffset(JavaInputDStream<ConsumerRecord<String, String>> stream,String topicName,String groupId) throws Exception;
}
