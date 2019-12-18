package com.opensharing.bigdata.template.streamingkafka;

import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.log.StaticLog;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author ludengke
 * @date 2019/12/13
 **/
public class OffsetInMysqlTemplate implements OffsetTemplate, Serializable {

    /**
     * offset 所需要的table的名称
     */
    private String offsetTableName;

    public OffsetInMysqlTemplate(String offsetTableName) {
        this.offsetTableName = offsetTableName;
    }

    /**
     * 从指定位置获取offset
     *
     * @return offset集
     */
    @Override
    public Map<TopicPartition, Long> getOffset(String topicName,String groupId) throws Exception {
        Map<TopicPartition, Long> fromOffsets = new HashMap<>(16);
        List<Entity> offsets = Db.use().find(Arrays.asList(new String[]{"topic","partition","offset"}),Entity.create(offsetTableName).set("topic", topicName).set("group_id",groupId));
        offsets.forEach(entity -> {
            StaticLog.info("FOUND OFFSET IN MYSQL, USE [ topic : {} ,partition : {} ,offset : {} ]",
                    entity.getStr("topic"),entity.getInt("partition"),entity.getInt("offset"));
            fromOffsets.put(new TopicPartition(entity.getStr("topic"), entity.getInt("partition")), entity.getLong("offset"));
        });
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
            for (OffsetRange o : offsetRanges) {
                Db.use().insertOrUpdate(Entity.create(offsetTableName)
                        .set("topic",topicName)
                        .set("partition",o.partition())
                        .set("offset",o.untilOffset())
                        .set("group_id",groupId),"topic","group_id","partition");
                StaticLog.info("UPDATE OFFSET TO MYSQL WITH [ topic : {} ,partition : {} ,offset: {} ~ {} ]",
                        o.topic(),o.partition(),o.fromOffset(),o.untilOffset());
            }
        });
    }
}
