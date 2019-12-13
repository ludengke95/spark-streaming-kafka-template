package com.opensharing.bigdata.template.streamingkafka;

import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.log.StaticLog;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author ludengke
 * @date 2019/12/13
 **/
public class KafkaToSparkStreamingOffsetInMysqlTemplate extends SparkStreamingKafkaFactory {
    /**
     * 从指定位置获取offset
     *
     * @return offset集
     */
    @Override
    protected Map<TopicPartition, Long> getOffset() throws Exception {
        Map<TopicPartition, Long> fromOffsets = new HashMap<>(16);
        List<Entity> offsets = Db.use().find(Arrays.asList(new String[]{"topic","partition","offset"}),Entity.create(OFFSET_TABLE_NAME).set("topic", CONSUMER_TOPIC_NAME).set("group_id",CONSUMER_GROUP_ID));
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
    protected void updateOffset(JavaInputDStream<ConsumerRecord<String, String>> stream) throws Exception{
        stream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            for (OffsetRange o : offsetRanges) {
                Db.use().insertOrUpdate(Entity.create(OFFSET_TABLE_NAME)
                        .set("topic",o.topic())
                        .set("partition",o.partition())
                        .set("offset",o.untilOffset())
                        .set("group_id",CONSUMER_GROUP_ID));
                StaticLog.info("UPDATE OFFSET TO MYSQL WITH [ topic : {} ,partition : {} ,offset: {} ~ {} ]",
                        o.topic(),o.partition(),o.fromOffset(),o.untilOffset());
            }
        });
    }
}
