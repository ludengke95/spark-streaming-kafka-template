package com.opensharing.bigdata.template.streamingkafka;

import com.opensharing.bigdata.Conf.TemplateConf;
import com.opensharing.bigdata.Conf.ZkConf;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.Durations;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ludengke
 * @date 2019/12/16
 **/
public class SparkStreamingKafkaTest {

    @Test
    public void test(){
        String topic = "spider-task";
        /*
        如果kafkaConfMap设置了group_id,SparkStreamingKafka可不设置group_id
         */
//        String groupId = "spark-template";
        Map<Object,Object> sparkConfMap = new HashMap<>();
        sparkConfMap.put(TemplateConf.APP_NAME,"test");
        sparkConfMap.put(TemplateConf.MASTER,"local[4]");
        sparkConfMap.put(TemplateConf.DURATION, Durations.seconds(10));
        Map<String,Object> kafkaConfMap = new HashMap<>();
        kafkaConfMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.2.58:9092,192.168.2.58:10092,192.168.2.58:11092");
        kafkaConfMap.put(ConsumerConfig.GROUP_ID_CONFIG, "spark-template");
        kafkaConfMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaConfMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        kafkaConfMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConfMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        Map<ZkConf,Object> zkConfMap = new HashMap<>();
        zkConfMap.put(ZkConf.URL,"127.0.0.1:2181");
        zkConfMap.put(ZkConf.CONNECTION_TIMEOUT,"3000");
        SparkStreamingKafka spark = SparkStreamingKafka.create(sparkConfMap, kafkaConfMap)
                .setTopicName(topic)
                .setOffsetTemplate(new OffsetInZookeeperTemplate(zkConfMap,"/ldk",topic,"spark-template"));
        spark.start();
    }
}
