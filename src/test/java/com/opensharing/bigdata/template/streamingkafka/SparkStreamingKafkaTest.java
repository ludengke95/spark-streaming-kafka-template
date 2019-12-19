package com.opensharing.bigdata.template.streamingkafka;

import com.opensharing.bigdata.conf.TemplateConfEnum;
import com.opensharing.bigdata.conf.ZkConfEnum;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.Durations;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * 手动维护偏移量最好不要启用checkpoint，
 * 如果暴力终止的话，checkpoint可能还未保存
 * 再次启动，会出现重复消费部分数据
 *
 * @author ludengke
 * @date 2019/12/16
 **/
public class SparkStreamingKafkaTest {

	@Test
	public void testZk() {
		String topic = "spider-task";
		//如果kafkaConfMap设置了group_id,SparkStreamingKafka可不设置group_id
		String groupId = "spark-template";
		Map<Object, Object> sparkConfMap = new HashMap<>();
		sparkConfMap.put(TemplateConfEnum.APP_NAME, "testZk");
		sparkConfMap.put(TemplateConfEnum.MASTER, "local[4]");
		sparkConfMap.put(TemplateConfEnum.DURATION, Durations.seconds(10));
		sparkConfMap.put("spark.streaming.kafka.maxRatePerPartition", "10");
		Map<String, Object> kafkaConfMap = new HashMap<>();
		kafkaConfMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.2.58:9092,192.168.2.58:10092,192.168.2.58:11092");
		kafkaConfMap.put(ConsumerConfig.GROUP_ID_CONFIG, "spark-template");
		kafkaConfMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		kafkaConfMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		kafkaConfMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaConfMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		Map<ZkConfEnum, Object> zkConfMap = new HashMap<>();
		zkConfMap.put(ZkConfEnum.URL, "127.0.0.1:2181");
		zkConfMap.put(ZkConfEnum.CONNECTION_TIMEOUT, "3000");
		SparkStreamingKafka spark = SparkStreamingKafka.create(sparkConfMap, kafkaConfMap);
//        SparkStreamingKafka spark = SparkStreamingKafka.create(sparkConfMap, kafkaConfMap,"./checkpointStreamingZk");
		spark.setTopicName(topic);
		spark.setOffsetTemplate(new OffsetInZookeeperTemplate(zkConfMap, "/ldk"));
		spark.start();
	}

	@Test
	public void testMysql() {
		String topic = "spider-task";
		//如果kafkaConfMap设置了group_id,SparkStreamingKafka可不设置group_id
		String groupId = "spark-template";
		Map<Object, Object> sparkConfMap = new HashMap<>();
		sparkConfMap.put(TemplateConfEnum.APP_NAME, "testMysql");
		sparkConfMap.put(TemplateConfEnum.MASTER, "local[4]");
		sparkConfMap.put(TemplateConfEnum.DURATION, Durations.seconds(10));
		Map<String, Object> kafkaConfMap = new HashMap<>();
		kafkaConfMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.2.58:9092,192.168.2.58:10092,192.168.2.58:11092");
		kafkaConfMap.put(ConsumerConfig.GROUP_ID_CONFIG, "spark-template");
		kafkaConfMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		kafkaConfMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		kafkaConfMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaConfMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		SparkStreamingKafka spark = SparkStreamingKafka.create(sparkConfMap, kafkaConfMap);
//        SparkStreamingKafka spark = SparkStreamingKafka.create(sparkConfMap, kafkaConfMap,"./checkpointStreamingMysql");
		spark.setTopicName(topic);
		spark.setOffsetTemplate(new OffsetInMysqlTemplate("kafka_offset"));
		spark.start();
	}

	@Test
	public void testKafka() {
		String topic = "spider-task";
		//如果kafkaConfMap设置了group_id,SparkStreamingKafka可不设置group_id
		String groupId = "spark-template";
		Map<Object, Object> sparkConfMap = new HashMap<>();
		sparkConfMap.put(TemplateConfEnum.APP_NAME, "testKafka");
		sparkConfMap.put(TemplateConfEnum.MASTER, "local[4]");
		sparkConfMap.put(TemplateConfEnum.DURATION, Durations.seconds(10));
		Map<String, Object> kafkaConfMap = new HashMap<>();
		kafkaConfMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.2.58:9092,192.168.2.58:10092,192.168.2.58:11092");
		kafkaConfMap.put(ConsumerConfig.GROUP_ID_CONFIG, "spark-template");
		kafkaConfMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		kafkaConfMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		kafkaConfMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaConfMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		SparkStreamingKafka spark = SparkStreamingKafka.create(sparkConfMap, kafkaConfMap);
//        SparkStreamingKafka spark = SparkStreamingKafka.create(sparkConfMap, kafkaConfMap,"./checkpointStreamingKafka");
		spark.setTopicName(topic);
		spark.setOffsetTemplate(new OffsetInKafkaTemplate(kafkaConfMap));
		spark.start();
	}
}
