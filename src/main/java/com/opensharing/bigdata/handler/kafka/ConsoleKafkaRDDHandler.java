package com.opensharing.bigdata.handler.kafka;

import cn.hutool.log.StaticLog;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * Streaming 控制台输出kafka消息基本信息实现类
 *
 * @author ludengke
 * @date 2019/12/13
 **/
public class ConsoleKafkaRDDHandler extends KafkaRDDHandlerImpl implements Serializable {

	/**
	 * rdd处理函数
	 * 最好不要对lines调用逆持久化。因为重复使用line对象，已经在工厂类进行过持久化
	 *
	 * @param sparkSession session对象，启用hive支持之后可以执行hiveSQL
	 * @param lines        从kafka取出的数据流
	 */
	@Override
	public void process(SparkSession sparkSession, JavaRDD<? super ConsumerRecord<String, String>> lines) {
		JavaRDD<ConsumerRecord<String, String>> tmp = (JavaRDD<ConsumerRecord<String, String>>) lines;
		tmp.foreachPartition(line -> {
			while (line.hasNext()) {
				ConsumerRecord<String, String> next = line.next();
				StaticLog.info("[TOPIC: {} ,PARTITION: {} ,OFFSET:  {} ,KEY: {}, VALUE:{} ]",
						next.topic(), next.partition(), next.offset(), next.key(), next.value());
			}
		});
	}
}
