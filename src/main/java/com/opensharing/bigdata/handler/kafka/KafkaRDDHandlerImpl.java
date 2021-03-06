package com.opensharing.bigdata.handler.kafka;

import com.opensharing.bigdata.handler.RDDHandlerInter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * @author ludengke
 * @date 2019/12/13
 **/
public abstract class KafkaRDDHandlerImpl implements RDDHandlerInter<ConsumerRecord<String, String>>, Serializable {

	/**
	 * rdd处理函数
	 * 最好不要对lines调用逆持久化。因为重复使用line对象，已经在工厂类进行过持久化
	 *
	 * @param sparkSession session对象，启用hive支持之后可以执行hiveSQL
	 * @param lines        从kafka取出的数据流
	 */
	@Override
	public abstract void process(SparkSession sparkSession, JavaRDD<? super ConsumerRecord<String, String>> lines);
}
