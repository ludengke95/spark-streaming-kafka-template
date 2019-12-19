package com.opensharing.bigdata.handler.kafka;

import com.opensharing.bigdata.handler.UpdateStateHandlerInter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import java.io.Serializable;

/**
 * @author ludengke
 * @date 2019/12/13
 **/
public abstract class KafkaUpdateStateHandlerImpl<K, V> implements UpdateStateHandlerInter<K, V, ConsumerRecord<String, String>>, Serializable {

	/**
	 * 合并old值和now值
	 *
	 * @param old 旧值
	 * @param now 现值
	 * @return 合并后的值
	 */
	@Override
	public abstract V updateValue(V old, V now);

	/**
	 * JavaInputDStream流转化为JavaPairDStream
	 *
	 * @param stream 从kafka取出的数据流
	 * @return 转化为key-value结构的数据流
	 */
	@Override
	public abstract JavaPairDStream<K, V> toPair(JavaInputDStream<? super ConsumerRecord<String, String>> stream);
}
