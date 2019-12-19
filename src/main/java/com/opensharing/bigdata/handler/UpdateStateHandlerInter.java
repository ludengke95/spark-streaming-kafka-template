package com.opensharing.bigdata.handler;

import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

/**
 * @author ludengke
 * @date 2019/12/13
 **/
public interface UpdateStateHandlerInter<K, V, R> {

	/**
	 * 合并old值和now值
	 *
	 * @param old 旧值
	 * @param now 现值
	 * @return 合并后的值
	 */
	V updateValue(V old, V now);

	/**
	 * JavaInputDStream流转化为JavaPairDStream
	 *
	 * @param stream 从kafka取出的数据流
	 * @return 转化为key-value结构的数据流
	 */
	JavaPairDStream<K, V> toPair(JavaInputDStream<? super R> stream);
}
