package com.opensharing.bigdata.handler;

import org.apache.spark.api.java.JavaPairRDD;

/**
 * @author ludengke
 * @date 2019年12月18日11:00:18
 **/
public interface PairRDDHandlerInter<K, V> {

	/**
	 * rdd处理函数
	 * 最好不要对lines调用逆持久化。因为重复使用line对象，已经在工厂类进行过持久化
	 *
	 * @param lines 从kafka取出的数据流
	 */
	void process(JavaPairRDD<? super K, ? super V> lines);
}
