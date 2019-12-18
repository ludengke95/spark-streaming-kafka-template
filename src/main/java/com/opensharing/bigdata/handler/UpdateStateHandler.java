package com.opensharing.bigdata.handler;

import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

/**
 * @author ludengke
 * @date 2019/12/13
 **/
public interface UpdateStateHandler<K,V,R> {
    /**
     * 合并old值和now值
     * @param old
     * @param now
     * @return 合并后的值
     */
    public V updateValue(V old, V now);

    /**
     * JavaInputDStream流转化为JavaPairDStream
     * @param stream
     * @return
     */
    public JavaPairDStream<K,V> toPair(JavaInputDStream<? super R> stream);
}
