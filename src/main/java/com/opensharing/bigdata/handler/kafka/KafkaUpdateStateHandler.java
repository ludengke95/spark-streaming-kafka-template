package com.opensharing.bigdata.handler.kafka;

import com.opensharing.bigdata.handler.UpdateStateHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import java.io.Serializable;

/**
 * @author ludengke
 * @date 2019/12/13
 **/
public abstract class KafkaUpdateStateHandler<K,V> implements UpdateStateHandler<K,V, ConsumerRecord<String, String>>, Serializable {
    /**
     * 合并old值和now值
     *
     * @param old
     * @param now
     * @return 合并后的值
     */
    @Override
    public abstract V updateValue(V old, V now);

    /**
     * JavaInputDStream流转化为JavaPairDStream
     *
     * @param stream
     * @return
     */
    @Override
    public abstract JavaPairDStream<K, V> toPair(JavaInputDStream<? super ConsumerRecord<String, String>> stream);
}
