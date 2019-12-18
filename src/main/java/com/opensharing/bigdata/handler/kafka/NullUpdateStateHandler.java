package com.opensharing.bigdata.handler.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * 根据新值和就值是否为null来判断保留谁
 * 把输入的JavaInputDStream转化为JavaPairDStream
 * @author ludengke
 * @date 2019/12/17
 **/
public class NullUpdateStateHandler<K,V> extends KafkaUpdateStateHandler<K,V> implements Serializable {

    /**
     * 合并old值和now值
     *
     * @param old
     * @param now
     * @return 合并后的值
     */
    @Override
    public V updateValue(V old, V now) {
        if (old==null){
            return now;
        }else if(now==null){
            return old;
        }else if(now ==null && old==null){
            return null;
        }else {
            return now;
        }
    }

    /**
     * JavaInputDStream流转化为JavaPairDStream
     *
     * @param stream
     * @return
     */
    @Override
    public JavaPairDStream<K, V> toPair(JavaInputDStream<? super ConsumerRecord<String, String>> stream) {
        JavaInputDStream<ConsumerRecord<String, String>> streamTmp = (JavaInputDStream<ConsumerRecord<String, String>>)stream;
        return streamTmp.mapPartitionsToPair(line->{
            ArrayList<Tuple2<K,V>> list = new ArrayList();
            while (line.hasNext()){
                ConsumerRecord<String, String> next = line.next();
                Tuple2<K,V> tmp = new Tuple2(next.value().length(),next.value());
                list.add(tmp);
            }
            return list.iterator();
        });
    }
}
