package com.opensharing.bigdata.handler.kafka;

import com.opensharing.bigdata.handler.RDDHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

/**
 * @author ludengke
 * @date 2019/12/13
 **/
public abstract class KafkaRDDHandler implements RDDHandler<ConsumerRecord<String, String>>, Serializable {

    /**
     * rdd处理函数
     * 最好不要对lines调用逆持久化。因为重复使用line对象，已经在工厂类进行过持久化
     * lines 可供处理的lines
     *
     * @param lines
     */
    @Override
    public abstract void process(JavaRDD<? super ConsumerRecord<String, String>> lines);
}
