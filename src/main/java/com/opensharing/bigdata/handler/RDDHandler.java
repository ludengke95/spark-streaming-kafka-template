package com.opensharing.bigdata.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;

/**
 * @author ludengke
 * @date 2019/12/13
 **/
public abstract class RDDHandler {
    public RDDHandler() {
    }

    /**
     * rdd处理函数
     * 最好不要对lines调用逆持久化。因为重复使用line对象，已经在工厂类进行过持久化
     * lines 可供处理的lines
     * @param lines
     */
    public abstract void process(JavaRDD<ConsumerRecord<String, String>> lines);
}
