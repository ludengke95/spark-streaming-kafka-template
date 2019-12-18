package com.opensharing.bigdata.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;

/**
 * @author ludengke
 * @date 2019年12月18日11:00:18
 **/
public interface RDDHandler<R> {
    /**
     * rdd处理函数
     * 最好不要对lines调用逆持久化。因为重复使用line对象，已经在工厂类进行过持久化
     * lines 可供处理的lines
     * @param lines
     */
    public void process(JavaRDD<? super R> lines);
}
