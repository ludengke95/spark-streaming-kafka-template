package com.opensharing.bigdata.handler.updatestate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;

/**
 * @author ludengke
 * @date 2019/12/13
 **/
public interface UpdateStateHandler<T> {
    /**
     * 合并old值和now值
     * @param old
     * @param now
     * @return 合并后的值
     */
    public T process(T old,T now);
}
