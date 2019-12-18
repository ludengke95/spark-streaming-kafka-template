package com.opensharing.bigdata.handler.kafka;

import cn.hutool.log.StaticLog;
import com.opensharing.bigdata.handler.PairRDDHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.Serializable;

/**
 * @author ludengke
 * @date 2019/12/18
 **/
public class ConsoleKafkaPairRDDHandler<K,V> implements PairRDDHandler<K,V>, Serializable {

    /**
     * rdd处理函数
     * 最好不要对lines调用逆持久化。因为重复使用line对象，已经在工厂类进行过持久化
     * lines 可供处理的lines
     *
     * @param lines
     */
    @Override
    public void process(JavaPairRDD<? super K, ? super V> lines) {
        lines.foreachPartition(line->{
            while (line.hasNext()){
                Tuple2<? super K, ? super V> next = line.next();
                StaticLog.info("StateData [KEY: {} ,VALUE: {}]",
                        next._1,next._2);
            }
        });
    }
}
