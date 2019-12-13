package com.opensharing.bigdata.toolfactory;

import com.sun.rowset.internal.Row;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;

/**
 * @author ludengke
 * @date 2019/12/11
 **/
public class SparkFactory {

    /**
     * 序列支持类列表
     */
    private static final Class[] SERIALIZER_CLASS = new Class[]{
            Row.class,
            Object.class,
    };

    /**
     * 设置默认的SparkConf,启用了Kryo序列化
     * spark.streaming.kafka.maxRatePerPartition:100
     * @return 默认的SparkConf
     */
    public static SparkConf getDefaultSparkConf() {
        return new SparkConf()
                .set("spark.streaming.kafka.maxRatePerPartition", "100")
                .set("spark.serializer", KryoSerializer.class.getCanonicalName())
                .registerKryoClasses(SERIALIZER_CLASS);
    }
}
