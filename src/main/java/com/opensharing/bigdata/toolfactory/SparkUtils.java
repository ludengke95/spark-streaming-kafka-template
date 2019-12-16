package com.opensharing.bigdata.toolfactory;

import cn.hutool.log.StaticLog;
import com.sun.rowset.internal.Row;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @author ludengke
 * @date 2019/12/11
 **/
public class SparkUtils {

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

    /**
     * 优雅的停止SparkStreaming
     * @param sc sc
     * @param hadoopUrl hadoop地址
     * @param path 停止信号文件路径
     * @param second 检测时间间隔
     * @throws Exception
     */
    public static void stopByMarkFile(JavaStreamingContext sc,String hadoopUrl,String path,Integer second) throws Exception {
        boolean isStop = false;
        /*
         * 判断信号文件是否存在，如果存在就关闭，更新配置中心状态值
         */
        while (!isStop) {
            StaticLog.info("[Ending] 调用awaitTerminationOrTimeout");
            isStop = sc.awaitTerminationOrTimeout(second);
            if (isStop) {
                StaticLog.info("[Ending] streaming context 已经停止");
            }
            else {
                StaticLog.info("[Ending] Streaming App 还在运行");
            }
            StaticLog.info("[Ending] 检查信号文件{}是否存在",path);
            boolean stopFlag = HadoopUtils.isExistsFile(hadoopUrl, path);
            if (!isStop && stopFlag) {
                StaticLog.info("[Ending] 调用停止流程");
                sc.stop(true, true);
                StaticLog.info("[Ending] 停止完成");
                HadoopUtils.delHdfsFile(hadoopUrl, path);
                StaticLog.info("[Ending] 信号文件删除");
            }
        }
    }
}
