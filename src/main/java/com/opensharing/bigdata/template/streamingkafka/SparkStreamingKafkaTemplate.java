package com.opensharing.bigdata.template.streamingkafka;

import cn.hutool.log.StaticLog;
import com.opensharing.bigdata.factory.SparkFactory;
import com.opensharing.bigdata.factory.ZookeeperFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.*;

/**
 * SparkStreaming读取kafka的数据的模板类
 * offset保存优先级：kafka > zk > mysql
 * offset保存方式：
 *      1. kafka：采用kafka的配置。
 *      2. zk：需要给出zk url 超时时间等配置
 *      3. mysql：需要传递Mysql的Connect
 * @author ludengke
 * @date 2019/12/11
 **/
public abstract class SparkStreamingKafkaTemplate {

    /**
     * SparkConf:spark应用的配置类
     */
    private SparkConf sparkConf;

    /**
     * JavaStreamingContext
     * 继承后需要子类调用init函数生成SparkConf，以及JavaStreamingContext
     */
    private JavaStreamingContext javaStreamingContext;

    /**
     * kafka配置
     */
    private Map<String, Object> kafkaConfMap;

    private static final String CONSUMER_TOPIC_NAME = "";

    private String OFFSET_DIR = "";

    private final static OffsetStore DefaultOffsetStore = OffsetStore.KAFKA;
    private OffsetStore offsetStore = DefaultOffsetStore;

    /**
     * 模板初始化函数
     * 传入外部初始化好的JavaStreamingContext，
     * 需设置
     *  1. 启动间隔
     *  2. spark.streaming.kafka.maxRatePerPartition：spark从kafka的每个分区每秒取出的数据条数
     * @param javaStreamingContext 已初始化的javaStreamingContext
     */
    protected void init(JavaStreamingContext javaStreamingContext){
        this.javaStreamingContext = javaStreamingContext;
        this.sparkConf = javaStreamingContext.sparkContext().getConf();
    }

    /**
     * 模板初始化函数
     * 传入基本的SparkConf配置
     * 包含：
     *  1.app_name ： {str，必须}
     *  2.duration ：{Duration，必须}
     *  3.master ：{str，非必须}
     *  4.kryo_classes ：{arr(Class数组)，非必须}
     *  ......与SparkConf一致，仅对1,2做检剩余的属性不做检查
     * @param sparkConfMap 需要设置的SparkConf属性和必须属性
     * @param kafkaConfMap kafka的基本配置
     */
    protected void init(Map<Object,Object> sparkConfMap,Map<String,Object> kafkaConfMap){
        SparkConf sparkConf = createSparkConf(sparkConfMap);
        if (sparkConfMap.containsKey(TemplateConf.APP_NAME)){
            sparkConf.setAppName(sparkConfMap.get(TemplateConf.APP_NAME).toString());
        }
        if (sparkConfMap.containsKey(TemplateConf.MASTER)){
            sparkConf.setMaster(sparkConfMap.get(TemplateConf.MASTER).toString());
        }
        if (sparkConfMap.containsKey(TemplateConf.KRYO_CLASSES)){
            sparkConf.registerKryoClasses((Class<?>[]) sparkConfMap.get(TemplateConf.KRYO_CLASSES));
        }
        Duration duration = sparkConfMap.containsKey(TemplateConf.DURATION)? (Duration) sparkConfMap.get(TemplateConf.DURATION) :Durations.seconds(10);
        this.javaStreamingContext = new JavaStreamingContext(sparkConf, duration);
        this.kafkaConfMap = kafkaConfMap;
    }

    /**
     * 设置offset的存储方式
     * @param offsetStore offset的存储方式，使用模板中的枚举类
     */
    private void setOffsetStore(OffsetStore offsetStore){
        this.offsetStore = offsetStore;
    }

    /**
     * 预处理kafka配置
     * eg:
     *      1. 如果不设置消费者提交方式，默认设置为手动提交
     *      2. 不设置key.deserializer和value.deserializer,默认设置为StringDeserializer
     * @param kafkaConfMap kafka基本设置
     */
    private void defaultKafkaConf(Map<String,Object> kafkaConfMap){
        if(!kafkaConfMap.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)){
            kafkaConfMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        }
        if(!kafkaConfMap.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)){
            kafkaConfMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        }
        if(!kafkaConfMap.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)){
            kafkaConfMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        }
    }

    /**
     * 启动SparkStreamingKafka
     */
    public void start(){
        this.work();
    }

    /**
     * 根据给出的配置创建SparkConf
     * 需要剔除自定义的配置，剩余的配置写入SparkConf
     * @return SparkConf
     */
    private SparkConf createSparkConf(Map<Object,Object> map){
        HashMap<String,String> tmp = new HashMap<>(16);
        map.forEach((key,value)->{
            List<TemplateConf> templateConf = Arrays.asList(TemplateConf.values());
            if(!templateConf.contains(TemplateConf.fromValue(key.toString()))){
                tmp.put(key.toString(),value.toString());
            }
        });
        SparkConf sparkConf = SparkFactory.getDefaultSparkConf();
        tmp.forEach(sparkConf::set);
        return sparkConf;
    }

    private void work() {

        // 根据 Kafka配置以及 sc对象生成 Streaming对象
        JavaInputDStream<ConsumerRecord<String, String>> stream = this.getStreaming();

        // Kafka 中的一条数据
        JavaDStream<String> lines = stream.map(ConsumerRecord::value);
        this.handle(lines);

        // 更新存储在 Zookeeper中的偏移量
        switch (this.offsetStore){
            case ZOOKEEPER:
                stream.foreachRDD(rdd -> {
                    OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                    for (OffsetRange o : offsetRanges) {
                        ZookeeperFactory.getZkUtils().updatePersistentPath(
                                String.join("/", OFFSET_DIR, String.valueOf(o.partition())),
                                String.valueOf(o.untilOffset()),
                                ZookeeperFactory.getZkUtils().defaultAcls(String.join("/", OFFSET_DIR, String.valueOf(o.partition())))
                        );
                        StaticLog.info("UPDATE OFFSET WITH [ topic :" + o.topic() + " partition :" + o.partition() + " offset :" + o.fromOffset() + " ~ " + o.untilOffset() + " ]");
                    }
                });
                break;
            case KAFKA:break;
            case MYSQL:break;
        }
    }

    protected abstract void handle(JavaDStream<String> lines);

    /**
     * 根据StreamingContext以及Kafka配置生成DStream
     */
    private JavaInputDStream<ConsumerRecord<String, String>> getStreaming() {
        // 获取偏移量存储路径下的偏移量节点
        if (!ZookeeperFactory.getZkClient().exists(OFFSET_DIR)) {
            ZookeeperFactory.getZkClient().createPersistent(OFFSET_DIR, true);
        }
        List<String> children = ZookeeperFactory.getZkClient().getChildren(OFFSET_DIR);

        if (children != null && !children.isEmpty()) {
            Map<TopicPartition, Long> fromOffsets = new HashMap<>(children.size());
            // 可以读取到存在Zookeeper中的偏移量 使用读取到的偏移量创建Streaming来读取Kafka
            for (String child : children) {
                long offset = Long.valueOf(ZookeeperFactory.getZkClient().readData(String.join("/", OFFSET_DIR, child)));
                fromOffsets.put(new TopicPartition(CONSUMER_TOPIC_NAME, Integer.valueOf(child)), offset);
                StaticLog.info("FOUND OFFSET IN ZOOKEEPER, USE [ partition :" + child + " offset :" + offset + " ]");
            }
            StaticLog.info("CREATE DIRECT STREAMING WITH CUSTOMIZED OFFSET..");
            return KafkaUtils.createDirectStream(
                    javaStreamingContext,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Assign(fromOffsets.keySet(), kafkaConfMap, fromOffsets)
            );
        } else {
            // Zookeeper内没有存储偏移量 使用默认的偏移量创建Streaming
            StaticLog.info("NO OFFSET FOUND, CREATE DIRECT STREAMING WITH DEFAULT OFFSET..");
            return KafkaUtils.createDirectStream(
                    javaStreamingContext,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(Collections.singleton(CONSUMER_TOPIC_NAME), kafkaConfMap)
            );
        }
    }

    /**
     * 模板配置枚举类
     */
    public enum TemplateConf {
        APP_NAME("app_name"), DURATION("duration"), MASTER("master"),KRYO_CLASSES("kryo_classes");

        TemplateConf(String value) {
            this.value = value;
        }

        private String value;

        String getValue() {
            return value;
        }

        public static TemplateConf fromValue(String value) {
            for (TemplateConf templateConf : TemplateConf.values()) {
                if (templateConf.getValue().equals(value)) {
                    return templateConf;
                }
            }
            //default value
            return null;
        }
    }

    /**
     * offset存储方式枚举类
     */
    public enum OffsetStore {
        KAFKA("kafka"), ZOOKEEPER("duration"), MYSQL("mysql");

        OffsetStore(String value) {
            this.value = value;
        }

        private String value;

        String getValue() {
            return value;
        }

        public static OffsetStore fromValue(String value) {
            for (OffsetStore offsetStore : OffsetStore.values()) {
                if (offsetStore.getValue().equals(value)) {
                    return offsetStore;
                }
            }
            //default value
            return null;
        }
    }
}