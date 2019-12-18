package com.opensharing.bigdata.template.streamingkafka;

import cn.hutool.core.util.StrUtil;
import cn.hutool.log.StaticLog;
import com.opensharing.bigdata.conf.TemplateConf;
import com.opensharing.bigdata.handler.kafkavalue.ConsoleKafkaRDDHandler;
import com.opensharing.bigdata.handler.kafkavalue.RDDHandler;
import com.opensharing.bigdata.toolfactory.SparkUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
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
public class SparkStreamingKafka{

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
    protected Map<String, Object> kafkaConfMap;

    /**
     * topic 的名称
     */
    protected String topicName = "";

    /**
     * 消费者组的id
     */
    protected String groupId = "";

    protected List<RDDHandler> handlers = new ArrayList<RDDHandler>();

    /**
     * offset存储模板，可以自定义，需要实现两个方法
     */
    protected OffsetTemplate offsetTemplate;

    /**
     * hdfs Url，选填，设置优雅停止的时候必填
     */
    private String hdfsUrl;

    /**
     * 优雅停止时，信号文件的hdfs地址，设置优雅停止的时候必填
     */
    private String stopFilePath;

    /**
     * 优雅停止时，检测文件间隔，设置优雅停止的时候必填
     */
    private Integer stopSecond;

    public SparkStreamingKafka() {
    }


    /**
     * 构造函数，和create功能一样
     * @param sparkConfMap
     * @param kafkaConfMap
     */
    protected SparkStreamingKafka(Map<Object,Object> sparkConfMap, Map<String,Object> kafkaConfMap) {
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
        Duration duration = sparkConfMap.containsKey(TemplateConf.DURATION)?(Duration) sparkConfMap.get(TemplateConf.DURATION) :Durations.seconds(10);
        this.javaStreamingContext = new JavaStreamingContext(sparkConf, duration);
        this.kafkaConfMap = defaultKafkaConf(kafkaConfMap);
    }

    /**
     * 构造函数，和create功能一样
     * @param javaStreamingContext 已初始化的javaStreamingContext
     * @param kafkaConfMap
     */
    protected SparkStreamingKafka(JavaStreamingContext javaStreamingContext, Map<String,Object> kafkaConfMap) {
        this.javaStreamingContext = javaStreamingContext;
        this.sparkConf = javaStreamingContext.sparkContext().getConf();
        this.kafkaConfMap = kafkaConfMap;
    }
    /**
     * 模板初始化函数
     * 传入外部初始化好的JavaStreamingContext，
     * 需设置
     *  1. 启动间隔
     *  2. spark.streaming.kafka.maxRatePerPartition：spark从kafka的每个分区每秒取出的数据条数
     * @param javaStreamingContext 已初始化的javaStreamingContext
     */
    public static SparkStreamingKafka create(JavaStreamingContext javaStreamingContext,Map<String,Object> kafkaConfMap){
        SparkStreamingKafka sparkStreamingKafka = new SparkStreamingKafka();
        sparkStreamingKafka.javaStreamingContext = javaStreamingContext;
        sparkStreamingKafka.sparkConf = javaStreamingContext.sparkContext().getConf();
        sparkStreamingKafka.kafkaConfMap = kafkaConfMap;
        return sparkStreamingKafka;
    }

    /**
     * 模板初始化函数
     * 传入基本的SparkConf配置
     * 包含：
     *  1.app_name ： {str，必须}
     *  2.duration ：{Duration，必须}
     *  3.master ：{str，本地运行必须，线上运行非必须}
     *  4.kryo_classes ：{arr(Class数组)，非必须}
     *  ......与SparkConf一致，仅对1,2做检查 剩余的属性不做检查
     * @param sparkConfMap 需要设置的SparkConf属性和必须属性
     * @param kafkaConfMap kafka的基本配置
     */
    public static SparkStreamingKafka create(Map<Object,Object> sparkConfMap, Map<String,Object> kafkaConfMap){
        SparkStreamingKafka sparkStreamingKafka = new SparkStreamingKafka();
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
        Duration duration = sparkConfMap.containsKey(TemplateConf.DURATION)?(Duration) sparkConfMap.get(TemplateConf.DURATION) :Durations.seconds(10);
        sparkStreamingKafka.javaStreamingContext = new JavaStreamingContext(sparkConf, duration);
        sparkStreamingKafka.kafkaConfMap = defaultKafkaConf(kafkaConfMap);
        return sparkStreamingKafka;
    }

    /**
     * 预处理kafka配置
     * eg:
     *      1. 如果不设置消费者提交方式，默认设置为手动提交
     *      2. 不设置key.deserializer和value.deserializer,默认设置为StringDeserializer
     * @param kafkaConfMap kafka基本设置
     */
    private static Map<String,Object> defaultKafkaConf(Map<String, Object> kafkaConfMap){
        if(!kafkaConfMap.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)){
            kafkaConfMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        }
        if(!kafkaConfMap.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)){
            kafkaConfMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        }
        if(!kafkaConfMap.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)){
            kafkaConfMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        }
        return kafkaConfMap;
    }

    /**
     * 启动SparkStreamingKafka
     */
    public void start(){
        if (handlers.isEmpty()) {
            handlers.add(new ConsoleKafkaRDDHandler());
        }
        if (offsetTemplate == null) {
            offsetTemplate = new OffsetInKafkaTemplate(kafkaConfMap);
        }
        if(StrUtil.isEmpty(groupId)){
            groupId = kafkaConfMap.get(ConsumerConfig.GROUP_ID_CONFIG).toString();
        }
        try {
            this.work();
            javaStreamingContext.start();
            if(stopFilePath!=null){
                SparkUtils.stopByMarkFile(javaStreamingContext, hdfsUrl,stopFilePath,stopSecond);
            }else {
                try {
                    javaStreamingContext.awaitTermination();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    javaStreamingContext.close();
                }
            }
        } catch (Exception e) {
            StaticLog.error(e,"执行异常");
        }
    }

    /**
     * 根据给出的配置创建SparkConf
     * 需要剔除自定义的配置，剩余的配置写入SparkConf
     * @return SparkConf
     */
    private static SparkConf createSparkConf(Map<Object, Object> map){
        HashMap<String,String> tmp = new HashMap<>(16);
        map.forEach((key,value)->{
            List<TemplateConf> templateConf = Arrays.asList(TemplateConf.values());
            if(!templateConf.contains(TemplateConf.fromValue(key.toString()))){
                tmp.put(key.toString(),value.toString());
            }
        });
        SparkConf sparkConf = SparkUtils.getDefaultSparkConf();
        tmp.forEach(sparkConf::set);
        return sparkConf;
    }

    /**
     * 处理kafka数据，调用RDDHandler接口
     * @throws Exception
     */
    protected void work() throws Exception {

        // 根据 Kafka配置以及 sc对象生成 Streaming对象
        JavaInputDStream<ConsumerRecord<String, String>> stream = this.getStreaming();

        stream.foreachRDD(line->{
            if(handlers.size()>1){
                line.persist(StorageLevel.MEMORY_AND_DISK_SER_2());
            }
            for (RDDHandler rddHandler : handlers) {
                rddHandler.process(line);
            }
            if(handlers.size()>1){
                line.unpersist();
            }
        });
        offsetTemplate.updateOffset(stream,topicName,groupId);
    }

    /**
     * 根据StreamingContext以及Kafka配置生成DStream
     */
    protected JavaInputDStream<ConsumerRecord<String, String>> getStreaming() throws Exception {
        Map<TopicPartition, Long> fromOffsets = new HashMap<>(16);

        fromOffsets = offsetTemplate.getOffset(topicName,groupId);


        if(!fromOffsets.isEmpty()){
            StaticLog.info("CREATE DIRECT STREAMING WITH CUSTOMIZED OFFSET..");
            return KafkaUtils.createDirectStream(
                    javaStreamingContext,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Assign(fromOffsets.keySet(), kafkaConfMap, fromOffsets)
            );
        } else {
            StaticLog.info("NO OFFSET FOUND, CREATE DIRECT STREAMING WITH DEFAULT OFFSET..");
            return KafkaUtils.createDirectStream(
                    javaStreamingContext,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(Collections.singleton(topicName), kafkaConfMap)
            );
        }
    }

    public SparkStreamingKafka addHandler(RDDHandler rddHandler){
        this.handlers.add(rddHandler);
        return this;
    }

    public Map<String, Object> getKafkaConfMap() {
        return kafkaConfMap;
    }

    public SparkStreamingKafka setKafkaConfMap(Map<String, Object> kafkaConfMap) {
        this.kafkaConfMap = kafkaConfMap;
        return this;
    }

    public String getTopicName() {
        return topicName;
    }

    public SparkStreamingKafka setTopicName(String topicName) {
        this.topicName = topicName;
        return this;
    }

    public String getGroupId() {
        return groupId;
    }

    public SparkStreamingKafka setGroupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public OffsetTemplate getOffsetTemplate() {
        return offsetTemplate;
    }

    public SparkStreamingKafka setOffsetTemplate(OffsetTemplate offsetTemplate) {
        this.offsetTemplate = offsetTemplate;
        return this;
    }

    public String getHdfsUrl() {
        return hdfsUrl;
    }

    public SparkStreamingKafka setHdfsUrl(String hdfsUrl) {
        this.hdfsUrl = hdfsUrl;
        return this;
    }

    public String getStopFilePath() {
        return stopFilePath;
    }

    public SparkStreamingKafka setStopFilePath(String stopFilePath) {
        this.stopFilePath = stopFilePath;
        return this;
    }

    public Integer getStopSecond() {
        return stopSecond;
    }

    public SparkStreamingKafka setStopSecond(Integer stopSecond) {
        this.stopSecond = stopSecond;
        return this;
    }
}
