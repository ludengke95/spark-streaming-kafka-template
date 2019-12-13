package com.opensharing.bigdata.template.sparksqlhive;

import cn.hutool.log.StaticLog;
import com.opensharing.bigdata.handler.ConsoleKafkaRDDHandler;
import com.opensharing.bigdata.handler.RDDHandler;
import com.opensharing.bigdata.toolfactory.SparkFactory;
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
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

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
public abstract class SparkSqlHiveFactory {

}
