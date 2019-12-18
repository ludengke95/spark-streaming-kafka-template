package com.opensharing.bigdata.template.statestreamingkafka;

import cn.hutool.core.util.StrUtil;
import cn.hutool.log.StaticLog;
import com.opensharing.bigdata.handler.PairRDDHandler;
import com.opensharing.bigdata.handler.kafka.*;
import com.opensharing.bigdata.template.streamingkafka.OffsetInKafkaTemplate;
import com.opensharing.bigdata.template.streamingkafka.SparkStreamingKafka;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.Optional;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * SparkStateStreaming读取kafka的数据，并且能够跨批次统计 的模板类
 * 暂不支持初始化
 * offset保存优先级：kafka > zk > mysql
 * offset保存方式：
 *      1. kafka：采用kafka的配置。
 *      2. zk：需要给出zk url 超时时间等配置
 *      3. mysql：需要传递Mysql的Connect
 * @author ludengke
 * @date 2019/12/11
 **/
public class SparkStateStreamingKafka<K,V> extends SparkStreamingKafka implements Serializable {

    /**
     * 设置更新方法
     */
    private KafkaUpdateStateHandler<K,V> updateStateHandler;
    /**
     * 设置处理方法组
     */
    private List<PairRDDHandler<K,V>> handlers = new ArrayList<PairRDDHandler<K,V>>();

    /**
     * key过期时间
     */
    public Duration timeOut;

    public SparkStateStreamingKafka() {
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
     * @param checkPointPath checkPoint的路径
     */
    public SparkStateStreamingKafka(Map<Object, Object> sparkConfMap, Map<String, Object> kafkaConfMap,String checkPointPath) {
        super(sparkConfMap, kafkaConfMap,checkPointPath);
    }

    /**
     * 处理kafka数据，调用UpdateStateHandler接口
     *
     * @throws Exception
     */
    @Override
    protected void work() throws Exception {
        if (handlers.isEmpty()) {
            handlers.add(new ConsoleKafkaPairRDDHandler<K,V>());
        }
        if (updateStateHandler == null) {
            updateStateHandler = new NullUpdateStateHandler<K,V>();
        }
        if (offsetTemplate == null) {
            offsetTemplate = new OffsetInKafkaTemplate(kafkaConfMap);
        }
        if(StrUtil.isEmpty(groupId)){
            groupId = kafkaConfMap.get(ConsumerConfig.GROUP_ID_CONFIG).toString();
        }
        JavaInputDStream<ConsumerRecord<String, String>> stream = this.getStreaming();
        JavaPairDStream<K,V> pair = updateStateHandler.toPair(stream);
        JavaPairDStream<K, V> union = pair.mapWithState(getMapWithStateFunction()).stateSnapshots();
        union.foreachRDD(line->{
            if(handlers.size()>1){
                line.persist(StorageLevel.MEMORY_AND_DISK_SER_2());
            }
            for (PairRDDHandler<K, V> handler : handlers) {
                handler.process(line);
            }
            if(handlers.size()>1){
                line.unpersist();
            }
        });

        offsetTemplate.updateOffset(stream,topicName,groupId);
    }

    /**
     * 获取状态更新函数
     * @return
     */
    private StateSpec<K, V, V, Tuple2<K, V>> getMapWithStateFunction() {
        StateSpec<K, V, V, Tuple2<K, V>> function = StateSpec.function((K key, Optional<V> now, State<V> curState) -> {
            if (curState.isTimingOut()) {
                StaticLog.info(key + " is Timeout");
                return new Tuple2<K, V>(key, now.get());
            } else {
                //判断now是否包含值
                if (now.isPresent()) {
                    //取出当前批次的值
                    V nowV = now.get();
                    //判断历史值是否存在，不存在直接新增，存在则判断是否更新
                    if (curState.exists()) {
                        //取出历史值，如果历史值为空或者当前值的修改时间大于历史值的修改时间，则更新数据为当前数据
                        V oldV = curState.getOption().isEmpty() ? null : curState.getOption().get();
                        curState.update(updateStateHandler.updateValue(oldV, nowV));
                    } else {
                        curState.update(nowV);
                    }
                }
                return new Tuple2<K, V>(key, curState.get());
            }
        });
        if(timeOut!=null){
            function.timeout(timeOut);
        }
        return function;
    }

    /**
     * 不知为何79行调用pair.mapWithState(StateSpec.function(SparkStateStreamingKafka::mappingFunc))报错
     * 根据key对应的当前数据和历史数据更新合并成新值（key所对应的value值）
     * @param key 历史数据的key
     * @param now key对应的当前数据
     * @param curState key对应的历史数据
     * @return Tuple2迭代器
     */
    private Tuple2<K,V> mappingFunc (K key, Optional<V> now, State<V> curState){

        if (curState.isTimingOut()) {
            StaticLog.info(key + " is Timeout");
            return new Tuple2<K, V>(key, now.get());
        } else {
            //判断now是否包含值
            if (now.isPresent()) {
                //取出当前批次的值
                V nowV = now.get();
                //判断历史值是否存在，不存在直接新增，存在则判断是否更新
                if (curState.exists()) {
                    //取出历史值，如果历史值为空或者当前值的修改时间大于历史值的修改时间，则更新数据为当前数据
                    V oldV = curState.getOption().isEmpty() ? null : curState.getOption().get();
                    curState.update(updateStateHandler.updateValue(oldV, nowV));
                } else {
                    curState.update(nowV);
                }
            }
            return new Tuple2<K, V>(key, curState.get());
        }
    }


    public SparkStateStreamingKafka addHandler(PairRDDHandler pairRDDHandler) {
        this.handlers.add(pairRDDHandler);
        return this;
    }

    public KafkaUpdateStateHandler<K, V> getUpdateStateHandler() {
        return updateStateHandler;
    }

    public void setUpdateStateHandler(KafkaUpdateStateHandler<K, V> updateStateHandler) {
        this.updateStateHandler = updateStateHandler;
    }

    public Duration getTimeOut() {
        return timeOut;
    }

    public void setTimeOut(Duration timeOut) {
        this.timeOut = timeOut;
    }
}
