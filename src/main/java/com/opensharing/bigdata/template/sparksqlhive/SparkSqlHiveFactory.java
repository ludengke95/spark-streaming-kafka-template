package com.opensharing.bigdata.template.sparksqlhive;

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
