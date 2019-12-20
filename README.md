# spark-streaming-kafka-template #

* [为何会产生一个名叫 spark-streaming-kafka-template 的轮子](#-1)
* [计划功能](#-1)
* [项目说明](#-1)
* [感谢一下开源项目](#-1)
* [后言](#-1)

##  1. <a name='-1'></a>为何会产生一个名叫 spark-streaming-kafka-template 的轮子(现在估计连轮子都算不上) 

+ 是不是还在头疼 SparkStreaming 如何组合kafka，SparkStateStreaming怎么实现数据去重
+ Kafka 引入了，但是 Offset 怎么存储又是个问题，是由 kafka 自动管理还是，存储到 zk ，还是写到 mysql。
+ spark-streaming-kafka-template 就是为了解决诸如此类的问题应运而生的，希望能够帮助你简化开发。

这个项目的初衷就是为了简化 SparkStreaming 对接 Kafka，至于这个轮子圆不圆，走不走的远就要靠大家来检验了。

##  2. <a name='-1'></a>计划功能 
+ [x] Spark Streaming Kafka Offset in Zookeeper
+ [x] Spark Streaming Kafka Offset in Mysql
+ [x] Spark Streaming Kafka Offset in Kafka
+ [x] Spark State Streaming Kafka Offset in Zookeeper
+ [x] Spark State Streaming Kafka Offset in Mysql
+ [x] Spark State Streaming Kafka Offset in Kafka
+ [x] Spark Streaming Kafka Export to Hive

##  3. <a name='-1'></a>文档说明 
1. [spark-streaming-kafka-template 文档](http://106.12.51.176)

##  4. <a name='-1'></a>感谢一下开源项目 
+ @[code4craft/webmagic](https://github.com/code4craft/webmagic)：十分感谢 webmegic ，其实这个项目的一些想法也是源于这个项目，可以看出作者逻辑十分清晰，代码扩展也很简单，希望 spark-streaming-kafka-template 也能够像 webmagic 一样简单，简化更多人的 spark 开发。
+ @[looly/hutool](https://github.com/looly/hutool)：第一次见到这个项目的时候，我都惊艳到了，功能挺全的。而且比较实用，简单，推荐给各位大哥。

##  5. <a name='-1'></a>后言 
+ 项目还在进行中，只有我一个人，如果你觉得可以动动你的小手，点一点 fork，star。
+ 如果你也对这个项目有想法，可以加入我们(一个人可以说我们嘛？)  
联系方式：ludengke95@gmail.com/ludengke95@163.com

