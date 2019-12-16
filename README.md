# spark-template #

<!-- vscode-markdown-toc -->
* [为何会产生一个名叫spark-template的轮子(现在估计连轮子都算不上，加油！！！)](#spark-template)
* [计划功能](#)
* [项目说明](#-1)
* [感谢一下开源项目](#-1)
* [后言](#-1)

<!-- vscode-markdown-toc-config
	numbering=true
	autoSave=true
	/vscode-markdown-toc-config -->
<!-- /vscode-markdown-toc -->

##  1. <a name='spark-template'></a>为何会产生一个名叫spark-template的轮子(现在估计连轮子都算不上，加油！！！) 

+ 是不是还在头疼spark2如何引入其他的组件，例如kafka，hive，HBase，TiDB。一个新手不知道该如何正确的将这些组件组合。
+ kafka引入了，但是offset怎么存储又是个问题，是由kafka自动管理还是，存储到zk，还是写到mysql。
+ 仅仅是想用SparkSql进行数据的统计，结果写入到hive或者关系型数据库，分布式事务怎么办。
+ spark-template就是为了解决诸如此类的问题应运而生的，希望能够帮助你简化开发。

这个项目的初衷就是为了简化Spark对接其他组件（尤其是kafka，对新手贼不友好）。

##  2. <a name=''></a>计划功能 
+ [x] Spark Streaming Kafka offset in zk
+ [ ] Spark Streaming Kafka offset in mysql
+ [ ] Spark Streaming Kafka offset in kafka
+ [ ] SparkSql to Hive
+ [ ] SparkSql to Mysql/TiDB
+ [ ] Spark to HBase
+ [ ] HBase to SparkSql

##  3. <a name='-1'></a>项目说明 
1. 现在仅仅开发了Spark Streaming Kafka offset in zk,可以查看test样例。

##  4. <a name='-1'></a>感谢一下开源项目 
+ @code4craft/webmagic：十分感谢webmegic，其实这个项目的一些想法也是源于这个项目，可以看出作者逻辑十分清晰，代码扩展也很简单，希望spark-template也能够像webmagic一样简单，简化更多人的spark开发。
+ @looly/hutool：第一次见到这个项目的时候，我都惊艳到了，功能十分的全，而且比较使用，简单，推荐给各位大哥。

##  5. <a name='-1'></a>后言 
+ 项目还在进行中，只有我一个人，如果你觉得可以动动你的小手，点一点fork，star。
+ 如果你也对这个项目有想法，可以加入我们(一个人可以说我们嘛？)
联系方式：ludengke95@gmail.com/ludengke95@163.com