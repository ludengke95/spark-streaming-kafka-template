package com.opensharing.bigdata.template.streamingkafka;

import cn.hutool.core.util.StrUtil;
import cn.hutool.log.StaticLog;
import com.opensharing.bigdata.conf.TemplateConfEnum;
import com.opensharing.bigdata.handler.kafka.ConsoleKafkaRDDHandler;
import com.opensharing.bigdata.handler.kafka.KafkaRDDHandlerImpl;
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
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.Serializable;
import java.util.*;

/**
 * SparkStreaming读取kafka的数据的模板类
 * offset保存优先级：kafka > zk > mysql
 *
 * @author ludengke
 * @date 2019/12/11
 **/
public class SparkStreamingKafka implements Serializable {

	/**
	 * JavaStreamingContext
	 * 因为不能序列化，只能通过static修饰
	 */
	protected static JavaStreamingContext javaStreamingContext;

	/**
	 * kafka配置
	 */
	protected Map<String, Object> kafkaConfMap;

	/**
	 * streaming 启动时间间隔
	 */
	protected Duration duration;

	/**
	 * topic 的名称
	 */
	protected String topicName = "";

	/**
	 * 消费者组的id
	 */
	protected String groupId = "";

	/**
	 * offset存储模板，可以自定义，需要实现两个方法
	 */
	protected OffsetTemplate offsetTemplate;

	/**
	 * hdfs Url，选填，设置优雅停止的时候必填
	 */
	protected String hdfsUrl;

	/**
	 * 优雅停止时，信号文件的hdfs地址，设置优雅停止的时候必填
	 */
	protected String stopFilePath;

	/**
	 * 优雅停止时，检测文件间隔，设置优雅停止的时候必填
	 */
	protected Integer stopSecond;

	/**
	 * checkpoint地址
	 */
	protected String checkPointPath;

	/**
	 * SparkConf:spark应用的配置类
	 */
	private SparkConf sparkConf;

	/**
	 * kafka数据流处理实现类组
	 */
	private List<KafkaRDDHandlerImpl> handlers = new ArrayList<KafkaRDDHandlerImpl>();

	public SparkStreamingKafka() {
	}

	/**
	 * 模板初始化函数
	 * 传入基本的SparkConf配置
	 * 包含：
	 * 1.app_name ： {str，必须}
	 * 2.duration ：{Duration，必须}
	 * 3.master ：{str，本地运行必须，线上运行非必须}
	 * 4.kryo_classes ：{arr(Class数组)，非必须}
	 * ......与SparkConf一致，仅对1,2做检查 剩余的属性不做检查
	 *
	 * @param sparkConfMap 需要设置的SparkConf属性和必须属性
	 * @param kafkaConfMap kafka的基本配置
	 */
	protected SparkStreamingKafka(Map<Object, Object> sparkConfMap, Map<String, Object> kafkaConfMap) {
		SparkConf sparkConf = createSparkConf(sparkConfMap);
		if (sparkConfMap.containsKey(TemplateConfEnum.APP_NAME)) {
			sparkConf.setAppName(sparkConfMap.get(TemplateConfEnum.APP_NAME).toString());
		}
		if (sparkConfMap.containsKey(TemplateConfEnum.MASTER)) {
			sparkConf.setMaster(sparkConfMap.get(TemplateConfEnum.MASTER).toString());
		}
		if (sparkConfMap.containsKey(TemplateConfEnum.KRYO_CLASSES)) {
			sparkConf.registerKryoClasses((Class<?>[]) sparkConfMap.get(TemplateConfEnum.KRYO_CLASSES));
		}
		Duration duration = sparkConfMap.containsKey(TemplateConfEnum.DURATION) ? (Duration) sparkConfMap.get(TemplateConfEnum.DURATION) : Durations.seconds(10);
		this.sparkConf = sparkConf;
		this.duration = duration;
		this.checkPointPath = "";
		this.kafkaConfMap = defaultKafkaConf(kafkaConfMap);
	}

	/**
	 * 模板初始化函数
	 * 传入基本的SparkConf配置
	 * 包含：
	 * 1.app_name ： {str，必须}
	 * 2.duration ：{Duration，必须}
	 * 3.master ：{str，本地运行必须，线上运行非必须}
	 * 4.kryo_classes ：{arr(Class数组)，非必须}
	 * ......与SparkConf一致，仅对1,2做检查 剩余的属性不做检查
	 *
	 * @param sparkConfMap   需要设置的SparkConf属性和必须属性
	 * @param kafkaConfMap   kafka的基本配置
	 * @param checkPointPath checkPoint的路径
	 */
	protected SparkStreamingKafka(Map<Object, Object> sparkConfMap, Map<String, Object> kafkaConfMap, String checkPointPath) {
		SparkConf sparkConf = createSparkConf(sparkConfMap);
		if (sparkConfMap.containsKey(TemplateConfEnum.APP_NAME)) {
			sparkConf.setAppName(sparkConfMap.get(TemplateConfEnum.APP_NAME).toString());
		}
		if (sparkConfMap.containsKey(TemplateConfEnum.MASTER)) {
			sparkConf.setMaster(sparkConfMap.get(TemplateConfEnum.MASTER).toString());
		}
		if (sparkConfMap.containsKey(TemplateConfEnum.KRYO_CLASSES)) {
			sparkConf.registerKryoClasses((Class<?>[]) sparkConfMap.get(TemplateConfEnum.KRYO_CLASSES));
		}
		Duration duration = sparkConfMap.containsKey(TemplateConfEnum.DURATION) ? (Duration) sparkConfMap.get(TemplateConfEnum.DURATION) : Durations.seconds(10);
		this.sparkConf = sparkConf;
		this.duration = duration;
		this.kafkaConfMap = defaultKafkaConf(kafkaConfMap);
		this.checkPointPath = checkPointPath;
	}

	/**
	 * 构造函数，和create功能一样，传入javaStreamingContext，不支持checkpoint
	 *
	 * @param javaStreamingContext 已初始化的javaStreamingContext
	 * @param kafkaConfMap
	 */
	protected SparkStreamingKafka(JavaStreamingContext javaStreamingContext, Map<String, Object> kafkaConfMap) {
		SparkStreamingKafka.javaStreamingContext = javaStreamingContext;
		this.sparkConf = javaStreamingContext.sparkContext().getConf();
		this.kafkaConfMap = kafkaConfMap;
	}

	/**
	 * 模板初始化函数，传入外部初始化好的JavaStreamingContext，传入javaStreamingContext，不支持checkpoint
	 * 需设置：1. 启动间隔；2. spark.streaming.kafka.maxRatePerPartition：spark从kafka的每个分区每秒取出的数据条数
	 *
	 * @param javaStreamingContext 已初始化的javaStreamingContext
	 */
	public static SparkStreamingKafka create(JavaStreamingContext javaStreamingContext, Map<String, Object> kafkaConfMap) {
		return new SparkStreamingKafka(javaStreamingContext, kafkaConfMap);
	}

	/**
	 * 模板初始化函数
	 * 传入基本的SparkConf配置
	 * 包含：
	 * 1.app_name ： {str，必须}
	 * 2.duration ：{Duration，必须}
	 * 3.master ：{str，本地运行必须，线上运行非必须}
	 * 4.kryo_classes ：{arr(Class数组)，非必须}
	 * ......与SparkConf一致，仅对1,2做检查 剩余的属性不做检查
	 *
	 * @param sparkConfMap 需要设置的SparkConf属性和必须属性
	 * @param kafkaConfMap kafka的基本配置
	 */
	public static SparkStreamingKafka create(Map<Object, Object> sparkConfMap, Map<String, Object> kafkaConfMap) {
		return new SparkStreamingKafka(sparkConfMap, kafkaConfMap);
	}

	/**
	 * 模板初始化函数
	 * 传入基本的SparkConf配置
	 * 包含：
	 * 1.app_name ： {str，必须}
	 * 2.duration ：{Duration，必须}
	 * 3.master ：{str，本地运行必须，线上运行非必须}
	 * 4.kryo_classes ：{arr(Class数组)，非必须}
	 * ......与SparkConf一致，仅对1,2做检查 剩余的属性不做检查
	 *
	 * @param sparkConfMap   需要设置的SparkConf属性和必须属性
	 * @param kafkaConfMap   kafka的基本配置
	 * @param checkPointPath checkPoint的路径
	 */
	public static SparkStreamingKafka create(Map<Object, Object> sparkConfMap, Map<String, Object> kafkaConfMap, String checkPointPath) {
		return new SparkStreamingKafka(sparkConfMap, kafkaConfMap, checkPointPath);
	}

	/**
	 * 预处理kafka配置
	 * eg:
	 * 1. 如果不设置消费者提交方式，默认设置为手动提交
	 * 2. 不设置key.deserializer和value.deserializer,默认设置为StringDeserializer
	 *
	 * @param kafkaConfMap kafka基本设置
	 * @return 经过默认化处理过的Kafka配置Map
	 */
	private static Map<String, Object> defaultKafkaConf(Map<String, Object> kafkaConfMap) {
		if (!kafkaConfMap.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
			kafkaConfMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		}
		if (!kafkaConfMap.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
			kafkaConfMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		}
		if (!kafkaConfMap.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
			kafkaConfMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		}
		return kafkaConfMap;
	}

	/**
	 * 根据给出的配置创建SparkConf
	 * 需要剔除自定义的配置，剩余的配置写入SparkConf
	 *
	 * @param map spark 基本配置Map
	 * @return 根据map生成基本的sparkConf
	 */
	private static SparkConf createSparkConf(Map<Object, Object> map) {
		HashMap<String, String> tmp = new HashMap<>(16);
		map.forEach((key, value) -> {
			List<TemplateConfEnum> templateConf = Arrays.asList(TemplateConfEnum.values());
			if (!templateConf.contains(TemplateConfEnum.fromValue(key.toString()))) {
				tmp.put(key.toString(), value.toString());
			}
		});
		SparkConf sparkConf = SparkUtils.getDefaultSparkConf();
		tmp.forEach(sparkConf::set);
		return sparkConf;
	}

	/**
	 * 启动SparkStreamingKafka
	 */
	public void start() {
		try {
			//javaStreamingContext是在start之前才进行初始化，reason：加入了checkpoint，附带checkpoint的javaStreamingContext无法在create阶段初始化，参数不完整。
			if (javaStreamingContext == null) {
				if (!StrUtil.isEmpty(checkPointPath)) {
					javaStreamingContext = JavaStreamingContext.getOrCreate(checkPointPath, () -> {
						javaStreamingContext = new JavaStreamingContext(sparkConf, duration);
						javaStreamingContext.checkpoint(checkPointPath);
						work();
						return javaStreamingContext;
					});
				} else {
					javaStreamingContext = new JavaStreamingContext(sparkConf, duration);
					this.work();
				}
			}
			javaStreamingContext.start();
			//循环检测终止信号文件
			if (stopFilePath != null) {
				SparkUtils.stopByMarkFile(javaStreamingContext, hdfsUrl, stopFilePath, stopSecond);
			} else {
				try {
					javaStreamingContext.awaitTermination();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					javaStreamingContext.close();
				}
			}
		} catch (Exception e) {
			StaticLog.error(e, "执行异常");
		}
	}

	/**
	 * 处理kafka数据，调用{@link KafkaRDDHandlerImpl} RDDHandlerInter 接口
	 *
	 * @throws Exception
	 */
	protected void work() throws Exception {

		//默认处理对象设置
		if (handlers.isEmpty()) {
			handlers.add(new ConsoleKafkaRDDHandler());
		}
		if (offsetTemplate == null) {
			offsetTemplate = new OffsetInKafkaTemplate(kafkaConfMap);
		}
		if (StrUtil.isEmpty(groupId)) {
			groupId = kafkaConfMap.get(ConsumerConfig.GROUP_ID_CONFIG).toString();
		}
		// 根据 Kafka配置以及 sc对象生成 Streaming对象
		JavaInputDStream<ConsumerRecord<String, String>> stream = this.getStreaming();

		//数据流依次调用预设的处理方法,并且根据处理方法的格式判断是否将原数据流是否初始化
		stream.foreachRDD(line -> {
			if (handlers.size() > 1) {
				line.persist(StorageLevel.MEMORY_AND_DISK_SER_2());
			}
			for (KafkaRDDHandlerImpl rddHandler : handlers) {
				rddHandler.process(line);
			}
			if (handlers.size() > 1) {
				line.unpersist();
			}
		});

		//数据处理完成之后更新offset值
		offsetTemplate.updateOffset(stream, topicName, groupId);
	}

	/**
	 * 根据StreamingContext以及Kafka配置生成DStream
	 *
	 * @return kafka数据流
	 * @throws Exception
	 */
	protected JavaInputDStream<ConsumerRecord<String, String>> getStreaming() throws Exception {
		Map<TopicPartition, Long> fromOffsets = new HashMap<>(16);

		//获取历史offset值
		fromOffsets = offsetTemplate.getOffset(topicName, groupId);

		//历史offset值丢失会重建，根据kafka配置选择从哪里继续开始；历史offset值存在，使用旧值初始化。
		if (!fromOffsets.isEmpty()) {
			StaticLog.info("CREATE DIRECT STREAMING WITH CUSTOMIZED OFFSET..");
			return KafkaUtils.createDirectStream(
					javaStreamingContext,
					LocationStrategies.PreferConsistent(),
					ConsumerStrategies.Assign(new HashSet<>(fromOffsets.keySet()), kafkaConfMap, fromOffsets)
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

	public void addHandler(KafkaRDDHandlerImpl rddHandler) {
		this.handlers.add(rddHandler);
	}

	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public OffsetTemplate getOffsetTemplate() {
		return offsetTemplate;
	}

	public void setOffsetTemplate(OffsetTemplate offsetTemplate) {
		this.offsetTemplate = offsetTemplate;
	}

	public String getHdfsUrl() {
		return hdfsUrl;
	}

	public void setHdfsUrl(String hdfsUrl) {
		this.hdfsUrl = hdfsUrl;
	}

	public String getStopFilePath() {
		return stopFilePath;
	}

	public void setStopFilePath(String stopFilePath) {
		this.stopFilePath = stopFilePath;
	}

	public Integer getStopSecond() {
		return stopSecond;
	}

	public void setStopSecond(Integer stopSecond) {
		this.stopSecond = stopSecond;
	}
}
