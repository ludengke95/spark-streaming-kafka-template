package com.opensharing.bigdata.conf;

/**
 * 模板配置枚举类
 */
public enum ZkConfEnum {

	/**
	 * zk连接url
	 */
	URL("url"),
	/**
	 * zk连接超时间
	 */
	CONNECTION_TIMEOUT("connection_timeout"),
	/**
	 * zk序列化对象
	 */
	ZK_SERIALIZER("zk_serializer");

	private Object value;

	ZkConfEnum(Object value) {
		this.value = value;
	}

	public static ZkConfEnum fromValue(String value) {
		for (ZkConfEnum zkConf : ZkConfEnum.values()) {
			if (zkConf.getValue().equals(value)) {
				return zkConf;
			}
		}
		//default value
		return null;
	}

	Object getValue() {
		return value;
	}
}
