package com.opensharing.bigdata.conf;

/**
 * 模板配置枚举类
 */
public enum ZkConf {
    URL("url"), CONNECTION_TIMEOUT("connection_timeout"),ZK_SERIALIZER("zk_serializer");

    private ZkConf(Object value) {
        this.value = value;
    }

    private Object value;

    Object getValue() {
        return value;
    }

    public static ZkConf fromValue(String value) {
        for (ZkConf zkConf : ZkConf.values()) {
            if (zkConf.getValue().equals(value)) {
                return zkConf;
            }
        }
        //default value
        return null;
    }
}
