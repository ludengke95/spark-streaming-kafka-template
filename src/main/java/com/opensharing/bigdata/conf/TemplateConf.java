package com.opensharing.bigdata.conf;

/**
 * 模板配置枚举类
 */
public enum TemplateConf {
    /**
     * 设置spark 应用的名称
     */
    APP_NAME("app_name"),

    /**
     * 本地执行的时候需要指定setMaster
     */
    MASTER("master"),

    /**
     * 设置Streaming的启动时间间隔，value是Duration对象或者整数
     */
    DURATION("duration"),

    /**
     * 用于设置启用kryo序列化的类
     */
    KRYO_CLASSES("kryo_classes");

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
