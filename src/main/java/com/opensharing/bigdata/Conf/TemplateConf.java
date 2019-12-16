package com.opensharing.bigdata.Conf;

/**
 * 模板配置枚举类
 */
public enum TemplateConf {
    APP_NAME("app_name"), DURATION("duration"), MASTER("master"),KRYO_CLASSES("kryo_classes");

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
