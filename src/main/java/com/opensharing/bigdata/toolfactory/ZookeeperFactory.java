package com.opensharing.bigdata.toolfactory;
import cn.hutool.core.util.NumberUtil;
import com.opensharing.bigdata.Serializer.CustomSerializer;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.util.Map;

/**
 * @author ludengke
 * @date 2019/12/12
 **/
public class ZookeeperFactory {

    private static ZkConnection ZK_CONNECTION = null;
    private static ZkClient ZK_CLIENT = null;
    private static ZkUtils ZK_UTILS = null;

    public void init(Map<String,Object> map){
        ZK_CONNECTION = new ZkConnection(map.get(ZkConf.URL).toString());
        if(map.containsKey(ZkConf.ZK_SERIALIZER)){
            ZK_CLIENT = new ZkClient(ZK_CONNECTION, NumberUtil.parseInt(map.get(ZkConf.CONNECTION_TIMEOUT).toString()),(ZkSerializer) map.get(ZkConf.ZK_SERIALIZER));
        }
        else {
            ZK_CLIENT = new ZkClient(ZK_CONNECTION,NumberUtil.parseInt(map.get(ZkConf.CONNECTION_TIMEOUT).toString()),new CustomSerializer());
        }
        ZK_UTILS = new ZkUtils(ZK_CLIENT,ZK_CONNECTION,false);
    };

    public static ZkConnection getZkConnection() {
        return ZK_CONNECTION;
    }

    public static ZkClient getZkClient() {
        return ZK_CLIENT;
    }

    public static ZkUtils getZkUtils() {
        return ZK_UTILS;
    }

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
}
