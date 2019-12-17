package com.opensharing.bigdata.toolfactory;
import cn.hutool.core.util.NumberUtil;
import com.opensharing.bigdata.conf.ZkConf;
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

    public static void init(Map<ZkConf,Object> map){
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

}
