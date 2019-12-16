package com.opensharing.bigdata.toolfactory;

import cn.hutool.log.StaticLog;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * hadoop组件工具类
 *
 * @author ludengke
 * @date 2019/9/3
 **/
public class HadoopUtils {
    /**
     * 判断hdfs文件是否存在
     *
     * @param master  hdfs地址
     * @param pathStr 文件路径
     * @return
     * @throws Exception
     */
    public static boolean isExistsFile(String master, String pathStr) throws Exception {
        Path path = new Path(pathStr);
        FileSystem fs = FileSystem.get(new URI(master), new Configuration());
        boolean flag = fs.exists(path);
        return flag;
    }

    /**
     * 删除hdfs文件
     *
     * @param master  hdfs地址
     * @param pathStr 文件路径
     * @return
     * @throws Exception
     */
    public static boolean delHdfsFile(String master, String pathStr) throws Exception {
        Path path = new Path(pathStr);
        FileSystem fs = FileSystem.get(new URI(master), new Configuration());
        boolean flag = fs.deleteOnExit(path);
        return flag;
    }

    /**
     * 给出hdfs文件下的文件个数
     *
     * @param master  hdfs地址
     * @param pathStr 文件路径
     * @return
     * @throws Exception
     */
    public static int getFileChildNumber(String master, String pathStr) throws Exception {
        Path path = new Path(pathStr);
        FileSystem fs = FileSystem.get(new URI(master), new Configuration());
        FileStatus[] fileStatuses = fs.listStatus(path);
        return fileStatuses.length;
    }


    /**
     * 把一个文件移动到另一个文件
     * 实际上也就是改名操作
     * @param master  hdfs地址
     * @param path    当前路径
     * @param newPath 目标路径
     * @return 是否移动成功
     * @throws Exception
     */
    public static boolean mvDirToDir(String master, String path, String newPath) {
        boolean result = false;
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI(master), new Configuration());
            if (!fs.exists(new Path(newPath))) {
                result = fs.rename(new Path(path), new Path(newPath));
            } else {
                StaticLog.warn("HDFS上目录： {} 被占用 :" + newPath);
            }
        } catch (Exception e) {
            StaticLog.info("移动HDFS上目录：{} 失败！ :" + path + e);
        } finally {
            //close(fs);
        }
        return result;
    }

}
