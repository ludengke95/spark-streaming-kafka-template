package com.opensharing.bigdata.handler.updatestate;

/**
 * 根据新值和就值是否为null来判断保留谁
 * @author ludengke
 * @date 2019/12/17
 **/
public class NullUpdateStateHandler<T> implements UpdateStateHandler<T> {
    /**
     * 合并old值和now值
     *
     * @param old
     * @param now
     * @return 合并后的值
     */
    @Override
    public T process(T old, T now) {
       if(old==null){
           return now;
       }
       else if(now==null){
           return old;
       }
       return null;
    }
}
