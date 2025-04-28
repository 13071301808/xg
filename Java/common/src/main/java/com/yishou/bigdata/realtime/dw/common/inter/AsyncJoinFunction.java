package com.yishou.bigdata.realtime.dw.common.inter;

import com.alibaba.fastjson.JSONObject;

/**
 * @date: 2022/12/28
 * @author: yangshibiao
 * @desc: 异步关联方法，用于使用异步IO关联对应的维度数据
 */
public interface AsyncJoinFunction<T> {

    /**
     * 通过对应的维度表名 和 输入数据，获取需要的维度信息
     *
     * @param tableName 维度表名
     * @param input     输入的数据
     * @return 对应的维度信息
     */
    JSONObject getDimInfo(String tableName, T input);

    /**
     * 将维度信息join到传入的数据中
     *
     * @param input   传入的数据
     * @param dimInfo 维度信息
     */
    void join(T input, JSONObject dimInfo);

}
