package com.yishou.bigdata.realtime.dw.common.utils;

import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.realtime.dw.common.inter.AsyncJoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @date: 2022/12/28
 * @author: yangshibiao
 * @desc: 异步匹配维度信息工具类
 */
public abstract class AsyncJoinDimUtil<T> extends RichAsyncFunction<T, T> implements AsyncJoinFunction<T> {

    static Logger logger = LoggerFactory.getLogger(AsyncJoinDimUtil.class);

    /**
     * 线程池
     */
    protected ThreadPoolExecutor threadPoolExecutor;

    /**
     * 该数据匹配的维度表名
     */
    protected String tableName;

    protected AsyncJoinDimUtil() {
    }

    /**
     * 通过传入的表名创建对应的对象
     *
     * @param tableName 维度表名
     */
    protected AsyncJoinDimUtil(String tableName) {
        this.tableName = tableName;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ThreadPoolUtil threadPoolUtil = new ThreadPoolUtil();
        threadPoolExecutor = threadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {

        threadPoolExecutor.execute(new Runnable() {
            @Override
            public void run() {

                try {

                    // 通过维表名和传入的数据，获取查询维表的SQL，并进行查询
                    JSONObject dimInfo = getDimInfo(tableName, input);

                    // 合并数据（注意：因为是从维表查，所以只有1条结果）
                    if (dimInfo != null) {
                        join(input, dimInfo);
                    }

                    // 写出结果
                    resultFuture.complete(Collections.singletonList(input));

                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("@@@@@ 关联维表失败（关联时抛出异常），传入的数据为：{}，抛出的异常为：{}", input, e.getMessage());
                }

            }
        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        resultFuture.complete(Collections.singletonList(input));
        logger.error("@@@@@ 关联维表超时（获取维度数据超时），已将传入数据直接传出（没有关联维度），传入的数据为：{}", input);
    }

}
