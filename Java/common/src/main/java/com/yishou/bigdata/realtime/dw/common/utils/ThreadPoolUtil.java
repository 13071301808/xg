package com.yishou.bigdata.realtime.dw.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @date: 2022/12/28
 * @author: yangshibiao
 * @desc: 线程池工具类
 */
public class ThreadPoolUtil {

    static Logger logger = LoggerFactory.getLogger(ThreadPoolUtil.class);

    /**
     * 线程池对象
     */
    private ThreadPoolExecutor threadPoolExecutor;

    /**
     * 空参构造函数（4个最少线程，10个最大线程，等待60秒）
     */
    public ThreadPoolUtil() {
        initThreadPoolExecutor(4, 10, 180, TimeUnit.SECONDS, new LinkedBlockingDeque<>());

    }

    /**
     * 构造函数
     *
     * @param corePoolSize    线程池维护线程的最少数量
     * @param maximumPoolSize 线程池维护线程的最大数量
     * @param keepAliveTime   线程池维护线程所允许的空闲时间（单位：秒）
     */
    public ThreadPoolUtil(int corePoolSize, int maximumPoolSize, long keepAliveTime) {
        initThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, TimeUnit.SECONDS, new LinkedBlockingDeque<>());

    }

    /**
     * 构造函数（默认等待60秒）
     *
     * @param corePoolSize    线程池维护线程的最少数量
     * @param maximumPoolSize 线程池维护线程的最大数量
     */
    public ThreadPoolUtil(int corePoolSize, int maximumPoolSize) {
        initThreadPoolExecutor(corePoolSize, maximumPoolSize, 180, TimeUnit.SECONDS, new LinkedBlockingDeque<>());

    }

    /**
     * 初始化线程池执行对象
     *
     * @param corePoolSize    线程池维护线程的最少数量
     * @param maximumPoolSize 线程池维护线程的最大数量
     * @param keepAliveTime   线程池维护线程所允许的空闲时间
     * @param unit            线程池维护线程所允许的空闲时间的单位
     * @param workQueue       线程池所使用的缓冲队列
     */
    public void initThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
        threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
        logger.info(
                "##### 创建线程池成功，其中 线程池维护线程的最少数量 = {}，线程池维护线程的最大数量 = {}， 线程池维护线程所允许的空闲时间(秒) = {} ",
                corePoolSize,
                maximumPoolSize,
                keepAliveTime
        );
    }

    /**
     * 获取线程池执行对象
     *
     * @return 线程池执行对象
     */
    public ThreadPoolExecutor getThreadPoolExecutor() {
        return threadPoolExecutor;
    }

}
