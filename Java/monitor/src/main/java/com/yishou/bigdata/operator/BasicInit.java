package com.yishou.bigdata.operator;

import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.realtime.dw.common.basic.IAppService;
import com.yishou.bigdata.realtime.dw.common.utils.DataStreamSourceUtil;
import com.yishou.bigdata.realtime.dw.common.utils.ModelUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class BasicInit implements IAppService {
    static Logger logger = LoggerFactory.getLogger(BasicInit.class);
    /**
     * 事件数据流（json格式数据流，用于通用主题）
     */
    protected Map<String, DataStream<JSONObject>> eventDataStreamMap = new HashMap<>();
    /**
     * Flink流的执行环境
     */
    protected StreamExecutionEnvironment env = null;

    /**
     * 初始化执行环境，并解析对应事件
     *
     * @param configMap       传入参数
     * @param applicationName app名（kafka消费者组id）
     * @param eventNames      需要解析的事件名
     * @throws ParseException 抛出的异常
     */
    protected void init(Map<String, String> configMap, String applicationName, String... eventNames) throws ParseException {
        // 专场开始调度
        this.scheduler();
        // 创建执行环境，并配置 checkpoint和重启策略
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        ModelUtil.deployRocksdbCheckpoint(env, applicationName, Long.parseLong(configMap.get("checkpoint_interval")));
        ModelUtil.deployRestartStrategy(env);

        // 获取app点击数据流
        DataStream<JSONObject> appclickDataStream = DataStreamSourceUtil
                .getNewAppLogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                .process(new AppClickLogProcess())
                .uid("app_click")
                .name("app_click")
                .setParallelism(6)
                .disableChaining()
                ;
        logger.info("##### 创建app点击数据流成功");
        // 获取h5和小程序的日志流
        DataStream<JSONObject> h5DataStream = DataStreamSourceUtil
                .getH5LogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                .process(new H5DataProcess())
                .uid("h5_data")
                .name("h5_data")
                .setParallelism(3)
                .disableChaining()
                ;
        logger.info("##### 创建H5数据流成功");
        // 获取app曝光日志库数据流
        DataStream<JSONObject> appexposureDataStream = DataStreamSourceUtil
                .getYishouLogExposureDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                .process(new AppExposureProcess())
                .uid("app_exposure")
                .name("app_exposure")
                .setParallelism(6)
                .disableChaining();
        logger.info("##### 创建app曝光数据流成功");
        // union合并
        DataStream<JSONObject> logDataStream = appclickDataStream.union(h5DataStream).union(appexposureDataStream);
        eventDataStreamMap.put("all_log_data", logDataStream);
        logger.info("##### 所有日志合并成功");

    }

    /**
     * 启动执行环境
     *
     * @param applicationName app名
     * @throws Exception 抛出的异常
     */
    protected void execute(String applicationName) throws Exception {
        env.execute(applicationName);
    }

    /**
     * 7点定时任务
     */
    protected void schedulerRun() throws ParseException, IOException {

    }


    private void scheduler() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime nextAM = now.withHour(7).withMinute(0).withSecond(0);

        if (now.isAfter(nextAM)) {
            nextAM = nextAM.plusDays(1);
        }

        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    schedulerRun();
                } catch (Throwable e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }, Duration.between(now, nextAM).getSeconds(), 24 * 60 * 60, TimeUnit.SECONDS); // 后续配置早上7点定时调度

    }
}
