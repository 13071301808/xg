package com.yishou.bigdata.realtime.dw.common.utils;

import com.alibaba.fastjson.JSON;
import com.google.common.base.CaseFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.TernaryBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @date: 2021/6/9
 * @author: yangshibiao
 * @desc: 模块工具类
 */
public class ModelUtil {

    static String currentEnv = ResourceBundle.getBundle("config").getString("profiles.active");;
    static ResourceBundle devConfig = ResourceBundle.getBundle("config-dev");
    static ResourceBundle testConfig = ResourceBundle.getBundle("config-test");
    static ResourceBundle prdConfig = ResourceBundle.getBundle("config-prd");

    static Logger logger = LoggerFactory.getLogger(ModelUtil.class);

    public static ResourceBundle getConfig(){
        switch (currentEnv) {
            case "prd":
                return prdConfig;
            case "test":
                return testConfig;
            case "dev":
            default:
                return devConfig;
        }
    }

    /**
     * 根据key获取配置值
     *
     * @param key 配置参数的key
     * @return 配置参数的value
     */
    public static String getConfigValue(String key) {

        switch (currentEnv) {
            case "prd":
                return prdConfig.getString(key);
            case "test":
                return testConfig.getString(key);
            case " dev":
            default:
                return devConfig.getString(key);
        }

    }

    /**
     * 将传入的字段中的驼峰命名 修改成下划线
     *
     * @param field 驼峰字段名
     * @return 相应的下划线命名
     */
    public static String humpToUnderline(String field) {
        return CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field);
    }

    /**
     * 将传入的字段中的下划线命名 修改成驼峰
     *
     * @param field 下划线字段名
     * @return 相应的驼峰命名
     */
    public static String underlineToHump(String field) {
        return CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, field);
    }

    /**
     * 通过解析传入参数的配置，并将配置封装到Map集合中，如果传入的参数中没有该配置，就使用配置文件中约定的配置
     *
     * @param args 传入的参数配置
     * @return 返回的Map集合
     */
    public static Map<String, String> analysisConfig(String[] args) throws ParseException {

        logger.info("##### 当前运行的环境为 {} 系统，系统版本为：{}，系统ARCH为：{}", SystemUtils.OS_NAME, SystemUtils.OS_VERSION, SystemUtils.OS_ARCH);
        logger.info("##### 传入的参数为：{}", Arrays.toString(args));

        // 使用flink中的工具类，解析传入的args
        ParameterTool params = ParameterTool.fromArgs(args);
        HashMap<String, String> configMap = new HashMap<>(params.toMap());

        // kafka_offset  kafka偏移量 : 解析方式为 当传入的参数有值，就用传入的参数，如果没值，就用当前时间
        // --kafka_offset 2022-04-28_12:00:00
        String kafkaOffset = params.get("kafka_offset");
        if (StringUtils.isBlank(kafkaOffset)) {
            kafkaOffset = DateFormatUtils.format(new Date(), "yyyy-MM-dd_HH:mm:ss");
        }
        configMap.put("kafka_offset", String.valueOf(DateUtils.parseDate(kafkaOffset, "yyyy-MM-dd_HH:mm:ss").getTime()));
        logger.info("##### 最终的kafka_offset为：{}", kafkaOffset);

        // checkpoint_interval  checkpoint时间间隔  ：  解析方式为 当传入的参数有值，就用传入的参数，如果没值，就用配置文件中约定的值
        // --checkpoint_interval 300
        String checkpointInterval = params.get("checkpoint_interval");
        if (StringUtils.isBlank(checkpointInterval)) {
            checkpointInterval = ModelUtil.getConfigValue("config.flink.checkpoint.interval");
        }
        configMap.put("checkpoint_interval", String.valueOf(Long.parseLong(checkpointInterval) * 1000));
        logger.info("##### 最终的checkpoint_interval为（单位：秒）：{}", checkpointInterval);

        // parallelism  flink并行度 : 解析方式为 当传入的参数有值，就用传入的参数，如果没值，就用配置文件中约定的值（注意：并行度需要在env中设置，不设置就没使用）
        // --parallelism 3
        String parallelism = params.get("parallelism");
        if (StringUtils.isBlank(parallelism)) {
            parallelism = ModelUtil.getConfigValue("config.flink.parallelism");
        }
        configMap.put("parallelism", parallelism);
        logger.info("##### 最终的parallelism为：{}", parallelism);


        // 返回配置Map
        logger.info("##### 最终的configMap为：{}", JSON.toJSONString(configMap));
        return configMap;

    }

    /**
     * 对传入的Flink的流的执行环境配置Checkpoint
     *
     * @param env             Flink的流的执行环境
     * @param applicationName 应用程序名，会在checkpoint的路径下创建该应用程序的文件夹，用来保存该应用程序的checkpoint
     * @param interval        checkpoint的时间间隔，单位：毫秒，filesystem模式的checkpoint建议间隔为 10s - 60s 之间的整10数
     */
    public static void deployFsCheckpoint(StreamExecutionEnvironment env, String applicationName, long interval) {

        // 设置 状态后端 为 filesystem 的模式，并通过配置文件指定文件夹（判断是本地调试还是集群环境）
        String checkpointPath = null;
        switch (currentEnv) {
            case "prd":
            case "test":
                String obsPath = "obs://" +
                        ModelUtil.getConfigValue("obs.ak") + ":" +
                        ModelUtil.getConfigValue("obs.sk") + "@" +
                        ModelUtil.getConfigValue("obs.endpoint") +
                        ModelUtil.getConfigValue("fs.checkpoint.path");
                checkpointPath = obsPath + applicationName + "/";
                logger.info("prd/test：checkpoint的路径为：【{}】",checkpointPath);
                break;
            case "dev":
            default:
                checkpointPath = ModelUtil.getConfigValue("fs.checkpoint.path") + applicationName + "/";
                logger.info("dev：checkpoint的路径为：【{}】",checkpointPath);
                break;
        }
        env.setStateBackend(new FsStateBackend(checkpointPath));

        // 启动checkpoint，设置为精确一次，并通过传入的参数设置时间间隔
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(interval);

        // 设置2个checkpoint之间的最小间隔，默认为0
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(interval / 2);

        // 设置能容忍100个检查点的失败
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(100);

        // 当作业被cancel时，不删除外部保存的检查点
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 当在设置的时间内还没有保存成功认为该检查点失败，设置为interval的10倍
        env.getCheckpointConfig().setCheckpointTimeout(interval * 10);

        // 设置同时可以进行10个checkpoint
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(10);

        logger.info(
                ">>>>> 正在进行环境设置，会创建fs的checkpoint环境，applicationName：{} ; 间隔时间interval：{} ;",
                applicationName,
                interval
        );

    }

    /**
     * 对传入的Flink的流的执行环境配置Checkpoint
     *
     * @param env             Flink的流的执行环境
     * @param applicationName 应用程序名，会在checkpoint的路径下创建该应用程序的文件夹，用来保存该应用程序的checkpoint
     * @param interval        checkpoint的时间间隔，单位：毫秒，RocksDB模式的checkpoint建议间隔为 1分钟到30分钟 之间的整分钟数
     */
    public static void deployRocksdbCheckpoint(StreamExecutionEnvironment env, String applicationName, long interval) {

        if (SystemUtils.IS_OS_WINDOWS) {
            logger.info(">>>>> 因为在windows中不能使用rocksdb的checkpoint，所以当本地运行时，需要使用fs的checkpoint");
            deployFsCheckpoint(env, applicationName, interval);
            return;
        }

        // 设置 状态后端 为 rocksdb 的模式，并通过配置文件指定文件夹（判断是本地调试还是集群环境）
        String checkpointPath = null;
        switch (currentEnv) {
            case "prd":
            case "test":
                String obsPath = "obs://" +
                        ModelUtil.getConfigValue("obs.ak") + ":" +
                        ModelUtil.getConfigValue("obs.sk") + "@" +
                        ModelUtil.getConfigValue("obs.endpoint") +
                        ModelUtil.getConfigValue("rocksdb.checkpoint.path");
                checkpointPath = obsPath + applicationName + "/";
                logger.info("prd/test：checkpoint的路径为：【{}】",checkpointPath);
                break;
            case "dev":
            default:
                checkpointPath = ModelUtil.getConfigValue("rocksdb.checkpoint.path") + applicationName + "/";
                logger.info("dev：checkpoint的路径为：【{}】",checkpointPath);
                break;
        }
        RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(new FsStateBackend(checkpointPath));
        env.setStateBackend(rocksDbBackend);

        // 启动checkpoint，设置为精确一次，并通过传入的参数设置时间间隔
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(interval);

        // 设置2个checkpoint之间的最小间隔，默认为0
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(interval / 2);

        // 设置能容忍100个检查点的失败
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(100);

        // 当作业被cancel时，不删除外部保存的检查点
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 当在设置的时间内还没有保存成功认为该检查点失败，设置为interval的10倍
        env.getCheckpointConfig().setCheckpointTimeout(interval * 10);

        // 设置同时可以进行10个checkpoint
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(10);

        logger.info(
                ">>>>> 正在进行环境设置，会创建rocksdb的checkpoint环境，applicationName：{} ; 间隔时间interval：{} ; ",
                applicationName,
                interval
        );

    }

    /**
     * 对传入的Flink的流的执行环境配置重启策略
     *
     * @param env Flink的流的执行环境
     */
    public static void deployRestartStrategy(StreamExecutionEnvironment env) {

        // 当任务中异常失败后，会重启任务3次，间隔时间为60秒
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(60, TimeUnit.SECONDS)));
        // logger.info(">>>>> 正在进行环境设置，重启策略为：重启任务3次，间隔时间为60秒");

        // 3分钟内最多失败3次,每次重启的时间间隔为30秒（排除了网络等问题，如果再失败，需要手动查明原因）
        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(
                        3,
                        Time.of(3, TimeUnit.MINUTES),
                        Time.of(30, TimeUnit.SECONDS)
                )
        );
        logger.info(">>>>> 正在进行环境设置，重启策略为：3分钟内最多失败3次,每次重启的时间间隔为30秒 ");

    }

    /**
     * 根据传入的time参数，获得StateTtlConfig对象
     *
     * @param time 时间参数
     * @return StateTtlConfig对象
     */
    public static StateTtlConfig getStateTtlConfig(Time time) {
        return StateTtlConfig
                .newBuilder(time)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
    }

}


