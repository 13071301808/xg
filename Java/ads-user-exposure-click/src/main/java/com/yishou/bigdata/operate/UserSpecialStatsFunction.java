package com.yishou.bigdata.operate;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.realtime.dw.common.utils.ModelUtil;
import com.yishou.bigdata.realtime.dw.common.utils.RedisUtil;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public class UserSpecialStatsFunction extends KeyedProcessFunction<String, Tuple4<String, String, String, Long>, Tuple2<String, List<JSONObject>>> {
    private static final Logger logger = LoggerFactory.getLogger(UserSpecialStatsFunction.class);

    // 状态1: 存储每个special_id的统计数据
    private transient ValueState<Map<String, Tuple2<Long, Long>>> statsState;

    // 状态2: 记录下次清空时间
    private transient ValueState<Long> nextClearTimeState;

    // Redis工具类实例
    private transient RedisUtil redisUtil;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化Redis连接
        redisUtil = new RedisUtil(
                ModelUtil.getConfigValue("redis.new.hist.production.hostname"),
                Integer.parseInt(ModelUtil.getConfigValue("redis.new.hist.production.port")),
                ModelUtil.getConfigValue("redis.new.hist.production.password")
        );

        // 配置状态TTL（24小时）
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(24))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .build();

        // 初始化统计数据状态
        ValueStateDescriptor<Map<String, Tuple2<Long, Long>>> statsDesc =
                new ValueStateDescriptor<>("specialStats",
                        TypeInformation.of(new TypeHint<Map<String, Tuple2<Long, Long>>>() {}));
        statsDesc.enableTimeToLive(ttlConfig);
        statsState = getRuntimeContext().getState(statsDesc);

        // 初始化清空时间状态
        ValueStateDescriptor<Long> clearTimeDesc =
                new ValueStateDescriptor<>("nextClearTime", Types.LONG);
        clearTimeDesc.enableTimeToLive(ttlConfig);
        nextClearTimeState = getRuntimeContext().getState(clearTimeDesc);
    }

    @Override
    public void processElement(Tuple4<String, String, String, Long> value,
                               Context ctx,
                               Collector<Tuple2<String, List<JSONObject>>> out) throws Exception {
        // 初始化清空时间（每天7点）
        if (nextClearTimeState.value() == null) {
            long firstClearTime = calculateNextClearTime(System.currentTimeMillis());
            nextClearTimeState.update(firstClearTime);
        }

        // 获取当前状态
        Map<String, Tuple2<Long, Long>> currentStats = statsState.value();
        if (currentStats == null) {
            currentStats = new HashMap<>();
        }

        // 更新统计数据
        String specialId = value.f1;
        String type = value.f2;
        Long count = value.f3;

        Tuple2<Long, Long> current = currentStats.getOrDefault(specialId, Tuple2.of(0L, 0L));

        if ("exposure".equals(type)) {
            current.f0 += count;
        } else if ("click".equals(type)) {
            current.f1 += count;
        }

        currentStats.put(specialId, current);
        statsState.update(currentStats);

        // 注册处理时间窗口触发
        ctx.timerService().registerProcessingTimeTimer(
                ((ctx.timerService().currentProcessingTime() / 10_000) + 1) * 10_000);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx,
                        Collector<Tuple2<String, List<JSONObject>>> out) throws Exception {
        // 检查是否到达清空时间
        Long nextClearTime = nextClearTimeState.value();
        if (timestamp >= nextClearTime) {
            // 清空状态并重置时间
            statsState.clear();
            long newClearTime = calculateNextClearTime(timestamp);
            nextClearTimeState.update(newClearTime);
            logger.info("Cleared state at {}", formatTimestamp(timestamp));
        }

        // 获取当前状态
        Map<String, Tuple2<Long, Long>> currentStats = statsState.value();
        if (currentStats == null || currentStats.isEmpty()) {
            return;
        }

        // 构建输出结果
        List<JSONObject> result = new ArrayList<>();
        for (Map.Entry<String, Tuple2<Long, Long>> entry : currentStats.entrySet()) {
            JSONObject stat = new JSONObject();
            stat.put("special_id", entry.getKey());
            stat.put("exposure", entry.getValue().f0);
            stat.put("click", entry.getValue().f1);
            result.add(stat);
        }

        // 输出并写入Redis
        String userId = ctx.getCurrentKey();
        out.collect(Tuple2.of(userId, result));

        // 直接写入Redis（仿照AdsFirstPageUserExposure）
        try {
            if("13885012".equalsIgnoreCase(userId) || "8004455".equalsIgnoreCase(userId)){
                logger.info("用户:{},统计结果：{}",userId,result);
            }
            redisUtil.setHash(
                    Integer.parseInt(ModelUtil.getConfigValue("redis.hist.production.db")),
                    ModelUtil.getConfigValue("redis.hist.production.tbn"),
                    userId,
                    JSON.toJSONString(result)
            );
        } catch (Exception e) {
            logger.error("Redis写入失败: {}", e.getMessage());
        }
    }

    // 计算下一个清空时间（北京时间7点）
    private long calculateNextClearTime(long currentTimestamp) {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"));
        cal.setTimeInMillis(currentTimestamp);

        // 设置为当天7点
        cal.set(Calendar.HOUR_OF_DAY, 7);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        // 如果当前时间已过7点，设置为次日7点
        if (cal.getTimeInMillis() <= currentTimestamp) {
            cal.add(Calendar.DATE, 1);
        }
        return cal.getTimeInMillis();
    }

    // 时间格式化方法
    private String formatTimestamp(long timestamp) {
        return DateFormatUtils.format(timestamp, "yyyy-MM-dd HH:mm:ss",
                TimeZone.getTimeZone("Asia/Shanghai"));
    }
}
