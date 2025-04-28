package com.yishou.bigdata;

import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.operate.UserSpecialStatsFunction;
import com.yishou.bigdata.realtime.dw.common.basic.BasicApp;
import com.yishou.bigdata.realtime.dw.common.utils.ModelUtil;
import com.yishou.bigdata.realtime.dw.common.utils.RedisUtil;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import java.text.ParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * 专场用户的曝光点击
 *
 */
public class UserExposureClickApp extends BasicApp {
    static Logger logger = LoggerFactory.getLogger(UserExposureClickApp.class);
    public static void main( String[] args ) throws Exception {
        // 创建对象
        final UserExposureClickApp userExposureClickApp = new UserExposureClickApp();
        // 解析参数获取配置
        final Map<String, String> configMap = ModelUtil.analysisConfig(args);
        final String applicationName = "UserExposureClickApp";

        // 初始化
        userExposureClickApp.init(configMap, applicationName, "special_exposure","special_exposure_click");
        // 处理
        userExposureClickApp.process(configMap);
        // 启动调度
        userExposureClickApp.execute(applicationName);
    }

    @Override
    public void process(Map<String, String> configMap) {
        // 曝光流：过滤出包含 special_id 的日志
        DataStream<JSONObject> exposureStream = eventDataStreamMap.get("special_exposure")
            .filter(new FilterFunction<JSONObject>() {
                @Override
                public boolean filter(JSONObject jsonObject) throws Exception {
                    try {
                        String special_id = jsonObject.getString("special_id");
                        String user_id = jsonObject.getString("user_id");
                        Integer index = jsonObject.getInteger("index");
                        // 对第一坑做特殊处理
                        if (index > 1) {
                            return special_id != null && !special_id.isEmpty() && user_id != null && !user_id.isEmpty();
                        }
                    } catch (Exception e) {
                        logger.info("获取曝光数据错误异常: {}日志信息: {}", e, jsonObject.toString());
                    }
                    return false;
                }
            }).name("filter_ex")
            .map(json -> {
                JSONObject obj = new JSONObject();
                obj.put("user_id", json.getString("user_id"));
                obj.put("special_id", json.getString("special_id"));
                // 标记类型
                obj.put("type", "exposure");
                return obj;
            }).name("map_ex");
        DataStream<JSONObject> clickStream = eventDataStreamMap.get("special_exposure_click")
            .filter(new FilterFunction<JSONObject>() {
                @Override
                public boolean filter(JSONObject jsonObject) throws Exception {
                    try {
                        String special_id = jsonObject.getString("special_id");
                        String user_id = jsonObject.getString("user_id");
                        return special_id != null && !special_id.isEmpty() && user_id != null && !user_id.isEmpty();
                    } catch (Exception e) {
                        logger.info("获取点击数据错误异常: {}日志信息: {}", e, jsonObject.toString());
                        return false;
                    }
                }
            }).name("filter_ck")
            .map(json -> {
                JSONObject obj = new JSONObject();
                obj.put("user_id", json.getString("user_id"));
                obj.put("special_id", json.getString("special_id"));
                // 标记类型
                obj.put("type", "click");
                return obj;
            }).name("map_ck");

        DataStream<JSONObject> mergedStream = exposureStream.union(clickStream);

        // 按(user_id,special_id, type)分组，统计次数
        // 合并流并进行窗口统计
        DataStream<Tuple4<String, String, String, Long>> typeCounts = mergedStream
                .keyBy(json -> Tuple3.of(
                        json.getString("user_id"),
                        json.getString("special_id"),
                        json.getString("type")
                ), Types.TUPLE(Types.STRING, Types.STRING, Types.STRING))
                .window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .aggregate(new DistinctUserAggregate(), new CountProcessWindowFunction()).name("union_windows_agg");

        // 用户维度累计统计
        DataStream<Tuple2<String, List<JSONObject>>> statsStream = typeCounts
                .keyBy(t -> t.f0) // 按user_id分组
                .process(new UserSpecialStatsFunction()).name("final_user_count");

    }

    // 去重用户聚合函数
    private static class DistinctUserAggregate implements AggregateFunction<JSONObject, Tuple2<Long, HashSet<String>>, Long> {
        @Override
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            return Tuple2.of(0L, new HashSet<>());
        }

        @Override
        public Tuple2<Long, HashSet<String>> add(JSONObject value, Tuple2<Long, HashSet<String>> accumulator) {
            String userId = value.getString("user_id");
            if (!accumulator.f1.contains(userId)) {
                accumulator.f1.add(userId);
                return Tuple2.of(accumulator.f0 + 1, accumulator.f1);
            }
            return accumulator;
        }

        @Override
        public Long getResult(Tuple2<Long, HashSet<String>> accumulator) {
            return accumulator.f0;
        }

        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> a, Tuple2<Long, HashSet<String>> b) {
            HashSet<String> mergedSet = new HashSet<>(a.f1);
            mergedSet.addAll(b.f1);
            return Tuple2.of((long) mergedSet.size(), mergedSet);
        }
    }

    // 修改后的窗口处理函数
    private static class CountProcessWindowFunction extends ProcessWindowFunction<Long,
            Tuple4<String, String, String, Long>,
            Tuple3<String, String, String>,
            TimeWindow> {

        @Override
        public void process(Tuple3<String, String, String> key,
                            Context context,
                            Iterable<Long> elements,
                            Collector<Tuple4<String, String, String, Long>> out) {

            Long count = elements.iterator().next();
            // 输出结构: (用户ID, 专场ID, 事件类型, 去重后次数)
            out.collect(Tuple4.of(key.f0, key.f1, key.f2, count));
        }
    }

    /**
     * 每天7点清理一次数据redis上的数据
     *
     * @throws ParseException
     */
    @Override
    protected void schedulerRun() throws ParseException {
        // 初始化 Redis 工具类
        RedisUtil newRedisUtil = new RedisUtil(
                ModelUtil.getConfigValue("redis.new.hist.production.hostname"),
                Integer.parseInt(ModelUtil.getConfigValue("redis.new.hist.production.port")),
                ModelUtil.getConfigValue("redis.new.hist.production.password")
        );

        // 获取 Redis 数据库索引和键模式
        int dbIndex = Integer.parseInt(ModelUtil.getConfigValue("redis.hist.production.db"));
        String hashKey = ModelUtil.getConfigValue("redis.hist.production.tbn");

        // 使用 try-with-resources 语句自动管理 Jedis 资源
        try (Jedis jedis = newRedisUtil.getJedis(dbIndex)) {
            jedis.sendCommand(Protocol.Command.UNLINK, hashKey);
        } catch (Exception e) {
            logger.error("发生异常: ", e);
        }
    }
}
