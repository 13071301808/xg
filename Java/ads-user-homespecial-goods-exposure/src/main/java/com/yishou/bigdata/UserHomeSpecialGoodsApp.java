package com.yishou.bigdata;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.realtime.dw.common.basic.BasicApp;
import com.yishou.bigdata.realtime.dw.common.utils.ModelUtil;
import com.yishou.bigdata.realtime.dw.common.utils.MySQLUtil;
import com.yishou.bigdata.realtime.dw.common.utils.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.springframework.jdbc.core.JdbcTemplate;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Hello world!
 *
 */
public class UserHomeSpecialGoodsApp extends BasicApp{
    static Logger logger = LoggerFactory.getLogger(UserHomeSpecialGoodsApp.class);
    public static void main( String[] args ) throws Exception{
        // 创建对象
        final UserHomeSpecialGoodsApp userHomeSpecialGoodsApp = new UserHomeSpecialGoodsApp();

        // 解析参数获取配置
        final Map<String, String> configMap = ModelUtil.analysisConfig(args);
        final String applicationName = "UserHomeSpecialGoodsApp";
        userHomeSpecialGoodsApp.init(configMap, applicationName, "special_exposure");
        // 业务处理
        userHomeSpecialGoodsApp.process(configMap);
        // 启动执行
        userHomeSpecialGoodsApp.execute(applicationName);
    }

    @Override
    public void process(Map<String, String> configMap) {
        DataStream<JSONObject> userHomeSpecialStream = eventDataStreamMap.get("special_exposure")
            .filter(new FilterFunction<JSONObject>() {
                @Override
                public boolean filter(JSONObject jsonObject) throws Exception {
                    try {
                        String goods_id = jsonObject.getString("goods_id");
                        String user_id = jsonObject.getString("user_id");
                        return user_id != null && !"".equalsIgnoreCase(user_id) && goods_id != null && !"".equalsIgnoreCase(goods_id);
                    } catch (Exception e) {
                        logger.info("错误异常: {}日志信息: {}", e, jsonObject.toString());
                        return false;
                    }
                }
            }).name("filter")
            .keyBy(record -> record.getString("user_id"))
            // 设置窗口类型,每天7点到第二天7点为一个窗口
            .window(TumblingProcessingTimeWindows.of(
                    org.apache.flink.streaming.api.windowing.time.Time.seconds(10)
            ))
            // 应用自定义的处理函数
            .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                ValueState<List<Integer>> goodsNoListState = null;
                ValueState<Long> StateLastUpdateTime = null;
                private JdbcTemplate JdbcTemplate;
                @Override
                public void open(Configuration parameters) throws Exception {
                    JdbcTemplate = new MySQLUtil(
                            ModelUtil.getConfigValue("mysql.yishou.r7.url"),
                            ModelUtil.getConfigValue("mysql.yishou.r7.username"),
                            ModelUtil.getConfigValue("mysql.yishou.r7.password"),
                            10,
                            1
                    ).getJdbcTemplate();
                    logger.info("连接成功");

                    // goods_no状态创建
                    StateTtlConfig stateTtlConfig = StateTtlConfig
                            .newBuilder(Time.hours(24))
                            .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                            .build();
                    ValueStateDescriptor<List<Integer>> goodsNoListStateDesc = new ValueStateDescriptor<>("goodsNoListState", new ListTypeInfo<>(Integer.class));
                    goodsNoListStateDesc.enableTimeToLive(stateTtlConfig);
                    goodsNoListState = getRuntimeContext().getState(goodsNoListStateDesc);
                    StateLastUpdateTime = getRuntimeContext().getState(new ValueStateDescriptor<>("window_end_time", Types.LONG));
                    super.open(parameters);
                }

                @Override
                public void process(String userId, Context context, Iterable<JSONObject> iterable, Collector<JSONObject> collector) throws Exception {
                    Long windowEndTime = StateLastUpdateTime.value();
                    if (windowEndTime == null) {
                        StateLastUpdateTime.update(DateUtils.addDays(DateUtils.parseDate(DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd 07:00:00"), "yyyy-MM-dd HH:mm:ss"), 1).getTime());
                        if ("13885012".equalsIgnoreCase(userId)) {
                            logger.info("user_id: " + userId + "设置窗口大小");
                        }
                    } else {
                        if (windowEndTime < context.window().getEnd()) {
                            StateLastUpdateTime.update(DateUtils.addDays(DateUtils.parseDate(DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd 07:00:00"), "yyyy-MM-dd HH:mm:ss"), 1).getTime());
                            goodsNoListState.clear();
                            if ("13885012".equalsIgnoreCase(userId)) {
                                logger.info("user_id: " + userId + "清理历史专场日状态：【{}】", userId);
                            }
                        }
                    }

                    List<Integer> goodsNoList = goodsNoListState.value();
                    // 如果无状态, 创建新列表
                    if (goodsNoList == null) {
                        goodsNoList = new ArrayList<>();
                        if ("13885012".equalsIgnoreCase(userId) || "8004455".equalsIgnoreCase(userId)) {
                            logger.info("user_id: {}, 无状态列表, 新建空列表", userId);
                        }
                    } else {
                        if ("13885012".equalsIgnoreCase(userId) || "8004455".equalsIgnoreCase(userId)) {
                            logger.info("user_id: {}, 存在状态列表: {}", userId, goodsNoList);
                        }
                    }

                    for (JSONObject jsonObject : iterable) {
                        String goods_ids = jsonObject.getString("goods_id");
                        // 使用正则表达式分割（兼容带空格的格式）
                        String[] idArray = goods_ids.split("\\s*,\\s*");
                        // 遍历方式2：增强型for循环
                        for (String goods_id : idArray) {
                            String sql = "select goods_no from yishou.fmys_goods where goods_id = ?";
                            // 查询配置表
                            List<Map<String, Object>> result = JdbcTemplate.queryForList(sql,goods_id);
                            if (result.isEmpty()) {
                                logger.warn("No goods_no found for goods_id: {}", goods_id);
                                continue; // 跳过当前循环
                            }
                            int goods_no = Integer.parseInt(result.get(0).get("goods_no").toString()); // 通用类型转换
                            if (goodsNoList.contains(goods_no)) {
                                // 如果 goodsNo 已经存在，则移除它
                                goodsNoList.remove(Integer.valueOf(goods_no));
                            }
                            // 将 goodsNo 插入到列表的最前面
                            goodsNoList.add(0, goods_no);
                        }
                    }
                    // 更新状态
                    goodsNoListState.update(goodsNoList);

                    // 写入结果
                    List<String> stringList = goodsNoList.stream()
                            .map(Object::toString)
                            .collect(Collectors.toList());

                    String jsonString = JSON.toJSONString(stringList);
                    // 构造输出结果
                    JSONObject output = new JSONObject();
                    output.put("user_id", userId);
                    output.put("goods_list", jsonString);
                    collector.collect(output);
                }
            }).name("process");
        userHomeSpecialStream.addSink(new RichSinkFunction<JSONObject>() {
            private transient RedisUtil redisUtil;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                redisUtil = new RedisUtil(
                        ModelUtil.getConfigValue("redis.new.hist.production.hostname"),
                        Integer.parseInt(ModelUtil.getConfigValue("redis.new.hist.production.port")),
                        ModelUtil.getConfigValue("redis.new.hist.production.password")
                );
            }

            @Override
            public void invoke(JSONObject value, Context context) throws Exception {
                String userId = value.getString("user_id");
                String jsonString = value.getString("goods_list");
                try {
                    redisUtil.setHash(
                            Integer.parseInt(ModelUtil.getConfigValue("redis.hist.production.db")),
                            ModelUtil.getConfigValue("redis.hist.production.tbn").getBytes(),
                            userId.getBytes(),
                            jsonString.getBytes()
                    );
                    if ("13885012".equalsIgnoreCase(userId) || "8004455".equalsIgnoreCase(userId)) {
                        logger.info("user_id: {}, 写入一条数据: {}", userId, jsonString);
                    }
                } catch (RuntimeException e) {
                    logger.error("Redis写入失败，用户ID: {}，数据: {}", userId, jsonString, e);
                }
            }
        }).name("final_sink");
    }

    /**
     * 每天7点清理一次数据redis上的数据click_to_recommend
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


