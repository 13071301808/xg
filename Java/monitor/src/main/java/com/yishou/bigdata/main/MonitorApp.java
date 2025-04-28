package com.yishou.bigdata.main;


import java.text.DecimalFormat;
import java.util.*;
import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.operator.*;

import com.yishou.bigdata.realtime.dw.common.utils.ModelUtil;
import org.apache.flink.api.common.functions.FilterFunction;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 实时埋点告警
 */
public class MonitorApp extends BasicInit{
    // 配置对应类日志
    static Logger logger = LoggerFactory.getLogger(MonitorApp.class);

    public static void main( String[] args ) throws Exception {
        // 创建对象
        final MonitorApp app = new MonitorApp();
        // 解析参数获取配置
        final Map<String, String> configMap = ModelUtil.analysisConfig(args);
        final String applicationName = "MonitorApp";

        // 初始化
        app.init(configMap, applicationName, "all_log_data");
        // 处理
        app.process(configMap);
        // 启动调度
        app.execute(applicationName);
    }

    public void process(Map<String, String> configMap) {
        DataStream<JSONObject> eventDataStream = eventDataStreamMap.get("all_log_data");
        // 通用过滤，将连用户 id 都没有的异常记录过滤掉
        eventDataStream.filter(new MonitorAppFliter()).name("filter_event")
        // 提取需要的字段
        .flatMap(new MonitorAppMap()).name("map_event")
        // 对要计算非空率的日志数据做二次过滤
        .filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject json) throws Exception {
                return json.containsKey("is_not_null_value") && json.containsKey("is_not_null_label")
                        && json.getString("event") != null && json.getString("log_database") != null;
            }
        }).name("filter_is_not_null_data")
        // 开窗统计
        .keyBy(new KeySelector<JSONObject, Tuple6<Long,String, String, String,String,Integer>>() {
            @Override
            public Tuple6<Long,String, String, String,String,Integer> getKey(JSONObject jsonObject) {
                Long is_not_null_job_id = jsonObject.getLong("is_not_null_job_id");
                String logDatabase = jsonObject.getString("log_database");
                String event = jsonObject.getString("event");
                String is_not_null_label = jsonObject.getString("is_not_null_label");
                String is_not_null_value = jsonObject.getString("is_not_null_value");
                Integer is_not_null_alarm = jsonObject.getInteger("is_not_null_alarm");
                return new Tuple6<>(is_not_null_job_id,logDatabase, event, is_not_null_label,is_not_null_value,is_not_null_alarm);
            }
        }).window(TumblingProcessingTimeWindows.of(Time.minutes(10)))
                .aggregate(new CountNotNullAggregator(), new ResultWindowFunction())
                .name("windows_event")
                .addSink(new MonitorAppSink())
                .name("sink_log")
        ;
    }

    // 调整窗口函数接收
    public static class ResultWindowFunction
            extends ProcessWindowFunction<Tuple2<Integer, Integer>, JSONObject, Tuple6<Long,String, String, String,String,Integer>, TimeWindow> {
        @Override
        public void process(
                Tuple6<Long,String, String, String,String,Integer> key, Context context, Iterable<Tuple2<Integer, Integer>> counts, Collector<JSONObject> out
        ) {
            Tuple2<Integer, Integer> countPair = counts.iterator().next();
            JSONObject result = new JSONObject();
            result.put("is_not_null_job_id", key.f0);
            result.put("log_database", key.f1);
            result.put("event", key.f2);
            result.put("is_not_null_label", key.f3);
            result.put("is_not_null_value", key.f4);
            result.put("is_not_null_alarm", key.f5);
            // true 的数量
            result.put("true_count", countPair.f0);
            // false 的数量
            result.put("false_count", countPair.f1);
            result.put("window_end", context.window().getEnd());
            // 计算非空率
            // 创建一个格式化对象，指定保留两位小数
            DecimalFormat df = new DecimalFormat("#.00");
            double notNullRate1 = (countPair.f1 + countPair.f0) > 0 ? ((double) countPair.f0 / (countPair.f1 + countPair.f0)) : 0.0;  // 处理除零情况
            double notNullRate = Double.parseDouble(df.format(notNullRate1));

            result.put("not_null_rate", notNullRate);
            out.collect(result);
        }
    }

}

