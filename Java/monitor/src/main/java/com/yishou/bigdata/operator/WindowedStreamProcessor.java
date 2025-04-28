package com.yishou.bigdata.operator;
import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.realtime.dw.common.utils.DateUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.yishou.bigdata.realtime.dw.common.utils.AlarmUtil;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class WindowedStreamProcessor {
    static Logger logger = LoggerFactory.getLogger(WindowedStreamProcessor.class);
    public void processWindowedStream(Map<String, DataStream<JSONObject>> eventDataStreamMap, String monitor_event, String monitor_label, String monitor_rule, String job_name, String monitor_log_name, String monitor_log_database, String warning_robot_url) {
        try {
            // 实时日志时间段监控（一段时间内的数据指标监控）
            DataStream<JSONObject> windowseventDataStream = eventDataStreamMap.get(monitor_event)
                    // 通用过滤，将连用户 id 都没有的异常记录过滤掉
                    .filter(new FilterFunction<JSONObject>() {
                        @Override
                        public boolean filter(JSONObject jsonObject) throws Exception {
                            try {
                                final String user_id = jsonObject.getString("user_id");
                                return user_id != null && !"".equalsIgnoreCase(user_id);
                            } catch (Exception e) {
                                return false;
                            }
                        }
                    });

            // 当前的数据量监控
            WindowedStream<JSONObject, String, TimeWindow> currentWindowedStream = windowseventDataStream
                    .keyBy(jsonObject -> {
                        // 将 13 位时间搓转为 yyyymmdd 格式
                        Long receive_time = null;
                        String receiveTimeStr = jsonObject.getString("receiveTime");
                        String receive_time_str = jsonObject.getString("receive_time");
                        if (receiveTimeStr != null) {
                            receive_time = Long.parseLong(receiveTimeStr);
                        } else if (receive_time_str != null) {
                            receive_time = Long.parseLong(receive_time_str);
                        }
                        logger.info("开始获取当前窗口：{}", receive_time);
                        return receive_time != null ? DateUtil.millisecondToDate(receive_time) : null;
                    })
                    .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                    .trigger(ContinuousProcessingTimeTrigger.of(Time.minutes(2)));
            // 当前小时的数据量监控处理
            currentWindowedStream.process(new ProcessWindowFunction<JSONObject, String, String, TimeWindow>() {
                @Override
                public void process(String key, Context context, Iterable<JSONObject> elements, Collector<String> out) throws Exception {
                    // 统计去重后的字段
                    Set<String> uniqueUserIds = new HashSet<>();
                    for (JSONObject element : elements) {
                        String userId = element.getString(monitor_label);
                        if (userId != null) {
                            uniqueUserIds.add(userId);
                        }
                    }
                    long currentUniqueUserCount = uniqueUserIds.size();
                    logger.info("处理当前窗口：{}", currentUniqueUserCount);
                    context.windowState().getState(new ValueStateDescriptor<>("currentUniqueUserCount", Long.class)).update(currentUniqueUserCount);
                }
            });

            // 上个小时的数据量监控
            WindowedStream<JSONObject, String, TimeWindow> previousWindowedStream = windowseventDataStream
                    .keyBy(jsonObject -> {
                        // 将 13 位时间搓转为 yyyymmdd 格式
                        Long receive_time = null;
                        String receiveTimeStr = jsonObject.getString("receiveTime");
                        String receive_time_str = jsonObject.getString("receive_time");
                        if (receiveTimeStr != null) {
                            receive_time = Long.parseLong(receiveTimeStr);
                        } else if (receive_time_str != null) {
                            receive_time = Long.parseLong(receive_time_str);
                        }
                        logger.info("开始获取历史窗口：{}", receive_time);
                        return receive_time != null ? DateUtil.millisecondToDate(receive_time - 120000) : null;
                    })
                    .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                    .trigger(ContinuousProcessingTimeTrigger.of(Time.minutes(2)));

            // 上个小时的数据量监控处理
            previousWindowedStream.process(new ProcessWindowFunction<JSONObject, String, String, TimeWindow>() {
                @Override
                public void process(String key, Context context, Iterable<JSONObject> elements, Collector<String> out) throws Exception {
                    // 统计去重后的字段
                    Set<String> uniqueUserIds = new HashSet<>();
                    for (JSONObject element : elements) {
                        String userId = element.getString(monitor_label);
                        if (userId != null) {
                            uniqueUserIds.add(userId);
                        }
                    }
                    long previousUniqueUserCount = uniqueUserIds.size();
                    logger.info("处理历史窗口：{}", previousUniqueUserCount);
                    context.windowState().getState(new ValueStateDescriptor<>("previousUniqueUserCount", Long.class)).update(previousUniqueUserCount);
                }
            });

            // 开始对比两个窗口的结果判断
            currentWindowedStream.process(new ProcessWindowFunction<JSONObject, String, String, TimeWindow>() {
                @Override
                public void process(String key, Context context, Iterable<JSONObject> elements, Collector<String> out) throws Exception {
                    ValueStateDescriptor<Long> currentDescriptor = new ValueStateDescriptor<>("currentUniqueUserCount", Long.class);
                    ValueStateDescriptor<Long> previousDescriptor = new ValueStateDescriptor<>("previousUniqueUserCount", Long.class);
                    Long currentUniqueUserCount = context.windowState().getState(currentDescriptor).value();
                    Long previousUniqueUserCount = context.windowState().getState(previousDescriptor).value();

                    if (currentUniqueUserCount == null) {
                        currentUniqueUserCount = 0L;
                    }
                    if (previousUniqueUserCount == null) {
                        previousUniqueUserCount = 0L;
                    }
                    logger.info("两个窗口开始对比");
                    logger.info("监控规则：{}", monitor_rule);
                    logger.info("currentUniqueUserCount: {}", currentUniqueUserCount);
                    logger.info("previousUniqueUserCount: {}", previousUniqueUserCount);
                    // 判断数据量变化
                    if (currentUniqueUserCount < previousUniqueUserCount / 5) {
                        logger.info("去重后的数据量突减,监控字段为：{}\n 目前该小时内数据量为：{}\n 上个小时的数据量为：{}", key, currentUniqueUserCount, previousUniqueUserCount);
                        // 发送飞书告警
                        // sendAlarm(job_name, monitor_log_name, monitor_log_database, monitor_event, monitor_label, monitor_rule);
                    } else {
                        logger.info("数量没有问题,监控字段为：{}\n 目前该小时内数据量为：{}\n 上个小时的数据量为：{}", key, currentUniqueUserCount, previousUniqueUserCount);
                        // 发送飞书告警
                        sendAlarm(job_name, monitor_log_name, monitor_log_database, monitor_event, monitor_label, monitor_rule, warning_robot_url);
                    }
                }
            });
        } catch (Exception e) {
            logger.error("报错信息：{}", String.valueOf(e));
        }
    }

    // 飞书告警调用
    private JSONObject sendAlarm(String job_name, String monitor_log_name, String monitor_log_database, String monitor_event, String monitor_label, String monitor_rule, String warning_robot_url)
            throws IOException {
        AlarmUtil alarm = new AlarmUtil(job_name);
        alarm.setColor("red");
        alarm.setAlarmField("日志组名称", monitor_log_name);
        alarm.setAlarmField("日志库", monitor_log_database);
        alarm.setAlarmField("告警事件", monitor_event);
        alarm.setAlarmField("告警字段", monitor_label);
        alarm.setAlarmField("告警规则", monitor_rule);
        alarm.setAlarmUrl(warning_robot_url);
        alarm.sendFetshuQIYU(alarm.buildAlarm());
        logger.info("告警发送到七鱼成功,{}", alarm.buildAlarm());
        return alarm.buildAlarm();
    }
}
