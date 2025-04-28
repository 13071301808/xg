package com.yishou.bigdata.operator;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.realtime.dw.common.utils.AlarmUtil;
import com.yishou.bigdata.realtime.dw.common.utils.ModelUtil;
import com.yishou.bigdata.realtime.dw.common.utils.MySQLUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MonitorAppMap extends RichFlatMapFunction<JSONObject, JSONObject> {
    public static Logger logger = LoggerFactory.getLogger(MonitorAppMap.class);

    private JdbcTemplate JdbcTemplate;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JdbcTemplate = new MySQLUtil(
                ModelUtil.getConfigValue("mysql.yishou.fmdes.url"),
                ModelUtil.getConfigValue("mysql.yishou.fmdes.username"),
                ModelUtil.getConfigValue("mysql.yishou.fmdes.password"),
                10,
                1
        ).getJdbcTemplate();
        logger.info("连接成功");
    }

    @Override
    public void flatMap(JSONObject jsonObject, Collector<JSONObject> out) throws Exception {
        String sql = "SELECT * FROM ys_dgc.alarm_job_py";
        // 查询配置表
        List<Map<String, Object>> result = JdbcTemplate.queryForList(sql);
        for (Map<String, Object> row : result) {
            // 获取配置表信息
            Long job_id = (Long) row.get("job_id");
            String job_name = (String) row.get("job_name");
            String monitor_log_name = (String) row.get("monitor_log_name");
            String monitor_log_database = (String) row.get("monitor_log_database");
            String monitor_event = (String) row.get("monitor_event");
            String monitor_label_str = (String) row.get("monitor_label");
            JSONObject monitor_label = JSON.parseObject(monitor_label_str);
            String monitor_rule = (String) row.get("monitor_rule");
            String warning_robot_url = (String) row.get("warning_robot_url");
            Integer is_alarm = (Integer) row.get("is_alarm");

            // 获取日志数据流信息
            String log_database = jsonObject.getString("log_database");
            String event = jsonObject.getString("event");

            // 判断数据流是否与配置表相符
            if(
                Objects.equals(log_database, monitor_log_database)
                    && event != null && event.equalsIgnoreCase(monitor_event)
                    && monitor_rule != null
            ){
                // 遍历数据结构
                for (String label_key : monitor_label.keySet()) {
                    // 克隆原始 JSON 对象，避免污染原数据
                    JSONObject clonedJson = (JSONObject) jsonObject.clone();
                    // 处理当前标签
                    JSONObject innerObject = monitor_label.getJSONObject(label_key);
                    // 判断是否日志数据匹配到配置表里面的监控字段
                    if(clonedJson.containsKey(label_key)){
                        // 遍历里面的数组
                        for (String rule_key : innerObject.keySet()) {
                            Object rule_value = innerObject.get(rule_key);
                            // 标记是否非空
                            String get_log_field = clonedJson.getString(label_key);
                            // 判断配置表中的监控规则
                            if (monitor_rule.contains("a") && Objects.equals(rule_key, "is_not_null")) {
                                // 往输出的日志流数据里面加对应的字段
                                clonedJson.put("is_not_null_job_id", job_id);
                                clonedJson.put("is_not_null_value", rule_value);
                                clonedJson.put("is_not_null_label", label_key);
                                clonedJson.put("is_not_null_alarm", is_alarm);
                                // 标记是否非空
//                                String get_log_field = clonedJson.getString(label_key);
                                clonedJson.put("is_not_null", get_log_field != null ? "true" : "false");
                            }
                            if (monitor_rule.contains("b") && Objects.equals(rule_key, "type_right")) {
                                if (rule_value == "string") {
                                    if (!isString(get_log_field) && is_alarm == 1) {
                                        // 定义具体告警信息
                                        String alarm_info = String.format("实际数据格式不为String,值为%s", get_log_field);
                                        String realtime_rule = "是否为空";
                                        String realtime_label = label_key;
                                        // 开始定义飞书告警
                                        sendAlarm(
                                            job_name, monitor_log_name, monitor_log_database, monitor_event,
                                                realtime_rule, realtime_label, alarm_info, warning_robot_url
                                        );
                                    }
                                } else if (rule_value == "double") {
                                    if (!isDouble(get_log_field) && is_alarm == 1) {
                                        // 定义具体告警信息
                                        String alarm_info = String.format("实际数据格式不为Double,值为%s", get_log_field);
                                        String realtime_rule = "是否为空";
                                        String realtime_label = label_key;
                                        // 开始定义飞书告警
                                        sendAlarm(
                                            job_name, monitor_log_name, monitor_log_database, monitor_event,
                                                realtime_rule, realtime_label, alarm_info, warning_robot_url
                                        );
                                    }
                                } else if (rule_value == "json") {
                                    if (!isJSONArray(get_log_field) && is_alarm == 1) {
                                        // 定义具体告警信息
                                        String alarm_info = String.format("实际数据格式不为json,值为%s", get_log_field);
                                        String realtime_rule = "是否为空";
                                        String realtime_label = label_key;
                                        // 开始定义飞书告警
                                        sendAlarm(
                                            job_name, monitor_log_name, monitor_log_database, monitor_event,
                                                realtime_rule, realtime_label, alarm_info, warning_robot_url
                                        );
                                    }
                                }
                            }
                        }
                        // 输出独立事件
                        out.collect(clonedJson);
                    }
                }
            }
        }
    }

    // 校验字段是否为 JSON 数组格式
    private boolean isJSONArray(String field) {
        try {
            JSONArray.parseArray(field);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // 校验字段是否为 字符串 格式
    private boolean isString(Object field) {
        return field instanceof String;
    }

    // 校验字段是否为 浮点数 格式
    private boolean isDouble(Object field) {
        return field instanceof Double;
    }

    // 飞书告警调用
    private JSONObject sendAlarm(String job_name, String monitor_log_name, String monitor_log_database, String monitor_event,
                                 String realtime_rule,String realtime_label,String alarm_info,String warning_robot_url)
            throws IOException {
        AlarmUtil alarm = new AlarmUtil(job_name);
        alarm.setColor("red");
        alarm.setAlarmField("日志组名称", monitor_log_name);
        alarm.setAlarmField("日志库", monitor_log_database);
        alarm.setAlarmField("告警事件", monitor_event);
        alarm.setAlarmField("告警规则", realtime_rule);
        alarm.setAlarmField("告警字段", realtime_label);
        alarm.setAlarmField("具体告警信息", alarm_info);
        alarm.setAlarmUrl(warning_robot_url);
        alarm.sendFetshuQIYU(alarm.buildAlarm());
        logger.info("告警发送到七鱼成功,{}", alarm.buildAlarm());
        return alarm.buildAlarm();
    }
}
