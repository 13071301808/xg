package com.yishou.bigdata.operator;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.realtime.dw.common.utils.AlarmUtil;

import com.yishou.bigdata.realtime.dw.common.utils.ModelUtil;
import com.yishou.bigdata.realtime.dw.common.utils.MySQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.jdbc.core.JdbcTemplate;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MonitorAppSink extends RichSinkFunction<JSONObject> {
    public static Logger logger = LoggerFactory.getLogger(MonitorAppSink.class);

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
        logger.info("sink连接成功");
    }

    @Override
    public void invoke(JSONObject result, Context context) throws IOException {
        // 获取计算汇总
        Long is_not_null_job_id = (Long) result.get("is_not_null_job_id");
        String log_database = result.getString("log_database");
        String event = result.getString("event");
        String is_not_null_label = result.getString("is_not_null_label");
        Double is_not_null_value = result.getDouble("is_not_null_value");
        Double not_null_rate = result.getDouble("not_null_rate");
        Integer is_not_null_alarm = result.getInteger("is_not_null_alarm");

        if(not_null_rate < is_not_null_value && is_not_null_alarm == 1){

            logger.info("触发告警，开始写入");
            // 正常写入监控记录表
            String monitor_field_label = "is_not_null";
            Integer is_alarm_happen = 1;
            insertMonitorRecord(
                is_not_null_job_id, log_database, event, is_not_null_label, monitor_field_label, String.valueOf(not_null_rate),is_alarm_happen
            );

            // 触发告警
            String alarm_sql = "select * from ys_dgc.alarm_job_py where job_id = ?";
            List<Map<String, Object>> alarm_result = JdbcTemplate.queryForList(alarm_sql,is_not_null_job_id);
            String job_name = alarm_result.get(0).get("job_name").toString();
            String monitor_log_name = alarm_result.get(0).get("monitor_log_name").toString();
            String monitor_log_database = alarm_result.get(0).get("monitor_log_database").toString();
            String monitor_event = alarm_result.get(0).get("monitor_event").toString();
            String monitor_rule = alarm_result.get(0).get("monitor_rule").toString();
            String alarm_info = String.format("发现数据缺失异常，请排查确认,对应job_id:%s", is_not_null_job_id);
            String warning_robot_url = alarm_result.get(0).get("warning_robot_url").toString();
            // 构建告警信息到自主取数服务器
            sendAlarm(job_name,monitor_log_name,monitor_log_database,monitor_event,monitor_rule,is_not_null_label,alarm_info,warning_robot_url);

        }else{
            logger.info("开始写入");
            // 正常写入监控记录表
            String monitor_field_label = "is_not_null";
            Integer is_alarm_happen = 0;
            insertMonitorRecord(
                is_not_null_job_id, log_database, event, is_not_null_label, monitor_field_label, String.valueOf(not_null_rate),is_alarm_happen
            );
        }

    }

    // 封装插入逻辑
    private void insertMonitorRecord(
        Long job_id, String monitor_log_database, String event, String monitor_field, String monitor_field_label, String monitor_field_value,Integer is_alarm_happen
    ) {
        try {
            String insertSql = "INSERT INTO ys_dgc.alarm_job_info_py("
                    +"job_id, monitor_log_database, monitor_event, monitor_field, monitor_field_label, monitor_field_value,is_alarm_happen)"
                    +"VALUES (?, ?, ?, ?, ?, ?,?)";
            JdbcTemplate.update(
                insertSql, job_id, monitor_log_database, event,monitor_field, monitor_field_label, monitor_field_value,is_alarm_happen
            );
            logger.info("成功插入监控记录,job_id为:{}",job_id);
        } catch (Exception e) {
            logger.error("插入监控记录时发生异常: {}", e.getMessage(), e);
        }
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
