package com.yishou.bigdata.test;

import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.operator.BasicInit;
import com.yishou.bigdata.realtime.dw.common.utils.ModelUtil;
import com.yishou.bigdata.realtime.dw.common.utils.MySQLUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;
import java.util.Objects;


public class MonitorAppTest extends BasicInit {
    // 配置对应类日志
    static Logger logger = LoggerFactory.getLogger(MonitorAppTest.class);

    public static void main( String[] args ) throws Exception {
        // 创建对象
        final MonitorAppTest app = new MonitorAppTest();
        // 解析参数获取配置
        final Map<String, String> configMap = ModelUtil.analysisConfig(args);
        final String applicationName = "MonitorAppTest";

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
        eventDataStream.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                try {
                    final String user_id = jsonObject.getString("user_id");
                    return user_id != null && !"".equalsIgnoreCase(user_id);
                } catch (Exception e) {
                    return false;
                }
            }
        }).name("filter_event");
        // sink,输出计算结果
        eventDataStream.addSink(new RichSinkFunction<JSONObject>() {
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
            public void invoke(JSONObject value, Context context) throws Exception {
                String sql = "SELECT * FROM ys_dgc.alarm_job_py WHERE is_alarm = 1";
                // 查询配置表
                List<Map<String, Object>> result = JdbcTemplate.queryForList(sql);

                for (Map<String, Object> row : result) {
                    // 获取配置表信息
                    String job_name = (String) row.get("job_name");
                    String monitor_log_name = (String) row.get("monitor_log_name");
                    String monitor_log_database = (String) row.get("monitor_log_database");
                    String monitor_event = (String) row.get("monitor_event");
                    String monitor_label = (String) row.get("monitor_label");
                    String monitor_rule = (String) row.get("monitor_rule");
                    String warn_robot_url = (String) row.get("monitor_rule");

                    // 获取日志数据流信息
                    String log_database = value.getString("log_database");
                    String event = value.getString("event");

                    // 判断数据流是否与配置表相符
                    if(Objects.equals(log_database, monitor_log_database)){
                        // 判断事件
                        if (Objects.equals(event, monitor_event)){
                            logger.info("获取的事件：{}",event);
                        }
                    }
                }
            }

        }).name("sink_log");
    }
}
