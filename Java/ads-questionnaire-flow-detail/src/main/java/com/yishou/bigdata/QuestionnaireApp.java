package com.yishou.bigdata;

import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.realtime.dw.common.utils.MySQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.yishou.bigdata.realtime.dw.common.basic.BasicApp;
import com.yishou.bigdata.realtime.dw.common.utils.ModelUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Map;

/**
 * Hello world!
 *
 */
public class QuestionnaireApp extends BasicApp {
    // 配置对应类日志
    static Logger logger = LoggerFactory.getLogger(QuestionnaireApp.class);
    public static void main( String[] args ) throws Exception{
        // 创建对象
        final QuestionnaireApp app = new QuestionnaireApp();
        // 解析参数获取配置
        final Map<String, String> configMap = ModelUtil.analysisConfig(args);
        final String applicationName = "QuestionnaireApp";

        // 初始化
        app.init(
                configMap,
                applicationName,
                "app_goods_exposure_questionnaire","h5_goods_exposure_questionnaire"
        );
        // 处理
        app.process(configMap);
        // 启动调度
        app.execute(applicationName);
    }

    public void process(Map<String, String> configMap) {
        DataStream<JSONObject> eventDataStream_1 = eventDataStreamMap.get("app_goods_exposure_questionnaire");
        DataStream<JSONObject> eventDataStream_2 = eventDataStreamMap.get("h5_goods_exposure_questionnaire");
        DataStream<JSONObject> eventDataStream = eventDataStream_1.union(eventDataStream_2);

        // 通用过滤，将连用户 id 都没有的异常记录过滤掉
        eventDataStream.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                try {
                    final String user_id = jsonObject.getString("user_id");
                    final String page_type = jsonObject.getString("page_type");
                    final String question_id = jsonObject.getString("question_id");
                    final String question_name = jsonObject.getString("question_name");

                    return page_type != null && !page_type.isEmpty()
                            && user_id != null && !user_id.isEmpty()
                            && question_id != null && !question_id.isEmpty()
                            && question_name != null && !question_name.isEmpty();
                } catch (Exception e) {
                    logger.info("获取数据错误异常: {}日志信息: {}", e, jsonObject.toString());
                }
                return false;
            }
        }).name("filter")
        .addSink(new RichSinkFunction<JSONObject>() {
            private JdbcTemplate JdbcTemplate;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                JdbcTemplate = new MySQLUtil(
                        ModelUtil.getConfigValue("mysql.yishou.ob.url"),
                        ModelUtil.getConfigValue("mysql.yishou.ob.username"),
                        ModelUtil.getConfigValue("mysql.yishou.ob.password"),
                        10,
                        1
                ).getJdbcTemplate();
                logger.info("sink连接成功");
            }

            @Override
            public void invoke(JSONObject result, Context context) {
                String user_id = result.getString("user_id");
                String page_type = result.getString("page_type");
                String question_id = result.getString("question_id");
                String question_name = result.getString("question_name");

                // 实时插后端
                String sql = "INSERT INTO dw_yishou_data.data_questionnaire_exposure_test (user_id, page_type, questionnaire_id, questionnaire_title) VALUES (?, ?, ?, ?)";
                JdbcTemplate.update(sql,user_id,page_type,question_id,question_name);
            }
        });
    }
}
