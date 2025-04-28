package com.yishou.bigdata.realtime.dw.common.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.realtime.dw.common.utils.DateUtil;
import com.yishou.bigdata.realtime.dw.common.utils.EventParseUtil;
import com.yishou.bigdata.realtime.dw.common.utils.ModelUtil;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class H5GoodsExposureQuestionnaireProcess extends ProcessFunction<String, JSONObject>{
    static Logger logger = LoggerFactory.getLogger(H5GoodsExposureQuestionnaireProcess.class);

    @Override
    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        try {
            JSONObject jsonText = JSON.parseObject(value);
            if (EventParseUtil.isEvent(jsonText, "ysh5goodsexposure")) {

                JSONObject data = jsonText.getJSONObject("data");
                if (data != null) {
                    // 数据解析
                    String logSource = jsonText.getString("__client_ip__");
                    String receiveTime = jsonText.getString("__time__");
                    String activity = jsonText.getString("activity");
                    String user_id = data.getString("uid");
                    String title = data.getString("title");
                    String page_type = null;
                    if ("首发新款".equals(title)) {
                        page_type = "今日新款";
                    }
                    String event_id = data.getString("event_id");
                    String question_id = data.getString("question_id");
                    String question_name = data.getString("question_name");

                    // 数据封装
                    JSONObject questionResult = new JSONObject();
                    questionResult.put(ModelUtil.humpToUnderline("receiveTime"), receiveTime);
                    questionResult.put(ModelUtil.humpToUnderline("user_id"), user_id);
                    questionResult.put(ModelUtil.humpToUnderline("page_type"), page_type);
                    questionResult.put(ModelUtil.humpToUnderline("event_id"), event_id);
                    questionResult.put(ModelUtil.humpToUnderline("question_id"), question_id);
                    questionResult.put(ModelUtil.humpToUnderline("question_name"), question_name);
                    questionResult.put("dt", DateUtil.secondToSpecialDate(Long.parseLong(receiveTime)));

                    // 数据发送
                    out.collect(questionResult);
                }

            }
        } catch (Exception e) {
            logger.warn(
                    "***** 埋点数据解析异常，不能解析成json字符串，传入的埋点数据为：{}， 抛出的异常信息为：{}",
                    value,
                    e.getMessage()
            );
        }

    }
}
