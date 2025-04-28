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

public class AppGoodsExposureQuestionnaireProcess extends ProcessFunction<String, JSONObject> {
    static Logger logger = LoggerFactory.getLogger(AppGoodsExposureQuestionnaireProcess.class);

    @Override
    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject jsonText = JSON.parseObject(value);
        if (EventParseUtil.isEvent(jsonText, "goodsexposure")) {

            JSONObject scdata = jsonText.getJSONObject("scdata");
            if (scdata != null) {
                JSONArray questionArr = scdata.getJSONArray("question_arr");
                if (questionArr != null) {
                    for (Object questionObject : questionArr) {
                        JSONObject questionJson = JSON.parseObject(JSON.toJSONString(questionObject));
                        if (questionJson != null) {
                            // 数据解析
                            String receiveTime = jsonText.getString("__time__");
                            String user_id = scdata.getString("user_id");
                            String pid = scdata.getString("pid");
                            String page_type = null;
                            if ("10".equals(pid)) {
                                page_type = "专场详情页";
                            } else if ("12".equals(pid)) {
                                page_type = "分类详情页";
                            }else if ("14".equals(pid)) {
                                page_type = "搜索结果页";
                            }
                            
                            String event_id = scdata.getString("event_id");
                            String question_id = questionJson.getString("question_id");
                            String question_name = questionJson.getString("question_name");

                            // 数据封装
                            JSONObject questionResult = new JSONObject();
                            questionResult.put(ModelUtil.humpToUnderline("receiveTime"), receiveTime);
                            questionResult.put(ModelUtil.humpToUnderline("user_id"), user_id);
                            questionResult.put(ModelUtil.humpToUnderline("page_type"), page_type);
                            questionResult.put(ModelUtil.humpToUnderline("event_id"), event_id);
                            questionResult.put(ModelUtil.humpToUnderline("question_id"), question_id);
                            questionResult.put(ModelUtil.humpToUnderline("question_name"), question_name);
                            questionResult.put("dt", DateUtil.secondToSpecialDate(Long.parseLong(receiveTime)));

                            // 输出数据
                            out.collect(questionResult);
                        }
                    }
                }
            }
        }
    }
}
