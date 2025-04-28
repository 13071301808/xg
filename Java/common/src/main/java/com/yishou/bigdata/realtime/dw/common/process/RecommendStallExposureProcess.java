package com.yishou.bigdata.realtime.dw.common.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.realtime.dw.common.utils.EventParseUtil;
import com.yishou.bigdata.realtime.dw.common.utils.ModelUtil;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @date: 2023/5/9
 * @author: yangshibiao
 * @desc: 推荐档口曝光解析
 */
public class RecommendStallExposureProcess extends ProcessFunction<String, JSONObject> {

    static Logger logger = LoggerFactory.getLogger(RecommendStallExposureProcess.class);

    @Override
    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

        try {

            JSONObject jsonText = JSON.parseObject(value);
            if (EventParseUtil.isEvent(jsonText, "goodsexposure")) {

                JSONObject scdata = jsonText.getJSONObject("scdata");
                if (scdata != null) {

                    JSONArray recommendStallExposureArr = scdata.getJSONArray("recommend_stall_arr");
                    if (recommendStallExposureArr != null) {

                        for (Object recommendStallExposureObject : recommendStallExposureArr) {

                            JSONObject recommendStallExposureJson = JSON.parseObject(JSON.toJSONString(recommendStallExposureObject));
                            if (recommendStallExposureJson != null) {

                                // 数据解析
                                String userId = scdata.getString("user_id");
                                String specialId = scdata.getString("special_id");
                                String os = scdata.getString("os");
                                String pid = scdata.getString("pid");
                                String source = scdata.getString("source");
                                String eventId = scdata.getString("event_id");
                                String searchEventId = scdata.getString("search_event_id");
                                String keyword = scdata.getString("keyword");
                                String appVersion = scdata.getString("app_version");
                                String catId = scdata.getString("cat_id");
                                String stallId = recommendStallExposureJson.getString("stall_id");
                                String index = recommendStallExposureJson.getString("index");
                                String plate = recommendStallExposureJson.getString("plate");
                                String scene = recommendStallExposureJson.getString("scene");
                                String strategyId = recommendStallExposureJson.getString("strategy_id");
                                String recReason = recommendStallExposureJson.getString("rec_reason");
                                String time = jsonText.getString("__time__");

                                // 数据封装
                                JSONObject recommendStallExposureResult = new JSONObject();
                                recommendStallExposureResult.put(ModelUtil.humpToUnderline("userId"), userId);
                                recommendStallExposureResult.put(ModelUtil.humpToUnderline("specialId"), specialId);
                                recommendStallExposureResult.put(ModelUtil.humpToUnderline("os"), os);
                                recommendStallExposureResult.put(ModelUtil.humpToUnderline("pid"), pid);
                                recommendStallExposureResult.put(ModelUtil.humpToUnderline("source"), source);
                                recommendStallExposureResult.put(ModelUtil.humpToUnderline("eventId"), eventId);
                                recommendStallExposureResult.put(ModelUtil.humpToUnderline("searchEventId"), searchEventId);
                                recommendStallExposureResult.put(ModelUtil.humpToUnderline("keyword"), keyword);
                                recommendStallExposureResult.put(ModelUtil.humpToUnderline("appVersion"), appVersion);
                                recommendStallExposureResult.put(ModelUtil.humpToUnderline("catId"), catId);
                                recommendStallExposureResult.put(ModelUtil.humpToUnderline("stallId"), stallId);
                                recommendStallExposureResult.put(ModelUtil.humpToUnderline("index"), index);
                                recommendStallExposureResult.put(ModelUtil.humpToUnderline("plate"), plate);
                                recommendStallExposureResult.put(ModelUtil.humpToUnderline("scene"), scene);
                                recommendStallExposureResult.put(ModelUtil.humpToUnderline("strategyId"), strategyId);
                                recommendStallExposureResult.put(ModelUtil.humpToUnderline("recReason"), recReason);
                                recommendStallExposureResult.put(ModelUtil.humpToUnderline("time"), time);
                                // 输出数据
                                out.collect(recommendStallExposureResult);

                            }

                        }

                    }
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
