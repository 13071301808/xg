package com.yishou.bigdata.realtime.dw.common.basic;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.realtime.dw.common.process.H5GoodsExposureProcess;
import com.yishou.bigdata.realtime.dw.common.utils.EventParseUtil;
import com.yishou.bigdata.realtime.dw.common.utils.ModelUtil;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class H5GoodsExposureClickProcess extends ProcessFunction<String, JSONObject> {
    static Logger logger = LoggerFactory.getLogger(H5GoodsExposureClickProcess.class);

    @Override
    public void processElement(String value, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        try {
            //曝光数据
            JSONObject jsonText = JSON.parseObject(value);
            if (EventParseUtil.isEvent(jsonText, "ysh5goodsexposure")) {

                JSONObject data = jsonText.getJSONObject("data");
                if (data != null) {

                    JSONArray goodsExposureArr = data.getJSONArray("goods_arr");
                    JSONArray goodsClickArr = data.getJSONArray("click_goods_arr");
                    if (goodsExposureArr != null) {

                        for (Object goodsExposureObject : goodsExposureArr) {

                            JSONObject goodsExposureJson = JSON.parseObject(JSON.toJSONString(goodsExposureObject));
                            if (goodsExposureJson != null) {
                                // 数据发送
                                collector.collect(paseGoodsH5Exposure(jsonText, data, goodsExposureJson));

                            }
                        }
                    }
                    if (goodsClickArr != null) {

                        for (Object goodsClickObject : goodsClickArr) {

                            JSONObject goodsClickJson = JSON.parseObject(JSON.toJSONString(goodsClickObject));
                            if (goodsClickJson != null) {

                                // 数据发送
                                collector.collect(paseGoodsH5Click(jsonText,data,goodsClickJson));

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

    public JSONObject paseGoodsH5Exposure(JSONObject jsonText, JSONObject scdata, JSONObject goodsExposureJson) {

        // 数据解析
        String logSource = jsonText.getString("__client_ip__");
        String receiveTime = jsonText.getString("__time__");
        String activity = jsonText.getString("activity");
        String userId = scdata.getString("uid");
        String appVersion = scdata.getString("app_version");
        String url = scdata.getString("url");
        String title = scdata.getString("title");
        String eventId = scdata.getString("event_id");
        String templateId = scdata.getString("template_id");
        String goodsId = goodsExposureJson.getString("goods_id");
        String plateName = goodsExposureJson.getString("plate_name");
        String tabName = goodsExposureJson.getString("tab_name");
        String index = goodsExposureJson.getString("index");
        String isDefault = goodsExposureJson.getString("is_default");
        String isOperat = goodsExposureJson.getString("is_operat");
        String goodsSeatId = goodsExposureJson.getString("goods_seat_id");
        String goodsNo = goodsExposureJson.getString("goods_no");

        // 数据封装
        JSONObject goodsExposureResult = new JSONObject();
        goodsExposureResult.put(ModelUtil.humpToUnderline("logSource"), logSource);
        goodsExposureResult.put(ModelUtil.humpToUnderline("receiveTime"), receiveTime);
        goodsExposureResult.put(ModelUtil.humpToUnderline("activity"), activity);
        goodsExposureResult.put(ModelUtil.humpToUnderline("userId"), userId);
        goodsExposureResult.put(ModelUtil.humpToUnderline("appVersion"), appVersion);
        goodsExposureResult.put(ModelUtil.humpToUnderline("url"), url);
        goodsExposureResult.put(ModelUtil.humpToUnderline("title"), title);
        goodsExposureResult.put(ModelUtil.humpToUnderline("eventId"), eventId);
        goodsExposureResult.put(ModelUtil.humpToUnderline("templateId"), templateId);
        goodsExposureResult.put(ModelUtil.humpToUnderline("goodsId"), goodsId);
        goodsExposureResult.put(ModelUtil.humpToUnderline("plateName"), plateName);
        goodsExposureResult.put(ModelUtil.humpToUnderline("tabName"), tabName);
        goodsExposureResult.put(ModelUtil.humpToUnderline("index"), index);
        goodsExposureResult.put(ModelUtil.humpToUnderline("isDefault"), isDefault);
        goodsExposureResult.put(ModelUtil.humpToUnderline("isOperat"), isOperat);
        goodsExposureResult.put(ModelUtil.humpToUnderline("goodsSeatId"), goodsSeatId);
        goodsExposureResult.put(ModelUtil.humpToUnderline("goodsNo"), goodsNo);
        goodsExposureResult.put(ModelUtil.humpToUnderline("dataType"),"exposure");
        goodsExposureResult.put("event", "exposure");
        return goodsExposureResult;
    }

    public JSONObject paseGoodsH5Click(JSONObject jsonText, JSONObject scdata, JSONObject goodsClickJson) {
        // 数据解析
        String logSource = jsonText.getString("__client_ip__");
        String receiveTime = jsonText.getString("__time__");
        String activity = jsonText.getString("activity");
        String userId = scdata.getString("uid");
        String appVersion = scdata.getString("app_version");
        String url = scdata.getString("url");
        String title = scdata.getString("title");
        String eventId = scdata.getString("event_id");
        String templateId = scdata.getString("template_id");
        String goodsId = goodsClickJson.getString("goods_id");
        String plateName = goodsClickJson.getString("plate_name");
        String tabName = goodsClickJson.getString("tab_name");
        String index = goodsClickJson.getString("index");
        String isDefault = goodsClickJson.getString("is_default");
        String isOperat = goodsClickJson.getString("is_operat");
        String goodsSeatId = goodsClickJson.getString("goods_seat_id");
        String goodsNo = goodsClickJson.getString("goods_no");

        // 数据封装
        JSONObject goodsClickResult = new JSONObject();
        goodsClickResult.put(ModelUtil.humpToUnderline("logSource"), logSource);
        goodsClickResult.put(ModelUtil.humpToUnderline("receiveTime"), receiveTime);
        goodsClickResult.put(ModelUtil.humpToUnderline("activity"), activity);
        goodsClickResult.put(ModelUtil.humpToUnderline("userId"), userId);
        goodsClickResult.put(ModelUtil.humpToUnderline("goodsId"), goodsId);
        goodsClickResult.put(ModelUtil.humpToUnderline("appVersion"), appVersion);
        goodsClickResult.put(ModelUtil.humpToUnderline("url"), url);
        goodsClickResult.put(ModelUtil.humpToUnderline("title"), title);
        goodsClickResult.put(ModelUtil.humpToUnderline("eventId"), eventId);
        goodsClickResult.put(ModelUtil.humpToUnderline("templateId"), templateId);
        goodsClickResult.put(ModelUtil.humpToUnderline("goodsId"), goodsId);
        goodsClickResult.put(ModelUtil.humpToUnderline("plateName"), plateName);
        goodsClickResult.put(ModelUtil.humpToUnderline("tabName"), tabName);
        goodsClickResult.put(ModelUtil.humpToUnderline("index"), index);
        goodsClickResult.put(ModelUtil.humpToUnderline("isDefault"), isDefault);
        goodsClickResult.put(ModelUtil.humpToUnderline("isOperat"), isOperat);
        goodsClickResult.put(ModelUtil.humpToUnderline("goodsSeatId"), goodsSeatId);
        goodsClickResult.put(ModelUtil.humpToUnderline("goodsNo"), goodsNo);
        goodsClickResult.put(ModelUtil.humpToUnderline("dataType"),"click");
        goodsClickResult.put("event", "click");

        return goodsClickResult;
    }
}
