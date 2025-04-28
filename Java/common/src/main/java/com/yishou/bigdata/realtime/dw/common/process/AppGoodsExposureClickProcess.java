package com.yishou.bigdata.realtime.dw.common.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.realtime.dw.common.utils.EventParseUtil;
import com.yishou.bigdata.realtime.dw.common.utils.ModelUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppGoodsExposureClickProcess extends ProcessFunction<String, JSONObject> {

    static Logger logger = LoggerFactory.getLogger(AppGoodsClickProcess.class);

    @Override
    public void processElement(String value, Context ctx, Collector<com.alibaba.fastjson.JSONObject> out) throws Exception {

        try {
                JSONObject jsonText = JSON.parseObject(value);
            if (EventParseUtil.isEvent(jsonText, "goodsexposure")) {
                JSONObject scdata = jsonText.getJSONObject("scdata");

                if (scdata != null) {

                    try {
                        //曝光数据
                        JSONArray goodsExposureArr = scdata.getJSONArray("goods_arr");
                        if (goodsExposureArr != null) {

                            for (Object goodsExposureObject : goodsExposureArr) {

                                JSONObject goodsExposureJson = JSON.parseObject(JSON.toJSONString(goodsExposureObject));
                                if (goodsExposureJson != null) {
                                    out.collect(parseGoodsExposure(jsonText,scdata,goodsExposureJson));

                                }
                            }
                        }

                        //点击数据
                        // 点击数据 （点击数据中可能是 JSONArray 也可能是 JSONObject，所以2种情况都要考虑）
                        // JSONArray
                        JSONArray goodsClickArr = scdata.getJSONArray("click_goods_arr");
                        if (goodsClickArr != null) {

                            for (Object goodsClickObject : goodsClickArr) {

                                com.alibaba.fastjson.JSONObject goodsClickJson = JSON.parseObject(JSON.toJSONString(goodsClickObject));
                                if (goodsClickJson != null) {
                                    // 输出数据
                                    out.collect(parseGoodsClick(jsonText, scdata, goodsClickJson));
                                }

                            }
                        }

                    } catch (Exception e) {

                        // JSONObject
                        com.alibaba.fastjson.JSONObject goodsClickJson = scdata.getJSONObject("click_goods_arr");
                        if (goodsClickJson != null) {
                            // 输出数据
                            out.collect(parseGoodsClick(jsonText, scdata, goodsClickJson));
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

    /**
     * 通过传入的数据解析出APP商品点击的最终结果
     *
     * @param jsonText       传入的完整日志
     * @param scdata         scdata标签数据
     * @param goodsClickJson click_goods_arr下单个商品点击的数据
     * @return 商品点击的最终结果
     */
    public com.alibaba.fastjson.JSONObject parseGoodsClick(com.alibaba.fastjson.JSONObject jsonText, com.alibaba.fastjson.JSONObject scdata, com.alibaba.fastjson.JSONObject goodsClickJson) {

        // 数据解析
        String userId = scdata.getString("user_id");
        String specialId = scdata.getString("special_id");
        String os = scdata.getString("os");
        String pid = scdata.getString("pid");
        String ptime = scdata.getString("ptime");
        String source = scdata.getString("source");
        String eventId = scdata.getString("event_id");
        String searchEventId = scdata.getString("search_event_id");
        String keyword = scdata.getString("keyword");
        String appVersion = scdata.getString("app_version");
        String goodsId = goodsClickJson.getString("goods_id");
        if (StringUtils.isBlank(goodsId)) {
            goodsId = goodsClickJson.getString("good_id");
        }
        String isRec = goodsClickJson.getString("is_rec");
        String goodsNo = goodsClickJson.getString("goods_no");
        String index = goodsClickJson.getString("index");
        String strategyId = goodsClickJson.getString("strategy_id");
        String isDefault = goodsClickJson.getString("is_default");
        String isOperat = goodsClickJson.getString("is_operat");
        String receiveTime = jsonText.getString("__time__");

        // 数据封装
        com.alibaba.fastjson.JSONObject goodsClickResult = new com.alibaba.fastjson.JSONObject();
        goodsClickResult.put(ModelUtil.humpToUnderline("userId"), userId);
        goodsClickResult.put(ModelUtil.humpToUnderline("goodsId"), goodsId);
        goodsClickResult.put(ModelUtil.humpToUnderline("specialId"), specialId);
        goodsClickResult.put(ModelUtil.humpToUnderline("os"), os);
        goodsClickResult.put(ModelUtil.humpToUnderline("pid"), pid);
        goodsClickResult.put(ModelUtil.humpToUnderline("ptime"), ptime);
        goodsClickResult.put(ModelUtil.humpToUnderline("source"), source);
        goodsClickResult.put(ModelUtil.humpToUnderline("eventId"), eventId);
        goodsClickResult.put(ModelUtil.humpToUnderline("searchEventId"), searchEventId);
        goodsClickResult.put(ModelUtil.humpToUnderline("keyword"), keyword);
        goodsClickResult.put(ModelUtil.humpToUnderline("appVersion"), appVersion);
        goodsClickResult.put(ModelUtil.humpToUnderline("goodsId"), goodsId);
        goodsClickResult.put(ModelUtil.humpToUnderline("isRec"), isRec);
        goodsClickResult.put(ModelUtil.humpToUnderline("goodsNo"), goodsNo);
        goodsClickResult.put(ModelUtil.humpToUnderline("index"), index);
        goodsClickResult.put(ModelUtil.humpToUnderline("strategyId"), strategyId);
        goodsClickResult.put(ModelUtil.humpToUnderline("isDefault"), isDefault);
        goodsClickResult.put(ModelUtil.humpToUnderline("isOperat"), isOperat);
        goodsClickResult.put(ModelUtil.humpToUnderline("receiveTime"), receiveTime);
        goodsClickResult.put(ModelUtil.humpToUnderline("dataType"),"click");

        // 返回结果
        return goodsClickResult;

    }
    public JSONObject parseGoodsExposure(JSONObject jsonText,JSONObject scdata,JSONObject goodsExposureJson){
        // 数据解析
        String userId = scdata.getString("user_id");
        String specialId = scdata.getString("special_id");
        String os = scdata.getString("os");
        String pid = scdata.getString("pid");
        String reportTime = scdata.getString("report_time");
        String source = scdata.getString("source");
        String eventId = scdata.getString("event_id");
        String searchEventId = scdata.getString("search_event_id");
        String keyword = scdata.getString("keyword");
        String appVersion = scdata.getString("app_version");
        String goodsId = goodsExposureJson.getString("goods_id");
        if (StringUtils.isBlank(goodsId)) {
            goodsId = goodsExposureJson.getString("good_id");
        }
        String isRec = goodsExposureJson.getString("is_rec");
        String goodsNo = goodsExposureJson.getString("goods_no");
        String index = goodsExposureJson.getString("index");
        String strategyId = goodsExposureJson.getString("strategy_id");
        String isDefault = goodsExposureJson.getString("is_default");
        String isOperat = goodsExposureJson.getString("is_operat");
        String receiveTime = jsonText.getString("__time__");

        // 数据封装
        JSONObject goodsExposureResult = new JSONObject();
        goodsExposureResult.put(ModelUtil.humpToUnderline("userId"), userId);
        goodsExposureResult.put(ModelUtil.humpToUnderline("goodsId"), goodsId);
        goodsExposureResult.put(ModelUtil.humpToUnderline("specialId"), specialId);
        goodsExposureResult.put(ModelUtil.humpToUnderline("os"), os);
        goodsExposureResult.put(ModelUtil.humpToUnderline("pid"), pid);
        goodsExposureResult.put(ModelUtil.humpToUnderline("reportTime"), reportTime);
        goodsExposureResult.put(ModelUtil.humpToUnderline("source"), source);
        goodsExposureResult.put(ModelUtil.humpToUnderline("eventId"), eventId);
        goodsExposureResult.put(ModelUtil.humpToUnderline("searchEventId"), searchEventId);
        goodsExposureResult.put(ModelUtil.humpToUnderline("keyword"), keyword);
        goodsExposureResult.put(ModelUtil.humpToUnderline("appVersion"), appVersion);
        goodsExposureResult.put(ModelUtil.humpToUnderline("goodsId"), goodsId);
        goodsExposureResult.put(ModelUtil.humpToUnderline("isRec"), isRec);
        goodsExposureResult.put(ModelUtil.humpToUnderline("goodsNo"), goodsNo);
        goodsExposureResult.put(ModelUtil.humpToUnderline("index"), index);
        goodsExposureResult.put(ModelUtil.humpToUnderline("strategyId"), strategyId);
        goodsExposureResult.put(ModelUtil.humpToUnderline("isDefault"), isDefault);
        goodsExposureResult.put(ModelUtil.humpToUnderline("isOperat"), isOperat);
        goodsExposureResult.put(ModelUtil.humpToUnderline("receiveTime"), receiveTime);
        goodsExposureResult.put(ModelUtil.humpToUnderline("dataType"),"exposure");

        // 输出数据
        return goodsExposureResult;

    }
}
