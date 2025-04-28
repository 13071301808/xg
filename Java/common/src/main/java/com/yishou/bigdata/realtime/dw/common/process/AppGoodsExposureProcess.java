package com.yishou.bigdata.realtime.dw.common.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.realtime.dw.common.utils.DateUtil;
import com.yishou.bigdata.realtime.dw.common.utils.EventParseUtil;
import com.yishou.bigdata.realtime.dw.common.utils.ModelUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @date: 2022/12/9
 * @author: yangshibiao
 * @desc: App商品曝光解析
 */
public class AppGoodsExposureProcess extends ProcessFunction<String, JSONObject> {

    static Logger logger = LoggerFactory.getLogger(AppGoodsExposureProcess.class);

    @Override
    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

        try {

            JSONObject jsonText = JSON.parseObject(value);
            if (EventParseUtil.isEvent(jsonText, "goodsexposure")) {

                JSONObject scdata = jsonText.getJSONObject("scdata");
                if (scdata != null) {

                    JSONArray goodsExposureArr = scdata.getJSONArray("goods_arr");
                    if (goodsExposureArr != null) {

                        for (Object goodsExposureObject : goodsExposureArr) {

                            JSONObject goodsExposureJson = JSON.parseObject(JSON.toJSONString(goodsExposureObject));
                            if (goodsExposureJson != null) {

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
                                String click_to_recommend = goodsExposureJson.getString("click_to_recommend");
                                String receiveTime = jsonText.getString("__time__");

                                // 数据封装
                                JSONObject goodsExposureResult = new JSONObject();
                                goodsExposureResult.put(ModelUtil.humpToUnderline("userId"), userId);
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
                                goodsExposureResult.put("click_to_recommend", click_to_recommend);
                                goodsExposureResult.put(ModelUtil.humpToUnderline("event"), "goodsexposure");
                                goodsExposureResult.put("dt", DateUtil.secondToSpecialDate(Long.parseLong(receiveTime)));
                                goodsExposureResult.put("event_name", "dwd_app_goods_exposure");

                                // 输出数据
                                out.collect(goodsExposureResult);

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

