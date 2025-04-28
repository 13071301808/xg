package com.yishou.bigdata.realtime.dw.common.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.realtime.dw.common.utils.EventParseUtil;
import com.yishou.bigdata.realtime.dw.common.utils.ModelUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppSpecialExposureClickProcess extends ProcessFunction<String, JSONObject> {
    static Logger logger = LoggerFactory.getLogger(AppSpecialExposureClickProcess.class);
    @Override
    public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
        try {
            JSONObject data = JSON.parseObject(value);
            if (EventParseUtil.isEvent(data, "specialexposure")) {
                JSONObject scData = data.getJSONObject("scdata");
                if (scData != null) {
                    JSONObject click_special_arr = scData.getJSONObject("click_special_arr");
                    // 曝光数组
                    if (click_special_arr != null) {
                        // 自定义
                        String specialEmbedRecommendArr = scData.getString("special_embed_recommend_arr");
                        String clientIp = data.getString("__client_ip__");
                        String receiveTime = data.getString("receiveTime");
                        String userId = StringUtils.isNotBlank(scData.getString("userid")) ? scData.getString("userid") : scData.getString("user_id");
                        String eventId = scData.getString("event_id");
                        String special_id = click_special_arr.getString("special_id");
                        String home_index = click_special_arr.getString("home_index");
                        String index = click_special_arr.getString("index");
                        String special_name = click_special_arr.getString("special_name");
                        String tab_name = click_special_arr.getString("tab_name");
                        String is_operat = click_special_arr.getString("is_operat");
                        String goods_id = click_special_arr.getString("goods_id");
                        String goods_no = click_special_arr.getString("goods_no");
                        String special_goods_url = click_special_arr.getString("special_goods_url");

                        // 输出
                        JSONObject clickSpecialExposureResult = new JSONObject();
                        clickSpecialExposureResult.put(ModelUtil.humpToUnderline("receiveTime"), receiveTime);
                        clickSpecialExposureResult.put(ModelUtil.humpToUnderline("ip"), clientIp);
                        clickSpecialExposureResult.put(ModelUtil.humpToUnderline("event_id"), eventId);
                        clickSpecialExposureResult.put(ModelUtil.humpToUnderline("special_embed_recommend_arr"), specialEmbedRecommendArr);
                        clickSpecialExposureResult.put(ModelUtil.humpToUnderline("special_id"), special_id);
                        clickSpecialExposureResult.put(ModelUtil.humpToUnderline("home_index"), home_index);
                        clickSpecialExposureResult.put(ModelUtil.humpToUnderline("index"), index);
                        clickSpecialExposureResult.put(ModelUtil.humpToUnderline("special_name"), special_name);
                        clickSpecialExposureResult.put(ModelUtil.humpToUnderline("tab_name"), tab_name);
                        clickSpecialExposureResult.put(ModelUtil.humpToUnderline("is_operat"), is_operat);
                        clickSpecialExposureResult.put(ModelUtil.humpToUnderline("goods_id"), goods_id);
                        clickSpecialExposureResult.put(ModelUtil.humpToUnderline("goods_no"), goods_no);
                        clickSpecialExposureResult.put(ModelUtil.humpToUnderline("special_goods_url"), special_goods_url);
                        clickSpecialExposureResult.put(ModelUtil.humpToUnderline("user_id"), userId);
                        clickSpecialExposureResult.put(ModelUtil.humpToUnderline("event"), "specialexposure");
                        if (StringUtils.isNotBlank(userId)) {
                            collector.collect(clickSpecialExposureResult);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.warn(
                    "***** 埋点数据解析异常，不能解析成 json 字符串，传入的埋点数据为：{}， 抛出的异常信息为：{}",
                    value,
                    e.getMessage());
        }
    }
}
