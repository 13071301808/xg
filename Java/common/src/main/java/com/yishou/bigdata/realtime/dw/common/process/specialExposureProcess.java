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

public class specialExposureProcess extends ProcessFunction<String, JSONObject> {

    static Logger logger = LoggerFactory.getLogger(specialExposureProcess.class);

    @Override
    public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
        try {
            JSONObject data = JSON.parseObject(value);
            if (EventParseUtil.isEvent(data, "specialexposure")) {
                JSONObject scData = data.getJSONObject("scdata");
                if (scData != null) {
                    JSONArray special_arr = scData.getJSONArray("special_arr");
                    String specialEmbedRecommendArr = scData.getString("special_embed_recommend_arr");
                    // 曝光数组
                    if (special_arr != null) {
                        for (Object special_arr_object : special_arr) {
                            JSONObject special_arr_json = JSON.parseObject(JSON.toJSONString(special_arr_object));

                            if (special_arr_json != null) {
                                // 自定义
                                String clientIp = data.getString("__client_ip__");
                                String receiveTime = data.getString("receiveTime");
                                String userId = StringUtils.isNotBlank(scData.getString("userid")) ? scData.getString("userid") : scData.getString("user_id");
                                String eventId = scData.getString("event_id");

                                String special_id = special_arr_json.getString("special_id");
                                String home_index = special_arr_json.getString("home_index");
                                String index = special_arr_json.getString("index");
                                String special_name = special_arr_json.getString("special_name");
                                String tab_name = special_arr_json.getString("tab_name");
                                String is_operat = special_arr_json.getString("is_operat");
                                String goods_id = special_arr_json.getString("goods_id");
                                String goods_no = special_arr_json.getString("goods_no");
                                String special_goods_url = special_arr_json.getString("special_goods_url");

                                // 输出
                                JSONObject specialExposureResult = new JSONObject();
                                specialExposureResult.put(ModelUtil.humpToUnderline("receiveTime"), receiveTime);
                                specialExposureResult.put(ModelUtil.humpToUnderline("ip"), clientIp);
                                specialExposureResult.put(ModelUtil.humpToUnderline("event_id"), eventId);
                                specialExposureResult.put(ModelUtil.humpToUnderline("special_embed_recommend_arr"), specialEmbedRecommendArr);
                                specialExposureResult.put(ModelUtil.humpToUnderline("special_id"), special_id);
                                specialExposureResult.put(ModelUtil.humpToUnderline("home_index"), home_index);
                                specialExposureResult.put(ModelUtil.humpToUnderline("index"), index);
                                specialExposureResult.put(ModelUtil.humpToUnderline("special_name"), special_name);
                                specialExposureResult.put(ModelUtil.humpToUnderline("tab_name"), tab_name);
                                specialExposureResult.put(ModelUtil.humpToUnderline("is_operat"), is_operat);
                                specialExposureResult.put(ModelUtil.humpToUnderline("goods_id"), goods_id);
                                specialExposureResult.put(ModelUtil.humpToUnderline("goods_no"), goods_no);
                                specialExposureResult.put(ModelUtil.humpToUnderline("special_goods_url"), special_goods_url);
                                specialExposureResult.put(ModelUtil.humpToUnderline("user_id"), userId);
                                specialExposureResult.put(ModelUtil.humpToUnderline("event"), "specialexposure");
                                if (StringUtils.isNotBlank(userId)) {
                                    collector.collect(specialExposureResult);
                                }
                            }
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