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

public class WxSpecialDetailExposureProcess extends ProcessFunction<String, JSONObject> {
    static Logger logger = LoggerFactory.getLogger(WxSpecialDetailExposureProcess.class);

    @Override
    public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
        try {
            JSONObject data = JSON.parseObject(value);
            JSONObject eventData = data.getJSONObject("data");
            if (eventData != null && EventParseUtil.isEvent(data, "specialdetailexposure")) {
                // 日志流自带
                String logTime = data.getString("__time__");
                String receiveTime = data.getString("__time__");
                String logSource = data.getString("__client_ip__");
                String activity = data.getString("activity");

                // 自定义数据
                String user_id = eventData.getString("userid");
                String top_exposure_arr = eventData.getString("top_exposure_arr");
                String banner_arr = eventData.getString("banner_arr");
                String goods_arr = eventData.getString("goods_arr");

                // 输出流
                JSONObject wxSpecialDetailExposureResult = new JSONObject();
                wxSpecialDetailExposureResult.put(ModelUtil.humpToUnderline("log_source"), logSource);
                wxSpecialDetailExposureResult.put(ModelUtil.humpToUnderline("log_time"), logTime);
                wxSpecialDetailExposureResult.put(ModelUtil.humpToUnderline("receive_time"), receiveTime);
                wxSpecialDetailExposureResult.put(ModelUtil.humpToUnderline("activity"), activity);
                wxSpecialDetailExposureResult.put(ModelUtil.humpToUnderline("user_id"), user_id);
                wxSpecialDetailExposureResult.put(ModelUtil.humpToUnderline("top_exposure_arr"), top_exposure_arr);
                wxSpecialDetailExposureResult.put(ModelUtil.humpToUnderline("banner_arr"), banner_arr);
                wxSpecialDetailExposureResult.put(ModelUtil.humpToUnderline("goods_arr"), goods_arr);
                wxSpecialDetailExposureResult.put("event", "specialdetailexposure");

                if (StringUtils.isNotBlank(user_id)) {
                    collector.collect(wxSpecialDetailExposureResult);
                }
            }
        } catch (Exception e) {
            logger.warn(
                    "***** 埋点数据解析异常，不能解析成json字符串，传入的埋点数据为：{}， 抛出的异常信息为：{}",
                    value,
                    e.getMessage());
        }
    }
}
