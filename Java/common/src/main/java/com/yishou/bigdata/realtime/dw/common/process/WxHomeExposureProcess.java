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

public class WxHomeExposureProcess extends ProcessFunction<String, JSONObject> {
    static Logger logger = LoggerFactory.getLogger(WxHomeExposureProcess.class);

    @Override
    public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
        try {
            JSONObject data = JSON.parseObject(value);
            JSONObject eventData = data.getJSONObject("data");
            if (eventData != null && EventParseUtil.isEvent(data, "homeexposure")) {
                // 日志流自带
                String logTime = data.getString("__time__");
                String receiveTime = data.getString("__time__");
                String logSource = data.getString("__client_ip__");
                String activity = data.getString("activity");

                // 自定义数据
                String user_id = eventData.getString("userid");
                String click_source = eventData.getString("click_source");
                String marketing_channel = eventData.getString("marketing_channel");
                String home_market_below_banner = eventData.getString("home_market_below_banner");
                String home_market_banner = eventData.getString("home_market_banner");

                // 输出流
                JSONObject wxHomeExposureResult = new JSONObject();
                wxHomeExposureResult.put(ModelUtil.humpToUnderline("log_source"), logSource);
                wxHomeExposureResult.put(ModelUtil.humpToUnderline("log_time"), logTime);
                wxHomeExposureResult.put(ModelUtil.humpToUnderline("receive_time"), receiveTime);
                wxHomeExposureResult.put(ModelUtil.humpToUnderline("activity"), activity);
                wxHomeExposureResult.put(ModelUtil.humpToUnderline("user_id"), user_id);
                wxHomeExposureResult.put(ModelUtil.humpToUnderline("click_source"), click_source);
                wxHomeExposureResult.put(ModelUtil.humpToUnderline("marketing_channel"), marketing_channel);
                wxHomeExposureResult.put(ModelUtil.humpToUnderline("home_market_below_banner"), home_market_below_banner);
                wxHomeExposureResult.put(ModelUtil.humpToUnderline("home_market_banner"), home_market_banner);
                wxHomeExposureResult.put("event", "homeexposure");

                if (StringUtils.isNotBlank(user_id)) {
                    collector.collect(wxHomeExposureResult);
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
