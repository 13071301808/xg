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

public class WxOrderListExposureProcess extends ProcessFunction<String, JSONObject>{
    static Logger logger = LoggerFactory.getLogger(WxOrderListExposureProcess.class);

    @Override
    public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
        try {
            JSONObject data = JSON.parseObject(value);
            JSONObject eventData = data.getJSONObject("data");
            if (eventData != null && EventParseUtil.isEvent(data, "orderlistexposure")) {
                // 日志流自带
                String logTime = data.getString("__time__");
                String receiveTime = data.getString("__time__");
                String logSource = data.getString("__client_ip__");
                String activity = data.getString("activity");

                // 自定义数据
                String user_id = eventData.getString("userid");
                String pid = eventData.getString("pid");
                String order_list = eventData.getString("orderList");

                // 输出流
                JSONObject wxOrderListExposureResult = new JSONObject();
                wxOrderListExposureResult.put(ModelUtil.humpToUnderline("log_source"), logSource);
                wxOrderListExposureResult.put(ModelUtil.humpToUnderline("log_time"), logTime);
                wxOrderListExposureResult.put(ModelUtil.humpToUnderline("receive_time"), receiveTime);
                wxOrderListExposureResult.put(ModelUtil.humpToUnderline("activity"), activity);
                wxOrderListExposureResult.put(ModelUtil.humpToUnderline("user_id"), user_id);
                wxOrderListExposureResult.put(ModelUtil.humpToUnderline("pid"), pid);
                wxOrderListExposureResult.put(ModelUtil.humpToUnderline("order_list"), order_list);
                wxOrderListExposureResult.put("event", "orderlistexposure");

                if (StringUtils.isNotBlank(user_id)) {
                    collector.collect(wxOrderListExposureResult);
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
