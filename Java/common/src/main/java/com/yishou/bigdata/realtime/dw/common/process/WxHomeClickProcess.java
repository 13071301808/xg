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

public class WxHomeClickProcess extends ProcessFunction<String, JSONObject> {
    static Logger logger = LoggerFactory.getLogger(WxHomeClickProcess.class);

    @Override
    public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
        try {
            JSONObject data = JSON.parseObject(value);
            JSONObject eventData = data.getJSONObject("data");
            if (eventData != null && EventParseUtil.isEvent(data, "homeclick")) {
                // 日志流自带
                String logTime = data.getString("__time__");
                String receiveTime = data.getString("__time__");
                String logSource = data.getString("__client_ip__");
                String activity = data.getString("activity");

                // 自定义数据
                String user_id = eventData.getString("userid");
                String click_source = eventData.getString("click_source");
                String banner_id = eventData.getString("banner_id");
                String banner_name = eventData.getString("banner_name");

                // 输出流
                JSONObject wxHomeClickResult = new JSONObject();
                wxHomeClickResult.put(ModelUtil.humpToUnderline("log_source"), logSource);
                wxHomeClickResult.put(ModelUtil.humpToUnderline("log_time"), logTime);
                wxHomeClickResult.put(ModelUtil.humpToUnderline("receive_time"), receiveTime);
                wxHomeClickResult.put(ModelUtil.humpToUnderline("activity"), activity);
                wxHomeClickResult.put(ModelUtil.humpToUnderline("user_id"), user_id);
                wxHomeClickResult.put(ModelUtil.humpToUnderline("click_source"), click_source);
                wxHomeClickResult.put(ModelUtil.humpToUnderline("banner_id"), banner_id);
                wxHomeClickResult.put(ModelUtil.humpToUnderline("banner_name"), banner_name);
                wxHomeClickResult.put("event", "homeclick");

                if (StringUtils.isNotBlank(user_id)) {
                    collector.collect(wxHomeClickResult);
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
