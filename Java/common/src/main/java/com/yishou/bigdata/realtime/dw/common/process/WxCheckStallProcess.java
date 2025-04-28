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

public class WxCheckStallProcess extends ProcessFunction<String, JSONObject> {
    static Logger logger = LoggerFactory.getLogger(WxCheckStallProcess.class);

    @Override
    public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
        try {
            JSONObject data = JSON.parseObject(value);
            JSONObject eventData = data.getJSONObject("data");
            if (eventData != null && EventParseUtil.isEvent(data, "checkstall")) {
                // 日志流自带
                String logTime = data.getString("__time__");
                String receiveTime = data.getString("__time__");
                String logSource = data.getString("__client_ip__");
                String activity = data.getString("activity");

                // 自定义数据
                String user_id = eventData.getString("userid");
                String market_id = eventData.getString("marketID");
                String special_id = eventData.getString("specialID");
                String special_name = eventData.getString("specialName");
                String stall_id = eventData.getString("stallID");
                String stall_source = eventData.getString("stallSource");
                String two_level_stall_source = eventData.getString("twoLevelStallSource");
                String is_operat = eventData.getString("is_operat");

                // 输出流
                JSONObject wxCheckStallResult = new JSONObject();
                wxCheckStallResult.put(ModelUtil.humpToUnderline("log_source"), logSource);
                wxCheckStallResult.put(ModelUtil.humpToUnderline("log_time"), logTime);
                wxCheckStallResult.put(ModelUtil.humpToUnderline("receive_time"), receiveTime);
                wxCheckStallResult.put(ModelUtil.humpToUnderline("activity"), activity);
                wxCheckStallResult.put(ModelUtil.humpToUnderline("user_id"), user_id);
                wxCheckStallResult.put(ModelUtil.humpToUnderline("market_id"), market_id);
                wxCheckStallResult.put(ModelUtil.humpToUnderline("special_id"), special_id);
                wxCheckStallResult.put(ModelUtil.humpToUnderline("special_name"), special_name);
                wxCheckStallResult.put(ModelUtil.humpToUnderline("stall_id"), stall_id);
                wxCheckStallResult.put(ModelUtil.humpToUnderline("stall_source"), stall_source);
                wxCheckStallResult.put(ModelUtil.humpToUnderline("two_level_stall_source"), two_level_stall_source);
                wxCheckStallResult.put(ModelUtil.humpToUnderline("is_operat"), is_operat);
                wxCheckStallResult.put("event", "checkstall");

                if (StringUtils.isNotBlank(user_id)) {
                    collector.collect(wxCheckStallResult);
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
