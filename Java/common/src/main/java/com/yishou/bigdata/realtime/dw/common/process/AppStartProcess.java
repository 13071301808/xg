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

public class AppStartProcess extends ProcessFunction<String, JSONObject> {

    static Logger logger = LoggerFactory.getLogger(AppStartProcess.class);

    @Override
    public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {

        try {
            JSONObject data = JSON.parseObject(value);
            JSONObject scdata = data.getJSONObject("scdata");
            if (scdata != null && EventParseUtil.isEvent(data, "APPStart")) {
                String time = scdata.getString("time");
                String distinct_id = scdata.getString("distinct_id");
                JSONObject properties = scdata.getJSONObject("properties");
                String os = properties.getString("os");
                String isAorB = properties.getString("isAorB");
                String is_first_day = properties.getString("is_first_day");
                String is_first_time = properties.getString("is_first_time");
                String resume_from_background = properties.getString("resume_from_background");
                String userid = properties.getString("userid");
                String app_version = properties.getString("app_version");
                String network_type = properties.getString("network_type");
                String manufacturer = properties.getString("manufacturer");
                String device_id = properties.getString("device_id");
                String os_version = properties.getString("os_version");
                String carrier = properties.getString("carrier");
                String isPro = properties.getString("isPro");
                String model = properties.getString("model");
                String oaid = properties.getString("oaid");
                String ad_id = properties.getString("ad_id");
                String ad_idMD5 = properties.getString("ad_idMD5");
                String level = properties.getString("level");
                String receiveTime = data.getString("__time__");
                String ip = data.getString("__client_ip__");

                JSONObject appStartResult = new JSONObject();
                appStartResult.put(ModelUtil.humpToUnderline("distinct_id"), distinct_id);
                appStartResult.put(ModelUtil.humpToUnderline("properties"), properties);
                appStartResult.put(ModelUtil.humpToUnderline("os"), os);
                appStartResult.put(ModelUtil.humpToUnderline("isAorB"), isAorB);
                appStartResult.put(ModelUtil.humpToUnderline("is_first_day"), is_first_day);
                appStartResult.put(ModelUtil.humpToUnderline("is_first_time"), is_first_time);
                appStartResult.put(ModelUtil.humpToUnderline("resume_from_background"), resume_from_background);
                appStartResult.put(ModelUtil.humpToUnderline("user_id"), userid);
                appStartResult.put(ModelUtil.humpToUnderline("app_version"), app_version);
                appStartResult.put(ModelUtil.humpToUnderline("network_type"), network_type);
                appStartResult.put(ModelUtil.humpToUnderline("manufacturer"), manufacturer);
                appStartResult.put(ModelUtil.humpToUnderline("device_id"), device_id);
                appStartResult.put(ModelUtil.humpToUnderline("os_version"), os_version);
                appStartResult.put(ModelUtil.humpToUnderline("carrier"), carrier);
                appStartResult.put(ModelUtil.humpToUnderline("isPro"), isPro);
                appStartResult.put(ModelUtil.humpToUnderline("model"), model);
                appStartResult.put(ModelUtil.humpToUnderline("oaid"), oaid);
                appStartResult.put(ModelUtil.humpToUnderline("ad_id"), ad_id);
                appStartResult.put(ModelUtil.humpToUnderline("ad_idMD5"), ad_idMD5);
                appStartResult.put(ModelUtil.humpToUnderline("level"), level);

                appStartResult.put("event_time", time);
                appStartResult.put("event", "appstart");
                appStartResult.put(ModelUtil.humpToUnderline("receiveTime"), receiveTime);
                appStartResult.put(ModelUtil.humpToUnderline("ip"), ip);

                if (StringUtils.isNotBlank(distinct_id)) {
                    collector.collect(appStartResult);
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
