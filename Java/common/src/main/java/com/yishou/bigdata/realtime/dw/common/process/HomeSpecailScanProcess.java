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

public class HomeSpecailScanProcess extends ProcessFunction<String, JSONObject> {

    static Logger logger = LoggerFactory.getLogger(HomeSpecailScanProcess.class);

    @Override
    public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {

        try {
            JSONObject data = JSON.parseObject(value);
            JSONObject scdata = data.getJSONObject("scdata");
            if (scdata != null && EventParseUtil.isEvent(data, "homeSpecailScan")) {
                String time = scdata.getString("time");
                String distinctId = scdata.getString("distinct_id");
                String receiveTime = data.getString("__time__");
                String ip = data.getString("__client_ip__");

                JSONObject properties = scdata.getJSONObject("properties");
                String tabId = properties.getString("tabId");
                String tabName = properties.getString("tabName");
                String appVersion = properties.getString("app_version");
                String carrier = properties.getString("carrier");
                String deviceId = properties.getString("device_id");
                String isAorB = properties.getString("isAorB");
                String isPro = properties.getString("isPro");
                String manufacturer = properties.getString("manufacturer");
                String networkType = properties.getString("network_type");
                String os = properties.getString("os");
                String osVersion = properties.getString("os_version");
                String ptime = properties.getString("ptime");
                String udid = properties.getString("udid");
                String userId = properties.getString("userid");
                String model = properties.getString("model");
                String oaid = properties.getString("oaid");
                String adId = properties.getString("ad_id");
                String adIdMD5 = properties.getString("ad_idMD5");
                String level = properties.getString("level");

                JSONObject eventResult = new JSONObject();
                eventResult.put(ModelUtil.humpToUnderline("time"), time);
                eventResult.put(ModelUtil.humpToUnderline("distinct_id"), distinctId);
                eventResult.put(ModelUtil.humpToUnderline("tab_id"), tabId);
                eventResult.put(ModelUtil.humpToUnderline("tab_name"), tabName);
                eventResult.put(ModelUtil.humpToUnderline("app_version"), appVersion);
                eventResult.put(ModelUtil.humpToUnderline("carrier"), carrier);
                eventResult.put(ModelUtil.humpToUnderline("device_id"), deviceId);
                eventResult.put(ModelUtil.humpToUnderline("is_aorb"), isAorB);
                eventResult.put(ModelUtil.humpToUnderline("is_pro"), isPro);
                eventResult.put(ModelUtil.humpToUnderline("manufacturer"), manufacturer);
                eventResult.put(ModelUtil.humpToUnderline("network_type"), networkType);
                eventResult.put(ModelUtil.humpToUnderline("os"), os);
                eventResult.put(ModelUtil.humpToUnderline("os_version"), osVersion);
                eventResult.put(ModelUtil.humpToUnderline("ptime"), ptime);
                eventResult.put(ModelUtil.humpToUnderline("udid"), udid);
                eventResult.put(ModelUtil.humpToUnderline("user_id"), userId);
                eventResult.put(ModelUtil.humpToUnderline("model"), model);
                eventResult.put(ModelUtil.humpToUnderline("oaid"), oaid);
                eventResult.put(ModelUtil.humpToUnderline("ad_id"), adId);
                eventResult.put(ModelUtil.humpToUnderline("ad_id_md5"), adIdMD5);
                eventResult.put(ModelUtil.humpToUnderline("level"), level);

                eventResult.put(ModelUtil.humpToUnderline("ip"), ip);
                eventResult.put(ModelUtil.humpToUnderline("event_time"), time);
                eventResult.put(ModelUtil.humpToUnderline("event"), "homeSpecailScan");
                eventResult.put(ModelUtil.humpToUnderline("receiveTime"), receiveTime);

                if (StringUtils.isNotBlank(distinctId)) {
                    collector.collect(eventResult);
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
