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

public class LoginProcess extends ProcessFunction<String, JSONObject> {

    static Logger logger = LoggerFactory.getLogger(LoginProcess.class);

    @Override
    public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {

        try {
            JSONObject data = JSON.parseObject(value);
            JSONObject scdata = data.getJSONObject("scdata");
            if (scdata != null && EventParseUtil.isEvent(data, "login")) {
                String time = scdata.getString("time");
                String distinctId = scdata.getString("distinct_id");
                String receiveTime = data.getString("__time__");

                JSONObject properties = scdata.getJSONObject("properties");
                String os = properties.getString("os");
                String isAorB = properties.getString("isAorB");
                String userId = properties.getString("userid");
                String appVersion = properties.getString("app_version");
                String deviceId = properties.getString("device_id");
                String networkType = properties.getString("network_type");
                String manufacturer = properties.getString("manufacturer");
                String carrier = properties.getString("carrier");
                String udid = properties.getString("udid");
                String model = properties.getString("model");
                String osVersion = properties.getString("os_version");
                String loginMethod = properties.getString("loginMethod");
                String isPro = properties.getString("isPro");
                String oaid = properties.getString("oaid");
                String adId = properties.getString("ad_id");
                String adIdMD5 = properties.getString("ad_idMD5");
                String level = properties.getString("level");

                String ip = data.getString("__client_ip__");

                JSONObject loginResult = new JSONObject();
                loginResult.put(ModelUtil.humpToUnderline("time"), time);
                loginResult.put(ModelUtil.humpToUnderline("distinct_id"), distinctId);
                loginResult.put(ModelUtil.humpToUnderline("os"), os);
                loginResult.put(ModelUtil.humpToUnderline("is_aorb"), isAorB);
                loginResult.put(ModelUtil.humpToUnderline("user_id"), userId);
                loginResult.put(ModelUtil.humpToUnderline("app_version"), appVersion);
                loginResult.put(ModelUtil.humpToUnderline("device_id"), deviceId);
                loginResult.put(ModelUtil.humpToUnderline("network_type"), networkType);
                loginResult.put(ModelUtil.humpToUnderline("manufacturer"), manufacturer);
                loginResult.put(ModelUtil.humpToUnderline("carrier"), carrier);
                loginResult.put(ModelUtil.humpToUnderline("udid"), udid);
                loginResult.put(ModelUtil.humpToUnderline("model"), model);
                loginResult.put(ModelUtil.humpToUnderline("os_version"), osVersion);
                loginResult.put(ModelUtil.humpToUnderline("login_method"), loginMethod);
                loginResult.put(ModelUtil.humpToUnderline("is_pro"), isPro);
                loginResult.put(ModelUtil.humpToUnderline("oaid"), oaid);
                loginResult.put(ModelUtil.humpToUnderline("ad_id"), adId);
                loginResult.put(ModelUtil.humpToUnderline("ad_id_md5"), adIdMD5);
                loginResult.put(ModelUtil.humpToUnderline("level"), level);

                loginResult.put(ModelUtil.humpToUnderline("ip"), ip);
                loginResult.put(ModelUtil.humpToUnderline("event_time"), time);
                loginResult.put(ModelUtil.humpToUnderline("event"), "login");
                loginResult.put(ModelUtil.humpToUnderline("receiveTime"), receiveTime);

                if (StringUtils.isNotBlank(userId)) {
                    collector.collect(loginResult);
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
