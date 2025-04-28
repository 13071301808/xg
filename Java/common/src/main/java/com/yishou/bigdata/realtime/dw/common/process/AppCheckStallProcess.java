package com.yishou.bigdata.realtime.dw.common.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.realtime.dw.common.utils.EventParseUtil;
import com.yishou.bigdata.realtime.dw.common.utils.ModelUtil;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @date: 2023/1/3
 * @author: yangshibiao
 * @desc: APP查看档口解析
 */
public class AppCheckStallProcess extends ProcessFunction<String, JSONObject> {

    static Logger logger = LoggerFactory.getLogger(AppCheckStallProcess.class);

    @Override
    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

        try {

            JSONObject jsonText = JSON.parseObject(value);
            if (EventParseUtil.isEvent(jsonText, "checkstall")) {

                JSONObject scdata = jsonText.getJSONObject("scdata");
                if (scdata != null) {

                    JSONObject properties = scdata.getJSONObject("properties");
                    if (properties != null) {

                        // 数据解析
                        String distinctId = scdata.getString("distinct_id");
                        String appVersion = properties.getString("app_version");
                        String carrier = properties.getString("carrier");
                        String deviceId = properties.getString("device_id");
                        String isAorB = properties.getString("isAorB");
                        String isPro = properties.getString("isPro");
                        String manufacturer = properties.getString("manufacturer");
                        String marketId = properties.getString("marketID");
                        String model = properties.getString("model");
                        String networkType = properties.getString("network_type");
                        String os = properties.getString("os");
                        String osVersion = properties.getString("os_version");
                        String ptime = properties.getString("ptime");
                        String specialId = properties.getString("special_id");
                        String specialName = properties.getString("specialName");
                        String stallId = properties.getString("stallID");
                        String stallName = properties.getString("stallName");
                        String stallSource = properties.getString("stallSource");
                        String udid = properties.getString("udid");
                        String userId = properties.getString("userid");
                        String eventId = properties.getString("event_id");
                        String landingEventId = properties.getString("landing_event_id");
                        String campaignEventId = properties.getString("campaign_event_id");
                        String twoLevelStallSource = properties.getString("twoLevelStallSource");
                        String searchText = properties.getString("searchText");
                        String receiveTime = jsonText.getString("__time__");

                        // 数据封装
                        JSONObject checkStallResult = new JSONObject();
                        checkStallResult.put(ModelUtil.humpToUnderline("distinctId"), distinctId);
                        checkStallResult.put(ModelUtil.humpToUnderline("appVersion"), appVersion);
                        checkStallResult.put(ModelUtil.humpToUnderline("carrier"), carrier);
                        checkStallResult.put(ModelUtil.humpToUnderline("deviceId"), deviceId);
                        checkStallResult.put("is_aorb", isAorB);
                        checkStallResult.put(ModelUtil.humpToUnderline("isPro"), isPro);
                        checkStallResult.put(ModelUtil.humpToUnderline("manufacturer"), manufacturer);
                        checkStallResult.put(ModelUtil.humpToUnderline("marketId"), marketId);
                        checkStallResult.put(ModelUtil.humpToUnderline("model"), model);
                        checkStallResult.put(ModelUtil.humpToUnderline("networkType"), networkType);
                        checkStallResult.put(ModelUtil.humpToUnderline("os"), os);
                        checkStallResult.put(ModelUtil.humpToUnderline("osVersion"), osVersion);
                        checkStallResult.put(ModelUtil.humpToUnderline("ptime"), ptime);
                        checkStallResult.put(ModelUtil.humpToUnderline("specialId"), specialId);
                        checkStallResult.put(ModelUtil.humpToUnderline("specialName"), specialName);
                        checkStallResult.put(ModelUtil.humpToUnderline("stallId"), stallId);
                        checkStallResult.put(ModelUtil.humpToUnderline("stallName"), stallName);
                        checkStallResult.put(ModelUtil.humpToUnderline("stallSource"), stallSource);
                        checkStallResult.put(ModelUtil.humpToUnderline("udid"), udid);
                        checkStallResult.put(ModelUtil.humpToUnderline("userId"), userId);
                        checkStallResult.put(ModelUtil.humpToUnderline("eventId"), eventId);
                        checkStallResult.put(ModelUtil.humpToUnderline("landingEventId"), landingEventId);
                        checkStallResult.put(ModelUtil.humpToUnderline("campaignEventId"), campaignEventId);
                        checkStallResult.put(ModelUtil.humpToUnderline("twoLevelStallSource"), twoLevelStallSource);
                        checkStallResult.put(ModelUtil.humpToUnderline("searchText"), searchText);
                        checkStallResult.put(ModelUtil.humpToUnderline("receiveTime"), receiveTime);

                        // 数据发送
                        out.collect(checkStallResult);

                    }

                }

            }

        } catch (Exception e) {
            logger.warn(
                    "***** 埋点数据解析异常，不能解析成json字符串，传入的埋点数据为：{}， 抛出的异常信息为：{}",
                    value,
                    e.getMessage()
            );
        }

    }

}
