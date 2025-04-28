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

/**
 * @date: 2023/1/5
 * @author: yangshibiao
 * @desc: APP个人中心订阅档口页曝光
 */
public class AppSubscribeStallPageExposureProcess extends ProcessFunction<String, JSONObject> {

    static Logger logger = LoggerFactory.getLogger(AppSubscribeStallPageExposureProcess.class);

    @Override
    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

        try {

            JSONObject jsonText = JSON.parseObject(value);
            if (EventParseUtil.isEvent(jsonText, "subscribestallpageexposure")) {

                JSONObject scdata = jsonText.getJSONObject("scdata");
                if (scdata != null) {

                    // 数据解析
                    String receiveTime = jsonText.getString("__time__");
                    String userId = scdata.getString("user_id");
                    if (StringUtils.isBlank(userId)) {
                        userId = scdata.getString("userid");
                    }
                    String os = scdata.getString("os");
                    String time = jsonText.getString("__time__");
                    String appVersion = scdata.getString("app_version");
                    String carrier = scdata.getString("carrier");
                    String deviceId = scdata.getString("device_id");
                    String isAorB = scdata.getString("isAorB");
                    String manufacturer = scdata.getString("manufacturer");
                    String model = scdata.getString("model");
                    String networkType = scdata.getString("network_type");
                    String osVersion = scdata.getString("os_version");
                    String eventId = scdata.getString("event_id");
                    String exposureType = scdata.getString("exposure_type");

                    // 数据封装
                    JSONObject subscribePageExposureResult = new JSONObject();
                    subscribePageExposureResult.put(ModelUtil.humpToUnderline("receiveTime"), receiveTime);
                    subscribePageExposureResult.put(ModelUtil.humpToUnderline("userId"), userId);
                    subscribePageExposureResult.put(ModelUtil.humpToUnderline("os"), os);
                    subscribePageExposureResult.put(ModelUtil.humpToUnderline("time"), time);
                    subscribePageExposureResult.put(ModelUtil.humpToUnderline("appVersion"), appVersion);
                    subscribePageExposureResult.put(ModelUtil.humpToUnderline("carrier"), carrier);
                    subscribePageExposureResult.put(ModelUtil.humpToUnderline("deviceId"), deviceId);
                    subscribePageExposureResult.put("is_AorB", isAorB);
                    subscribePageExposureResult.put(ModelUtil.humpToUnderline("manufacturer"), manufacturer);
                    subscribePageExposureResult.put(ModelUtil.humpToUnderline("model"), model);
                    subscribePageExposureResult.put(ModelUtil.humpToUnderline("networkType"), networkType);
                    subscribePageExposureResult.put(ModelUtil.humpToUnderline("osVersion"), osVersion);
                    subscribePageExposureResult.put(ModelUtil.humpToUnderline("eventId"), eventId);
                    subscribePageExposureResult.put(ModelUtil.humpToUnderline("exposureType"), exposureType);

                    // 注意：根据业务，不管 supplier_arr 是否为null，都要输出数据（如果为null就不输出这些属性，不为null就输出这些属性）
                    JSONArray supplierArr = scdata.getJSONArray("supplier_arr");
                    if (supplierArr != null) {

                        for (Object supplierObject : supplierArr) {

                            JSONObject supplierJson = JSON.parseObject(JSON.toJSONString(supplierObject));
                            if (supplierJson != null) {

                                // 数据解析
                                String supplierId = supplierJson.getString("supplier_id");
                                String supplierName = supplierJson.getString("supplier_name");
                                String location = supplierJson.getString("location");
                                String homeIndex = supplierJson.getString("home_index");
                                String firstTab = supplierJson.getString("first_tab");
                                String contentType = supplierJson.getString("content_type");

                                // 数据封装
                                subscribePageExposureResult.put(ModelUtil.humpToUnderline("supplierId"), supplierId);
                                subscribePageExposureResult.put(ModelUtil.humpToUnderline("supplierName"), supplierName);
                                subscribePageExposureResult.put(ModelUtil.humpToUnderline("location"), location);
                                subscribePageExposureResult.put(ModelUtil.humpToUnderline("homeIndex"), homeIndex);
                                subscribePageExposureResult.put(ModelUtil.humpToUnderline("firstTab"), firstTab);
                                subscribePageExposureResult.put(ModelUtil.humpToUnderline("contentType"), contentType);

                                // 数据发送
                                out.collect(subscribePageExposureResult);

                            }

                        }

                    } else {

                        // 数据输出
                        out.collect(subscribePageExposureResult);

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
