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

public class GoodsDetailPageExposureProcess extends ProcessFunction<String, JSONObject> {
    static Logger logger = LoggerFactory.getLogger(GoodsDetailPageExposureProcess.class);

    @Override
    public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
        try {
            JSONObject jsonText = JSON.parseObject(value);
            if (EventParseUtil.isEvent(jsonText, "gooddetailpageexposure")) {
                JSONObject scdata = jsonText.getJSONObject("scdata");
                if (scdata != null) {
                    JSONObject properties = scdata.getJSONObject("properties");
                    String time = scdata.getString("time");
                    String appVersion = properties.getString("app_version");
                    String os = properties.getString("os");
                    String userId = properties.getString("userid");
                    String manufacturer = properties.getString("manufacturer");
                    String stayTime = properties.getString("ptime");
                    String deviceId = properties.getString("device_id");
                    String osVersion = properties.getString("os_version");
                    String goodsId = properties.getString("goods_id");
                    String carrier = properties.getString("carrier");
                    String networkType = properties.getString("network_type");
                    String event = scdata.getString("event");


                    JSONObject goodsDetailPageExposureResult = new JSONObject();
                    goodsDetailPageExposureResult.put("event_time", time);
                    goodsDetailPageExposureResult.put(ModelUtil.humpToUnderline("appVersion"), appVersion);
                    goodsDetailPageExposureResult.put("os", os);
                    goodsDetailPageExposureResult.put(ModelUtil.humpToUnderline("userId"), userId);
                    goodsDetailPageExposureResult.put("manufacturer", manufacturer);
                    goodsDetailPageExposureResult.put(ModelUtil.humpToUnderline("stayTime"), stayTime);
                    goodsDetailPageExposureResult.put(ModelUtil.humpToUnderline("deviceId"), deviceId);
                    goodsDetailPageExposureResult.put(ModelUtil.humpToUnderline("osVersion"), osVersion);
                    goodsDetailPageExposureResult.put(ModelUtil.humpToUnderline("goodsId"), goodsId);
                    goodsDetailPageExposureResult.put("carrier", carrier);
                    goodsDetailPageExposureResult.put(ModelUtil.humpToUnderline("networkType"), networkType);
                    goodsDetailPageExposureResult.put("event", event);
                    //过滤掉没有user_id的数据
                    if (!StringUtils.isBlank(userId)) {
                        collector.collect(goodsDetailPageExposureResult);
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
