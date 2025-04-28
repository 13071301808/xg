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

public class AppAddcartCollectGoodsProcess extends ProcessFunction<String, JSONObject> {
    static Logger logger = LoggerFactory.getLogger(AppAddcartCollectGoodsProcess.class);

    @Override
    public void processElement(String value, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

        try {
            JSONObject data = JSON.parseObject(value);
            JSONObject scdata = data.getJSONObject("scdata");
            JSONObject properties = scdata.getJSONObject("properties");
            // 判断加购数据
            if (EventParseUtil.isEvent(data, "addcart")) {
                collector.collect(parseGoodsAddcart(data, scdata, properties));
            }
            //判断收藏数据
            if (EventParseUtil.isEvent(data, "goodcollect")) {
                collector.collect(parseGoodsCollect(data, scdata, properties));
            }
        } catch (Exception e) {
            logger.warn(
                    "***** 埋点数据解析异常，不能解析成json字符串，传入的埋点数据为：{}， 抛出的异常信息为：{}",
                    value,
                    e.getMessage());
        }

    }

    public JSONObject parseGoodsAddcart(JSONObject jsonText, JSONObject scdata, JSONObject properties) {
        String appVersion = properties.getString("app_version");
        String userId = properties.getString("userid");
        String goodsId = properties.getString("goodID");
        String source = properties.getString("source");
        String deviceId = properties.getString("device_id");
        String goodsNo = properties.getString("goods_no");
        String catId = properties.getString("cat_id");
        String networkType = properties.getString("network_type");
        String carrier = properties.getString("carrier");
        String goodsNumber = properties.getString("goodNumber");
        String goodsPrice = properties.getString("AppGoodPrice");
        String time = scdata.getString("time");
        String systemOs = scdata.getString("systemOs");
        String event = scdata.getString("event");
        String receiveTime = jsonText.getString("__time__");

        JSONObject addCartResult = new JSONObject();
        addCartResult.put(ModelUtil.humpToUnderline("appVersion"), appVersion);
        addCartResult.put(ModelUtil.humpToUnderline("userId"), userId);
        addCartResult.put(ModelUtil.humpToUnderline("goodsId"), goodsId);
        addCartResult.put("source", source);
        addCartResult.put(ModelUtil.humpToUnderline("deviceId"), deviceId);
        addCartResult.put(ModelUtil.humpToUnderline("goodsNo"), goodsNo);
        addCartResult.put(ModelUtil.humpToUnderline("catId"), catId);
        addCartResult.put(ModelUtil.humpToUnderline("networkType"), networkType);
        addCartResult.put("carrier", carrier);
        addCartResult.put(ModelUtil.humpToUnderline("goodsNumber"), goodsNumber);
        addCartResult.put(ModelUtil.humpToUnderline("goodsPrice"), goodsPrice);
        addCartResult.put("event_time", time);
        addCartResult.put(ModelUtil.humpToUnderline("systemOs"), systemOs);
        addCartResult.put("event", event);
        addCartResult.put(ModelUtil.humpToUnderline("receiveTime"), receiveTime);
        addCartResult.put(ModelUtil.humpToUnderline("dataType"), "addcart");
        if (StringUtils.isNotBlank(goodsId)) {
            return addCartResult;
        } else {
            return null;
        }

    }

    public JSONObject parseGoodsCollect(JSONObject jsonText, JSONObject scdata, JSONObject properties) {
        String distinctId = scdata.getString("distinct_id");
        String systemOs = scdata.getString("systemOs");
        String time = scdata.getString("time");
        String appVersion = properties.getString("app_version");
        String goodId = properties.getString("goodID");
        String source = properties.getString("source");
        String userId = properties.getString("userid");
        String catId = properties.getString("cat_id");
        String carrier = properties.getString("carrier");
        String networkType = properties.getString("network_type");
        String osVersion = properties.getString("os_version");
        String goodsNo = properties.getString("goods_no");
        String event = scdata.getString("event");
        String receiveTime = jsonText.getString("__time__");

        JSONObject goodsCollectResult = new JSONObject();
        goodsCollectResult.put(ModelUtil.humpToUnderline("distinctId"), distinctId);
        goodsCollectResult.put(ModelUtil.humpToUnderline("systemOs"), systemOs);
        goodsCollectResult.put("event_time", time);
        goodsCollectResult.put(ModelUtil.humpToUnderline("appVersion"), appVersion);
        goodsCollectResult.put("goods_id", goodId);
        goodsCollectResult.put("source", source);
        goodsCollectResult.put(ModelUtil.humpToUnderline("userId"), userId);
        goodsCollectResult.put(ModelUtil.humpToUnderline("catId"), catId);
        goodsCollectResult.put("carrier", carrier);
        goodsCollectResult.put(ModelUtil.humpToUnderline("networkType"), networkType);
        goodsCollectResult.put(ModelUtil.humpToUnderline("osVersion"), osVersion);
        goodsCollectResult.put(ModelUtil.humpToUnderline("goodsNo"), goodsNo);
        goodsCollectResult.put("event", event);
        goodsCollectResult.put(ModelUtil.humpToUnderline("receiveTime"), receiveTime);
        goodsCollectResult.put(ModelUtil.humpToUnderline("dataType"), "collect");

        if (StringUtils.isNotBlank(goodId)) {
            return goodsCollectResult;
        } else {
            return null;
        }

    }
}
