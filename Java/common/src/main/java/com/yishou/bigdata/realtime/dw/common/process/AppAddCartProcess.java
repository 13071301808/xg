package com.yishou.bigdata.realtime.dw.common.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.realtime.dw.common.utils.DateUtil;
import com.yishou.bigdata.realtime.dw.common.utils.EventParseUtil;
import com.yishou.bigdata.realtime.dw.common.utils.ModelUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppAddCartProcess extends ProcessFunction<String, JSONObject> {

    static Logger logger = LoggerFactory.getLogger(AppAddCartProcess.class);

    @Override
    public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {

        try {
            JSONObject data = JSON.parseObject(value);
            if (EventParseUtil.isEvent(data, "addcart")) {
                JSONObject scdata = data.getJSONObject("scdata");
                JSONObject properties = scdata.getJSONObject("properties");
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
                String appGoodsPriceTotal = properties.getString("AppGoodsPriceTotal");
                String time = scdata.getString("time");
                String systemOs = scdata.getString("systemOs");
                String event = scdata.getString("event");
                String receiveTime = data.getString("__time__");

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
                addCartResult.put(ModelUtil.humpToUnderline("appGoodsPriceTotal"), appGoodsPriceTotal);
                addCartResult.put("event_time", time);
                addCartResult.put(ModelUtil.humpToUnderline("systemOs"), systemOs);
                addCartResult.put("event", "addcart");
                addCartResult.put(ModelUtil.humpToUnderline("receiveTime"), receiveTime);
                addCartResult.put("dt", DateUtil.secondToSpecialDate(Long.parseLong(receiveTime)));
                addCartResult.put("event_name", "dwd_goods_add_cart");

                if (StringUtils.isNotBlank(goodsId)) {
                    collector.collect(addCartResult);
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
