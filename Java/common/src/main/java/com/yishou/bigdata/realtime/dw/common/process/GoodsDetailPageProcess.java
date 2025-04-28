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

public class GoodsDetailPageProcess extends ProcessFunction<String, JSONObject> {
    static Logger logger = LoggerFactory.getLogger(GoodsDetailPageProcess.class);

    @Override
    public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
        try {
            JSONObject data = JSON.parseObject(value);
            if (EventParseUtil.isEvent(data, "gooddetailpage")) {

                JSONObject scdata = data.getJSONObject("scdata");
                JSONObject properties = scdata.getJSONObject("properties");
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
                String receiveTime = data.getString("__time__");

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
                goodsCollectResult.put("event", "gooddetailpage");
                goodsCollectResult.put(ModelUtil.humpToUnderline("receiveTime"), receiveTime);

                if (StringUtils.isNotBlank(goodId)) {
                    collector.collect(goodsCollectResult);
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
