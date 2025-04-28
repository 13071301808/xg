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

public class WxAddCartProcess extends ProcessFunction<String, JSONObject> {
    static Logger logger = LoggerFactory.getLogger(WxEnterProcess.class);

    @Override
    public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
        try {
            JSONObject data = JSON.parseObject(value);
            JSONObject eventData = data.getJSONObject("data");
            if (eventData != null && EventParseUtil.isEvent(data, "addCart")) {
                // 日志流自带
                String logTime = data.getString("__time__");
                String receiveTime = data.getString("__time__");
                String logSource = data.getString("__client_ip__");
                String activity = data.getString("activity");

                // 自定义数据
                String user_id = eventData.getString("userid");
                String add_cart_id = eventData.getString("addCartId");
                String goods_id = eventData.getString("goodsId");
                String goods_no = eventData.getString("goodsNo");
                String model_source = eventData.getString("modelSource");
                String source = eventData.getString("source");
                String h5_url = eventData.getString("h5Url");
                String h5_activity_info = eventData.getString("h5ActivityInfo");
                String special_id = eventData.getString("specialId");
                String supply_id = eventData.getString("supplyId");
                String good_price = eventData.getString("goodPrice");
                String good_number = eventData.getString("goodNumber");
                String promotion_id = eventData.getString("promotion_id");
                String share_user_id = eventData.getString("share_user_id");
                String wx_enter_source_id = eventData.getString("wx_enter_source_id");
                String poster_share_user_id = eventData.getString("poster_share_user_id");

                // 输出流
                JSONObject wxAddCartResult = new JSONObject();
                wxAddCartResult.put(ModelUtil.humpToUnderline("log_source"), logSource);
                wxAddCartResult.put(ModelUtil.humpToUnderline("log_time"), logTime);
                wxAddCartResult.put(ModelUtil.humpToUnderline("receive_time"), receiveTime);
                wxAddCartResult.put(ModelUtil.humpToUnderline("activity"), activity);
                wxAddCartResult.put(ModelUtil.humpToUnderline("user_id"), user_id);
                wxAddCartResult.put(ModelUtil.humpToUnderline("add_cart_id"), add_cart_id);
                wxAddCartResult.put(ModelUtil.humpToUnderline("goods_id"), goods_id);
                wxAddCartResult.put(ModelUtil.humpToUnderline("goods_no"), goods_no);
                wxAddCartResult.put(ModelUtil.humpToUnderline("model_source"), model_source);
                wxAddCartResult.put(ModelUtil.humpToUnderline("source"), source);
                wxAddCartResult.put(ModelUtil.humpToUnderline("h5_url"), h5_url);
                wxAddCartResult.put(ModelUtil.humpToUnderline("h5_activity_info"), h5_activity_info);
                wxAddCartResult.put(ModelUtil.humpToUnderline("special_id"), special_id);
                wxAddCartResult.put(ModelUtil.humpToUnderline("supply_id"), supply_id);
                wxAddCartResult.put(ModelUtil.humpToUnderline("good_price"), good_price);
                wxAddCartResult.put(ModelUtil.humpToUnderline("good_number"), good_number);
                wxAddCartResult.put(ModelUtil.humpToUnderline("promotion_id"), promotion_id);
                wxAddCartResult.put(ModelUtil.humpToUnderline("share_user_id"), share_user_id);
                wxAddCartResult.put(ModelUtil.humpToUnderline("wx_enter_source_id"), wx_enter_source_id);
                wxAddCartResult.put(ModelUtil.humpToUnderline("poster_share_user_id"), poster_share_user_id);
                wxAddCartResult.put("event", "addcart");

                if (StringUtils.isNotBlank(user_id)) {
                    collector.collect(wxAddCartResult);
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
