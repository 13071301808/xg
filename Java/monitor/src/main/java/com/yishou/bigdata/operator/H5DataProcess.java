package com.yishou.bigdata.operator;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.realtime.dw.common.utils.ModelUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Objects;

public class H5DataProcess extends ProcessFunction<String, JSONObject> {
    static Logger logger = LoggerFactory.getLogger(H5DataProcess.class);

    @Override
    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        try {
            JSONObject data = JSON.parseObject(value);
            JSONObject eventData = data.getJSONObject("data");
            String event = data.getString("__topic__");

            // 日志流自带
            String receive_time = data.getString("__time__");
            String activity = data.getString("activity");

            // 自定义数据
            String user_id = eventData.getString("userid");
            String add_cart_id = eventData.getString("addCartId");
            String goods_id = "";
            String goods_no= "";
            String special_id= "";
            String supply_id= "";
            String good_price = eventData.getString("goodPrice");
            String good_number = eventData.getString("goodNumber");
            String promotion_id = eventData.getString("promotion_id");
            String share_user_id = eventData.getString("share_user_id");
            String wx_enter_source_id = eventData.getString("wx_enter_source_id");
            String poster_share_user_id = eventData.getString("poster_share_user_id");

            if (eventData.containsKey("goodsId")) {
                goods_id = eventData.getString("goodsId");
            }else{
                goods_id = eventData.getString("goods_id");
            }
            if (eventData.containsKey("goodsNo")) {
                goods_no = eventData.getString("goodsNo");
            }else{
                goods_no = eventData.getString("goods_no");
            }
            String source = eventData.getString("source");
            if (eventData.containsKey("specialId")) {
                special_id = eventData.getString("specialId");
            }else{
                special_id = eventData.getString("special_id");
            }
            if (eventData.containsKey("supplyId")) {
                supply_id = eventData.getString("supplyId");
            }else{
                supply_id = eventData.getString("supply_id");
            }

            // 输出流
            JSONObject h5DataResult = new JSONObject();
            h5DataResult.put("log_database","h5_log_store");
            h5DataResult.put(ModelUtil.humpToUnderline("receive_time"), receive_time);
            h5DataResult.put(ModelUtil.humpToUnderline("activity"), activity);
            h5DataResult.put(ModelUtil.humpToUnderline("event"), event);
            h5DataResult.put(ModelUtil.humpToUnderline("user_id"), user_id);
            h5DataResult.put(ModelUtil.humpToUnderline("add_cart_id"), add_cart_id);
            h5DataResult.put(ModelUtil.humpToUnderline("goods_id"), goods_id);
            h5DataResult.put(ModelUtil.humpToUnderline("goods_no"), goods_no);
            h5DataResult.put(ModelUtil.humpToUnderline("source"), source);
            h5DataResult.put(ModelUtil.humpToUnderline("special_id"), special_id);
            h5DataResult.put(ModelUtil.humpToUnderline("supply_id"), supply_id);
            h5DataResult.put(ModelUtil.humpToUnderline("good_price"), good_price);
            h5DataResult.put(ModelUtil.humpToUnderline("good_number"), good_number);
            h5DataResult.put(ModelUtil.humpToUnderline("promotion_id"), promotion_id);
            h5DataResult.put(ModelUtil.humpToUnderline("share_user_id"), share_user_id);
            h5DataResult.put(ModelUtil.humpToUnderline("wx_enter_source_id"), wx_enter_source_id);
            h5DataResult.put(ModelUtil.humpToUnderline("poster_share_user_id"), poster_share_user_id);

            if (StringUtils.isNotBlank(user_id)) {
                out.collect(h5DataResult);
            }
        } catch (Exception e) {
            logger.warn(
                    "***** 埋点数据解析异常，不能解析成json字符串，传入的埋点数据为：{}， 抛出的异常信息为：{}",
                    value,
                    e.getMessage());
        }
    }
}
