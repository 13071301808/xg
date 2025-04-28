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

public class WxGoodsDetailPageProcess extends ProcessFunction<String, JSONObject> {
    static Logger logger = LoggerFactory.getLogger(WxGoodsDetailPageProcess.class);
    @Override
    public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
        try {
            JSONObject data = JSON.parseObject(value);
            JSONObject eventData = data.getJSONObject("data");
            if (eventData != null && EventParseUtil.isEvent(data, "gooddetailpage")) {
                // 日志流自带
                String logTime = data.getString("__time__");
                String receiveTime = data.getString("__time__");
                String logSource = data.getString("__client_ip__");
                String activity = data.getString("activity");

                // 自定义数据
                String user_id = eventData.getString("userid");
                String goods_id = eventData.getString("goodsId");
                String share_user_id = eventData.getString("share_user_id");
                String source = eventData.getString("source");

                // 输出流
                JSONObject wxGoodsDetailPageResult = new JSONObject();
                wxGoodsDetailPageResult.put(ModelUtil.humpToUnderline("log_source"), logSource);
                wxGoodsDetailPageResult.put(ModelUtil.humpToUnderline("log_time"), logTime);
                wxGoodsDetailPageResult.put(ModelUtil.humpToUnderline("receive_time"), receiveTime);
                wxGoodsDetailPageResult.put(ModelUtil.humpToUnderline("activity"), activity);
                wxGoodsDetailPageResult.put(ModelUtil.humpToUnderline("user_id"), user_id);
                wxGoodsDetailPageResult.put(ModelUtil.humpToUnderline("goods_id"), goods_id);
                wxGoodsDetailPageResult.put(ModelUtil.humpToUnderline("share_user_id"), share_user_id);
                wxGoodsDetailPageResult.put(ModelUtil.humpToUnderline("source"), source);
                wxGoodsDetailPageResult.put("event", "gooddetailpage");

                if (StringUtils.isNotBlank(user_id)) {
                    collector.collect(wxGoodsDetailPageResult);
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
