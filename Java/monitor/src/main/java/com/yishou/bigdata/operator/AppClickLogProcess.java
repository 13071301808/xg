package com.yishou.bigdata.operator;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.realtime.dw.common.utils.DateUtil;
import com.yishou.bigdata.realtime.dw.common.utils.ModelUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AppClickLogProcess extends ProcessFunction<String, JSONObject> {
    static Logger logger = LoggerFactory.getLogger(AppClickLogProcess.class);

    @Override
    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        try {
            // 大体解析
            JSONObject data = JSON.parseObject(value);
            JSONObject scdata = data.getJSONObject("scdata");
            JSONObject properties = scdata.getJSONObject("properties");
            // 通用字段解析
            String event = scdata.getString("event");
            String receive_time = data.getString("__time__");
            // 自定义字段解析
            String user_id = properties.getString("userid");
            String goods_id = null;
            String source = properties.getString("source");
            String goods_no = properties.getString("goods_no");
            String cat_id = properties.getString("cat_id");
            String click_to_recommend = properties.getString("click_to_recommend");
            String event_id = properties.getString("event_id");
            String time = scdata.getString("time");

            // 兼容处理
            if (properties.containsKey("goodID")) {
                goods_id = properties.getString("goodID");
            } else if (properties.containsKey("goodId")) {
                goods_id = properties.getString("goodId");
            }else if (properties.containsKey("goods_id")) {
                goods_id = properties.getString("goods_id");
            }

            // 输出
            JSONObject appClickResult = new JSONObject();
            appClickResult.put("log_database","new_app_log_store");
            appClickResult.put(ModelUtil.humpToUnderline("user_id"), user_id);
            appClickResult.put(ModelUtil.humpToUnderline("goods_id"), goods_id);
            appClickResult.put(ModelUtil.humpToUnderline("source"), source);
            appClickResult.put(ModelUtil.humpToUnderline("goods_no"), goods_no);
            appClickResult.put(ModelUtil.humpToUnderline("event_id"), event_id);
            appClickResult.put(ModelUtil.humpToUnderline("cat_id"), cat_id);
            appClickResult.put(ModelUtil.humpToUnderline("click_to_recommend"), click_to_recommend);
            appClickResult.put(ModelUtil.humpToUnderline("time"), time);
            appClickResult.put(ModelUtil.humpToUnderline("event"), event);
            appClickResult.put(ModelUtil.humpToUnderline("receive_time"), receive_time);
            appClickResult.put("dt", DateUtil.secondToSpecialDate(Long.parseLong(receive_time)));
            // 简易判断
            if (StringUtils.isNotBlank(user_id)) {
                out.collect(appClickResult);
            }
        } catch (Exception e) {
            logger.warn(
                    "***** 埋点数据解析异常，不能解析成json字符串，传入的埋点数据为：{}， 抛出的异常信息为：{}",
                    value,
                    e.getMessage());
        }
    }
}
