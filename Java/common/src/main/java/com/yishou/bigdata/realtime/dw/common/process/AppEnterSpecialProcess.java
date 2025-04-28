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

public class AppEnterSpecialProcess extends ProcessFunction<String, JSONObject> {
    static Logger logger = LoggerFactory.getLogger(AppEnterSpecialProcess.class);

    @Override
    public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
        try {
            JSONObject data = JSON.parseObject(value);
            if (EventParseUtil.isEvent(data, "enterspecial")) {
                JSONObject scdata = data.getJSONObject("scdata");
                JSONObject properties = scdata.getJSONObject("properties");
                String appVersion = properties.getString("app_version");
                String time = scdata.getString("time");
                String systemOs = scdata.getString("systemOs");
                String event = scdata.getString("event");
                String receiveTime = data.getString("__time__");
                String user_id = properties.getString("userid");

                String tab_name = properties.getString("tabName");
                String special_id = properties.getString("specialId");
                String special_name = properties.getString("specialName");
                String special_source = properties.getString("specialSource");
                String special_pos = properties.getString("specialPos");
                String source_event_id = properties.getString("source_event_id");
                String event_id = properties.getString("event_id");
                String is_operat = properties.getString("is_operat");
                String source_special_id = properties.getString("source_special_id");
                String source_special_name = properties.getString("source_special_name");
                String special_goods_url = properties.getString("special_goods_url");
                String goods_no = properties.getString("goods_no");


                JSONObject enterSpecialResult = new JSONObject();
                enterSpecialResult.put(ModelUtil.humpToUnderline("appVersion"), appVersion);
                enterSpecialResult.put(ModelUtil.humpToUnderline("time"), time);
                enterSpecialResult.put(ModelUtil.humpToUnderline("systemOs"), systemOs);
                enterSpecialResult.put("event", "enterspecial");
                enterSpecialResult.put(ModelUtil.humpToUnderline("receiveTime"), receiveTime);
                enterSpecialResult.put("dt", DateUtil.secondToSpecialDate(Long.parseLong(receiveTime)));
                enterSpecialResult.put(ModelUtil.humpToUnderline("user_id"), user_id);
                enterSpecialResult.put(ModelUtil.humpToUnderline("tab_name"), tab_name);
                enterSpecialResult.put(ModelUtil.humpToUnderline("special_id"), special_id);
                enterSpecialResult.put(ModelUtil.humpToUnderline("special_name"), special_name);
                enterSpecialResult.put(ModelUtil.humpToUnderline("special_source"), special_source);
                enterSpecialResult.put(ModelUtil.humpToUnderline("special_pos"), special_pos);
                enterSpecialResult.put(ModelUtil.humpToUnderline("source_event_id"), source_event_id);
                enterSpecialResult.put(ModelUtil.humpToUnderline("event_id"), event_id);
                enterSpecialResult.put(ModelUtil.humpToUnderline("is_operat"), is_operat);
                enterSpecialResult.put(ModelUtil.humpToUnderline("source_special_id"), source_special_id);
                enterSpecialResult.put(ModelUtil.humpToUnderline("source_special_name"), source_special_name);
                enterSpecialResult.put(ModelUtil.humpToUnderline("special_goods_url"), special_goods_url);
                enterSpecialResult.put(ModelUtil.humpToUnderline("goods_no"), goods_no);

                if (StringUtils.isNotBlank(user_id)) {
                    collector.collect(enterSpecialResult);
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
