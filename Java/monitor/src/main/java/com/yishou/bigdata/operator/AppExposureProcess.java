package com.yishou.bigdata.operator;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.realtime.dw.common.process.AppAddCartProcess;
import com.yishou.bigdata.realtime.dw.common.utils.DateUtil;
import com.yishou.bigdata.realtime.dw.common.utils.ModelUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class AppExposureProcess extends ProcessFunction<String, JSONObject> {

    static Logger logger = LoggerFactory.getLogger(AppAddCartProcess.class);
    // 定义要监控的事件
    private static final Set<String> monitor_events = new HashSet<>(Arrays.asList(
        "goodsExposure","specialExposure"
    ));
    @Override
    public void processElement(String value, Context ctx, Collector<JSONObject> out) {
        try {
            // 大体解析
            JSONObject data = JSON.parseObject(value);
            JSONObject scdata = data.getJSONObject("scdata");
            String event = data.getString("topic");
            // 包含监控
            if (scdata != null && monitor_events.contains(event)) {
                // 通用字段解析
                String receive_time = data.getString("__time__");
                String user_id = scdata.getString("user_id");

                // 自定义字段
                JSONObject goods_arr = scdata.getJSONObject("goods_arr");
                String goods_id;
                String special_id;
                String pid;
                String goods_no;
                String index;
                String is_operat = null;
                String click_to_recommend = null;
                String event_id = scdata.getString("event_id");
                String search_event_id = scdata.getString("search_event_id");
                String source = scdata.getString("source");
                String keyword = scdata.getString("keyword");

                // 兼容处理
                if (goods_arr != null) {
                    goods_id = goods_arr.getString("goods_id");
                    special_id = goods_arr.getString("special_id");
                    pid = goods_arr.getString("pid");
                    goods_no = goods_arr.getString("goods_no");
                    index = goods_arr.getString("index");
                    is_operat = goods_arr.getString("is_operat");
                    click_to_recommend = goods_arr.getString("click_to_recommend");
                } else {
                    goods_id = scdata.getString("goods_id");
                    special_id = scdata.getString("special_id");
                    pid = scdata.getString("pid");
                    goods_no = scdata.getString("goods_no");
                    index = scdata.getString("index");
                }


                // 输出
                JSONObject appExposureResult = new JSONObject();
                appExposureResult.put("log_database","yishou_log_exposure");
                appExposureResult.put(ModelUtil.humpToUnderline("user_id"), user_id);
                appExposureResult.put(ModelUtil.humpToUnderline("goods_id"), goods_id);
                appExposureResult.put(ModelUtil.humpToUnderline("special_id"), special_id);
                appExposureResult.put(ModelUtil.humpToUnderline("pid"), pid);
                appExposureResult.put(ModelUtil.humpToUnderline("index"), index);
                appExposureResult.put(ModelUtil.humpToUnderline("is_operat"), is_operat);
                appExposureResult.put(ModelUtil.humpToUnderline("click_to_recommend"), click_to_recommend);
                appExposureResult.put(ModelUtil.humpToUnderline("event_id"), event_id);
                appExposureResult.put(ModelUtil.humpToUnderline("search_event_id"), search_event_id);
                appExposureResult.put(ModelUtil.humpToUnderline("keyword"), keyword);
                appExposureResult.put("source", source);
                appExposureResult.put(ModelUtil.humpToUnderline("goods_no"), goods_no);
                appExposureResult.put(ModelUtil.humpToUnderline("event"), event);
                appExposureResult.put(ModelUtil.humpToUnderline("receive_time"), receive_time);
                appExposureResult.put("dt", DateUtil.secondToSpecialDate(Long.parseLong(receive_time)));
                // 简易判断
                if (StringUtils.isNotBlank(user_id)) {
                    out.collect(appExposureResult);
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
