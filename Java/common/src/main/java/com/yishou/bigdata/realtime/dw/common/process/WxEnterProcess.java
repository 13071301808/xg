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

public class WxEnterProcess extends ProcessFunction<String, JSONObject> {

    static Logger logger = LoggerFactory.getLogger(WxEnterProcess.class);

    @Override
    public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {

        try {
            JSONObject data = JSON.parseObject(value);
            JSONObject eventData = data.getJSONObject("data");
            if (eventData != null && EventParseUtil.isEvent(data, "wx_enter")) {
                String logTime = data.getString("__time__");
                String receiveTime = data.getString("__time__");
                String logSource = data.getString("__client_ip__");

                String time = eventData.getString("created");

                JSONObject param = eventData.getJSONObject("param");
                String userId = param.getString("uid");
                String platType = param.getString("plat_type");
                String logTopic = param.getString("log_topic");
                String scene = param.getString("scene");
                String pagePath = param.getString("page_path");
                String sourceId = param.getString("source_id");
                String parentPath = param.getString("parent_path");
                String versionName = param.getString("version_name");
                String version = param.getString("version");
                String openid = param.getString("openid");
                String model = param.getString("model");

                JSONObject wxEnterResult = new JSONObject();
                wxEnterResult.put(ModelUtil.humpToUnderline("log_source"), logSource);
                wxEnterResult.put(ModelUtil.humpToUnderline("log_time"), logTime);
                wxEnterResult.put(ModelUtil.humpToUnderline("time"), time);
                wxEnterResult.put(ModelUtil.humpToUnderline("user_id"), userId);
                wxEnterResult.put(ModelUtil.humpToUnderline("plat_type"), platType);
                wxEnterResult.put(ModelUtil.humpToUnderline("log_topic"), logTopic);
                wxEnterResult.put(ModelUtil.humpToUnderline("scene"), scene);
                wxEnterResult.put(ModelUtil.humpToUnderline("page_path"), pagePath);
                wxEnterResult.put(ModelUtil.humpToUnderline("source_id"), sourceId);
                wxEnterResult.put(ModelUtil.humpToUnderline("parent_path"), parentPath);
                wxEnterResult.put(ModelUtil.humpToUnderline("version_name"), versionName);
                wxEnterResult.put(ModelUtil.humpToUnderline("version"), version);
                wxEnterResult.put(ModelUtil.humpToUnderline("openid"), openid);
                wxEnterResult.put(ModelUtil.humpToUnderline("model"), model);

                wxEnterResult.put("event_time", time);
                wxEnterResult.put("event", "wx_enter");
                wxEnterResult.put(ModelUtil.humpToUnderline("receiveTime"), receiveTime);
                wxEnterResult.put(ModelUtil.humpToUnderline("ip"), logSource);

                if (StringUtils.isNotBlank(userId)) {
                    collector.collect(wxEnterResult);
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
