package com.yishou.bigdata.operator;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitorAppFliter implements FilterFunction<JSONObject> {
    public static Logger logger = LoggerFactory.getLogger(MonitorAppFliter.class);
    @Override
    public boolean filter(JSONObject jsonObject) throws Exception {
        try {
            final String user_id = jsonObject.getString("user_id");
            return user_id != null && !"".equalsIgnoreCase(user_id);
        } catch (Exception e) {
            logger.info("发现有异常数据：{}",e.getMessage());
            return false;
        }
    }
}
