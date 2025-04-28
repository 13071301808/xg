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

/**
 * @date: 2023/5/9
 * @author: yangshibiao
 * @desc: 推荐档口曝光解析
 */
public class GoodsPageExposureProcess extends ProcessFunction<String, JSONObject> {

    static Logger logger = LoggerFactory.getLogger(GoodsPageExposureProcess.class);

    @Override
    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        try {
            JSONObject jsonText = JSON.parseObject(value);
            if (EventParseUtil.isEvent(jsonText, "pagestaytimeexposure")) {
                JSONObject scdata = jsonText.getJSONObject("scdata");
                if (scdata != null) {
                    String pageName = scdata.getString("page_name");
                    String userId = scdata.getString("user_id");
                    String stayTime = scdata.getString("stay_time");
                    String goodsId = scdata.getString("goods_id");
                    String receiveTime = jsonText.getString("__time__");
                    if ("商品详情".equals(pageName)
                            && StringUtils.isNumeric(userId)
                            && StringUtils.isNotBlank(stayTime)
                            && StringUtils.isNumeric(stayTime.split("[,.]")[0])
                            && Long.parseLong(stayTime.split("[,.]")[0]) > 0
                            && StringUtils.isNumeric(receiveTime)
                    ) {
                        JSONObject stallPageExposureResult = new JSONObject();
                        stallPageExposureResult.put(ModelUtil.humpToUnderline("goodsId"), goodsId);
                        stallPageExposureResult.put(ModelUtil.humpToUnderline("userId"), userId);
                        stallPageExposureResult.put(ModelUtil.humpToUnderline("stayTime"), stayTime.split("[,.]")[0]);
                        stallPageExposureResult.put(ModelUtil.humpToUnderline("receiveTime"), receiveTime);
                        // 输出数据
                        out.collect(stallPageExposureResult);
                    }
                }
            }
        } catch (Exception e) {
            logger.warn(
                    "***** 埋点数据解析异常，不能解析成json字符串，传入的埋点数据为：{}， 抛出的异常信息为：{}",
                    value,
                    e.getMessage()
            );
        }

    }

}
