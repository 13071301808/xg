package com.yishou.bigdata.realtime.dw.common.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.realtime.dw.common.utils.DateUtil;
import com.yishou.bigdata.realtime.dw.common.utils.EventParseUtil;
import com.yishou.bigdata.realtime.dw.common.utils.ModelUtil;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @date: 2022/12/17
 * @author: yangshibiao
 * @desc: H5商品曝光解析
 */
public class H5GoodsExposureProcess extends ProcessFunction<String, JSONObject> {

    static Logger logger = LoggerFactory.getLogger(H5GoodsExposureProcess.class);

    @Override
    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

        try {

            JSONObject jsonText = JSON.parseObject(value);
            if (EventParseUtil.isEvent(jsonText, "ysh5goodsexposure")) {

                JSONObject data = jsonText.getJSONObject("data");
                if (data != null) {

                    JSONArray goodsExposureArr = data.getJSONArray("goods_arr");
                    if (goodsExposureArr != null) {

                        for (Object goodsExposureObject : goodsExposureArr) {

                            JSONObject goodsExposureJson = JSON.parseObject(JSON.toJSONString(goodsExposureObject));
                            if (goodsExposureJson != null) {

                                // 数据解析
                                String logSource = jsonText.getString("__client_ip__");
                                String receiveTime = jsonText.getString("__time__");
                                String activity = jsonText.getString("activity");
                                String userId = data.getString("uid");
                                String appVersion = data.getString("app_version");
                                String url = data.getString("url");
                                String title = data.getString("title");
                                String eventId = data.getString("event_id");
                                String templateId = data.getString("template_id");
                                String goodsId = goodsExposureJson.getString("goods_id");
                                String plateName = goodsExposureJson.getString("plate_name");
                                String tabName = goodsExposureJson.getString("tab_name");
                                String index = goodsExposureJson.getString("index");
                                String isDefault = goodsExposureJson.getString("is_default");
                                String isOperat = goodsExposureJson.getString("is_operat");
                                String goodsSeatId = goodsExposureJson.getString("goods_seat_id");
                                String goodsNo = goodsExposureJson.getString("goods_no");

                                // 数据封装
                                JSONObject goodsExposureResult = new JSONObject();
                                goodsExposureResult.put(ModelUtil.humpToUnderline("logSource"), logSource);
                                goodsExposureResult.put(ModelUtil.humpToUnderline("receiveTime"), receiveTime);
                                goodsExposureResult.put(ModelUtil.humpToUnderline("activity"), activity);
                                goodsExposureResult.put(ModelUtil.humpToUnderline("userId"), userId);
                                goodsExposureResult.put(ModelUtil.humpToUnderline("appVersion"), appVersion);
                                goodsExposureResult.put(ModelUtil.humpToUnderline("url"), url);
                                goodsExposureResult.put(ModelUtil.humpToUnderline("title"), title);
                                goodsExposureResult.put(ModelUtil.humpToUnderline("eventId"), eventId);
                                goodsExposureResult.put(ModelUtil.humpToUnderline("templateId"), templateId);
                                goodsExposureResult.put(ModelUtil.humpToUnderline("goodsId"), goodsId);
                                goodsExposureResult.put(ModelUtil.humpToUnderline("plateName"), plateName);
                                goodsExposureResult.put(ModelUtil.humpToUnderline("tabName"), tabName);
                                goodsExposureResult.put(ModelUtil.humpToUnderline("index"), index);
                                goodsExposureResult.put(ModelUtil.humpToUnderline("isDefault"), isDefault);
                                goodsExposureResult.put(ModelUtil.humpToUnderline("isOperat"), isOperat);
                                goodsExposureResult.put(ModelUtil.humpToUnderline("goodsSeatId"), goodsSeatId);
                                goodsExposureResult.put(ModelUtil.humpToUnderline("goodsNo"), goodsNo);
                                goodsExposureResult.put(ModelUtil.humpToUnderline("event"), "ysh5goodsexposure");
                                goodsExposureResult.put("dt", DateUtil.secondToSpecialDate(Long.parseLong(receiveTime)));
                                goodsExposureResult.put("event_name", "dwd_h5_goods_exposure");

                                // 数据发送
                                out.collect(goodsExposureResult);

                            }
                        }
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
