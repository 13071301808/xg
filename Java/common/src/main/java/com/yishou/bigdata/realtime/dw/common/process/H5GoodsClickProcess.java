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
 * @desc: H5商品点击解析
 */
public class H5GoodsClickProcess extends ProcessFunction<String, JSONObject> {

    static Logger logger = LoggerFactory.getLogger(H5GoodsClickProcess.class);

    @Override
    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

        try {

            JSONObject jsonText = JSON.parseObject(value);
            if (EventParseUtil.isEvent(jsonText, "ysh5goodsexposure")) {

                JSONObject data = jsonText.getJSONObject("data");
                if (data != null) {

                    JSONArray goodsClickArr = data.getJSONArray("click_goods_arr");
                    if (goodsClickArr != null) {

                        for (Object goodsClickObject : goodsClickArr) {

                            JSONObject goodsClickJson = JSON.parseObject(JSON.toJSONString(goodsClickObject));
                            if (goodsClickJson != null) {

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
                                String goodsId = goodsClickJson.getString("goods_id");
                                String plateName = goodsClickJson.getString("plate_name");
                                String tabName = goodsClickJson.getString("tab_name");
                                String index = goodsClickJson.getString("index");
                                String isDefault = goodsClickJson.getString("is_default");
                                String isOperat = goodsClickJson.getString("is_operat");
                                String goodsSeatId = goodsClickJson.getString("goods_seat_id");
                                String goodsNo = goodsClickJson.getString("goods_no");

                                // 数据封装
                                JSONObject goodsClickResult = new JSONObject();
                                goodsClickResult.put(ModelUtil.humpToUnderline("logSource"), logSource);
                                goodsClickResult.put(ModelUtil.humpToUnderline("receiveTime"), receiveTime);
                                goodsClickResult.put(ModelUtil.humpToUnderline("activity"), activity);
                                goodsClickResult.put(ModelUtil.humpToUnderline("userId"), userId);
                                goodsClickResult.put(ModelUtil.humpToUnderline("appVersion"), appVersion);
                                goodsClickResult.put(ModelUtil.humpToUnderline("url"), url);
                                goodsClickResult.put(ModelUtil.humpToUnderline("title"), title);
                                goodsClickResult.put(ModelUtil.humpToUnderline("eventId"), eventId);
                                goodsClickResult.put(ModelUtil.humpToUnderline("templateId"), templateId);
                                goodsClickResult.put(ModelUtil.humpToUnderline("goodsId"), goodsId);
                                goodsClickResult.put(ModelUtil.humpToUnderline("plateName"), plateName);
                                goodsClickResult.put(ModelUtil.humpToUnderline("tabName"), tabName);
                                goodsClickResult.put(ModelUtil.humpToUnderline("index"), index);
                                goodsClickResult.put(ModelUtil.humpToUnderline("isDefault"), isDefault);
                                goodsClickResult.put(ModelUtil.humpToUnderline("isOperat"), isOperat);
                                goodsClickResult.put(ModelUtil.humpToUnderline("goodsSeatId"), goodsSeatId);
                                goodsClickResult.put(ModelUtil.humpToUnderline("goodsNo"), goodsNo);
                                goodsClickResult.put("dt", DateUtil.secondToSpecialDate(Long.parseLong(receiveTime)));
                                goodsClickResult.put("event_name", "dwd_h5_goods_click");
                                goodsClickResult.put("process_type", "ysh5goodsexposure");

                                // 数据发送
                                out.collect(goodsClickResult);

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
