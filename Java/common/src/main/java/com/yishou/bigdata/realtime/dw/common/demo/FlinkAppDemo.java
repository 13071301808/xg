package com.yishou.bigdata.realtime.dw.common.demo;

import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.realtime.dw.common.basic.BasicApp;
import com.yishou.bigdata.realtime.dw.common.utils.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @date: 2022/12/28
 * @author: yangshibiao
 * @desc: Flink的APP项目（实时作业）的Demo
 * 从流量日志中获取 APP商品点击、APP商品曝光、H5商品点击、H5商品曝光 这4个事件的数据，并通过异步IO将这4个事件的数据匹配对应维度信息，最终将数据写入到MySQL数据库里面
 */
public class FlinkAppDemo extends BasicApp {

    public static void main(String[] args) throws Exception {

        // 创建对象
        FlinkAppDemo flinkAppDemo = new FlinkAppDemo();

        // 解析参数获取配置
        Map<String, String> configMap = ModelUtil.analysisConfig(args);
        String applicationName = ClassUtil.getClassName(flinkAppDemo.getClass().getName());

        // 初始化
        flinkAppDemo.init(
                configMap,
                applicationName,
                "app_goods_click", "app_goods_exposure", "h5_goods_click", "h5_goods_exposure"
        );

        // 业务处理
        flinkAppDemo.process(configMap);

        // 启动执行
        flinkAppDemo.execute(applicationName);

    }

    @Override
    public void process(Map<String, String> configMap) {

        Random random = new Random();

        DataStream<JSONObject> appGoodsClickStream = eventDataStreamMap
                .get("app_goods_click")
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        return !StringUtils.isBlank(jsonObject.getString("user_id")) && !StringUtils.isBlank(jsonObject.getString("goods_id"));
                    }
                })
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        JSONObject resultJson = new JSONObject();
                        resultJson.put("user_id", jsonObject.getString("user_id"));
                        resultJson.put("goods_id", jsonObject.getString("goods_id"));
                        resultJson.put("receive_time", jsonObject.getString("receive_time"));
                        resultJson.put("start_time", System.currentTimeMillis());
                        resultJson.put("type", "app_goods_click");
                        return resultJson;
                    }
                })
                .uid("app_goods_click_map")
                .name("app_goods_click_map")
                .disableChaining();

        DataStream<JSONObject> appGoodsExposureStream = eventDataStreamMap
                .get("app_goods_exposure")
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        return !StringUtils.isBlank(jsonObject.getString("user_id")) && !StringUtils.isBlank(jsonObject.getString("goods_id"));
                    }
                })
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        JSONObject resultJson = new JSONObject();
                        resultJson.put("user_id", jsonObject.getString("user_id"));
                        resultJson.put("goods_id", jsonObject.getString("goods_id"));
                        resultJson.put("receive_time", jsonObject.getString("receive_time"));
                        resultJson.put("start_time", System.currentTimeMillis());
                        resultJson.put("type", "app_goods_exposure");
                        return resultJson;
                    }
                })
                .uid("app_goods_exposure_map")
                .name("app_goods_exposure_map")
                .disableChaining();

        DataStream<JSONObject> h5GoodsClickStream = eventDataStreamMap
                .get("h5_goods_click")
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        return !StringUtils.isBlank(jsonObject.getString("user_id")) && !StringUtils.isBlank(jsonObject.getString("goods_id"));
                    }
                })
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        JSONObject resultJson = new JSONObject();
                        resultJson.put("user_id", jsonObject.getString("user_id"));
                        resultJson.put("goods_id", jsonObject.getString("goods_id"));
                        resultJson.put("receive_time", jsonObject.getString("receive_time"));
                        resultJson.put("start_time", System.currentTimeMillis());
                        resultJson.put("type", "h5_goods_click");
                        return resultJson;
                    }
                })
                .uid("h5_goods_click_map")
                .name("h5_goods_click_map")
                .disableChaining();

        DataStream<JSONObject> h5GoodsExposureStream = eventDataStreamMap
                .get("h5_goods_exposure")
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        return !StringUtils.isBlank(jsonObject.getString("user_id")) && !StringUtils.isBlank(jsonObject.getString("goods_id"));
                    }
                })
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        JSONObject resultJson = new JSONObject();
                        resultJson.put("user_id", jsonObject.getString("user_id"));
                        resultJson.put("goods_id", jsonObject.getString("goods_id"));
                        resultJson.put("receive_time", jsonObject.getString("receive_time"));
                        resultJson.put("start_time", System.currentTimeMillis());
                        resultJson.put("type", "h5_goods_exposure");
                        return resultJson;
                    }
                })
                .uid("h5_goods_exposure_map")
                .name("h5_goods_exposure_map")
                .disableChaining();

        SingleOutputStreamOperator<JSONObject> mainStreamAddGoodsDim = AsyncDataStream
                .unorderedWait(
                        // 传入的核心流（注意：建议在核心流后使用keyBy，因为AsynIO在前面是几个并发，就还是几个并发，使用keyBy会对数据进行打散分发）
                        appGoodsClickStream.union(appGoodsExposureStream, h5GoodsClickStream, h5GoodsExposureStream).keyBy(value -> random.nextInt(1000)),
                        new AsyncJoinDimUtil<JSONObject>("yishou.fmys_goods") {

                            @Override
                            public JSONObject getDimInfo(String tableName, JSONObject input) {
                                List<JSONObject> dimInfos = MySQLR7Util.queryListByKey(
                                        tableName,
                                        "goods_id",
                                        input.getString("goods_id"),
                                        JSONObject.class,
                                        "goods_no", "goods_kh", "goods_name"
                                );
                                if (!dimInfos.isEmpty()) {
                                    return dimInfos.get(0);
                                } else {
                                    return null;
                                }
                            }

                            @Override
                            public void join(JSONObject input, JSONObject dimInfo) {
                                input.put("goods_no", dimInfo.getString("goods_no"));
                                input.put("goods_kh", dimInfo.getString("goods_kh"));
                                input.put("goods_name", dimInfo.getString("goods_name"));
                            }
                        },
                        120,
                        TimeUnit.SECONDS
                )
                .uid("async_add_goods_dim")
                .name("async_add_goods_dim")
                .disableChaining();

        mainStreamAddGoodsDim
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        jsonObject.put("end_time", System.currentTimeMillis());
                        jsonObject.put("query_time", jsonObject.getLong("end_time") - jsonObject.getLong("start_time"));
                        return jsonObject;
                    }
                })
                .addSink(new RichSinkFunction<JSONObject>() {

//                    private MySQLUtil yishouDataMySQLUtil;

                    @Override
                    public void open(Configuration parameters) throws Exception {
//                        yishouDataMySQLUtil = new MySQLUtil(
//                                ModelUtil.getConfigValue("mysql.yishou.data.url"),
//                                ModelUtil.getConfigValue("mysql.yishou.data.username"),
//                                ModelUtil.getConfigValue("mysql.yishou.data.password"),
//                                10,
//                                2
//                        );
                    }

                    @Override
                    public void invoke(JSONObject value, Context context) throws Exception {
//                        long startTime = System.currentTimeMillis();
//                        yishouDataMySQLUtil.insert(
//                                "yishou_data.flow_table",
//                                false,
//                                value
//                        );
//                        long endTime = System.currentTimeMillis();
//                        System.out.println("插入数据成功，耗时：" + (endTime - startTime));
                        System.out.println(value);
                    }
                })
                .uid("sink_function")
                .name("sink_function");

    }

}
