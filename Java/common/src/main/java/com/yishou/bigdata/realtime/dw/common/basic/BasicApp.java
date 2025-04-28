package com.yishou.bigdata.realtime.dw.common.basic;

import com.alibaba.fastjson.JSONObject;
import com.yishou.bigdata.realtime.dw.common.bean.avro.CommonRow;
import com.yishou.bigdata.realtime.dw.common.process.*;
import com.yishou.bigdata.realtime.dw.common.utils.DataStreamSourceUtil;
import com.yishou.bigdata.realtime.dw.common.utils.ModelUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @date: 2022/12/16
 * @author: yangshibiao
 * @desc: Flink项目中的基础APP
 */
public abstract class BasicApp implements IAppService {

    static Logger logger = LoggerFactory.getLogger(BasicApp.class);

    /**
     * 事件数据流（json格式数据流，用于通用主题）
     */
    protected Map<String, DataStream<JSONObject>> eventDataStreamMap = new HashMap<>();

    /**
     * MySQL的binlog数据流，用于进行反序列化的业务binlog形成的avro数据
     */
    protected Map<String, DataStream<CommonRow>> binlogDataStreamMap = new HashMap<>();

    /**
     * Flink流的执行环境
     */
    protected StreamExecutionEnvironment env = null;

    /**
     * 初始化执行环境，并解析对应事件
     *
     * @param configMap       传入参数
     * @param applicationName app名（kafka消费者组id）
     * @param eventNames      需要解析的事件名
     * @throws ParseException 抛出的异常
     */
    protected void init(Map<String, String> configMap, String applicationName, String... eventNames) throws ParseException {
        // 专场开始调度
        this.scheduler();
        // 创建执行环境，并配置 checkpoint和重启策略
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        ModelUtil.deployRocksdbCheckpoint(env, applicationName, Long.parseLong(configMap.get("checkpoint_interval")));
        ModelUtil.deployRestartStrategy(env);

        // 根据传入的 eventName 获取对应的解析流
        for (String eventName : eventNames) {
            switch (eventName) {

                // 流量日志
                /**
                 * goodsexposure
                 */
                case "app_goods_exposure":
                    DataStream<JSONObject> appGoodsExposureDataStream = DataStreamSourceUtil
                            .getYishouLogExposureDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new AppGoodsExposureProcess())
                            .setParallelism(3)
                            .uid("app_goods_exposure")
                            .name("app_goods_exposure")
                            .disableChaining();
                    eventDataStreamMap.put("app_goods_exposure", appGoodsExposureDataStream);
                    logger.info("##### 创建app_goods_exposure数据流成功");
                    break;
                /**
                 * goodsexposure.click_goods_arr
                 */
                case "app_goods_click":
                    DataStream<JSONObject> appGoodsClickDataStream = DataStreamSourceUtil
                            .getYishouLogExposureDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new AppGoodsClickProcess())
                            .setParallelism(3)
                            .uid("app_goods_click")
                            .name("app_goods_click")
                            .disableChaining();
                    eventDataStreamMap.put("app_goods_click", appGoodsClickDataStream);
                    logger.info("##### 创建app_goods_click数据流成功");
                    break;
                //从同一个topic分别取APP端 曝光 + 点击数据
                case "app_goods_exposure_click":
                    DataStream<JSONObject> appGoodsExposureClickDataStream = DataStreamSourceUtil
                            .getYishouLogExposureDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new AppGoodsExposureClickProcess())
                            .setParallelism(3)
                            .uid("app_goods_exposure_click")
                            .name("app_goods_exposure_click")
                            .disableChaining();
                    eventDataStreamMap.put("app_goods_exposure_click", appGoodsExposureClickDataStream);
                    logger.info("##### 创建app_goods_exposure_click数据流成功");
                    break;
                //从同一个topic分别获取H5端的 曝光 + 点击数据
                case "h5_goods_exposure_click":
                    DataStream<JSONObject> h5GoodsExposureClickDataStream = DataStreamSourceUtil
                            .getH5LogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new H5GoodsExposureClickProcess())
                            .setParallelism(3)
                            .uid("h5_goods_exposure_click")
                            .name("h5_goods_exposure_click")
                            .disableChaining();
                    eventDataStreamMap.put("h5_goods_exposure_click", h5GoodsExposureClickDataStream);
                    logger.info("##### 创建h5_goods_exposure_click数据流成功");
                    break;

                case "h5_goods_exposure":
                    DataStream<JSONObject> h5GoodsExposureDataStream = DataStreamSourceUtil
                            .getH5LogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new H5GoodsExposureProcess())
                            .setParallelism(3)
                            .uid("h5_goods_exposure")
                            .name("h5_goods_exposure")
                            .disableChaining();
                    eventDataStreamMap.put("h5_goods_exposure", h5GoodsExposureDataStream);
                    logger.info("##### 创建h5_goods_exposure数据流成功");
                    break;
                case "h5_goods_click":
                    DataStream<JSONObject> h5GoodsClickDataStream = DataStreamSourceUtil
                            .getH5LogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new H5GoodsClickProcess())
                            .setParallelism(3)
                            .uid("h5_goods_click")
                            .name("h5_goods_click")
                            .disableChaining();
                    eventDataStreamMap.put("h5_goods_click", h5GoodsClickDataStream);
                    logger.info("##### 创建h5_goods_click数据流成功");
                    break;
                case "h5_goods_exposure_questionnaire":
                    DataStream<JSONObject> h5GoodsExposureQuestionnaireDataStream = DataStreamSourceUtil
                            .getH5LogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new H5GoodsExposureQuestionnaireProcess())
                            .setParallelism(3)
                            .uid("h5_goods_exposure_questionnaire")
                            .name("h5_goods_exposure_questionnaire")
                            .disableChaining();
                    eventDataStreamMap.put("h5_goods_exposure_questionnaire", h5GoodsExposureQuestionnaireDataStream);
                    logger.info("##### 创建h5_goods_exposure_questionnaire数据流成功");
                    break;
                case "app_subscribe_page_exposure":
                    DataStream<JSONObject> appSubscribePageExposureDataStream = DataStreamSourceUtil
                            .getYishouLogExposureDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new AppSubscribePageExposureProcess())
                            .setParallelism(3)
                            .uid("app_subscribe_page_exposure")
                            .name("app_subscribe_page_exposure")
                            .disableChaining();
                    eventDataStreamMap.put("app_subscribe_page_exposure", appSubscribePageExposureDataStream);
                    logger.info("##### 创建app_subscribe_page_exposure数据流成功");
                    break;
                case "app_check_stall":
                    SingleOutputStreamOperator<JSONObject> appCheckStallDataStream = DataStreamSourceUtil
                            .getNewAppLogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new AppCheckStallProcess())
                            .setParallelism(3)
                            .uid("app_check_stall")
                            .name("app_check_stall")
                            .disableChaining();
                    eventDataStreamMap.put("app_check_stall", appCheckStallDataStream);
                    logger.info("##### 创建app_check_stall数据流成功");
                    break;
                case "app_subscribe_stall_page_exposure":
                    SingleOutputStreamOperator<JSONObject> appSubscribeStallPageExposureDataStream = DataStreamSourceUtil
                            .getYishouLogExposureDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new AppSubscribeStallPageExposureProcess())
                            .setParallelism(3)
                            .uid("app_subscribe_stall_page_exposure")
                            .name("app_subscribe_stall_page_exposure")
                            .disableChaining();
                    eventDataStreamMap.put("app_subscribe_stall_page_exposure", appSubscribeStallPageExposureDataStream);
                    logger.info("##### 创建app_subscribe_stall_page_exposure数据流成功");
                    break;
                case "goods_detail_page_exposure":
                    SingleOutputStreamOperator<JSONObject> goodsDetailPageExposureDataStream = DataStreamSourceUtil
                            .getNewAppLogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new GoodsDetailPageExposureProcess())
                            .setParallelism(3)
                            .uid("goods_detail_page_exposure")
                            .name("goods_detail_page_exposure")
                            .disableChaining();
                    eventDataStreamMap.put("goods_detail_page_exposure", goodsDetailPageExposureDataStream);
                    logger.info("##### 创建goods_detail_page_exposure数据流成功");
                    break;
                case "goods_add_cart":
                    SingleOutputStreamOperator<JSONObject> addcartDataStream = DataStreamSourceUtil
                            .getNewAppLogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new AppAddCartProcess())
                            .setParallelism(3)
                            .uid("goods_add_cart")
                            .name("goods_add_cart")
                            .disableChaining();
                    eventDataStreamMap.put("goods_add_cart", addcartDataStream);
                    logger.info("##### 创建goods_add_cart数据流成功");
                    break;
                case "goods_collect":
                    SingleOutputStreamOperator<JSONObject> goodsCollectDataStream = DataStreamSourceUtil
                            .getNewAppLogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new AppCollectGoodsProcess())
                            .setParallelism(3)
                            .uid("goods_collect")
                            .name("goods_collect")
                            .disableChaining();
                    eventDataStreamMap.put("goods_collect", goodsCollectDataStream);
                    logger.info("##### 创建goods_collect数据流成功");
                    break;
                //从同一个topic分别获取加购收藏数据
                case "goods_addcart_collect":
                    SingleOutputStreamOperator<JSONObject> goodsAddcartCollectDataStream = DataStreamSourceUtil
                            .getNewAppLogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new AppAddcartCollectGoodsProcess())
                            .setParallelism(3)
                            .uid("goods_addcart_collect")
                            .name("goods_addcart_collect")
                            .disableChaining();
                    eventDataStreamMap.put("goods_addcart_collect", goodsAddcartCollectDataStream);
                    logger.info("##### 创建goods_addcart_collect数据流成功");
                    break;
                case "recommend_stall_exposure":
                    SingleOutputStreamOperator<JSONObject> RecommendStallExposureDataStream = DataStreamSourceUtil
                            .getYishouLogExposureDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new RecommendStallExposureProcess())
                            .setParallelism(3)
                            .uid("recommend_stall_exposure")
                            .name("recommend_stall_exposure")
                            .disableChaining();
                    eventDataStreamMap.put("recommend_stall_exposure", RecommendStallExposureDataStream);
                    logger.info("##### 创建recommend_stall_exposure数据流成功");
                    break;
                case "stall_page_exposure":
                    SingleOutputStreamOperator<JSONObject> StallPageExposureDataStream = DataStreamSourceUtil
                            .getYishouLogExposureDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new StallPageExposureProcess())
                            .setParallelism(3)
                            .uid("stall_page_exposure")
                            .name("stall_page_exposure")
                            .disableChaining();
                    eventDataStreamMap.put("stall_page_exposure", StallPageExposureDataStream);
                    logger.info("##### 创建stall_page_exposure数据流成功");
                    break;
                case "goods_page_exposure":
                    SingleOutputStreamOperator<JSONObject> goodsPageExposureDataStream = DataStreamSourceUtil
                            .getYishouLogExposureDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new GoodsPageExposureProcess())
                            .setParallelism(3)
                            .uid("goods_page_exposure")
                            .name("goods_page_exposure")
                            .disableChaining();
                    eventDataStreamMap.put("goods_page_exposure", goodsPageExposureDataStream);
                    logger.info("##### 创建goods_page_exposure数据流成功");
                    break;
                case "goods_detail_page":
                    SingleOutputStreamOperator<JSONObject> goodsDetailPageDataStream = DataStreamSourceUtil
                            .getNewAppLogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new GoodsDetailPageProcess())
                            .setParallelism(3)
                            .uid("goods_detail_page")
                            .name("goods_detail_page")
                            .disableChaining();
                    eventDataStreamMap.put("goods_detail_page", goodsDetailPageDataStream);
                    logger.info("##### 创建goods_detail_page数据流成功");
                    break;
                case "app_start":
                    SingleOutputStreamOperator<JSONObject> appStartStream = DataStreamSourceUtil
                            .getNewAppLogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new AppStartProcess())
                            .setParallelism(3)
                            .uid("app_start")
                            .name("app_start")
                            .disableChaining();
                    eventDataStreamMap.put("app_start", appStartStream);
                    logger.info("##### 创建app_start数据流成功");
                    break;
                case "home_specail_scan":
                    SingleOutputStreamOperator<JSONObject> homeSpecailScanStream = DataStreamSourceUtil
                            .getNewAppLogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new HomeSpecailScanProcess())
                            .setParallelism(3)
                            .uid("home_specail_scan")
                            .name("home_specail_scan")
                            .disableChaining();
                    eventDataStreamMap.put("home_specail_scan", homeSpecailScanStream);
                    logger.info("##### 创建home_specail_scan数据流成功");
                    break;
                case "login":
                    SingleOutputStreamOperator<JSONObject> loginStream = DataStreamSourceUtil
                            .getNewAppLogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new LoginProcess())
                            .setParallelism(3)
                            .uid("login")
                            .name("login")
                            .disableChaining();
                    eventDataStreamMap.put("login", loginStream);
                    logger.info("##### 创建login数据流成功");
                    break;
                case "special_exposure":
                    SingleOutputStreamOperator<JSONObject> specialExposureStream = DataStreamSourceUtil
                            .getYishouLogExposureDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new specialExposureProcess())
                            .setParallelism(3)
                            .uid("special_exposure")
                            .name("special_exposure")
                            .disableChaining();
                    eventDataStreamMap.put("special_exposure", specialExposureStream);
                    logger.info("##### 创建special_exposure数据流成功");
                    break;
                case "special_exposure_click":
                    SingleOutputStreamOperator<JSONObject> specialExposureClickStream = DataStreamSourceUtil
                            .getYishouLogExposureDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new AppSpecialExposureClickProcess())
                            .setParallelism(3)
                            .uid("special_exposure_click")
                            .name("special_exposure_click")
                            .disableChaining();
                    eventDataStreamMap.put("special_exposure_click", specialExposureClickStream);
                    logger.info("##### 创建special_exposure_click数据流成功");
                    break;
                case "enter_special":
                    SingleOutputStreamOperator<JSONObject> enterSpecialStream = DataStreamSourceUtil
                            .getYishouLogExposureDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new AppEnterSpecialProcess())
                            .setParallelism(3)
                            .uid("enter_special")
                            .name("enter_special")
                            .disableChaining();
                    eventDataStreamMap.put("enter_special", enterSpecialStream);
                    logger.info("##### 创建enter_special数据流成功");
                    break;
                case "app_goods_exposure_questionnaire":
                    SingleOutputStreamOperator<JSONObject> appGoodsExposureQuestionnaireDataStream = DataStreamSourceUtil
                            .getYishouLogExposureDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new AppGoodsExposureQuestionnaireProcess())
                            .setParallelism(3)
                            .uid("app_goods_exposure_questionnaire")
                            .name("app_goods_exposure_questionnaire")
                            .disableChaining();
                    eventDataStreamMap.put("app_goods_exposure_questionnaire", appGoodsExposureQuestionnaireDataStream);
                    logger.info("##### 创建app_goods_exposure_questionnaire数据流成功");
                    break;
                case "wx_enter":
                    SingleOutputStreamOperator<JSONObject> wxEnterStream = DataStreamSourceUtil
                            .getH5LogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new WxEnterProcess())
                            .setParallelism(3)
                            .uid("wx_enter")
                            .name("wx_enter")
                            .disableChaining();
                    eventDataStreamMap.put("wx_enter", wxEnterStream);
                    logger.info("##### 创建wx_enter数据流成功");
                    break;
                case "wx_goods_add_cart":
                    SingleOutputStreamOperator<JSONObject> wxGoodsAddCartStream = DataStreamSourceUtil
                            .getH5LogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new WxAddCartProcess())
                            .setParallelism(3)
                            .uid("wx_goods_add_cart")
                            .name("wx_goods_add_cart")
                            .disableChaining();
                    eventDataStreamMap.put("wx_goods_add_cart", wxGoodsAddCartStream);
                    logger.info("##### 创建wx_goods_add_cart数据流成功");
                    break;
                case "wx_goods_detail_page":
                    SingleOutputStreamOperator<JSONObject> wxGoodsDetailPageDataStream = DataStreamSourceUtil
                            .getH5LogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new WxGoodsDetailPageProcess())
                            .setParallelism(3)
                            .uid("wx_goods_detail_page")
                            .name("wx_goods_detail_page")
                            .disableChaining();
                    eventDataStreamMap.put("wx_goods_detail_page", wxGoodsDetailPageDataStream);
                    logger.info("##### 创建wx_goods_detail_page数据流成功");
                    break;
                case "wx_enter_special":
                    SingleOutputStreamOperator<JSONObject> wxEnterSpecialDataStream = DataStreamSourceUtil
                            .getH5LogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new WxEnterSpecialProcess())
                            .setParallelism(3)
                            .uid("wx_enter_special")
                            .name("wx_enter_special")
                            .disableChaining();
                    eventDataStreamMap.put("wx_enter_special", wxEnterSpecialDataStream);
                    logger.info("##### 创建wx_enter_special数据流成功");
                    break;
                case "wx_home_click":
                    SingleOutputStreamOperator<JSONObject> wxHomeClickDataStream = DataStreamSourceUtil
                            .getH5LogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new WxHomeClickProcess())
                            .setParallelism(3)
                            .uid("wx_home_click")
                            .name("wx_home_click")
                            .disableChaining();
                    eventDataStreamMap.put("wx_home_click", wxHomeClickDataStream);
                    logger.info("##### 创建wx_home_click数据流成功");
                    break;
                case "wx_check_stall":
                    SingleOutputStreamOperator<JSONObject> wxCheckStallDataStream = DataStreamSourceUtil
                            .getH5LogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new WxCheckStallProcess())
                            .setParallelism(3)
                            .uid("wx_check_stall")
                            .name("wx_check_stall")
                            .disableChaining();
                    eventDataStreamMap.put("wx_check_stall", wxCheckStallDataStream);
                    logger.info("##### 创建wx_check_stall数据流成功");
                    break;
                case "wx_home_exposure":
                    SingleOutputStreamOperator<JSONObject> wxHomeExposureDataStream = DataStreamSourceUtil
                            .getH5LogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new WxHomeExposureProcess())
                            .setParallelism(3)
                            .uid("wx_home_exposure")
                            .name("wx_home_exposure")
                            .disableChaining();
                    eventDataStreamMap.put("wx_home_exposure", wxHomeExposureDataStream);
                    logger.info("##### 创建wx_home_exposure数据流成功");
                    break;
                case "wx_order_list_exposure":
                    SingleOutputStreamOperator<JSONObject> wxOrderListExposureDataStream = DataStreamSourceUtil
                            .getH5LogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new WxOrderListExposureProcess())
                            .setParallelism(3)
                            .uid("wx_order_list_exposure")
                            .name("wx_order_list_exposure")
                            .disableChaining();
                    eventDataStreamMap.put("wx_order_list_exposure", wxOrderListExposureDataStream);
                    logger.info("##### 创建wx_order_list_exposure数据流成功");
                    break;
                case "wx_special_detail_exposure":
                    SingleOutputStreamOperator<JSONObject> wxSpecialDetailExposureDataStream = DataStreamSourceUtil
                            .getH5LogStoreDataStream(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new WxSpecialDetailExposureProcess())
                            .setParallelism(3)
                            .uid("wx_special_detail_exposure")
                            .name("wx_special_detail_exposure")
                            .disableChaining();
                    eventDataStreamMap.put("wx_special_detail_exposure", wxSpecialDetailExposureDataStream);
                    logger.info("##### 创建wx_special_detail_exposure数据流成功");
                    break;

                // 业务数据
                case "yishou.fmys_order":
                    SingleOutputStreamOperator<CommonRow> fmysOrderDataStream = DataStreamSourceUtil
                            .getMySQLBinlogAvro(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new MySQLBinlogAvroProcess("yishou.fmys_order"))
                            .setParallelism(3)
                            .uid("yishou.fmys_order")
                            .name("yishou.fmys_order")
                            .disableChaining();
                    binlogDataStreamMap.put("yishou.fmys_order", fmysOrderDataStream);
                    logger.info("##### 创建fmys_order_data_stream数据流成功(yishou数据库的 用户订单表)");
                    break;
                case "yishou.fmys_order_infos":
                    SingleOutputStreamOperator<CommonRow> fmysOrderInfosDataStream = DataStreamSourceUtil
                            .getMySQLBinlogAvro(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new MySQLBinlogAvroProcess("yishou.fmys_order_infos"))
                            .setParallelism(3)
                            .uid("yishou.fmys_order_infos")
                            .name("yishou.fmys_order_infos")
                            .disableChaining();
                    binlogDataStreamMap.put("yishou.fmys_order_infos", fmysOrderInfosDataStream);
                    logger.info("##### 创建fmys_order_infos_data_stream数据流成功(yishou数据库的 用户订单详情)");
                    break;
                case "yishou.fmys_order_cancel_record":
                    SingleOutputStreamOperator<CommonRow> fmysOrderCancelRecordDataStream = DataStreamSourceUtil
                            .getMySQLBinlogAvro(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new MySQLBinlogAvroProcess("yishou.fmys_order_cancel_record"))
                            .setParallelism(3)
                            .uid("yishou.fmys_order_cancel_record")
                            .name("yishou.fmys_order_cancel_record")
                            .disableChaining();
                    binlogDataStreamMap.put("yishou.fmys_order_cancel_record", fmysOrderCancelRecordDataStream);
                    logger.info("##### 创建fmys_order_cancel_record_data_stream数据流成功(yishou数据库的 订单整笔取消记录)");
                    break;
                case "yishou.fmys_goods":
                    SingleOutputStreamOperator<CommonRow> fmysGoodsDataStream = DataStreamSourceUtil
                            .getMySQLBinlogAvro(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new MySQLBinlogAvroProcess("yishou.fmys_goods"))
                            .setParallelism(3)
                            .uid("yishou.fmys_goods")
                            .name("yishou.fmys_goods")
                            .disableChaining();
                    binlogDataStreamMap.put("yishou.fmys_goods", fmysGoodsDataStream);
                    logger.info("##### 创建fmys_goods_data_stream数据流成功(yishou数据库的 商品表)");
                    break;
                case "yishou.fmys_goods_ext":
                    SingleOutputStreamOperator<CommonRow> fmysGoodsExtDataStream = DataStreamSourceUtil
                            .getMySQLBinlogAvro(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new MySQLBinlogAvroProcess("yishou.fmys_goods_ext"))
                            .setParallelism(3)
                            .uid("yishou.fmys_goods_ext")
                            .name("yishou.fmys_goods_ext")
                            .disableChaining();
                    binlogDataStreamMap.put("yishou.fmys_goods_ext", fmysGoodsExtDataStream);
                    logger.info("##### 创建fmys_goods_ext_data_stream数据流成功(yishou数据库的 商品扩展信息)");
                    break;
                case "yishou.fmys_goods_lib":
                    SingleOutputStreamOperator<CommonRow> fmysGoodsLibDataStream = DataStreamSourceUtil
                            .getMySQLBinlogAvro(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new MySQLBinlogAvroProcess("yishou.fmys_goods_lib"))
                            .setParallelism(3)
                            .uid("yishou.fmys_goods_lib")
                            .name("yishou.fmys_goods_lib")
                            .disableChaining();
                    binlogDataStreamMap.put("yishou.fmys_goods_lib", fmysGoodsLibDataStream);
                    logger.info("##### 创建fmys_goods_lib_data_stream数据流成功(yishou数据库的 商品库)");
                    break;
                case "yishou.fmys_goods_lib_ext":
                    SingleOutputStreamOperator<CommonRow> fmysGoodsLibExtDataStream = DataStreamSourceUtil
                            .getMySQLBinlogAvro(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new MySQLBinlogAvroProcess("yishou.fmys_goods_lib_ext"))
                            .setParallelism(3)
                            .uid("yishou.fmys_goods_lib_ext")
                            .name("yishou.fmys_goods_lib_ext")
                            .disableChaining();
                    binlogDataStreamMap.put("yishou.fmys_goods_lib_ext", fmysGoodsLibExtDataStream);
                    logger.info("##### 创建fmys_goods_lib_ext_data_stream数据流成功(yishou数据库的 商品库拓展表)");
                    break;
                case "yishou.fmys_supply_follow_records":
                    SingleOutputStreamOperator<CommonRow> fmysSupplyFollowlRecordsDataStream = DataStreamSourceUtil
                            .getMySQLBinlogAvro(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new MySQLBinlogAvroProcess("yishou.fmys_supply_follow_records"))
                            .setParallelism(3)
                            .uid("yishou.fmys_supply_follow_records")
                            .name("yishou.fmys_supply_follow_records")
                            .disableChaining();
                    binlogDataStreamMap.put("yishou.fmys_supply_follow_records", fmysSupplyFollowlRecordsDataStream);
                    logger.info("##### 创建fmys_supply_follow_records_data_stream数据流成功(yishou数据库的 供应商关注与取消关注记录)");
                    break;
                /**
                 * 加购和支付
                 */
                case "yishou.fmys_cart_detail,yishou.fmys_order_infos,yishou.fmys_order_cancel_record":
                    SingleOutputStreamOperator<CommonRow> fmys_cart_detail = DataStreamSourceUtil
                            .getMySQLBinlogAvro(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new MySQLBinlogAvroConcurrentProcess("yishou.fmys_cart_detail,yishou.fmys_order_infos,yishou.fmys_order_cancel_record"))
                            .setParallelism(3)
                            .uid("yishou.fmys_cart_detail,yishou.fmys_order_infos,yishou.fmys_order_cancel_record")
                            .name("yishou.fmys_cart_detail,yishou.fmys_order_infos,yishou.fmys_order_cancel_record")
                            .disableChaining();
                    binlogDataStreamMap.put("yishou.fmys_cart_detail,yishou.fmys_order_infos,yishou.fmys_order_cancel_record", fmys_cart_detail);
                    logger.info("##### 创建yishou.fmys_cart_detail,yishou.fmys_order_infos,yishou.fmys_order_cancel_record数据流成功(yishou数据库的加购支付数据记录)");
                    break;
                /**
                 * 支付订单宽表
                 */
                case "yishou.fmys_order_infos,yishou.fmys_order,yishou.fmys_order_cancel_record":
                    SingleOutputStreamOperator<CommonRow> wide_fmys_order_info_details = DataStreamSourceUtil
                            .getMySQLBinlogAvro(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new MySQLBinlogAvroConcurrentProcess("yishou.fmys_order_infos,yishou.fmys_order,yishou.fmys_order_cancel_record"))
                            .setParallelism(3)
                            .uid("yishou.fmys_order_infos,yishou.fmys_order,yishou.fmys_order_cancel_record")
                            .name("yishou.fmys_order_infos,yishou.fmys_order,yishou.fmys_order_cancel_record")
                            .disableChaining();
                    binlogDataStreamMap.put("yishou.fmys_order_infos,yishou.fmys_order,yishou.fmys_order_cancel_record", wide_fmys_order_info_details);
                    logger.info("##### 创建yishou.fmys_order_infos,yishou.fmys_order,yishou.fmys_order_cancel_record数据流成功(yishou数据库的支付订单宽表数据记录)");
                    break;
                /**
                 * 上架+商品库+库存
                 */
                case "yishou.fmys_goods,yishou.fmys_goods_lib,yishou.fmys_goods_in_stock":
                    SingleOutputStreamOperator<CommonRow> onSale = DataStreamSourceUtil
                            .getMySQLBinlogAvro(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new MySQLBinlogAvroConcurrentProcess("yishou.fmys_goods,yishou.fmys_goods_lib,yishou.fmys_goods_in_stock"))
                            .setParallelism(3)
                            .uid("yishou.fmys_goods,yishou.fmys_goods_lib,yishou.fmys_goods_in_stock")
                            .name("yishou.fmys_goods,yishou.fmys_goods_lib,yishou.fmys_goods_in_stock")
                            .disableChaining();
                    binlogDataStreamMap.put("yishou.fmys_goods,yishou.fmys_goods_lib,yishou.fmys_goods_in_stock", onSale);
                    logger.info("##### 创建yishou.fmys_goods,yishou.fmys_goods_lib,yishou.fmys_goods_in_stock数据流成功(yishou数据库的加购支付数据记录)");
                    break;
                case "yishou.fmys_recall_strategy_goods":
                    SingleOutputStreamOperator<CommonRow> fmysRecallStrategyGoodsDataStream = DataStreamSourceUtil
                            .getMySQLBinlogAvro(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new MySQLBinlogAvroProcess("yishou.fmys_recall_strategy_goods"))
                            .uid("yishou.fmys_recall_strategy_goods")
                            .name("yishou.fmys_recall_strategy_goods")
                            .disableChaining();
                    binlogDataStreamMap.put("yishou.fmys_recall_strategy_goods", fmysRecallStrategyGoodsDataStream);
                    logger.info("##### 创建fmys_recall_strategy_goods数据流成功(yishou数据库的 算法策略选品商品)");
                    break;
                case "yishou.fmys_goods_in_stock":
                    SingleOutputStreamOperator<CommonRow> fmysGoodsInStockDataStream = DataStreamSourceUtil
                            .getMySQLBinlogAvro(env, applicationName, Long.parseLong(configMap.get("kafka_offset")))
                            .process(new MySQLBinlogAvroProcess("yishou.fmys_goods_in_stock"))
                            .uid("yishou.fmys_goods_in_stock")
                            .name("yishou.fmys_goods_in_stock")
                            .disableChaining();
                    binlogDataStreamMap.put("yishou.fmys_goods_in_stock", fmysGoodsInStockDataStream);
                    logger.info("##### 创建fmys_goods_in_stock数据流成功(yishou数据库的 算法策略选品商品)");
                    break;
                // 没有匹配到对应值，抛出异常
                default:
                    logger.info("@@@@@ 传入的事件名称没有匹配到，需要进行对应匹配开发");
                    throw new RuntimeException("传入的事件名称没有匹配到，需要进行对应匹配开发");

            }
        }

    }

    /**
     * 启动执行环境
     *
     * @param applicationName app名
     * @throws Exception 抛出的异常
     */
    protected void execute(String applicationName) throws Exception {
        env.execute(applicationName);
    }

    /**
     * 7点定时任务
     */
    protected void schedulerRun() throws ParseException, IOException {

    }


    private void scheduler() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime nextAM = now.withHour(7).withMinute(0).withSecond(0);

        if (now.isAfter(nextAM)) {
            nextAM = nextAM.plusDays(1);
        }

        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    schedulerRun();
                } catch (Throwable e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }, Duration.between(now, nextAM).getSeconds(), 24 * 60 * 60, TimeUnit.SECONDS); // 后续配置早上7点定时调度

    }


}
