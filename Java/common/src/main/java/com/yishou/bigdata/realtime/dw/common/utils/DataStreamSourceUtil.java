package com.yishou.bigdata.realtime.dw.common.utils;

import com.yishou.bigdata.realtime.dw.common.bean.avro.CommonRow;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @date: 2022/12/9
 * @author: yangshibiao
 * @desc: Flink数据源工具类（根据传入的执行环境、消费者组id、时间偏移量等配置返回对应的数据流）
 */
public class DataStreamSourceUtil {

    static Logger logger = LoggerFactory.getLogger(DataStreamSourceUtil.class);

    /**
     * kafka各Topic对应的数据流
     */
    static DataStream<String> appLogStoreDataStream = null;
    static DataStream<String> baobanLogExposureDataStream = null;
    static DataStream<String> goodsClickStoreDataStream = null;
    static DataStream<String> h5LogStoreDataStream = null;
    static DataStream<String> newAppLogStoreDataStream = null;
    static DataStream<String> appClickLogStoreDataStream = null;
    static DataStream<String> slsH5LogDataStream = null;
    static DataStream<String> yishouLogExposureDataStream = null;
    static DataStream<String> yishouRouteLogDataStream = null;
    static DataStream<CommonRow> mySQLBinlogAvroDataStream = null;


    /**
     * 通过传入的env，消费者组id，kafka消息的时间偏移量获取 AppLogStoreDataStream
     *
     * @param env       执行环境
     * @param groupId   消费者组id
     * @param timestamp kafka消息的时间偏移量（13为长整形时间戳）
     * @return DataStreamSource
     */
    public static DataStream<String> getAppLogStoreDataStream(StreamExecutionEnvironment env, String groupId, Long timestamp) {
        if (appLogStoreDataStream == null) {
            appLogStoreDataStream = env
                    .addSource(
                            KafkaUtil.getKafkaConsumer(
                                    ModelUtil.getConfigValue("kafka.bigdata.app.log.store.topic"),
                                    groupId,
                                    timestamp
                            )
                    )
                    .uid("appLogStoreDataStream")
                    .name("appLogStoreDataStream")
                    .disableChaining()
                    .rebalance();
            logger.info("创建appLogStoreDataStream对象成功，其中的消费者组id为：{}，数据偏移量起止时间戳为：{}", groupId, timestamp);
        }
        return appLogStoreDataStream;
    }

    /**
     * 通过传入的env，消费者组id，kafka消息的时间偏移量获取 BaobanLogExposureDataStream
     *
     * @param env       执行环境
     * @param groupId   消费者组id
     * @param timestamp kafka消息的时间偏移量（13为长整形时间戳）
     * @return DataStreamSource
     */
    public static DataStream<String> getBaobanLogExposureDataStream(StreamExecutionEnvironment env, String groupId, Long timestamp) {
        if (baobanLogExposureDataStream == null) {
            baobanLogExposureDataStream = env
                    .addSource(
                            KafkaUtil.getKafkaConsumer(
                                    ModelUtil.getConfigValue("kafka.bigdata.baoban.log.exposure.topic"),
                                    groupId,
                                    timestamp
                            )
                    )
                    .uid("baobanLogExposureDataStream")
                    .name("baobanLogExposureDataStream")
                    .disableChaining()
                    .rebalance();
            logger.info("创建baobanLogExposureDataStream对象成功，其中的消费者组id为：{}，数据偏移量起止时间戳为：{}", groupId, timestamp);
        }
        return baobanLogExposureDataStream;
    }

    /**
     * 通过传入的env，消费者组id，kafka消息的时间偏移量获取 GoodsClickStoreDataStream
     *
     * @param env       执行环境
     * @param groupId   消费者组id
     * @param timestamp kafka消息的时间偏移量（13为长整形时间戳）
     * @return DataStreamSource
     */
    public static DataStream<String> getGoodsClickStoreDataStream(StreamExecutionEnvironment env, String groupId, Long timestamp) {
        if (goodsClickStoreDataStream == null) {
            goodsClickStoreDataStream = env
                    .addSource(
                            KafkaUtil.getKafkaConsumer(
                                    ModelUtil.getConfigValue("kafka.bigdata.goods.click.store.topic"),
                                    groupId,
                                    timestamp
                            )
                    )
                    .uid("goodsClickStoreDataStream")
                    .name("goodsClickStoreDataStream")
                    .disableChaining()
                    .rebalance();
            logger.info("创建goodsClickStoreDataStream对象成功，其中的消费者组id为：{}，数据偏移量起止时间戳为：{}", groupId, timestamp);
        }
        return goodsClickStoreDataStream;
    }

    /**
     * 通过传入的env，消费者组id，kafka消息的时间偏移量获取 H5LogStoreDataStream
     *
     * @param env       执行环境
     * @param groupId   消费者组id
     * @param timestamp kafka消息的时间偏移量（13为长整形时间戳）
     * @return DataStreamSource
     */
    public static DataStream<String> getH5LogStoreDataStream(StreamExecutionEnvironment env, String groupId, Long timestamp) {
        if (h5LogStoreDataStream == null) {
            h5LogStoreDataStream = env
                    .addSource(
                            KafkaUtil.getKafkaConsumer(
                                    ModelUtil.getConfigValue("kafka.bigdata.h5.log.store.topic"),
                                    groupId,
                                    timestamp
                            )
                    ).setParallelism(3)
                    .uid("h5LogStoreDataStream")
                    .name("h5LogStoreDataStream")
                    .disableChaining()
                    .rebalance();
            logger.info("创建h5LogStoreDataStream对象成功，其中的消费者组id为：{}，数据偏移量起止时间戳为：{}", groupId, timestamp);
        }
        return h5LogStoreDataStream;
    }

    /**
     * 通过传入的env，消费者组id，kafka消息的时间偏移量获取 NewAppLogStoreDataStream
     *
     * @param env       执行环境
     * @param groupId   消费者组id
     * @param timestamp kafka消息的时间偏移量（13为长整形时间戳）
     * @return DataStreamSource
     */
    public static DataStream<String> getNewAppLogStoreDataStream(StreamExecutionEnvironment env, String groupId, Long timestamp) {
        if (newAppLogStoreDataStream == null) {
            newAppLogStoreDataStream = env
                    .addSource(
                            KafkaUtil.getKafkaConsumer(
                                    ModelUtil.getConfigValue("kafka.bigdata.new.app.log.store.topic"),
                                    groupId,
                                    timestamp
                            )
                    ).setParallelism(3)
                    .uid("newAppLogStoreDataStream")
                    .name("newAppLogStoreDataStream")
                    .disableChaining()
                    .rebalance();
            logger.info("创建newAppLogStoreDataStream对象成功，其中的消费者组id为：{}，数据偏移量起止时间戳为：{}", groupId, timestamp);
        }
        return newAppLogStoreDataStream;
    }

    /**
     * 通过传入的env，消费者组id，kafka消息的时间偏移量获取 AppClickLogStoreDataStream
     *
     * @param env       执行环境
     * @param groupId   消费者组id
     * @param timestamp kafka消息的时间偏移量（13为长整形时间戳）
     * @return DataStreamSource
     */
    public static DataStream<String> getAppClickLogStoreDataStream(StreamExecutionEnvironment env, String groupId, Long timestamp) {
        if (appClickLogStoreDataStream == null) {
            appClickLogStoreDataStream = env
                    .addSource(
                            KafkaUtil.getKafkaConsumer(
                                    ModelUtil.getConfigValue("kafka.bigdata.app.click.log.store.topic"),
                                    groupId,
                                    timestamp
                            )
                    )
                    .uid("appClickLogStoreDataStream")
                    .name("appClickLogStoreDataStream")
                    .disableChaining()
                    .rebalance();
            logger.info("创建appClickLogStoreDataStream对象成功，其中的消费者组id为：{}，数据偏移量起止时间戳为：{}", groupId, timestamp);
        }
        return appClickLogStoreDataStream;
    }

    /**
     * 通过传入的env，消费者组id，kafka消息的时间偏移量获取 SlsH5LogDataStream
     *
     * @param env       执行环境
     * @param groupId   消费者组id
     * @param timestamp kafka消息的时间偏移量（13为长整形时间戳）
     * @return DataStreamSource
     */
    public static DataStream<String> getSlsH5LogDataStream(StreamExecutionEnvironment env, String groupId, Long timestamp) {
        if (slsH5LogDataStream == null) {
            slsH5LogDataStream = env
                    .addSource(
                            KafkaUtil.getKafkaConsumer(
                                    ModelUtil.getConfigValue("kafka.bigdata.sls.h5.log.topic"),
                                    groupId,
                                    timestamp
                            )
                    )
                    .uid("slsH5LogDataStream")
                    .name("slsH5LogDataStream")
                    .disableChaining()
                    .rebalance();
            logger.info("创建slsH5LogDataStream对象成功，其中的消费者组id为：{}，数据偏移量起止时间戳为：{}", groupId, timestamp);
        }
        return slsH5LogDataStream;
    }

    /**
     * 通过传入的env，消费者组id，kafka消息的时间偏移量获取 YishouLogExposureDataStream
     *
     * @param env       执行环境
     * @param groupId   消费者组id
     * @param timestamp kafka消息的时间偏移量（13为长整形时间戳）
     * @return DataStreamSource
     */
    public static DataStream<String> getYishouLogExposureDataStream(StreamExecutionEnvironment env, String groupId, Long timestamp) {
        if (yishouLogExposureDataStream == null) {
            yishouLogExposureDataStream = env
                    .addSource(
                            KafkaUtil.getKafkaConsumer(
                                    ModelUtil.getConfigValue("kafka.bigdata.yishou.log.exposure.topic"),
                                    groupId,
                                    timestamp
                            )
                    ).setParallelism(3)
                    .uid("yishouLogExposureDataStream")
                    .name("yishouLogExposureDataStream")
                    .disableChaining()
                    .rebalance();
            logger.info("创建yishouLogExposureDataStream对象成功，其中的消费者组id为：{}，数据偏移量起止时间戳为：{}", groupId, timestamp);
        }
        return yishouLogExposureDataStream;
    }

    /**
     * 通过传入的env，消费者组id，kafka消息的时间偏移量获取 YishouRouteLogDataStream
     *
     * @param env       执行环境
     * @param groupId   消费者组id
     * @param timestamp kafka消息的时间偏移量（13为长整形时间戳）
     * @return DataStreamSource
     */
    public static DataStream<String> getYishouRouteLogDataStream(StreamExecutionEnvironment env, String groupId, Long timestamp) {
        if (yishouRouteLogDataStream == null) {
            yishouRouteLogDataStream = env
                    .addSource(
                            KafkaUtil.getKafkaConsumer(
                                    ModelUtil.getConfigValue("kafka.bigdata.yishou.route.log.topic"),
                                    groupId,
                                    timestamp
                            )
                    )
                    .uid("yishouRouteLogDataStream")
                    .name("yishouRouteLogDataStream")
                    .disableChaining()
                    .rebalance();
            logger.info("创建yishouRouteLogDataStream对象成功，其中的消费者组id为：{}，数据偏移量起止时间戳为：{}", groupId, timestamp);
        }
        return yishouRouteLogDataStream;
    }


    /**
     * 通过传入的env，消费者组id，kafka消息的时间偏移量获取 MySQLBinlogAvroDataStream
     *
     * @param env       执行环境
     * @param groupId   消费者组id
     * @param timestamp kafka消息的时间偏移量（13为长整形时间戳）
     * @return DataStreamSource
     */
    public static DataStream<CommonRow> getMySQLBinlogAvro(StreamExecutionEnvironment env, String groupId, Long timestamp) {
        if (mySQLBinlogAvroDataStream == null) {
            mySQLBinlogAvroDataStream = env
                    .addSource(
                            KafkaUtil.getKafkaConsumerAvro(
                                    ModelUtil.getConfigValue("kafka.bigdata.mysql.binlog.avro.topic"),
                                    groupId,
                                    CommonRow.class,
                                    timestamp
                            )
                    ).setParallelism(3)
                    .uid("mySQLBinlogAvroDataStream")
                    .name("mySQLBinlogAvroDataStream")
                    .disableChaining();
            logger.info("创建mySQLBinlogAvroDataStream对象成功，其中的消费者组id为：{}，数据偏移量起止时间戳为：{}", groupId, timestamp);
        }
        return mySQLBinlogAvroDataStream;
    }

}
