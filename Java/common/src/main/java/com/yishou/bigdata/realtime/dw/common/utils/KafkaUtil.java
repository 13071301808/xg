package com.yishou.bigdata.realtime.dw.common.utils;

import com.alibaba.fastjson.JSON;
import com.yishou.bigdata.realtime.dw.common.bean.avro.CommonRow;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @date: 2021/5/22
 * @author: yangshibiao
 * @desc: Kafka工具类
 */
public class KafkaUtil {

    static Logger logger = LoggerFactory.getLogger(KafkaUtil.class);

    /**
     * 通过消费者组id 获取对应的kafka消费者配置
     *
     * @param groupId 消费者组id
     * @return 配置对象
     */
    public static Properties getKafkaConsumerProperties(String groupId) {

        // Kakfa的参数设置
        Properties props = new Properties();
        // 集群地址 和 消费者组id
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ModelUtil.getConfigValue("bootstrap.servers"));
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // 偏移量自动提交，虽然在使用checkpoint时可以保存，但还是自动提交偏移量比较稳妥
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ModelUtil.getConfigValue("enable.auto.commit"));
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, ModelUtil.getConfigValue("auto.commit.interval.ms"));
        // 设置事务的提交发送
        props.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        // 开启一个后台线程每隔5s检测一下Kafka的分区情况
        props.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, ModelUtil.getConfigValue("flink.partition-discovery.interval-millis"));
        // 当在kafka中保存偏移量的topic中有偏移量时从偏移量消费，没有从最新开始消费（其他还可以设置earliest，从最开始的数据开始消费等）
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ModelUtil.getConfigValue("auto.offset.reset"));

        // 返回参数设置对象
        return props;
    }


    /**
     * 获取kafka生产者配置
     *
     * @return 配置对象
     */
    public static Properties getKafkaProducerProperties() {

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ModelUtil.getConfigValue("bootstrap.servers"));
        //如果 10 分钟没有更新状态，则超时( 默认超时时间是1分钟)，表示已经提交事务到kafka，但10分钟还没有上传数据，结束事务
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, String.valueOf(10 * 60 * 1000));
        // 配置生产者的kafka的单条消息的最大大小，并使用snapp压缩，并将消息记录封装成批，大于32K就发送，并设置客户端本地内存缓冲为32M
        props.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(10 * 1024 * 1024));
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(32 * 1024));
        props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(32 * 1024 * 1024));
        // 设置kafka的acks为 -1（相当于all，需要所有副本都接收到数据才算），结合min.insync.replicas来设置，必须要有1个副本接收才算正常发送
        props.setProperty(ProducerConfig.ACKS_CONFIG, String.valueOf(-1));

        // 返回参数设置对象
        return props;
    }

    /**
     * 封装kafka消费者
     *
     * @param topicName 主题名
     * @param groupId   消费者组id
     * @return 消费者对象
     */
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topicName, String groupId) {

        // 获取kafka的配置对象
        Properties props = KafkaUtil.getKafkaConsumerProperties(groupId);

        // 创建一个FlinkKafka的消费者
        return new FlinkKafkaConsumer<>(topicName, new SimpleStringSchema(), props);

    }


    /**
     * 封装kafka消费者，从传入的时间戳开始消费
     *
     * @param topicName 主题名
     * @param groupId   消费者组id
     * @param timestamp 13为长整形时间戳
     * @return 消费者对象
     */
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topicName, String groupId, Long timestamp) {

        // 获取kafka的配置对象
        Properties props = KafkaUtil.getKafkaConsumerProperties(groupId);

        // 创建一个FlinkKafka的消费者
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topicName, new SimpleStringSchema(), props);

        // 设置从指定时间戳开始消费
        logger.info(">>>>> 从kafka的指定时间戳开始消费，时间戳：{}", timestamp);
        consumer.setStartFromTimestamp(timestamp);

        // 返回消费者对象
        return consumer;

    }


    /**
     * 封装kafka消费者，反序列化Avro为指定类型
     *
     * @param topicName 主题名
     * @param groupId   消费者组id
     * @param clz       指定类型
     * @return 消费者对象
     */
    public static <T> FlinkKafkaConsumer<T> getKafkaConsumerAvro(String topicName, String groupId, Class<T> clz) {

        // 获取kafka的配置对象
        Properties props = KafkaUtil.getKafkaConsumerProperties(groupId);

        // kafka反序列化对象
        KafkaDeserializationSchema<T> deserializationSchema = new KafkaDeserializationSchema<T>() {
            ReflectDatumReader<T> reflectDatumReader = new ReflectDatumReader<>(ReflectData.AllowNull.get().getSchema(CommonRow.class));

            @Override
            public TypeInformation<T> getProducedType() {
                return TypeInformation.of(clz);
            }

            @Override
            public boolean isEndOfStream(T nextElement) {
                return false;
            }

            @Override
            public T deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                try {
                    return reflectDatumReader.read(null, DecoderFactory.get().binaryDecoder(record.value(), null));
                } catch (Exception e) {
                    logger.error(
                            "@@@@@ 从kafka中获取的avro数据反序列化失败，不能按照传入的类进行反序列化，传入的类名为：{}；传入的数据为：{}；抛出的异常信息为：{}",
                            clz.getName(),
                            new String(record.value()),
                            e.getMessage()
                    );
                    return null;
                }
            }
        };

        // 创建基于flink的kafka消费者
        return new FlinkKafkaConsumer<T>(topicName, deserializationSchema, props);

    }


    /**
     * 封装kafka消费者，反序列化Avro为指定类型，并从指定时间戳开始消费
     *
     * @param topicName 主题名
     * @param groupId   消费者组id
     * @param clz       指定类型
     * @param timestamp 指定的时间戳，13位长整型
     * @return 消费者对象
     */
    public static <T> FlinkKafkaConsumer<T> getKafkaConsumerAvro(String topicName, String groupId, Class<T> clz, Long timestamp) {

        // 获取kafka的配置对象
        Properties props = KafkaUtil.getKafkaConsumerProperties(groupId);

        // kafka反序列化对象
        KafkaDeserializationSchema<T> deserializationSchema = new KafkaDeserializationSchema<T>() {
            @Override
            public TypeInformation<T> getProducedType() {
                return TypeInformation.of(clz);
            }

            @Override
            public boolean isEndOfStream(T nextElement) {
                return false;
            }

            @Override
            public T deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                try {
                    ReflectDatumReader<T> reflectDatumReader = new ReflectDatumReader<>(ReflectData.AllowNull.get().getSchema(clz));
                    return reflectDatumReader.read(null, DecoderFactory.get().binaryDecoder(record.value(), null));
                } catch (Exception e) {
                    logger.error(
                            "@@@@@ 从kafka中获取的avro数据反序列化失败，不能按照传入的类进行反序列化，传入的类名为：{}；传入的数据为：{}；抛出的异常信息为：{}",
                            clz.getName(),
                            new String(record.value()),
                            e.getMessage()
                    );
                    return null;
                }
            }
        };

        // 创建基于flink的kafka消费者
        FlinkKafkaConsumer<T> consumer = new FlinkKafkaConsumer<>(topicName, deserializationSchema, props);

        // 设置从指定时间戳开始消费
        logger.info(">>>>> 从kafka的指定时间戳开始消费，时间戳：{}", timestamp);
        consumer.setStartFromTimestamp(timestamp);

        return consumer;

    }


    /**
     * 封装kafka消费者（返回一个Tuple2，其中第一个元素为kafka的value值，第二个为该消息在kafka中对应的时间戳）
     *
     * @param topicName 主题名
     * @param groupId   消费者组id
     * @return 消费者对象
     */
    public static FlinkKafkaConsumer<Tuple2<String, Long>> getKafkaConsumerAndTimestamp(String topicName, String groupId) {

        // 获取kafka的配置对象
        Properties props = KafkaUtil.getKafkaConsumerProperties(groupId);

        // 自定义kafka的反序列化器
        KafkaDeserializationSchema<Tuple2<String, Long>> deserializationSchema = new KafkaDeserializationSchema<Tuple2<String, Long>>() {

            @Override
            public TypeInformation<Tuple2<String, Long>> getProducedType() {
                return TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                });
            }

            @Override
            public boolean isEndOfStream(Tuple2<String, Long> nextElement) {
                return false;
            }

            @Override
            public Tuple2<String, Long> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                String message = new String(record.value(), StandardCharsets.UTF_8);
                long timestamp = record.timestamp() / 1000;
                return Tuple2.of(message, timestamp);
            }
        };

        // 创建一个FlinkKafka的消费者，其中包含kafka中的value和该条消息到kafka的时间
        return new FlinkKafkaConsumer<>(topicName, deserializationSchema, props);
    }

    /**
     * 封装kafka消费者，从传入的时间戳开始消费（返回一个Tuple2，其中第一个元素为kafka的value值，第二个为该消息在kafka中对应的时间戳）
     *
     * @param topicName 主题名
     * @param groupId   消费者组id
     * @param timestamp 13位长整形时间戳
     * @return 消费者对象
     */
    public static FlinkKafkaConsumer<Tuple2<String, Long>> getKafkaConsumerAndTimestamp(String topicName, String groupId, Long timestamp) {

        // 获取kafka的配置对象
        Properties props = KafkaUtil.getKafkaConsumerProperties(groupId);

        // 自定义kafka的反序列化器
        KafkaDeserializationSchema<Tuple2<String, Long>> deserializationSchema = new KafkaDeserializationSchema<Tuple2<String, Long>>() {

            @Override
            public TypeInformation<Tuple2<String, Long>> getProducedType() {
                return TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                });
            }

            @Override
            public boolean isEndOfStream(Tuple2<String, Long> nextElement) {
                return false;
            }

            @Override
            public Tuple2<String, Long> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                String message = new String(record.value(), StandardCharsets.UTF_8);
                long timestamp = record.timestamp() / 1000;
                return Tuple2.of(message, timestamp);
            }
        };

        // 创建一个FlinkKafka的消费者，其中包含kafka中的value和该条消息到kafka的时间
        FlinkKafkaConsumer<Tuple2<String, Long>> consumer = new FlinkKafkaConsumer<>(topicName, deserializationSchema, props);

        // 设置从指定时间戳开始消费
        logger.info(">>>>> 从kafka的指定时间戳开始消费，时间戳：{}", timestamp);
        consumer.setStartFromTimestamp(timestamp);

        // 返回消费者对象
        return consumer;

    }

    /**
     * 封装kafka生产者
     *
     * @param topicName 主题名
     * @return 生产者对象
     */
    public static FlinkKafkaProducer<String> getKafkaProducer(String topicName) {
        return new FlinkKafkaProducer<>(ModelUtil.getConfigValue("bootstrap.servers"), topicName, new SimpleStringSchema());
    }

    /**
     * 封装 Kafka 生产者，动态指定多个不同主题，并使用精确一次语议
     *
     * @param serializationSchema 序列化模式
     * @param <T>                 来源数据类型
     * @return FlinkKafkaProducer
     */
    public static <T> FlinkKafkaProducer<T> getKafkaProducerForExactlyOnce(KafkaSerializationSchema<T> serializationSchema) {

        Properties props = KafkaUtil.getKafkaProducerProperties();
        return new FlinkKafkaProducer<>(ModelUtil.getConfigValue("kafka.default.topic"), serializationSchema, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

    }

    /**
     * 封装 Kafka 精确一次语议 生产者对象
     *
     * @param topicName 主题名
     * @param <T>       来源数据类型
     * @return FlinkKafkaProducer
     */
    public static <T> FlinkKafkaProducer<T> getKafkaProducerForExactlyOnce(String topicName) {
        return KafkaUtil.getKafkaProducerForExactlyOnce((KafkaSerializationSchema<T>) (t, aLong) -> new ProducerRecord<>(topicName, JSON.toJSONBytes(t)));
    }

    /**
     * 封装 Kafka 精确一次语议 生产者对象，使用ReflectDatum模式的Avro
     *
     * @param topicName 主题名
     * @param <T>       来源数据类型
     * @return FlinkKafkaProducer
     */
    public static <T> FlinkKafkaProducer<T> getKafkaProducerAvroForExactlyOnce(String topicName, Class<T> clz) {
        return KafkaUtil.getKafkaProducerForExactlyOnce((KafkaSerializationSchema<T>) (element, timestamp) -> {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            ReflectDatumWriter<T> writer = new ReflectDatumWriter<>(ReflectData.AllowNull.get().getSchema(clz));
            byte[] value = null;
            try {
                writer.write(element, encoder);
                encoder.flush();
                value = out.toByteArray();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("将数据序列化成Avro格式异常，异常信息如下 \r\n " + e.getMessage());
            }
            return new ProducerRecord<>(topicName, value);
        });
    }


}
