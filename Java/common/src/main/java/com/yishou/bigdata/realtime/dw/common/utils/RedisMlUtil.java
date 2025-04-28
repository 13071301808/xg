package com.yishou.bigdata.realtime.dw.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @date: 2022/12/29
 * @author: yangshibiao
 * @desc: ml-redis集群工具类
 */
public class RedisMlUtil {

    static Logger logger = LoggerFactory.getLogger(RedisMlUtil.class);

    /**
     * JedisPool对象
     */
    private static JedisPool jedisPool;

    /**
     * 获取JedisPool对象
     *
     * @return JedisPool对象
     */
    public static JedisPool getJedisPool() {

        if (jedisPool == null) {
            synchronized (RedisMlUtil.class) {
                if (jedisPool == null) {
                    JedisPoolConfig poolConfig = new JedisPoolConfig();
                    poolConfig.setMaxTotal(50);
                    poolConfig.setMaxIdle(50);
                    poolConfig.setMinIdle(5);
                    jedisPool = new JedisPool(
                            poolConfig,
                            ModelUtil.getConfigValue("redis.ml.hostname"),
                            Integer.parseInt(ModelUtil.getConfigValue("redis.ml.port")),
                            3000,
                            ModelUtil.getConfigValue("redis.ml.password")
                    );
                    logger.info(
                            "根据传入的参数创建redis连接池jedisPool对象成功，使用的host为：{}，使用的port为：{}，最大连接数为{}，最大空闲连接数为{}，最小空闲连接数为：{}，连接超时时间为(毫秒)：{}",
                            ModelUtil.getConfigValue("redis.ml.hostname"),
                            Integer.parseInt(ModelUtil.getConfigValue("redis.ml.port")),
                            50,
                            50,
                            5,
                            3000
                    );
                }
            }
        }

        return jedisPool;

    }

    /**
     * 获取对应的 Jedis
     * 注意：该jedis对象是从连接池中返回，使用完之后需要关闭
     *
     * @param index redis对应的索引
     * @return Jedis
     */
    public static Jedis getJedis(int index) {
        Jedis jedis = getJedisPool().getResource();
        jedis.select(index);
        return jedis;
    }

    /**
     * 通过传入的key获取对应的value值
     *
     * @param key 键
     * @return 值
     */
    public static String getValue(int index, String key) {
        Jedis jedis = getJedis(index);
        String value = jedis.get(key);
        jedis.close();
        return value;
    }

}
