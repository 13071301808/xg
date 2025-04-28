package com.yishou.bigdata.realtime.dw.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;


/**
 * @date: 2022/12/6
 * @author: yangshibiao
 * @desc: RedisUtil
 */
public class RedisUtil {

    static Logger logger = LoggerFactory.getLogger(RedisUtil.class);

    /**
     * jedis连接池
     */
    private JedisPool jedisPool;

    /**
     * 通过传入的参数创建RedisUtil对象
     *
     * @param host     redis的host
     * @param port     redis的端口
     * @param password redis的password
     */
    public RedisUtil(String host, int port, String password) {
        initJedisPool(host, port, password, 30, 2, 1, 3000);
    }

    /**
     * 通过传入的参数创建RedisUtil对象
     *
     * @param host     redis的host
     * @param port     redis的端口
     * @param password redis的password
     * @param maxTotal 连接池中最大连接数
     * @param maxIdle  连接池中最大空闲连接数
     * @param minIdle  连接池中最小空闲连接数
     * @param timeout  连接Redis超时时间
     */
    public RedisUtil(String host, int port, String password, int maxTotal, int maxIdle, int minIdle, int timeout) {
        initJedisPool(host, port, password, maxTotal, maxIdle, minIdle, timeout);
    }

    /**
     * 初始化Jedis对象
     *
     * @param host     redis的host
     * @param port     redis的端口
     * @param password redis的password
     * @param maxTotal 连接池中最大连接数
     * @param maxIdle  连接池中最大空闲连接数
     * @param minIdle  连接池中最小空闲连接数
     * @param timeout  连接Redis超时时间
     */
    public void initJedisPool(String host, int port, String password, int maxTotal, int maxIdle, int minIdle, int timeout) {

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(maxTotal);
        poolConfig.setMaxIdle(maxIdle);
        poolConfig.setMinIdle(minIdle);
        poolConfig.setMaxWaitMillis(5000L);
        poolConfig.setTestOnBorrow(true);  // 启用连接有效性检查
        jedisPool = new JedisPool(
                poolConfig,
                host,
                port,
                timeout,
                password
        );

        logger.info(
                "根据传入的参数创建redis连接池jedisPool对象成功，使用的host为：{}，使用的port为：{}，最大连接数为{}，最大空闲连接数为{}，最小空闲连接数为：{}，连接超时时间为(毫秒)：{}",
                host,
                port,
                maxTotal,
                maxIdle,
                minIdle,
                timeout
        );

    }

    /**
     * 获取对应的 Jedis
     * 注意：该jedis对象是从连接池中返回，使用完之后需要关闭
     *
     * @param index redis对应的索引
     * @return Jedis
     */
    public Jedis getJedis(int index) {
        Jedis jedis = jedisPool.getResource();
        jedis.select(index);
        return jedis;
    }

    /**
     * 根据传入的数据库索引和key获取对应的值
     * 注意：如果该key不存在，就返回 'nil'，如果存储在key的值不是字符串，则返回错误
     *
     * @param index redis的索引
     * @param key   redis的key
     * @return value
     */
    public String getValue(int index, String key) {
        Jedis jedis = getJedis(index);
        String value = jedis.get(key);
        jedis.close();
        return value;
    }

    /**
     * 根据传入的数据库索引,key,field获取对应的值
     * 注意：如果该key不存在，就返回 'nil'，如果存储在key的值不是字符串，则返回错误
     *
     * @param index redis的索引
     * @param key   redis的key
     * @param field redis的field
     * @return value
     */
    public String getHashValue(int index, String key, String field) {
        Jedis jedis = getJedis(index);
        String value = jedis.hget(key, field);
        jedis.close();
        return value;
    }

    public byte[] getHashValue(int index, byte[] key, byte[] field) {
        Jedis jedis = getJedis(index);
        byte[] value = jedis.hget(key, field);
        jedis.close();
        return value;
    }

    public void delHashValue(int index, byte[] key, byte[] field) {
        Jedis jedis = getJedis(index);
        jedis.hdel(key,field);
        jedis.close();
    }

    /**
     * 将哈希表key中的域field的值设为value
     *
     * @param index 库（索引）
     * @param key   键
     * @param field 字段
     * @param value 值
     * @return 如果该字段已经存在，并且HSET刚刚更新了该值，则返回0，否则，如果创建了一个新字段，则返回1
     */
    public long setHash(int index, String key, String field, String value) {
        Jedis jedis = getJedis(index);
        long num = jedis.hset(key, field, value);
        jedis.close();
        return num;
    }

    public long setHash(int index, byte[] key, byte[] field, byte[] value) {
        Jedis jedis = getJedis(index);
        long num = jedis.hset(key, field, value);
        jedis.close();
        return num;
    }

    /**
     * 根据传入的库（索引）和key，删除对应库中的key
     *
     * @param index 库（索引）
     * @param key   键（一个或者多个）
     * @return 如果删除了一个或多个键，则返回大于0的整数;如果指定的键不存在，则返回0
     */
    public long delKey(int index, String... key) {
        Jedis jedis = getJedis(index);
        long num = jedis.del(key);
        jedis.close();
        return num;
    }

    public void close(){
        this.jedisPool.close();
    }

    public void setHashWithRetry(int db, byte[] key, byte[] field, byte[] value, int maxRetries, long retryInterval) {
        int retries = 0;
        while (retries < maxRetries) {
            try (Jedis jedis = getJedis(db)) {
                jedis.hset(key, field, value);
                return; // 成功后退出
            } catch (Throwable e) {
                logger.warn("Redis connection reset, retrying... ({}/{})", retries + 1, maxRetries);
                retries++;
                if (retries >= maxRetries) {
                    logger.error("Max retries reached, giving up.", e);
                    throw new RuntimeException("Failed to set hash after multiple retries", e);
                }
                try {
                    Thread.sleep(retryInterval);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    logger.error("Thread interrupted while waiting for retry.", ie);
                    throw new RuntimeException("Thread interrupted while waiting for retry", ie);
                }
            }
        }
    }
}
