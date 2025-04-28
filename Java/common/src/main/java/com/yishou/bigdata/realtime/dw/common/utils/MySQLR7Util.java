package com.yishou.bigdata.realtime.dw.common.utils;

import com.google.common.base.CaseFormat;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @date: 2021/7/5
 * @author: yangshibiao
 * @desc: 对数据库R7（MySQL）的工具类
 */
public class MySQLR7Util {

    static Logger logger = LoggerFactory.getLogger(MySQLR7Util.class);

    /**
     * jdbcTemplate
     */
    private static JdbcTemplate jdbcTemplate;

    /**
     * 使用单例模式获取MySQL数据库R7的连接
     *
     * @return jdbc连接
     */
    public static JdbcTemplate getJdbcTemplate() {
        if (jdbcTemplate == null) {
            synchronized (MySQLR7Util.class) {
                if (jdbcTemplate == null) {
                    try {
                        jdbcTemplate = createJdbcTemplate(ModelUtil.getConfigValue("mysql.r7.yishou.url"), ModelUtil.getConfigValue("mysql.r7.username"), ModelUtil.getConfigValue("mysql.r7.password"));
                    } catch (Exception e) {
                        logger.error("jdbcUrl:[{}]", ModelUtil.getConfigValue("mysql.r7.yishou.url"));
                        logger.error("jdbcUserName:[{}]", ModelUtil.getConfigValue("mysql.r7.username"));
                        logger.error(e.getMessage(), e);
                        throw new RuntimeException("创建 MySQL R7 数据库连接失败");
                    }
                }
            }
        }
        return jdbcTemplate;
    }

    public static JdbcTemplate createJdbcTemplate(String url, String userName, String password) {
        try {
            HikariDataSource ds = new HikariDataSource();
            ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
            ds.setJdbcUrl(url);
            ds.setUsername(userName);
            ds.setPassword(password);
            ds.setMaximumPoolSize(50);
            ds.setMinimumIdle(5);
            logger.info(
                    "##### 使用HikariPool连接池初始化JdbcTemplate成功，使用的URL为：{} , 其中最大连接大小为：{} , 最小连接大小为：{} ;",
                    ds.getJdbcUrl(),
                    ds.getMaximumPoolSize(),
                    ds.getMinimumIdle()
            );
            return new JdbcTemplate(ds);
        } catch (Exception e) {
            logger.error("jdbcUrl:[{}]", url);
            logger.error("jdbcUserName:[{}]", userName);
            logger.error(e.getMessage(), e);
            throw new RuntimeException("创建 MySQL 数据库连接失败");
        }
    }

    /**
     * 处理传入数据中的特殊字符（例如： 单引号）
     *
     * @param data 传入的数据
     * @return 返回的结果
     */
    public static String disposeSpecialCharacter(String data) {

        // 处理其中的单引号
        data = data.replace("'", "''");

        // 返回结果
        return data;

    }

    /**
     * 通过传入的表名，主键key，主键value，样例类 和 需要的值，获取对应的数据
     *
     * @param tableName    表名
     * @param primaryKey   主键字段
     * @param primaryValue 主键值
     * @param clz          返回的数据类型
     * @param fields       需要的字段名
     * @param <T>          样例类
     * @return 样例类集合
     */
    public static <T> List<T> queryListByKey(String tableName, String primaryKey, String primaryValue, Class<T> clz, String... fields) {

        // 拼接SQL
        String sql = " select " +
                Arrays.stream(fields).map(String::valueOf).collect(Collectors.joining(",")) +
                " from " + tableName +
                " where " + primaryKey + " = '" + disposeSpecialCharacter(primaryValue) + "'";

        // 执行SQL并返回结果
        return queryList(sql, clz);

    }

    /**
     * 如果传入的clz中的属性又包含对象，会报错，此时传入JSONObject对象即可
     *
     * @param sql 执行的查询语句
     * @param clz 返回的数据类型
     * @param <T> 样例类
     * @return 样例类集合
     */
    public static <T> List<T> queryList(String sql, Class<T> clz) {
        try {
            List<Map<String, Object>> mapList = MySQLR7Util.getJdbcTemplate().queryForList(sql);
            List<T> resultList = new ArrayList<>();
            for (Map<String, Object> map : mapList) {
                Set<String> keys = map.keySet();
                // 当返回的结果中存在数据，通过反射将数据封装成样例类对象
                T result = clz.newInstance();
                for (String key : keys) {
                    BeanUtils.setProperty(
                            result,
                            key,
                            map.get(key)
                    );
                }
                resultList.add(result);
            }
            return resultList;
        } catch (Exception exception) {
            exception.printStackTrace();
            throw new RuntimeException(
                    "\r\n从 MySQL R7 数据库中 查询 数据失败，" +
                            "\r\n抛出的异常信息为：" + exception.getMessage() +
                            "\r\n查询的SQL为：" + sql
            );
        }
    }

    /**
     * 如果传入的clz中的属性又包含对象，会报错，此时传入JSONObject对象即可
     *
     * @param sql               执行的查询语句
     * @param underScoreToCamel 是否将下划线转换为驼峰命名法
     * @param clz               返回的数据类型
     * @param <T>               样例类
     * @return 样例类集合
     */
    public static <T> List<T> queryList(String sql, boolean underScoreToCamel, Class<T> clz) {
        try {
            List<Map<String, Object>> mapList = MySQLR7Util.getJdbcTemplate().queryForList(sql);
            List<T> resultList = new ArrayList<>();
            for (Map<String, Object> map : mapList) {
                Set<String> keys = map.keySet();
                // 当返回的结果中存在数据，通过反射将数据封装成样例类对象
                T result = clz.newInstance();
                for (String key : keys) {
                    String propertyName = underScoreToCamel ? CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, key) : key;
                    BeanUtils.setProperty(
                            result,
                            propertyName,
                            map.get(key)
                    );
                }
                resultList.add(result);
            }
            return resultList;
        } catch (Exception exception) {
            exception.printStackTrace();
            throw new RuntimeException(
                    "\r\n从 MySQL R7 数据库中 查询 数据失败，" +
                            "\r\n抛出的异常信息为：" + exception.getMessage() +
                            "\r\n查询的SQL为：" + sql
            );
        }
    }

}
