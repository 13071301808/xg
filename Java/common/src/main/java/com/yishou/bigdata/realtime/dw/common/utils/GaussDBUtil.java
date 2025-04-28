package com.yishou.bigdata.realtime.dw.common.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import com.google.common.collect.Lists;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.*;

/**
 * @date: 2021/6/9
 * @author: yangshibiao
 * @desc: GaussDB中DWS服务的工具类
 */
public class GaussDBUtil {

    static Logger logger = LoggerFactory.getLogger(GaussDBUtil.class);

    /**
     * jdbcTemplate
     */
    private static JdbcTemplate jdbcTemplate;

    /**
     * 使用单例模式获取实时数仓的GaussDB的jdbcTemplate
     *
     * @return jdbcTemplate
     */
    public static JdbcTemplate getJdbcTemplate() {
        if (jdbcTemplate == null) {
            synchronized (GaussDBUtil.class) {
                if (jdbcTemplate == null) {
                    try {
                        Properties props = new Properties();
                        props.put("batchMode", "OFF");

                        HikariDataSource ds = new HikariDataSource();
                        ds.setDriverClassName("org.postgresql.Driver");
                        ds.setJdbcUrl(ModelUtil.getConfigValue("gaussdb.realtime.dw.url"));
                        ds.setUsername(ModelUtil.getConfigValue("gaussdb.realtime.dw.username"));
                        ds.setPassword(ModelUtil.getConfigValue("gaussdb.realtime.dw.password"));
                        ds.setMaximumPoolSize(10);
                        ds.setMinimumIdle(2);
                        ds.setDataSourceProperties(props);
                        jdbcTemplate = new JdbcTemplate(ds);

                        logger.info("使用HikariPool连接池初始化JdbcTemplate成功，其中最大连接大小为：{} , 最小连接大小为：{} ;", ds.getMaximumPoolSize(), ds.getMinimumIdle());

                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException("创建GaussDB数据库的jdbcTemplate失败，抛出的异常信息为：" + e.getMessage());
                    }
                }
            }
        }
        return jdbcTemplate;
    }

    /**
     * 处理传入数据中的特殊字符（例如： 单引号）
     *
     * @param object 传入的数据对象
     * @return 返回的结果
     */
    public static String disposeSpecialCharacter(Object object) {

        String result = null;

        if (object instanceof String) {
            result = object.toString();
        } else {
            result = JSON.parseObject(JSON.toJSONString(object)).toString();
        }

        return result.replace("'", "''");

    }


    /**
     * 如果传入的clz中的属性又包含对象，会报错，此时传入JSONObject对象即可
     *
     * @param sql               执行的查询语句
     * @param clz               返回的数据类型
     * @param underScoreToCamel 是否将下划线转换为驼峰命名法
     * @param <T>               样例类
     * @return 样例类集合
     */
    public static <T> List<T> queryList(String sql, Class<T> clz, boolean underScoreToCamel) {
        try {
            List<Map<String, Object>> mapList = GaussDBUtil.getJdbcTemplate().queryForList(sql);
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
                    "\r\n从GaussDB数据库中 查询 数据失败，" +
                            "\r\n抛出的异常信息为：" + exception.getMessage() +
                            "\r\n查询的SQL为：" + sql
            );
        }
    }

    /**
     * 将传入的数据插入到对应的GaussDB的表中
     *
     * @param tableName         表名
     * @param underScoreToCamel 是否将驼峰转换为下划线
     * @param object            数据对象
     *                          INSERT INTO customer_t1 (c_customer_sk, c_first_name) VALUES (3769, 'Grace');
     */
    public static void insert(String tableName, boolean underScoreToCamel, Object object) {

        // 将传入的对象转换成JSONObject格式（并将其中的特殊字符进行替换）
        JSONObject data = JSON.parseObject(GaussDBUtil.disposeSpecialCharacter(object));

        // 从传入的数据中获取出对应的key和value，因为要一一对应，所以使用list
        ArrayList<String> fieldList = Lists.newArrayList(data.keySet());
        ArrayList<String> valueList = new ArrayList<>();
        for (String field : fieldList) {
            valueList.add(data.getString(field));
        }

        // 拼接SQL
        StringBuilder sql = new StringBuilder();
        sql.append(" INSERT INTO ").append(tableName);
        sql.append(" ( ");
        for (String field : fieldList) {
            if (underScoreToCamel) {
                sql.append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field)).append(",");
            } else {
                sql.append(field).append(",");
            }
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(" ) ");
        sql.append(" values ('").append(StringUtils.join(valueList, "','")).append("')");

        // 执行插入操作
        try {
            GaussDBUtil.getJdbcTemplate().execute(sql.toString());
        } catch (Exception exception) {
            exception.printStackTrace();
            throw new RuntimeException(
                    "\r\n向GaussDB数据库中 插入 数据失败，" +
                            "\r\n抛出的异常信息为：" + exception.getMessage() +
                            "\r\n执行的SQL为：" + sql
            );
        }
    }

    /**
     * 根据主键删除对应数据
     * 注意：传入的字段名要和数据库中一一匹配，即数据库中有下划线，那传入的字段名也要有下划线
     *
     * @param tableName         表名
     * @param fieldNameAndValue 更新时匹配的字段（key）和值（value）（注意：传入的字段名要和数据库中一一匹配，即数据库中有下划线，那传入的字段名也要有下划线）
     * @return 删除时影响的条数
     */
    public static int delete(String tableName, Map<String, Object> fieldNameAndValue) {

        // 拼接SQL
        StringBuilder sql = new StringBuilder();
        sql.append(" delete from ").append(tableName);
        if (fieldNameAndValue.size() > 0) {
            sql.append(" WHERE ");
            for (Map.Entry<String, Object> fieldNameAndValueEntry : fieldNameAndValue.entrySet()) {
                sql
                        .append(fieldNameAndValueEntry.getKey())
                        .append(" = ")
                        .append("'")
                        .append(GaussDBUtil.disposeSpecialCharacter(fieldNameAndValueEntry.getValue()))
                        .append("'")
                        .append(" AND ");

            }
            sql.delete(sql.length() - 4, sql.length() - 1);
        } else {
            throw new RuntimeException("从GaussDB中删除数据异常，输入的删除条件没有指定字段名和对应的值，会进行全表删除， 拼接的SQL为：" + sql);
        }

        // 执行删除操作
        try {
            return GaussDBUtil.getJdbcTemplate().update(sql.toString());
        } catch (Exception exception) {
            exception.printStackTrace();
            throw new RuntimeException(
                    "\r\n向GaussDB数据库中 删除 数据失败，" +
                            "\r\n抛出的异常信息为：" + exception.getMessage() +
                            "\r\n执行的SQL为：" + sql
            );
        }
    }

    /**
     * 根据传入的表名、数据、字段名，删除表中对应的数据
     *
     * @param tableName         表名
     * @param underScoreToCamel 是否将驼峰转换为下划线
     * @param object            数据对象
     * @param fields            更新时匹配的字段名
     */
    public static int delete(String tableName, boolean underScoreToCamel, Object object, String... fields) {

        // 将传入的对象转换成JSONObject格式
        JSONObject data = JSON.parseObject(GaussDBUtil.disposeSpecialCharacter(object));

        // 根据传入的字段，获取要更新的主键值
        HashMap<String, Object> fieldNameAndValue = new HashMap<>();
        for (String field : fields) {
            if (underScoreToCamel) {
                // data中的均为驼峰，获取数据时需要使用驼峰；但是将数据写入到fieldNameAndValue中时，需要全部转换成下划线
                fieldNameAndValue.put(
                        field.contains("_") ? field : CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field),
                        data.getString(field.contains("_") ? CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, field) : field)
                );
            } else {
                // data中均为下划线，field中也是下划线
                fieldNameAndValue.put(field, data.getString(field));
            }
        }

        // 调用重载函数，删除数据
        return GaussDBUtil.delete(tableName, fieldNameAndValue);

    }

    /**
     * 将传入的数据 更新 到对应的GaussDB的表中
     *
     * @param tableName         表名
     * @param underScoreToCamel 是否将驼峰转换为下划线
     * @param object            数据对象（既可以包含更新的主键，也可以不包含）
     * @param fieldNameAndValue 更新时匹配的字段和对应的值
     * @return 返回更新的条数
     */
    public static int update(String tableName, boolean underScoreToCamel, Object object, Map<String, Object> fieldNameAndValue) {

        // 将传入的对象转换成JSONObject格式，并判断输入的数据是否符合更新条件
        JSONObject data = JSON.parseObject(GaussDBUtil.disposeSpecialCharacter(object));
        if (fieldNameAndValue == null || fieldNameAndValue.size() == 0) {
            throw new RuntimeException("向GaussDB中更新数据异常，输入的更新条件没有指定数据，不能更新（这样更新会全表更新），传入的数据为：" + data);
        }

        // 拼接SQL
        StringBuilder sql = new StringBuilder();
        sql.append(" UPDATE ").append(tableName);
        sql.append(" SET ");

        if (underScoreToCamel) {

            // 删除传入对象中要更新的数据
            for (String key : fieldNameAndValue.keySet()) {
                data.remove(key.contains("_") ? CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, key) : key);
            }

            // 拼接要更新的结果值
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                sql
                        .append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, entry.getKey()))
                        .append(" = ")
                        .append("'")
                        .append(entry.getValue())
                        .append("'")
                        .append(",");
            }
            sql.deleteCharAt(sql.length() - 1);

            // 拼接判断条件
            sql.append(" WHERE ");
            for (Map.Entry<String, Object> fieldNameAndValueEntry : fieldNameAndValue.entrySet()) {
                String key = fieldNameAndValueEntry.getKey();
                Object value = fieldNameAndValueEntry.getValue();
                sql
                        .append(key.contains("_") ? key : CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, key))
                        .append(" = ")
                        .append("'")
                        .append(value)
                        .append("'")
                        .append(" AND ");
            }

        } else {

            // 删除传入对象中要更新的数据
            for (String key : fieldNameAndValue.keySet()) {
                data.remove(key);
            }

            // 拼接要更新的结果值
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                sql
                        .append(entry.getKey())
                        .append(" = ")
                        .append("'")
                        .append(entry.getValue())
                        .append("'")
                        .append(",");
            }
            sql.deleteCharAt(sql.length() - 1);

            // 拼接判断条件
            sql.append(" WHERE ");
            for (Map.Entry<String, Object> fieldNameAndValueEntry : fieldNameAndValue.entrySet()) {
                String key = fieldNameAndValueEntry.getKey();
                Object value = fieldNameAndValueEntry.getValue();
                sql
                        .append(key)
                        .append(" = ")
                        .append("'")
                        .append(value)
                        .append("'")
                        .append(" AND ");
            }
        }
        sql.delete(sql.length() - 4, sql.length() - 1);

        // 执行更新操作
        try {
            return GaussDBUtil.getJdbcTemplate().update(sql.toString());
        } catch (Exception exception) {
            exception.printStackTrace();
            throw new RuntimeException(
                    "\r\n向GaussDB数据库中 更新 数据失败，" +
                            "\r\n抛出的异常信息为：" + exception.getMessage() +
                            "\r\n执行的SQL为：" + sql
            );
        }
    }

    /**
     * 将传入的数据 更新 到对应的GaussDB的表中
     *
     * @param tableName         表名
     * @param underScoreToCamel 是否将驼峰转换为下划线
     * @param object            数据对象
     * @param fields            更新时匹配的字段名（如果underScoreToCamel为true，传入的字段为驼峰，如果underScoreToCamel为false，传入的字段为下划线）
     * @return 返回更新的条数
     */
    public static int update(String tableName, boolean underScoreToCamel, Object object, String... fields) {

        // 将传入的对象转换成JSONObject格式
        JSONObject data = JSON.parseObject(GaussDBUtil.disposeSpecialCharacter(object));

        // 根据传入的字段，获取要更新的主键值
        HashMap<String, Object> fieldNameAndValue = new HashMap<>();
        for (String field : fields) {
            if (underScoreToCamel) {
                field = field.contains("_") ? CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, field) : field;
            }
            fieldNameAndValue.put(field, data.getString(field));
        }

        // 调用重载函数，更新数据
        return GaussDBUtil.update(tableName, underScoreToCamel, object, fieldNameAndValue);

    }

    /**
     * 将传入的数据 upsert 到对应的GaussDB的表中（注意：需要表中有唯一约束，并且传入的字段必须是唯一约束）
     *
     * @param tableName         表名
     * @param underScoreToCamel 是否将驼峰转换为下划线
     * @param object            数据对象
     * @param fields            更新时匹配的字段名（如果underScoreToCamel为true，传入的字段为驼峰，如果underScoreToCamel为false，传入的字段为下划线）
     * @return 返回更改的条数
     */
    public static int upsertByPrimaryKey(String tableName, boolean underScoreToCamel, Object object, String... fields) {

        // 将传入的对象转换成JSONObject格式
        JSONObject data = JSON.parseObject(GaussDBUtil.disposeSpecialCharacter(object));

        // 根据传入的字段，获取要更新的主键值
        HashMap<String, Object> fieldNameAndValue = new HashMap<>();
        for (String field : fields) {
            if (underScoreToCamel) {
                field = field.contains("_") ? CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, field) : field;
            }
            fieldNameAndValue.put(field, data.getString(field));
        }

        // 调用重载函数，更新数据
        return GaussDBUtil.upsertByPrimaryKey(tableName, underScoreToCamel, object, fieldNameAndValue);

    }

    /**
     * 将传入的数据 upsert 到对应的GaussDB的表中（注意：需要表中有唯一约束，并且传入的字段必须是唯一约束）
     * <p>
     * dws中的upsert语法：
     * INSERT INTO test.reason_t1 (r_reason_desc, r_reason_sk, r_reason_id) values ('$$$$$$$$$$$$', '4', '444444444') on conflict ( r_reason_sk ) do update set r_reason_desc = EXCLUDED.r_reason_desc, r_reason_id   = EXCLUDED.r_reason_id
     *
     * @param tableName         表名
     * @param underScoreToCamel 是否将驼峰转换为下划线
     * @param object            数据对象
     * @param fieldNameAndValue 更新时匹配的字段名（如果underScoreToCamel为true，传入的字段为驼峰，如果underScoreToCamel为false，传入的字段为下划线）
     * @return 返回更改的条数
     */
    public static int upsertByPrimaryKey(String tableName, boolean underScoreToCamel, Object object, Map<String, Object> fieldNameAndValue) {

        // 将传入的对象转换成JSONObject格式，并判断输入的数据是否符合更新条件
        JSONObject data = JSON.parseObject(GaussDBUtil.disposeSpecialCharacter(object));
        if (fieldNameAndValue == null || fieldNameAndValue.size() == 0) {
            throw new RuntimeException("向GaussDB中更新数据异常，输入的更新条件没有指定数据，不能更新（这样更新会全表更新），传入的数据为：" + data);
        }

        // 将传入的更新匹配字段和值（即fieldNameAndValue），添加到数据对象中（即data）
        for (Map.Entry<String, Object> entry : fieldNameAndValue.entrySet()) {
            data.put(entry.getKey(), entry.getValue());
        }
        data = JSON.parseObject(GaussDBUtil.disposeSpecialCharacter(data));

        // 根据所有数据（data）和需要更新的数据（fieldNameAndValue），求出更新的字段名和被更新的字段名
        Set<String> updateKey = fieldNameAndValue.keySet();
        Set<String> beUpdateKey = new TreeSet<>();
        for (String key : data.keySet()) {
            if (!updateKey.contains(key)) {
                beUpdateKey.add(key);
            }
        }

        // 拼接SQL
        StringBuilder sql = new StringBuilder();
        sql.append(" INSERT INTO ").append(tableName);

        if (underScoreToCamel) {

            sql.append(" ( ");
            for (String key : data.keySet()) {
                sql.append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, key)).append(",");
            }
            sql.deleteCharAt(sql.length() - 1);
            sql.append(" ) ");

            sql.append(" values ");

            sql.append(" ( ");
            for (Object value : data.values()) {
                sql
                        .append("'")
                        .append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, value.toString()))
                        .append("'")
                        .append(",");
            }
            sql.deleteCharAt(sql.length() - 1);
            sql.append(" ) ");

            sql.append(" on conflict ");

            sql.append(" ( ");
            for (String key : updateKey) {
                sql.append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, key)).append(",");
            }
            sql.deleteCharAt(sql.length() - 1);
            sql.append(" ) ");

            sql.append(" do update set ");
            for (String key : beUpdateKey) {
                sql
                        .append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, key))
                        .append(" = EXCLUDED.")
                        .append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, key))
                        .append(",");
            }
            sql.deleteCharAt(sql.length() - 1);

        } else {

            sql.append(" ( ");
            for (String key : data.keySet()) {
                sql.append(key).append(",");
            }
            sql.deleteCharAt(sql.length() - 1);
            sql.append(" ) ");

            sql.append(" values ");

            sql.append(" ( ");
            for (Object value : data.values()) {
                sql
                        .append("'")
                        .append(value.toString())
                        .append("'")
                        .append(",");
            }
            sql.deleteCharAt(sql.length() - 1);
            sql.append(" ) ");

            sql.append(" on conflict ");

            sql.append(" ( ");
            for (String key : updateKey) {
                sql.append(key).append(",");
            }
            sql.deleteCharAt(sql.length() - 1);
            sql.append(" ) ");

            sql.append(" do update set ");
            for (String key : beUpdateKey) {
                sql
                        .append(key)
                        .append(" = EXCLUDED.")
                        .append(key)
                        .append(",");
            }
            sql.deleteCharAt(sql.length() - 1);
        }

        // 执行upsert操作
        try {
            return GaussDBUtil.getJdbcTemplate().update(sql.toString());
        } catch (Exception exception) {
            exception.printStackTrace();
            throw new RuntimeException(
                    "\r\n向GaussDB数据库中 upsert 数据失败，" +
                            "\r\n抛出的异常信息为：" + exception.getMessage() +
                            "\r\n执行的SQL为：" + sql
            );
        }

    }

    /**
     * 将传入的数据 upsert 到对应的GaussDB的表中
     *
     * @param tableName         表名
     * @param underScoreToCamel 是否将驼峰转换为下划线
     * @param object            数据对象
     * @param fields            更新时匹配的字段名（如果underScoreToCamel为true，传入的字段为驼峰，如果underScoreToCamel为false，传入的字段为下划线）
     * @return 返回更改的条数
     */
    public static int upsert(String tableName, boolean underScoreToCamel, Object object, String... fields) {

        int updateNum = GaussDBUtil.update(tableName, underScoreToCamel, object, fields);

        if (updateNum == 0) {
            GaussDBUtil.insert(tableName, underScoreToCamel, object);
        }

        return updateNum;
    }

    /**
     * 将传入的数据 upsert 到对应的GaussDB的表中
     *
     * @param tableName         表名
     * @param underScoreToCamel 是否将驼峰转换为下划线
     * @param object            数据对象
     * @param fieldNameAndValue 更新时匹配的字段名（如果underScoreToCamel为true，传入的字段为驼峰，如果underScoreToCamel为false，传入的字段为下划线）
     * @return 返回更改的条数
     */
    public static int upsert(String tableName, boolean underScoreToCamel, Object object, Map<String, Object> fieldNameAndValue) {

        int updateNum = GaussDBUtil.update(tableName, underScoreToCamel, object, fieldNameAndValue);

        if (updateNum == 0) {
            GaussDBUtil.insert(tableName, underScoreToCamel, object);
        }

        return updateNum;
    }

}
