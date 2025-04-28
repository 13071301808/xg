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
import java.util.concurrent.TimeUnit;

/**
 * @date: 2022/5/17
 * @author: yangshibiao
 * @desc: MySQLUtil
 */
public class MySQLUtil {

    static Logger logger = LoggerFactory.getLogger(MySQLUtil.class);

    /**
     * jdbcTemplate
     */
    private JdbcTemplate jdbcTemplate;

    /**
     * 通过传入的参数创建MySQLUtil对象
     *
     * @param url        mysql的url
     * @param username   mysql的username
     * @param password   mysql的password
     * @param maxConnect 连接池中最大连接数
     * @param minConnect 连接池中最小连接数
     */
    public MySQLUtil(String url, String username, String password, int maxConnect, int minConnect) {
        initJdbcTemplate(url, username, password, maxConnect, minConnect);
    }

    /**
     * 通过传入的参数创建MySQLUtil对象
     *
     * @param url      mysql的url
     * @param username mysql的username
     * @param password mysql的password
     */
    public MySQLUtil(String url, String username, String password) {
        initJdbcTemplate(url, username, password, 2, 1);
    }

    /**
     * 初始化MySQL的jdbcTemplate
     *
     * @param url        mysql的url
     * @param username   mysql的username
     * @param password   mysql的password
     * @param maxConnect 连接池中最大连接数
     * @param minConnect 连接池中最小连接数
     */
    public void initJdbcTemplate(String url, String username, String password, int maxConnect, int minConnect) {
        try {
            HikariDataSource ds = new HikariDataSource();
            Thread.sleep(1000);
            ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
            ds.setJdbcUrl(url);
            ds.setUsername(username);
            ds.setPassword(password);
            ds.setMaximumPoolSize(maxConnect);
            ds.setMinimumIdle(minConnect);
            ds.setConnectionTimeout(TimeUnit.MINUTES.toMillis(2L));
            jdbcTemplate = new JdbcTemplate(ds);

            logger.info(
                    "使用HikariPool连接池初始化JdbcTemplate成功，使用的URL为：{} , 其中最大连接大小为：{} , 最小连接大小为：{} ;",
                    ds.getJdbcUrl(),
                    ds.getMaximumPoolSize(),
                    ds.getMinimumIdle()
            );

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("创建MySQL数据库的jdbcTemplate失败，抛出的异常信息为：" + e.getMessage());
        }
    }

    /**
     * 获取对应的 JdbcTemplate
     *
     * @return JdbcTemplate
     */
    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    /**
     * 处理传入数据中的特殊字符（例如： 单引号），并将其中数据为空的过滤
     *
     * @param object 传入的数据对象
     * @return 返回的结果
     */
    public String disposeSpecialCharacter(Object object) {

        // 根据传入的情况，将数据转换成json格式（如果传入为string，那就本来是json格式，不需要转）
        String data;
        if (object instanceof String) {
            data = object.toString();
        } else {
            data = JSON.parseObject(JSON.toJSONString(object)).toString();
        }

        // 处理传入数据中的特殊字符（例如： 单引号）
        data = data.replace("'", "''");


        // 将其中为空值的去掉（注意：如果不能转换成json并从中获取数据的，就是从delete中传过来的，只有单纯的value值）
        try {
            JSONObject result = new JSONObject();
            for (Map.Entry<String, Object> entry : JSON.parseObject(data).entrySet()) {
                if (StringUtils.isNotEmpty(entry.getValue().toString())) {
                    result.put(entry.getKey(), entry.getValue());
                }
            }
            data = result.toJSONString();
        } catch (Exception exception) {
            exception.printStackTrace();
            logger.warn("传入的数据为：{}；该数据是从delete中传入的，不能转换成json值", object);
        }

        // 返回数据
        return data;

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
    public <T> List<T> queryList(String sql, Class<T> clz, boolean underScoreToCamel) {
        try {
            List<Map<String, Object>> mapList = jdbcTemplate.queryForList(sql);
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
                    "\r\n从MySQL数据库中 查询 数据失败，" +
                            "\r\n抛出的异常信息为：" + exception.getMessage() +
                            "\r\n查询的SQL为：" + sql
            );
        }
    }

    /**
     * 将传入的数据插入到对应的MySQL的表中
     *
     * @param tableName         表名
     * @param underScoreToCamel 是否将驼峰转换为下划线
     * @param object            数据对象
     *                          INSERT INTO customer_t1 (c_customer_sk, c_first_name) VALUES (3769, 'Grace');
     */
    public void insert(String tableName, boolean underScoreToCamel, Object object) {

        // 将传入的对象转换成JSONObject格式（并将其中的特殊字符进行替换）
        JSONObject data = JSON.parseObject(disposeSpecialCharacter(object));

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
            jdbcTemplate.execute(sql.toString());
        } catch (Exception exception) {
            exception.printStackTrace();
            throw new RuntimeException(
                    "\r\n向MySQL数据库中 插入 数据失败，" +
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
    public int delete(String tableName, Map<String, Object> fieldNameAndValue) {

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
                        .append(disposeSpecialCharacter(fieldNameAndValueEntry.getValue()))
                        .append("'")
                        .append(" AND ");
            }
            sql.delete(sql.length() - 4, sql.length() - 1);
        } else {
            throw new RuntimeException("从MySQL中删除数据异常，输入的删除条件没有指定字段名和对应的值，会进行全表删除， 拼接的SQL为：" + sql);
        }

        // 执行删除操作
        try {
            return jdbcTemplate.update(sql.toString());
        } catch (Exception exception) {
            exception.printStackTrace();
            throw new RuntimeException(
                    "\r\n向MySQL数据库中 删除 数据失败，" +
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
    public int delete(String tableName, boolean underScoreToCamel, Object object, String... fields) {

        // 将传入的对象转换成JSONObject格式
        JSONObject data = JSON.parseObject(disposeSpecialCharacter(object));

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
        return delete(tableName, fieldNameAndValue);

    }
    /**
     * 将传入的sql 更新 对应的MySQL的表的数据
     *
     * @param sql         查询的sql
     * @return 返回更新的条数
     */
    public int updateForSQL(String sql) {
        // 执行upsert操作
        try {
            return jdbcTemplate.update(sql);
        } catch (Exception exception) {
            exception.printStackTrace();
            throw new RuntimeException(
                    "\r\n向MySQL数据库中 upsert 数据失败，" +
                            "\r\n抛出的异常信息为：" + exception.getMessage() +
                            "\r\n执行的SQL为：" + sql
            );
        }
    }
    /**
     * 将传入的数据 更新 到对应的MySQL的表中
     *
     * @param tableName         表名
     * @param underScoreToCamel 是否将驼峰转换为下划线
     * @param object            数据对象（既可以包含更新的主键，也可以不包含）
     * @param fieldNameAndValue 更新时匹配的字段和对应的值
     * @return 返回更新的条数
     */
    public int update(String tableName, boolean underScoreToCamel, Object object, Map<String, Object> fieldNameAndValue) {

        // 将传入的对象转换成JSONObject格式，并判断输入的数据是否符合更新条件
        JSONObject data = JSON.parseObject(disposeSpecialCharacter(object));
        if (fieldNameAndValue == null || fieldNameAndValue.size() == 0) {
            throw new RuntimeException("向MySQL中更新数据异常，输入的更新条件没有指定数据，不能更新（这样更新会全表更新），传入的数据为：" + data);
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
            return jdbcTemplate.update(sql.toString());
        } catch (Exception exception) {
            exception.printStackTrace();
            throw new RuntimeException(
                    "\r\n向MySQL数据库中 更新 数据失败，" +
                            "\r\n抛出的异常信息为：" + exception.getMessage() +
                            "\r\n执行的SQL为：" + sql
            );
        }
    }

    /**
     * 将传入的数据 更新 到对应的MySQL的表中
     *
     * @param tableName         表名
     * @param underScoreToCamel 是否将驼峰转换为下划线
     * @param object            数据对象
     * @param fields            更新时匹配的字段名（如果underScoreToCamel为true，传入的字段为驼峰，如果underScoreToCamel为false，传入的字段为下划线）
     * @return 返回更新的条数
     */
    public int update(String tableName, boolean underScoreToCamel, Object object, String... fields) {

        // 将传入的对象转换成JSONObject格式
        JSONObject data = JSON.parseObject(disposeSpecialCharacter(object));

        // 根据传入的字段，获取要更新的主键值
        HashMap<String, Object> fieldNameAndValue = new HashMap<>();
        for (String field : fields) {
            if (underScoreToCamel) {
                field = field.contains("_") ? CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, field) : field;
            }
            fieldNameAndValue.put(field, data.getString(field));
        }

        // 调用重载函数，更新数据
        return update(tableName, underScoreToCamel, object, fieldNameAndValue);

    }

    /**
     * 将传入的数据 upsert 到对应的MySQL的表中
     * 会根据MySQL表中的主键进行更新，如果该主键在表中有对应数据，就更新，没有就插入
     * 传入的数据中必须有主键
     * <p>
     * mysql中的upsert语法：
     * INSERT INTO Student(Stud_ID, Name, Email, City) VALUES (4, 'John', 'john@lidihuo.com', 'New York') ON DUPLICATE KEY UPDATE City = 'California';
     *
     * @param tableName         表名
     * @param underScoreToCamel 是否将驼峰转换为下划线
     * @param object            数据对象
     * @param fieldNameAndValue 更新时匹配的字段名（如果underScoreToCamel为true，传入的字段为驼峰，如果underScoreToCamel为false，传入的字段为下划线）
     * @return 返回更改的条数
     */
    public int upsertByPrimaryKey(String tableName, boolean underScoreToCamel, Object object, Map<String, Object> fieldNameAndValue) {
        // 判断输入的数据是否符合更新条件
        if (fieldNameAndValue == null || fieldNameAndValue.size() == 0) {
            throw new RuntimeException("向MySQL中更新数据异常，输入的更新条件没有指定数据，不能更新（这样更新会全表更新），传入的数据为：" + object);
        }

        // 将传入的object转换成json类型，并将传入的更新匹配字段和值（即fieldNameAndValue），添加到数据对象中（即data）
        JSONObject data = JSON.parseObject(JSON.toJSONString(object));
        for (Map.Entry<String, Object> entry : fieldNameAndValue.entrySet()) {
            data.put(entry.getKey(), entry.getValue());
        }
        data = JSON.parseObject(disposeSpecialCharacter(data));
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

            sql.append(" ON DUPLICATE KEY UPDATE ");

            for (String key : data.keySet()) {
                sql
                        .append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, key))
                        .append(" = ")
                        .append("'")
                        .append(data.getString(key))
                        .append("'")
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

            sql.append(" ON DUPLICATE KEY UPDATE ");

            for (String key : data.keySet()) {
                sql
                        .append(key)
                        .append(" = ")
                        .append("'")
                        .append(data.getString(key))
                        .append("'")
                        .append(",");
            }
            sql.deleteCharAt(sql.length() - 1);

        }

        // 执行upsert操作
        try {
            return jdbcTemplate.update(sql.toString());
        } catch (Exception exception) {
            exception.printStackTrace();
            throw new RuntimeException(
                    "\r\n向MySQL数据库中 upsert 数据失败，" +
                            "\r\n抛出的异常信息为：" + exception.getMessage() +
                            "\r\n执行的SQL为：" + sql
            );
        }

    }



    /**
     * 将传入的数据 upsert 到对应的MySQL的表中
     * 会根据MySQL表中的主键进行更新，如果该主键在表中有对应数据，就更新，没有就插入
     * 传入的数据中必须有主键
     * <p>
     * mysql中的upsert语法：
     * insert into fmdes.online_report_supply_order_infos (buy_num,pay_status,rec_id) values ('7','3','260876861') on DUPLICATE key update buy_num = '7', pay_status = '3',rec_id = '260876861'
     *
     * @param tableName         表名
     * @param underScoreToCamel 是否将驼峰转换为下划线
     * @param object            数据对象
     * @return 返回更改的条数
     */
    public int onDuplicateKeyUpdate(String tableName, boolean underScoreToCamel, Object object) {

        // 将传入的object转换成json类型，并将传入的更新匹配字段和值（即fieldNameAndValue），添加到数据对象中（即data）
        JSONObject data = JSON.parseObject(JSON.toJSONString(object));
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

            sql.append(" ON DUPLICATE KEY UPDATE ");

            for (String key : data.keySet()) {
                sql
                        .append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, key))
                        .append(" = ")
                        .append("'")
                        .append(data.getString(key))
                        .append("'")
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

            sql.append(" ON DUPLICATE KEY UPDATE ");

            for (String key : data.keySet()) {
                sql
                        .append(key)
                        .append(" = ")
                        .append("'")
                        .append(data.getString(key))
                        .append("'")
                        .append(",");
            }
            sql.deleteCharAt(sql.length() - 1);

        }

        // 执行upsert操作
        try {
            return jdbcTemplate.update(sql.toString());
        } catch (Exception exception) {
            exception.printStackTrace();
            throw new RuntimeException(
                    "\r\n向MySQL数据库中 upsert 数据失败，" +
                            "\r\n抛出的异常信息为：" + exception.getMessage() +
                            "\r\n执行的SQL为：" + sql
            );
        }

    }
    /**
     * 将传入的数据 upsert 到对应的MySQL的表中
     * 会根据MySQL表中的主键进行更新，如果该主键在表中有对应数据，就更新，没有就插入
     * 传入的数据中必须有主键
     *
     * @param tableName         表名
     * @param underScoreToCamel 是否将驼峰转换为下划线
     * @param object            数据对象
     * @param fields            更新时匹配的字段名（如果underScoreToCamel为true，传入的字段为驼峰，如果underScoreToCamel为false，传入的字段为下划线）
     * @return 返回更改的条数
     */
    public int upsertByPrimaryKey(String tableName, boolean underScoreToCamel, Object object, String... fields) {

        // 将传入的对象转换成JSONObject格式
        JSONObject data = JSON.parseObject(disposeSpecialCharacter(object));

        // 根据传入的字段，获取要更新的主键值
        HashMap<String, Object> fieldNameAndValue = new HashMap<>();
        for (String field : fields) {
            if (underScoreToCamel) {
                field = field.contains("_") ? CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, field) : field;
            }
            fieldNameAndValue.put(field, data.getString(field));
        }

        // 调用重载函数，更新数据
        return upsertByPrimaryKey(tableName, underScoreToCamel, object, fieldNameAndValue);

    }

    /**
     * 将传入的数据 upsert 到对应的MySQL数据库的表中
     * 使用的是先用update进行数据更新，如果更新的条数为0，就进行插入
     * 如果表中包含主键，那在传入的数据中也必须要有主键
     *
     * @param tableName         表名
     * @param underScoreToCamel 是否将驼峰转换为下划线
     * @param object            数据对象
     * @param fields            更新时匹配的字段名（如果underScoreToCamel为true，传入的字段为驼峰，如果underScoreToCamel为false，传入的字段为下划线）
     * @return 返回更改的条数
     */
    public int upsert(String tableName, boolean underScoreToCamel, Object object, String... fields) {

        int updateNum = update(tableName, underScoreToCamel, object, fields);

        if (updateNum == 0) {
            insert(tableName, underScoreToCamel, object);
            updateNum = 1;
        }

        return updateNum;
    }

    /**
     * 将传入的表格清空
     *
     * @param tableName 表名
     */
    public void truncate(String tableName) {

        try {
            jdbcTemplate.execute("truncate table " + tableName);
        } catch (Exception ignored) {

        }
    }

    /**
     * 将传入的数据 upsert 到对应的MySQL数据库的表中
     * 使用的是先用update进行数据更新，如果更新的条数为0，就进行插入
     * 如果表中包含主键，那在传入的数据中也必须要有主键
     *
     * @param tableName         表名
     * @param underScoreToCamel 是否将驼峰转换为下划线
     * @param object            数据对象
     * @param fieldNameAndValue 更新时匹配的字段名（如果underScoreToCamel为true，传入的字段为驼峰，如果underScoreToCamel为false，传入的字段为下划线）
     * @return 返回更改的条数
     */
    public int upsert(String tableName, boolean underScoreToCamel, Object object, Map<String, Object> fieldNameAndValue) {

        int updateNum = update(tableName, underScoreToCamel, object, fieldNameAndValue);

        if (updateNum == 0) {
            insert(tableName, underScoreToCamel, object);
            updateNum = 1;
        }

        return updateNum;
    }

}
