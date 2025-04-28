package com.yishou.bigdata.realtime.dw.common.generation;

/**
 * @date: 2023/6/16
 * @author: yangshibiao
 * @desc: 主键重复（分区表，使用的是昨天的分区）监控代码生成
 */
public class PkRepeMonitoPartitionGeneration {

    public static void main(String[] args) {

        // 表名 ☆☆☆☆☆
        String tableName = "yishou_daily.ads_classify_recall_for_buy_cf";

        // 主键字段名  ☆☆☆☆☆
        String primaryKey = "key";

        // 运行的环境（dli、dws）
        String dataSource = "dli";

        // 报错的作业名即可   ☆☆☆☆☆
        String jobName = "ads_classify_recall_for_buy_cf_协同过滤购买推荐商品召回";

        // 运行环境（dev：告警二群，prod：大数据告警群， analyst：分析师作业监控群）
        String env = "prod";

        // 是否阻断（Y|N, 默认为Y，阻断）
        String intercept = "Y";

        StringBuilder sql = new StringBuilder();

        sql.append("-q \"");

        sql.append(" select * ");
        sql.append(" from ( ");
        sql.append(" select ");
        sql.append(" concat('主键字段为" + primaryKey + "，其中主键重复数为', count(*)) as 主键存在重复 ");
        sql.append(" from ( ");
        sql.append(" select ");
        sql.append(" " + primaryKey + ", count(*) as cnt ");
        sql.append(" from " + tableName + " ");
        sql.append(" where dt = ${one_day_ago} ");
        sql.append(" group by " + primaryKey + " ");
        sql.append(" having cnt > 1 ");
        sql.append(" ) ");
        sql.append(" having count(*) > 0 ");
        sql.append(" ) as alarm_1 ");
        sql.append(" full join ( ");
        sql.append(" select ");
        sql.append(" '" + tableName + "表数据条数为0' as 数据条数异常 ");
        sql.append(" from ( ");
        sql.append(" SELECT ");
        sql.append(" count(*) as cnt ");
        sql.append(" from " + tableName + " ");
        sql.append(" where dt = ${one_day_ago} ");
        sql.append(" ) ");
        sql.append(" where cnt = 0 ");
        sql.append(" ) as alarm_2 ");
        sql.append(" on 1 = 1 ");
        sql.append(" full join ( ");
        sql.append(" select ");
        sql.append(" concat(${one_day_ago}, '数据条数为', one_day_ago, '条；', ${two_day_ago}, '数据条数为', two_day_ago, '条；数据条数差异比为', round(abs(two_day_ago - one_day_ago) / two_day_ago * 100, 2), '%') as 分区差异过大 ");
        sql.append(" from ( ");
        sql.append(" SELECT ");
        sql.append(" count(*) as two_day_ago ");
        sql.append(" from " + tableName + " ");
        sql.append(" where dt = ${two_day_ago} ");
        sql.append(" ) as two_day_ago ");
        sql.append(" full join ( ");
        sql.append(" SELECT ");
        sql.append(" count(*) as one_day_ago ");
        sql.append(" from " + tableName + " ");
        sql.append(" where dt = ${one_day_ago} ");
        sql.append(" ) as one_day_ago ");
        sql.append(" on 1 = 1 ");
        sql.append(" having abs(two_day_ago - one_day_ago) / two_day_ago > 0.5 ");
        sql.append(" ) as alarm_3 ");
        sql.append(" on 1 = 1 ");

        sql.append("\"");

        sql.append(" -t " + dataSource);
        sql.append(" -n " + jobName);
        sql.append(" -d 数据异常");
        sql.append(" -g " + env);
        sql.append(" -b " + intercept);

        System.out.println(sql);

    }

}
