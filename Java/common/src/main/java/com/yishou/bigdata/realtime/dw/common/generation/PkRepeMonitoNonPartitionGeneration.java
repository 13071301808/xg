package com.yishou.bigdata.realtime.dw.common.generation;

/**
 * @date: 2023/6/16
 * @author: yangshibiao
 * @desc: 主键重复（非分区表）监控代码生成
 */
public class PkRepeMonitoNonPartitionGeneration {

    public static void main(String[] args) {

        // 表名 ☆☆☆☆☆
        String tableName = "yishou_data.dim_goods_id_info_hour";

        // 主键字段名  ☆☆☆☆☆
        String primaryKey = "goods_id";

        // 运行的环境（dli、dws）
        String dataSource = "dli";

        // 报错的作业名即可   ☆☆☆☆☆
        String jobName = "dim_goods_id_info_hour";

        // 运行环境（dev：告警二群，prod：大数据告警群， analyst：分析师作业监控群）
        String env = "prod";

        // 是否阻断（Y|N, 默认为Y，阻断）
        String intercept = "Y";

        StringBuilder sql = new StringBuilder();

        sql.append("-q \"");

        sql.append("select ");
        sql.append(" concat('主键字段为" + primaryKey + "，其中主键重复数为', count(*)) as 主键存在重复 ");
        sql.append(" from ( ");
        sql.append(" select ");
        sql.append(" " + primaryKey + ", count(*) as cnt ");
        sql.append(" from " + tableName + " ");
        sql.append(" group by " + primaryKey + " ");
        sql.append(" having cnt > 1 ");
        sql.append(" ) ");
        sql.append(" having count(*) > 0 ");

        sql.append("\"");

        sql.append(" -t " + dataSource);
        sql.append(" -n " + jobName);
        sql.append(" -d 数据异常");
        sql.append(" -g " + env);
        sql.append(" -b " + intercept);

        System.out.println(sql);

    }

}
