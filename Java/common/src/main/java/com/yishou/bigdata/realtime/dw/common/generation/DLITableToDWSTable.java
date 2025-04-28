package com.yishou.bigdata.realtime.dw.common.generation;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @date: 2023/11/30
 * @author: yangshibiao
 * @desc: 项目描述
 */
public class DLITableToDWSTable {

    public static Map<String, String> typeMapping = new HashMap();

    static {
        typeMapping.put("TINYINT", "TINYINT");
        typeMapping.put("SMALLINT", "SMALLINT");
        typeMapping.put("INT", "INTEGER");
        typeMapping.put("BIGINT", "BIGINT");
        typeMapping.put("FLOAT", "REAL");
        typeMapping.put("DOUBLE", "DOUBLE PRECISION");
        typeMapping.put("DATE", "DATE");
        typeMapping.put("TIMESTAMP", "TIMESTAMP");
        typeMapping.put("BOOLEAN", "BOOLEAN");
        typeMapping.put("STRING", "TEXT");
    }

    public static void main(String[] args) {

        String dwsDataBaseName = "yishou_daily";
        String dwsTableName = "ads_supply_avg_limit_day_dt";
        String dliDDL = "CREATE EXTERNAL TABLE `ads_supply_avg_limit_day_dt` (\n" +
                "\t`supply_id` BIGINT COMMENT '供应商id',\n" +
                "\t`limit_day` BIGINT COMMENT '预计排单天数（算法中的pred_limitday字段）'\n" +
                ") COMMENT '供应商货期结果表（历史数据保存）' PARTITIONED BY (`dt` STRING COMMENT '日期分区') ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'\n" +
                "WITH\n" +
                "\tSERDEPROPERTIES ('serialization.format' = '1') STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' LOCATION 'obs://yishou-bigdata/yishou_daily.db/ads_supply_avg_limit_day_dt' TBLPROPERTIES (\n" +
                "\t\t'hive.serialization.extend.nesting.levels' = 'true',\n" +
                "\t\t'transient_lastDdlTime' = '1724404344',\n" +
                "\t\t'ddlUpdateTime' = '1724404343786',\n" +
                "\t\t'partitionsDdlUpdateTime' = '{\"dt\":1724404343786}'\n" +
                "\t)";

        if (dliDDL.contains("PARTITIONED BY")) {
            // 分区表
            System.out.println("-- 这是分区表，进行分区表解析");
            partitionParser(dwsDataBaseName, dwsTableName, dliDDL);
        } else {
            // 非分区表
            System.out.println("-- 这是非分区表，进行非分区解析");
            nonPartitionParser(dwsDataBaseName, dwsTableName, dliDDL);
        }


    }

    /**
     * 非分区表建表语句
     *
     * @param dwsDataBaseName dws对应的库名
     * @param dwsTableName    dws对应的表名
     * @param dliDDL          使用DGC复制出来的DDL
     */
    public static void nonPartitionParser(String dwsDataBaseName, String dwsTableName, String dliDDL) {

        StringBuilder dwsDDL = new StringBuilder();

        dwsDDL.append("\n");
        dwsDDL.append("-- 外部表建表语句\n");
        dwsDDL.append("CREATE FOREIGN TABLE ").append(dwsDataBaseName).append(".").append(dwsTableName).append(" (");
        dwsDDL.append("\n");

        for (String field : dliDDL.split("` \\(")[1].split("\\) ")[0].split(",\n\t")) {
            field = field.replace("\n", "").replace("\t", "");
            String[] fieldArr = field.split(" ");
            dwsDDL.append("\t");
            dwsDDL.append(fieldArr[0].replace("`", ""));
            dwsDDL.append(" ");
            dwsDDL.append(getDWSDataType(fieldArr[1]));
            dwsDDL.append(",");
            dwsDDL.append("\n");
        }

        dwsDDL.deleteCharAt(dwsDDL.length() - 2);
        dwsDDL.append(") SERVER obs_server OPTIONS (");
        dwsDDL.append("\n").append("\t").append("encoding 'utf8',");
        dwsDDL.append("\n").append("\t").append("foldername '").append(dliDDL.split("obs:/")[1].split("'")[0]).append("/',");
        dwsDDL.append("\n").append("\t").append("format 'orc'");
        dwsDDL.append("\n").append(") DISTRIBUTE BY ROUNDROBIN;");

        dwsDDL.append("\n");
        for (String field : dliDDL.split("` \\(")[1].split("\\) ")[0].split(",\n\t")) {
            field = field.replace("\n", "").replace("\t", "");
            String[] fieldArr = field.split(" ");
            if (fieldArr.length >= 3) {
                dwsDDL.append("COMMENT ON COLUMN ");
                dwsDDL.append(dwsDataBaseName).append(".").append(dwsTableName).append(".").append(fieldArr[0].replace("`", ""));
                dwsDDL.append(" IS ");
                dwsDDL.append(field.replace(fieldArr[0], "").replace(fieldArr[1], "").replace(fieldArr[2], "").replace(" ", ""));
                dwsDDL.append(";");
                dwsDDL.append("\n");
            }
        }

        System.out.println(dwsDDL);

    }

    /**
     * 分区表建表语句
     *
     * @param dwsDataBaseName dws对应的库名
     * @param dwsTableName    dws对应的表名
     * @param dliDDL          使用DGC复制出来的DDL
     */
    public static void partitionParser(String dwsDataBaseName, String dwsTableName, String dliDDL) {

        StringBuilder dwsDDL = new StringBuilder();

        dwsDDL.append("\n");
        dwsDDL.append("-- 外部表建表语句\n");
        dwsDDL.append("CREATE FOREIGN TABLE ").append(dwsDataBaseName).append(".").append(dwsTableName).append(" (");
        dwsDDL.append("\n");

        for (String field : dliDDL.split("` \\(")[1].split("\\) ")[0].split(",\n\t")) {
            field = field.replace("\n", "").replace("\t", "");
            String[] fieldArr = field.split(" ");
            dwsDDL.append("\t");
            dwsDDL.append(fieldArr[0].replace("`", ""));
            dwsDDL.append(" ");
            dwsDDL.append(getDWSDataType(fieldArr[1]));
            dwsDDL.append(",");
            dwsDDL.append("\n");
        }
        dwsDDL.append("\t").append(dliDDL.split("PARTITIONED BY \\(`")[1].split("`")[0]).append(" ").append(getDWSDataType(dliDDL.split("PARTITIONED BY \\(`")[1].split(" ")[1]));

        dwsDDL.append("\n").append(") SERVER obs_server OPTIONS (");
        dwsDDL.append("\n").append("\t").append("encoding 'utf8',");
        dwsDDL.append("\n").append("\t").append("foldername '").append(dliDDL.split("obs:/")[1].split("'")[0]).append("/',");
        dwsDDL.append("\n").append("\t").append("format 'orc'");
        dwsDDL.append("\n").append(") DISTRIBUTE BY ROUNDROBIN");
        dwsDDL.append("\n").append("PARTITION BY (").append(dliDDL.split("PARTITIONED BY \\(`")[1].split("`")[0]).append(") AUTOMAPPED;");

        dwsDDL.append("\n");
        for (String field : dliDDL.split("` \\(")[1].split("\\) ")[0].split(",\n\t")) {
            field = field.replace("\n", "").replace("\t", "");
            String[] fieldArr = field.split(" ");
            if (fieldArr.length >= 3) {
                dwsDDL.append("COMMENT ON COLUMN ");
                dwsDDL.append(dwsDataBaseName).append(".").append(dwsTableName).append(".").append(fieldArr[0].replace("`", ""));
                dwsDDL.append(" IS ");
                dwsDDL.append(field.replace(fieldArr[0], "").replace(fieldArr[1], "").replace(fieldArr[2], "").replace(" ", ""));
                dwsDDL.append(";");
                dwsDDL.append("\n");
            }
        }

        String partitionField = dliDDL.split("PARTITIONED BY \\(")[1].split("\\) ROW")[0];
        String[] partitionFieldArr = partitionField.split(" ");
        if (partitionFieldArr.length >= 3) {
            dwsDDL.append("COMMENT ON COLUMN ");
            dwsDDL.append(dwsDataBaseName).append(".").append(dwsTableName).append(".").append(partitionFieldArr[0].replace("`", ""));
            dwsDDL.append(" IS ");
            dwsDDL.append(partitionField.replace(partitionFieldArr[0], "").replace(partitionFieldArr[1], "").replace(partitionFieldArr[2], "").replace(" ", ""));
            dwsDDL.append(";");
            dwsDDL.append("\n");
        }

        System.out.println(dwsDDL);
    }

    public static String getDWSDataType(String dliDataType) {
        String dwsDataType = typeMapping.get(dliDataType.toUpperCase());
        if (StringUtils.isBlank(dwsDataType)) {
            throw new RuntimeException("该DLI数据类型不能获取到对应的DWS数据类型，传入的DLI类型为：" + dliDataType);
        } else {
            return dwsDataType;
        }
    }


}
