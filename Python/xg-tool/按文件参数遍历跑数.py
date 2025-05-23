from yssdk.dli.sql_client import YsDliSQLCLient
import pandas as pd

# Spark SQL 配置选项
spark_options = {
    "spark.sql.mergeSmallFiles.enabled": "false",
    "spark.sql.adaptive.skewJoin.optimizeGeneralJoins": "true",
    "spark.sql.adaptive.join.enabled": "true",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.skewJoin.optimizeMultiTableJoins": "true",
    "spark.sql.adaptive.skewedJoin.enabled": "true",
    # 'spark.scheduler.pool': 'high',
    "spark.sql.shuffle.partitions": "400"
}


def load_csv(file_path):
    return pd.read_csv(file_path, encoding='gbk')


if __name__ == '__main__':

    client = YsDliSQLCLient(queue='analyst', database='yishou_daily')
    file_path = "E:/资料/data_ratio.csv"
    df = load_csv(file_path)
    for i in range(len(df)):
        # ratio = df['比例'][i]
        sql = f'''
        INSERT OVERWRITE TABLE yishou_data.dcl_event_goods_exposure_d PARTITION (dt)
        select 
            user_id, goods_id, special_id, os, goods_no, pid, ptime, special_time, source, report_time, event_id,
            search_event_id, goods_count, click_goods_id_count, keyword, app_version, log_type, is_rec, abtest, index, 
            strategy_id, is_operat, is_default, special_index, page_name, room_id, dt
        from yishou_data.dcl_event_goods_exposure_d_backup 
        where dt = {df['日期'][i]} and pid != 32 
        union all  
        select 
            user_id, goods_id, special_id, os, goods_no, pid, ptime, special_time, source, report_time, event_id, 
            search_event_id, goods_count, click_goods_id_count, keyword, app_version, log_type, is_rec, abtest, index, 
            strategy_id, is_operat, is_default, special_index, page_name, room_id, dt
        from (
            select 
                *,
                row_number() over(partition by user_id,goods_id order by event_id) nu 
            from yishou_data.dcl_event_goods_exposure_d_backup 
            where dt = {df['日期'][i]} and pid = 32  
            having nu = 1 
        )
        where (MOD(ABS(HASH(CONCAT(user_id,goods_id))), 200000000) / 200000000.0) < {df['比例'][i]}
        DISTRIBUTE BY floor(rand()*200)
        ;
        '''
        # print(sql)
        # 执行sql语句
        client.exec_sql(sql, output=False)
        print("完成", df['日期'][i])
