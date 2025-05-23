# -*-coding: Utf-8 -*-
# @File : run_dli_sql.py
# author: ShanFeng
# Time：2024/3/7/0007

import time
from datetime import datetime, timedelta
from functools import partial
from threading import Lock
from yssdk.dli.sql_client import YsDliSQLCLient
from loguru import logger
from utils.date_generator import DateGenerator
from concurrent.futures import ThreadPoolExecutor

# 创建一个锁对象，用于线程同步
lock = Lock()

# Spark SQL 配置选项
spark_options = {
    "spark.sql.mergeSmallFiles.enabled": "false",
    "spark.sql.adaptive.skewJoin.optimizeGeneralJoins": "true",
    "spark.sql.adaptive.join.enabled": "true",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.skewJoin.optimizeMultiTableJoins": "true",
    "spark.sql.adaptive.skewedJoin.enabled": "true",
    # 'spark.scheduler.pool': 'high',
    "spark.sql.shuffle.partitions": "600"
}

# 创建开发队列和分析队列的 SQL 客户端
dev_dli_client = YsDliSQLCLient(queue='develop', database='yishou_data', options=spark_options)
ana_dli_client = YsDliSQLCLient(queue='analyst', database='yishou_data', options=spark_options)

# 初始化计数器
dev_queue_counter = 0  # 开发队列计数器
current_job_counter = 0  # 当前作业计数器
start_time = time.time()  # 记录开始时间


def get_previous_day(date_str):
    # 将字符串转换为 datetime 对象
    date_obj = datetime.strptime(date_str, '%Y%m%d')

    # 减去一天
    previous_day = date_obj - timedelta(days=1)

    # 将 datetime 对象转换回字符串
    previous_day_str = previous_day.strftime('%Y%m%d')

    return previous_day_str


def supplement_data_with_3_params(date_range, param_1, param_2, param_3):
    """
    使用三个参数补数数据
    :param date_range: 日期范围列表
    :param param_1: 第一个参数
    :param param_2: 第二个参数
    :param param_3: 第三个参数
    """
    global dev_queue_counter, current_job_counter, start_time

    # 读取 SQL 模板文件
    with open("sql_template/supplement_sql.sql", "r", encoding='utf8') as f:
        sql_template = f.read()

    # 替换 SQL 文件中的参数
    sql = sql_template.replace(param_1, date_range[0])
    sql = sql.replace(param_2, date_range[1])
    sql = sql.replace(param_3, date_range[2])
    sql = f'-- 补数 SQL,开始日期->{date_range[0]}|结束日期->{date_range[1]}\n' + sql

    start_time_job = time.time()
    if __name__ == '__main__':
        use_dev_queue = False

        # 判断是否使用开发队列
        if dev_queue_counter < DEV_QUEUE_SIZE:
            with lock:
                dev_queue_counter += 1
            use_dev_queue = True

        # 执行 SQL
        if use_dev_queue:
            dev_dli_client.exec_sql(sql, output=False)
            with lock:
                dev_queue_counter -= 1
        else:
            ana_dli_client.exec_sql(sql, output=False)

    queue_type = 'develop' if use_dev_queue else 'analyst'
    logger.info(
        f'补数 SQL 运行完成, 开始日期: {date_range[0]}, 结束日期: {date_range[1]} | 提交队列: {queue_type} | 程序运行时长为: {round(time.time() - start_time_job, 2)}s')
    with lock:
        current_job_counter += 1
        if current_job_counter % 5 == 0:
            elapsed_time = time.time() - start_time
            avg_time_per_job = elapsed_time / current_job_counter
            estimated_total_time = start_time + total_job_count * avg_time_per_job
            estimated_completion_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(estimated_total_time))
            logger.info(
                f'全量作业数: {total_job_count} | 当前完成作业数: {current_job_counter} | 完成进度: {round(current_job_counter / total_job_count * 100, 2)}% | 预估补数完成时间: {estimated_completion_time}')


if __name__ == '__main__':
    ANA_QUEUE_SIZE = 1  # 分析队列最大作业数
    DEV_QUEUE_SIZE = 0  # 开发队列最大作业数

    # 创建线程池
    MAX_WORKERS = ANA_QUEUE_SIZE + DEV_QUEUE_SIZE
    pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    # 多参数补数
    date_generator = DateGenerator(20250412, 20250502)
    # date_ranges = date_generator.generate_month_ranges_date_3()  # 会生成三个参数
    date_ranges = date_generator.generate_custom_ranges_date_3(1)  # 会生成三个参数

    # date_ranges.reverse()  # 是否逆转日期顺序
    logger.info(date_ranges)
    total_job_count = len(date_ranges)
    logger.info(f"全量补数作业数, 共{total_job_count}个")

    # 提交多参数补数任务到线程池
    # for date_range in date_ranges:
    #     partial_func = partial(supplement_data_with_3_params, date_range, '${bdp.system.bizdate}', '${xxxxxxxxxx}',
    #                            '${xxxxxxxxx}')  # 根据三个参数, 填入对应的变量名
    #     pool.submit(partial_func)
    # pool.shutdown()

    for date_range in date_ranges:
        supplement_data_with_3_params(date_range, '${bdp.system.bizdate}', '${xxxxxxxxxx}','${xxxxxxxxx}')