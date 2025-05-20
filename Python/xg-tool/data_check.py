# -*-coding: Utf-8 -*-
# @File : data_check .py
# author: ShanFeng
# Time：2024/3/6/0006
import csv
import decimal
import os
from decimal import Decimal
from yssdk.dli.sql_client import YsDliSQLCLient
from loguru import logger
from date_generator import DateGenerator
from collections import defaultdict

one_day_ago = DateGenerator.one_day_ago()
one_quarter_ago = DateGenerator.interval_day(-90)
one_year_ago = DateGenerator.interval_day(-365)


def count_occurrences(ls1, ls2):
    # 将 ls2 展开为一个平面列表
    flat_ls2 = [item[0] for sublist in ls2 for item in sublist]

    # 初始化字典来存储每个元素的出现次数
    counts = defaultdict(int)

    # 统计 ls1 中每个元素在 flat_ls2 中出现的次数
    for item in ls1:
        counts[item] = flat_ls2.count(item)

    return dict(counts)


def generate_primary_keys(all_result, primary_keys):
    num_primary_keys = len(primary_keys)
    if num_primary_keys == 1:
        return [row[0] for row in all_result]
    elif num_primary_keys > 1:
        return [f"{'@@@'.join(str(row[i]) for i in range(num_primary_keys))}" for row in all_result]
    else:
        raise ValueError("主键数量必须至少为1")


def get_table_columns(table_name):
    sql = f"DESCRIBE {table_name}"
    dli.exec_sql(sql, output=False)
    columns_info = dli.fetch_all()
    columns = []
    for column in columns_info:
        if column[0] == 'dt':
            break
        columns.append(column[0])
    return columns


def compare_results(result1, result2, check_mode):
    global result_dif
    if check_mode:
        if len(result1) != len(result2):  # 先判断行数
            logger.warning(f'随机{limit}条数据抽取, 行数不一致, all表{len(result1)}条, ods表{len(result2)}条')
            return False

        # 精确至每个单元格判断, 记录不同的单元格数据
        result1 = sorted(result1)
        result2 = sorted(result2)
        for row in range(len(result1)):
            dif = []
            for cell in range(len(result1[row])):
                cell1 = result1[row][cell]
                cell2 = result2[row][cell]

                if cell1 is None:
                    cell1 = 0
                if cell2 is None:
                    cell2 = 0

                try:
                    cell1 = Decimal(str(cell1))
                    cell2 = Decimal(str(cell2))
                    if abs(cell1 - cell2) >= 0.001:
                        dif.append((columns[cell], cell1, cell2))
                except decimal.InvalidOperation:
                    if cell1 != cell2:
                        dif.append((columns[cell], cell1, cell2))
            if len(dif) > 0:
                result_dif.append(dif)

        if len(result_dif) > 0:
            return False
        return True

    return sorted(result1) == sorted(result2)


def write_to_csv(data, filename, columns_str):
    with open(filename, mode='w', newline='', encoding='utf8') as file:
        writer = csv.writer(file)
        columns = [column.strip() for column in columns_str.split(',')]  # 去除列名中的空格
        writer.writerow(columns)
        writer.writerows(data)


def main(primary_keys, all_table, all_condition, ods_table, ods_condition, check_mode, columns_str=None):
    global result_dif, columns
    logger.info(
        f"\033[92m0: Start comparing data:\033[0m\n\tmysql_table:\033[91m{mysql_table}\033[0m\n\tall_table: \033[91m{all_table}\033[0m\n\tods_table: \033[91m{ods_table}\033[0m")

    # 1. 获取字段列表, 以主键为第一位, "," 拼接
    logger.info("\033[92m1: Getting fields list...\033[0m")
    if columns_str is None:
        columns = get_table_columns(ods_table)
        logger.info(columns)
        for pk in primary_keys:
            columns.remove(pk)
        columns = primary_keys + columns
        # 将目标列移动到列表的第一位
        columns_str = ",".join(columns)
    columns = columns_str.split(',')

    # 2. 执行 SQL 查询语句
    logger.info("\033[92m2: Executing the all_table sql statement...\033[0m")
    try:
        all_sql = f"SELECT {columns_str} FROM {all_table} where 1=1 {all_condition} ORDER BY rand() * {rand} LIMIT {limit};"
        dli.exec_sql(all_sql, output=False)
    except:
        logger.warning("\033[94m2.1: Failed to execute the SQL statement, pull all table fields...\033[0m")
        columns = get_table_columns(all_table)
        logger.info(columns)
        for pk in primary_keys:
            columns.remove(pk)
        columns = primary_keys + columns
        # 将目标列移动到列表的第一位
        columns_str = ",".join(columns)
        columns = columns_str.split(',')

        all_sql = f"SELECT {columns_str} FROM {all_table} where 1=1 {all_condition} ORDER BY rand() * {rand} LIMIT {limit};"
        dli.exec_sql(all_sql, output=False)

    all_result = dli.fetch_all()
    logger.info(f'抽取数据条数为: {len(all_result)}条')

    # 3. 获取第一步 SQL 结果的主键 id
    logger.info("\033[92m3: Getting primary key list...\033[0m")
    result_primary_keys = generate_primary_keys(all_result, primary_keys)

    # 4. 执行 SQL 查询语句
    logger.info("\033[92m4: Executing the ods_table sql statement...\033[0m")
    ods_sql = fr"""SELECT {columns_str} FROM {ods_table} WHERE 1=1 {ods_condition} AND concat({",'@@@',".join(primary_keys)}) in ('{"','".join(map(str, result_primary_keys))}');"""
    dli.exec_sql(ods_sql, output=False)
    ods_result = dli.fetch_all()

    # 5. 输出结果到 CSV 文件
    logger.info("\033[92m5: Writing to csv file...\033[0m")
    if not os.path.exists('data_check_dir'):
        # 如果不存在，则创建目录
        os.makedirs('data_check_dir')
    try:
        write_to_csv(all_result, "data_check_dir/all_result.csv", columns_str)
        write_to_csv(ods_result, "data_check_dir/ods_result.csv", columns_str)
    except PermissionError:
        write_to_csv(all_result, "data_check_dir/all_result_copy.csv", columns_str)
        write_to_csv(ods_result, "data_check_dir/ods_result_copy.csv", columns_str)

    # 6. 比对两个 SQL 的执行结果是否一致
    logger.info("\033[92m6: Comparing SQL results...\033[0m")
    result_dif = []
    compare = compare_results(all_result, ods_result, check_mode)

    # 7. 比对两个表格的数据条数是否一致:
    logger.info("\033[92m7: Comparing table rows...\033[0m")
    sql = fr"""SELECT "{all_table}", count(1), count(distinct concat({",'@@@',".join(primary_keys)})) FROM {all_table} where 1=1 {all_condition} UNION ALL select "{ods_table}", count(1), count(distinct concat({",'@@@',".join(primary_keys)})) FROM {ods_table} where 1=1 {ods_condition}"""
    logger.info(sql)
    dli.exec_sql(sql, output=False)

    # 8. 比对结果输出:
    logger.info("\033[92m8: Output table comparison results...\033[0m")

    for x in dli.fetch_all():
        logger.info(f"{x[0]} 数据条数为: {x[1]}, 重复主键数:{x[1] - x[2]}")

    if compare:
        logger.info(f"随机{limit}条数据校验结果: 一致!")
    else:
        logger.warning(f"随机{limit}条数据校验结果: 存在不一致的情况,不一致数据条数为{len(result_dif)}, 请查看CSV文件")
        for i in result_dif:
            print(i)
        dif_dict = count_occurrences(columns, result_dif)
        logger.info(dif_dict)
        for k, v in dif_dict.items():
            if v != 0:
                logger.warning(f'{k}: {v}')


if __name__ == "__main__":
    dli = YsDliSQLCLient()
    columns_str = ''

    # 默认参数 非必要不调节
    limit = 3000
    rand = 113

    # 需人为手动传入
    check_mode = True  # True: 精确至每个单元格; False: 全文匹配,精度不同也会返回错误
    primary_keys = ['id']  # 主键ID
    mysql_table = "fmys_card_order"  # 业务表名
    all_table = f"yishou_data.all_{mysql_table}"  # all表名, 视情况改
    all_condition = f''  # all条件, 以 and... 输入
    ods_table = f"yishou_data.ods_{mysql_table}_dt"  # ods表名, 视情况改
    ods_condition = f'and dt = {one_day_ago}'  # all条件, 以 and... 输入
    # columns_str = 'as_package_id, shipping_num, shipping_company, user_id, receive_time, receive_admin, receive_num, receive_img, package_status, unpack_begin_time, unpack_submit_time, unpack_complete_time, origin, create_time, update_time'

    if columns_str == '':
        main(primary_keys, all_table, all_condition, ods_table, ods_condition, check_mode)
    else:
        main(primary_keys, all_table, all_condition, ods_table, ods_condition, check_mode, columns_str)
