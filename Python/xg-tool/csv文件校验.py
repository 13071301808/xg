# -*-coding: Utf-8 -*-
# @File : csv_data_check .py
# author: ShanFeng
# Time：2024/12/17/星期二
# Purpose:
import pandas as pd


def load_csv(file_path):
    return pd.read_csv(file_path)


def compare_tables(main_table, sub_table, keys):
    result = {
        'total_records': 0,
        'passed_records': 0,
        'failed_records': {},
    }

    merged_data = pd.merge(main_table, sub_table, on=keys, suffixes=('_main', '_sub'), how='outer', indicator=True)
    result['total_records'] = len(merged_data)

    match_mask = merged_data['_merge'] == 'both'
    matched_data = merged_data[match_mask]
    unmatched_data = merged_data[~match_mask]

    passed_records = len(matched_data)
    result['passed_records'] = passed_records

    # Check differences in matched records
    if not matched_data.empty:
        for column in main_table.columns:
            if column not in keys:
                diff_mask = matched_data[f'{column}_main'] != matched_data[f'{column}_sub']
                diff_records = matched_data[diff_mask]
                if not diff_records.empty:
                    result['failed_records'][column] = len(diff_records)
                    for idx, row in diff_records.iterrows():
                        print(f"{column}: A 表的值={row[f'{column}_main']}, B 表的值={row[f'{column}_sub']}")

    # Handle cases in unmatched data
    for idx, row in unmatched_data.iterrows():
        status = row['_merge']
        if status == 'left_only':
            print(f"存在于主表但不存在于副表的记录: {row[keys].to_dict()}")
        elif status == 'right_only':
            print(f"存在于副表但不存在于主表的记录: {row[keys].to_dict()}")

    return result


if __name__ == "__main__":
    main_csv_path = '../../../xg_utils/data/001.csv'  # 主表 CSV 文件路径
    sub_csv_path = '../../../xg_utils/data/002.csv'  # 副表 CSV 文件路径
    keys = 'special_date,special_id'.split(',')  # 主键，以逗号分隔

    main_table = load_csv(main_csv_path).fillna(0)
    sub_table = load_csv(sub_csv_path).fillna(0)

    result = compare_tables(main_table, sub_table, keys)

    print("\n 校验结果:")
    print(f"总记录数: {result['total_records']}")
    print(f"校验通过记录数: {result['passed_records']}")
    if result['failed_records']:
        print("不一致记录数目:")
        for field, count in result['failed_records'].items():
            print(f"{field}: {count}")
    else:
        print("所有记录都校验通过")