from yssdk.dli.sql_client import YsDliSQLCLient
import pandas as pd


def load_csv(file_path):
    return pd.read_csv(file_path, encoding='utf-8')


if __name__ == '__main__':
    client = YsDliSQLCLient(queue='analyst', database='yishou_daily')
    file_path = "E:/资料/data_ratio.csv"
    df = load_csv(file_path)
    for i in range(len(df)):
        ratio = df['比例'][i]
        sql = f'''
            select count(1) from yishou_data.dcl_wx_user_center_click_d where dt = {df['日期'][i]}
        '''
        # 执行sql语句
        client.exec_sql(sql, output=False)
        result = client.fetch_one()
        print("查询结果：", result[0])
