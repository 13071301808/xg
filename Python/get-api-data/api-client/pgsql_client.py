# dws
import pandas as pd
import psycopg2

class DWS_cli:
    # 初始化
    def __init__(self, database, user, password, host, port, sslmode):
        self.conn = None
        self.conn = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port,
            sslmode=sslmode
        )
        # 转换数据的编写格式
        self.conn.set_client_encoding('utf-8')
        self.cursor = self.conn.cursor()

    # 结束
    def __del__(self):
        self.close()

    # 查询
    def select(self, sql):
        self.cursor.execute(query=sql)
        return self.cursor.fetchall()

    def exec_sql_return_dataframe(self, sql):
        self.cursor.execute(sql)
        column_list = []
        for column_desc in self.cursor.description:
            column_list.append(column_desc[0])
        data = list(self.cursor.fetchall())
        return pd.DataFrame(data, columns=column_list)

    # 删除、更新、插入
    def update(self, sql):
        temp = self.cursor.execute(sql)
        self.conn.commit()
        return temp

    # 关闭连接
    def close(self):
        if self.conn:
            self.cursor.close()
            self.conn.close()
