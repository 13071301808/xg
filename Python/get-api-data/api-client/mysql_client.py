import pymysql


class MysqlClient:
    # 初始化
    def __init__(self, db, host, user, password, timeout=10):
        self.conn = None
        self.conn = pymysql.connect(
            host=host,
            user=user,
            password=password,
            db=db,
            charset="utf8mb4",
            connect_timeout=timeout
        )
        self.cursor = self.conn.cursor()

    # 结束
    def __del__(self):
        self.close()

    # 查询
    def select(self, sql, args):
        self.cursor.execute(query=sql, args=args)
        return self.cursor.fetchall()

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
