from flask import Flask, render_template, request, jsonify
def chat():
    message = request.form['message']
    try:
        # 将输入的文字交互到生成模型
        sql_model = chatsql_api.get_question_return(message)
        # # 解析并计算用户输入的消息
        # result = sympify(message)
        # response = str(result)
        # # 给个生成图片的条件
        # if message == '生成图片':
        #     # 生成图表
        #     b = madetu_api.made_tu()
        #     made = b.render_embed()
        #     # 设置存放路径
        #     b.render('static/sheet/test.html')
        #     return made
    except:
        response = "输入有问题，请重新输入"
    # 这里简单示例直接返回用户输入的消息
    return jsonify(response)

# dws_config = {
#     "host": "yishou-dws.dws.myhuaweiclouds.com",
#     "user": "dbadmin",
#     "password": "yishou@999"
# }

# jdbc:postgresql://yishou-dws.dws.myhuaweiclouds.com
# class DWS_cli:
#     @staticmethod
#     def get_dws_conn(sql_model):
#         # 建立dws连接
#         gaussdb_client = YsGaussDBClient()
#         line = gaussdb_client.select(sql_model)
#         print(line)
#         return line
#         # dws_conn = psycopg2.connect(database="postgres", user=dws_config["user"], password=dws_config["password"],
#         #                             host=dws_config["host"], port="8000", sslmode="disable")
#         # dws_conn.set_client_encoding('utf-8')
#         # print(dws_conn)
#         # return dws_conn

# class YsGaussDBClient:
#     def __init__(self, conf_mode='prod'):
#         conf = Config.get_config(conf_mode)
#         self.__db = conf['gaussdb_conf']['ys_db']
#         self.__host = conf['gaussdb_conf']['ys_host']
#         self.__user = conf['gaussdb_conf']['ys_user']
#         self.__password = conf['gaussdb_conf']['ys_password']
#         self.__port = conf['gaussdb_conf']['ys_port']
#
#         Printter.info("Init GaussDB Connection...")
#         self.conn = psycopg2.connect(database=self.__db, user=self.__user, password=self.__password, host=self.__host,
#                                      port=self.__port)
#         self.conn.set_client_encoding('UTF-8')
#         self.cursor = self.conn.cursor()
#
#     def get_connection(self):
#         return self.conn
#
#     def select(self, sql):
#         try:
#             self.cursor.execute(query=sql)
#             return self.cursor.fetchall()
#         except Exception as e:
#             Printter.error(traceback.format_exc())
#             sys.exit('执行失败,失败SQL:' + sql)
#
#     # 删除、更新、插入
#     def update(self, sql):
#         try:
#             Printter.info("执行SQL:" + sql)
#             self.cursor.execute(sql)
#             self.conn.commit()
#         except Exception as e:
#             Printter.error(traceback.format_exc())
#             sys.exit('执行失败,失败SQL:' + sql)
#
#     def exec_sql_return_dataframe(self, sql):
#         Printter.info(sql)
#         try:
#             self.cursor.execute(sql)
#             column_list = []
#             for column_desc in self.cursor.description:
#                 column_list.append(column_desc[0])
#             data = list(self.cursor.fetchall())
#             return pd.DataFrame(data, columns=column_list)
#         except Exception as e:
#             Printter.error(traceback.format_exc())
#
#     def close(self):
#         self.conn.close()
#
#     def __del__(self):
#         self.close()