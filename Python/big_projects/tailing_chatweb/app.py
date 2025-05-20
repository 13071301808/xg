# 导入依赖包
import logging
import time
from datetime import datetime
from flask import Flask, render_template, request, jsonify
from newmodel_demo import chatsql_model
# from demo import chatsql_model
from web_utils.dws_cli import DWS_cli
from web_utils.madetu import Madetu
import pandas as pd


# 全局配置
app = Flask(__name__, static_url_path='static')
# 调用模型路径
# model_path = '/data/laiguibin/ChatSQL_web/model/chatglm-6b'
# pt_model_path = '/data/laiguibin/ChatGLM2-6B-model/ptuning/output/mysqlv5-chatglm2-6b-pt-128-1e-2-20231130/checkpoint-600'

# 实例化api
chatsql_api = chatsql_model(model_name_or_path="/data/laiguibin/model/ZhipuAI/chatglm3-6b",
                              template="chatglm3",
                              finetuning_type="lora",
                              checkpoint_dir="/data/laiguibin/LLaMA-Factory/output_dir/20231229_sft_checkpoint-chatglm3-6b-v15/checkpoint-2000",
                              max_new_tokens=5120,
                              top_p=1,
                              temperature=0.001,
                            )
madetu_api = Madetu()
# 配置日志参数
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename=f'/data/laiguibin/ChatSQL_web/log/{str(datetime.today())[:10]}_chatsql_log.txt',
                    filemode='w')


# 默认访问
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/chat', methods=['POST', 'GET'])
def chat():
    # 获取输入的信息
    message = request.form['message']
    logging.info(f'>>>>> 问的问题:{message}')
    # 运行前时间
    start_time = datetime.now()
    # 将输入的信息生成模型为查询的sql
    sql_model = chatsql_api.get_question_return(message)
    # 运行后时间
    end_time = datetime.now()
    timeout_check = (end_time - start_time).total_seconds()
    logging.info(f'>>>>> 建立sql所用时间:{timeout_check}')
    # 添加日志和控制台输出
    logging.info(f'>>>>> 建立sql成功:\n{sql_model}')
    # 判断sql语句中是否将special_date作为查询字段
    if 'special_date' in sql_model.split('from')[0]:
        dws_api = DWS_cli("postgres", "dbadmin", "yishou@999", "122.9.106.223", 8000, "disable")
        try:
            # 生成表格数据
            data_pd = dws_api.exec_sql_return_dataframe(sql_model)
            # 重置索引，消除默认生成的序号数据
            data_pd = data_pd.reset_index(drop=True)
            logging.info(f'>>>>> 是以专场日期作为查询字段')
            logging.info(f'>>>>> 获取表数据成功:\n{data_pd}')
            # 添加睡眠
            time.sleep(0.5)
            # 生成图片
            table = madetu_api.made_tu_line(data_pd)
            print('创建折线图成功')
            logging.info(f'>>>>> 创建折线图成功')
            # 将 line 对象转换为 HTML 字符串
            html = table.render_embed()
        except Exception as e:
            html = "没有查询到相关数据，无法生成图片"
            logging.info(f'>>>>> 没有查询到相关数据，无法生成图片')
            data_pd = pd.DataFrame({"message": ["没有查询到相关数据"]})
            logging.info(f'>>>>> 没有查询到相关数据')
            logging.info(f"sql生成数据错误：\n{str(e)}")
    else:
        dws_api = DWS_cli("postgres", "dbadmin", "yishou@999", "122.9.106.223", 8000, "disable")
        try:
            # 生成表格数据
            data_pd = dws_api.exec_sql_return_dataframe(sql_model)
            # 重置索引，消除默认生成的序号数据
            data_pd = data_pd.reset_index(drop=True)
            logging.info(f'>>>>> 没有以专场日期作为查询字段')
            logging.info(f'>>>>> 获取表数据成功:\n{data_pd}')
            # 添加睡眠
            time.sleep(0.5)
            # 生成图片
            table = madetu_api.made_tu_bar(data_pd)
            print('创建柱状图成功')
            logging.info(f'>>>>> 创建柱状图成功')
            # 将 Bar 对象转换为 HTML 字符串
            html = table.render_embed()
        except Exception as e:
            html = "没有查询到相关数据，无法生成图片"
            logging.info(f'>>>>> 没有查询到相关数据，无法生成图片')
            data_pd = pd.DataFrame({"message": ["没有查询到相关数据"]})
            logging.info(f'>>>>> 没有查询到相关数据')
            logging.info(f"sql生成数据错误：\n{str(e)}")
    return jsonify(html=html, data=data_pd.to_html(index=False))


if __name__ == '__main__':
    # 设置无ip访问限制，端口
    app.run(host="0.0.0.0", port=5002)
