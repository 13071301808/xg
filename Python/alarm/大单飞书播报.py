import requests
import json
from yssdk.dli.sql_client import YsDliSQLCLient
from datetime import datetime, timedelta
import pandas as pd

sql = f'''
select 
    b.user_id as flower_user_id
    , a.*
from (
    select 
        sp_date
        , order_id
        , order_states
        , supply_name
        , goods_no 
        , goods_kh
        , round(real_gmv,2) as real_gmv
        , real_buy_num
        , real_buy_num_out
        , real_buy_num_not_yet_out
        , shop_assistant
    from cross_origin_v2.finebi_user_big_order_broadcast_v6_dt  
    where dt = to_char(DATEADD(current_date(),-1,'dd'),'yyyymmdd')
    and order_states <> '未6天发部分_正常'
) a 
left join yishou_data.all_fmys_feishu_user_test b on a.shop_assistant = b.nickname
;
'''

def send_message(result):
    # 飞书机器人hook url
    webhook_url = "https://open.feishu.cn/open-apis/bot/v2/hook/d4e33f10-8105-44f5-b49a-0ccd28df8350"
    # 测试
    # webhook_url = "https://open.feishu.cn/open-apis/bot/v2/hook/fd310f97-48ef-45fb-8977-1664a87526f0"
    # 构造请求头
    headers = {
        "Content-type": "application/json",
        "charset": "utf-8"
    }
    # 遍历dataframe,拼接消息json
    for row in result.itertuples():
        msg = {
            "msg_type": "text",
            "content": {
                "text": f'''<at user_id="{getattr(row, 'flower_user_id')}"></at>播报信息：
专场日: {getattr(row, 'sp_date')}
订单号: {getattr(row, 'order_id')}
订单状态: {getattr(row, 'order_states')}
商家名称: {getattr(row, 'supply_name')}
货号: {getattr(row, 'goods_no')}
款号: {getattr(row, 'goods_kh')}
订单gmv: {getattr(row, 'real_gmv')}
订单件数: {getattr(row, 'real_buy_num')}
发货件数: {getattr(row, 'real_buy_num_out')}
未发货件数: {getattr(row, 'real_buy_num_not_yet_out')}
商运花名: {getattr(row, 'shop_assistant')}'''
            }
        }
        msg_encode = json.dumps(msg, ensure_ascii=True).encode("utf-8")
        reponse = requests.post(url=webhook_url, data=msg_encode, headers=headers)
        print(reponse)

if __name__ == '__main__':
    pd.set_option('max_colwidth', 1000)
    # 建立数据连接
    client = YsDliSQLCLient(queue='analyst', database='yishou_daily')
    # 执行sql语句
    client.exec_sql(sql)
    # 获取全部结果转为dataframe形式数据
    result = client.fetch_all_dataframe()
    #批量发送消息
    send_message(result)

