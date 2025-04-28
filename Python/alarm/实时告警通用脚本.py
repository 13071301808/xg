## PYTHON
## ******************************************************************** ##
## author: chenzhigao
## create time: 2025/02/11 13:54:14 GMT+08:00
## ******************************************************************** ##
## 通用告警脚本（实时版）
import argparse
import time
from datetime import datetime, timedelta
from yssdk.common.alarm import Alarm
from yssdk.dli.sql_client import YsDliSQLCLient

# 获取日期
one_hour_ago = (datetime.now() - timedelta(hours=1)).strftime("%Y%m%d%H")

sql = f'''
    SELECT 
        reg_user_num
        ,login_dau
        ,app_goods_exposure_uv
        ,h5_goods_exposure_uv
        ,app_goods_click_uv
        ,h5_goods_click_uv
        ,app_add_cart_user_num
        ,wx_add_cart_user_num
        ,buy_amount
        ,buy_num
        ,dt
        ,ht
    FROM yishou_daily.ads_log_monitor_infos_ht
    WHERE ht = {one_hour_ago}
    ;
'''

if __name__ == '__main__':
    # 建立数据连接
    client = YsDliSQLCLient(queue='develop', database='yishou_daily')
    # 执行sql语句
    client.exec_sql(sql)
    result = client.fetch_all()
    alarm = Alarm(alarm_title='通用实时告警')
    # 写入告警机器人
    alarm.set_color('blue')
    alarm.set_alarm_field('日期', result[0][10])
    alarm.set_alarm_field('小时', str(result[0][11])[-2:])
    alarm.set_alarm_field('注册用户数', result[0][0])
    alarm.set_alarm_field('登录DAU', result[0][1])
    alarm.set_alarm_field('app商品曝光uv', result[0][2])
    alarm.set_alarm_field('h5商品曝光uv', result[0][3])
    alarm.set_alarm_field('app商品点击uv', result[0][4])
    alarm.set_alarm_field('h5商品点击uv', result[0][5])
    alarm.set_alarm_field('app加购人数', result[0][6])
    alarm.set_alarm_field('wx加购人数', result[0][7])
    alarm.set_alarm_field('实际GMV', result[0][8])
    alarm.set_alarm_field('实际购买件数', result[0][9])
    alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/01b23ab9-830d-46a9-a44a-62e3a7f85031')
    alarm.send_to_feishu(alarm.build_alarm())


