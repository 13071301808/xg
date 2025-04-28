# 赛马因子_曝光数据打压
# 1. 主键无重复
# 2. 和昨天差距不大（百分之20以内）
# 3. 打印数据条数
# 4. 对 special_ids 炸裂，然后对 用户和专场 看是否有重复项
# 5. 打印用户最大的 打压专场数 和 最小的 打压专场数

# 导入依赖包
import string
import argparse
import pandas as pd
from yssdk.dli.sql_client import YsDliSQLCLient
from yssdk.common.printter import Printter
from yssdk.common.alarm import Alarm
import sys
from datetime import datetime, timedelta

if __name__ == '__main__':

    # 获取前一天日期
    previous_date_str = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    # 检测sql
    sql = f''' 
        select 
        (
            (SELECT count(user_id) FROM yishou_daily.dtl_user_exposure_special) - (select count(DISTINCT user_id) from yishou_daily.dtl_user_exposure_special)
        ) as 主键重复数,
        (
            ((select count(*) from yishou_daily.dtl_user_exposure_special) / (select count(*) from yishou_daily.dtl_user_exposure_special_dt where dt={previous_date_str})) - 1
        ) as 与昨天相差比例,
        (
            select count(*) from yishou_daily.dtl_user_exposure_special
        ) as 数据条数,
        (
            (SELECT count(t1.user_id,special_id) FROM yishou_daily.dtl_user_exposure_special t1 lateral view explode(split(special_ids,",")) t2 AS special_id) 
            - 
            (SELECT count(DISTINCT t1.user_id,t2.special_id) FROM yishou_daily.dtl_user_exposure_special t1 lateral view explode(split(special_ids,",")) t2 AS special_id)
        ) as 对special_ids炸裂后的用户和专场重复数,
        (   select max(t3.special_ids_count) 
            from (
                SELECT user_id,count(special_id) as special_ids_count FROM yishou_daily.dtl_user_exposure_special t1 
                lateral view explode(split(special_ids,",")) t2 AS special_id
                group by user_id
            ) as t3
        ) as 最大打压专场数,
        (
            select min(t3.special_ids_count) 
            from (
                SELECT user_id,count(special_id) as special_ids_count FROM yishou_daily.dtl_user_exposure_special t1 
                lateral view explode(split(special_ids,",")) t2 AS special_id
                group by user_id
            ) as t3
        ) as 最小打压专场数
        ;
    '''
    # 建立数据连接
    client = YsDliSQLCLient(queue='develop')
    # 设置飞书告警总标题
    alarm = Alarm(alarm_title='检查 赛马因子_曝光数据打压 情况')
    # 执行sql语句
    client.exec_sql(sql)
    # 获取全部结果
    result = client.fetch_all()
    # print(result[0][1])
    # 检测是否有重复主键
    if int(result[0][0]) != 0:
        # 设置告警颜色
        alarm.set_color('red')
        # 设置告警字段
        alarm.set_alarm_field('告警口径', '主键无重复')
        alarm.set_alarm_field('告警作业所在空间', 'yishou_daily')
        alarm.set_alarm_field('告警作业名称', '赛马因子_曝光数据打压')
        alarm.set_alarm_field('告警级别', 'P0')
        alarm.set_alarm_field('运行日期', str(datetime.now())[:19])
        alarm.set_alarm_field('后端计算时间', '最晚10点前')
        alarm.set_alarm_field('主键重复数量', result[0][0])
        alarm.set_alarm_field('与昨天相差比例', f'{round(float(result[0][1]) * 100, 6)}%')
        alarm.set_alarm_field('数据条数', result[0][2])
        alarm.set_alarm_field('用户和专场重复数', result[0][3])
        alarm.set_alarm_field('最大打压专场数', result[0][4])
        alarm.set_alarm_field('最小打压专场数', result[0][5])
        # 设置发送飞书群（大数据P0P1级重点业务告警）
        alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/cbf88ed2-a19a-4a11-af26-a6bd9da70181')
        alarm.send_to_feishu(alarm.build_alarm())
        # 设置第二个告警群url -- 卖货-频道运营监控
        alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/3cd84980-0bcc-462c-8a1c-d33349c7a99b')
        alarm.send_to_feishu(alarm.build_alarm())
        # 抛出异常,阻断程序
        raise Exception("告警:有重复主键(重复supply_id),阻断程序")
    # 检测是否与昨天相差比例大于20%
    elif int(result[0][1]) > abs(0.2):
        # 设置告警颜色
        alarm.set_color('red')
        # 设置告警字段
        alarm.set_alarm_field('告警口径', '与昨天相差不到20%')
        alarm.set_alarm_field('告警作业所在空间', 'yishou_daily')
        alarm.set_alarm_field('告警作业名称', '赛马因子_曝光数据打压')
        alarm.set_alarm_field('告警级别', 'P0')
        alarm.set_alarm_field('运行日期', str(datetime.now())[:19])
        alarm.set_alarm_field('后端计算时间', '最晚10点前')
        alarm.set_alarm_field('主键重复数量', result[0][0])
        alarm.set_alarm_field('与昨天相差比例', f'{round(float(result[0][1]) * 100, 6)}%')
        alarm.set_alarm_field('数据条数', result[0][2])
        alarm.set_alarm_field('用户和专场重复数', result[0][3])
        alarm.set_alarm_field('最大打压专场数', result[0][4])
        alarm.set_alarm_field('最小打压专场数', result[0][5])
        # 设置发送飞书群（大数据P0P1级重点业务告警）
        alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/cbf88ed2-a19a-4a11-af26-a6bd9da70181')
        alarm.send_to_feishu(alarm.build_alarm())
        # 设置第二个告警群url -- 卖货-频道运营监控
        alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/3cd84980-0bcc-462c-8a1c-d33349c7a99b')
        alarm.send_to_feishu(alarm.build_alarm())
        # 抛出异常,阻断程序
        raise Exception("告警:与昨天相差比例大于20%,阻断程序")
    # 检测用户和专场看有重复项
    elif int(result[0][3]) != 0:
        # 设置告警颜色
        alarm.set_color('red')
        # 设置告警字段
        alarm.set_alarm_field('告警口径', '用户和专场看是否有重复项')
        alarm.set_alarm_field('告警作业所在空间', 'yishou_daily')
        alarm.set_alarm_field('告警作业名称', '赛马因子_曝光数据打压')
        alarm.set_alarm_field('告警级别', 'P0')
        alarm.set_alarm_field('运行日期', str(datetime.now())[:19])
        alarm.set_alarm_field('后端计算时间', '最晚10点前')
        alarm.set_alarm_field('主键重复数量', result[0][0])
        alarm.set_alarm_field('与昨天相差比例', f'{round(float(result[0][1]) * 100, 6)}%')
        alarm.set_alarm_field('数据条数', result[0][2])
        alarm.set_alarm_field('用户和专场重复数', result[0][3])
        alarm.set_alarm_field('最大打压专场数', result[0][4])
        alarm.set_alarm_field('最小打压专场数', result[0][5])
        # 设置发送飞书群（大数据P0P1级重点业务告警）
        alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/cbf88ed2-a19a-4a11-af26-a6bd9da70181')
        alarm.send_to_feishu(alarm.build_alarm())
        # 设置第二个告警群url -- 卖货-频道运营监控
        alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/3cd84980-0bcc-462c-8a1c-d33349c7a99b')
        alarm.send_to_feishu(alarm.build_alarm())
        # 抛出异常,阻断程序
        raise Exception("告警:用户和专场看有重复项,阻断程序")
    # 正常情况
    else:
        # 设置告警颜色
        alarm.set_color('green')
        # 设置告警字段
        alarm.set_alarm_desc('检查 赛马因子_曝光数据打压 正确')
        alarm.set_alarm_field('告警作业所在空间', 'yishou_daily')
        alarm.set_alarm_field('告警作业名称', '赛马因子_曝光数据打压')
        alarm.set_alarm_field('运行日期', str(datetime.now())[:19])
        alarm.set_alarm_field('告警级别', 'P0')
        alarm.set_alarm_field('后端计算时间', '最晚10点前')
        alarm.set_alarm_field('主键重复数量', result[0][0])
        alarm.set_alarm_field('与昨天相差比例', f'{round(float(result[0][1]) * 100, 6)}%')
        alarm.set_alarm_field('数据条数', result[0][2])
        alarm.set_alarm_field('用户和专场重复数', result[0][3])
        alarm.set_alarm_field('最大打压专场数', result[0][4])
        alarm.set_alarm_field('最小打压专场数', result[0][5])
        # 设置发送飞书群（大数据P0P1级重点业务告警）
        alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/cbf88ed2-a19a-4a11-af26-a6bd9da70181')
        alarm.send_to_feishu(alarm.build_alarm())
        # 设置第二个告警群url -- 卖货-频道运营监控
        alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/3cd84980-0bcc-462c-8a1c-d33349c7a99b')
        alarm.send_to_feishu(alarm.build_alarm())

