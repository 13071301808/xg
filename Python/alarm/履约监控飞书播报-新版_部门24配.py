## PYTHON
## ******************************************************************** ##
## author: chenzhigao
## create time: 2025/05/07 10:40:37 GMT+08:00
## ******************************************************************** ##

# 导入依赖包
import urllib3
import textwrap
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from yssdk.dli.sql_client import YsDliSQLCLient
from yssdk.common.alarm import Alarm
from matplotlib.font_manager import *
from datetime import datetime, timedelta

# 全局配置参数
# 添加忽略网络安全提示
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
pd.set_option('display.colheader_justify', 'center')
pd.set_option('display.max_rows', None)  # 设置行数为无限制
pd.set_option('display.max_columns', 6)  # 设置列数为无限制
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('display.width', 60)  # 设置打印宽度(**重要**)

this_hour = (datetime.now()).strftime("%H")
this_day = (datetime.now()).strftime("%Y%m%d")
one_day_ago = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

# 检测sql
sql1 = f''' 
select 
    需求周期
    , 商运或买手
    , 供应商id_合并
    , 供应商名称
    , concat(round(商家24小时履约率 * 100,4),'%') as 商家24小时履约率
from (
    select * from (
        select 
            需求周期
            ,供给部门_合并
            ,商运或买手
            ,供应商id_合并
            ,供应商名称
            ,订货单计划代采 + 前置仓发货 as 商家采购金额
            ,round((开单h24小时履约金额_剔除大单+前置仓h24小时履约金额_剔除大单) / (订货单计划代采_剔除大单+前置仓发货_剔除大单),4) as 商家24小时履约率
        from (
            select 
                concat(
                    to_char(dateadd(to_date1({one_day_ago},'yyyymmdd'),-2,'dd'),'yyyymmdd'),
                    '-',
                    {one_day_ago}
                )as 需求周期
                ,供给部门_合并
                ,商运或买手
                ,供应商id_合并
                ,供应商名称
                ,sum(订货单计划代采) as 订货单计划代采
                ,sum(前置仓发货) as 前置仓发货
                ,sum(前置仓h24小时履约金额) as 前置仓h24小时履约金额
                ,sum(开单h24小时履约金额) as 开单h24小时履约金额
                ,sum(订货单计划代采_剔除大单) as 订货单计划代采_剔除大单
                ,sum(前置仓发货_剔除大单) as 前置仓发货_剔除大单
                ,sum(前置仓h24小时履约金额_剔除大单) as 前置仓h24小时履约金额_剔除大单
                ,sum(开单h24小时履约金额_剔除大单) as 开单h24小时履约金额_剔除大单
            from yishou_daily.temp_realtime_new_supply_allot_alarm
            where 供给部门_合并 = '1十三行-许愿' 
            and to_char(需求日期,'yyyymmdd') between to_char(dateadd(to_date1({one_day_ago},'yyyymmdd'),-2,'dd'),'yyyymmdd') and {one_day_ago}
            group by 1,2,3,4,5
        )
    )
    order by 商家采购金额 desc 
    limit 100
)
where 商家24小时履约率 < 0.55
;
'''
sql2 = f''' 
select 
    需求周期
    , 商运或买手
    , 供应商id_合并
    , 供应商名称
    , concat(round(商家24小时履约率 * 100,4),'%') as 商家24小时履约率
from (
    select * from (
        select 
            需求周期
            ,供给部门_合并
            ,商运或买手
            ,供应商id_合并
            ,供应商名称
            ,订货单计划代采 + 前置仓发货 as 商家采购金额
            ,round((开单h24小时履约金额_剔除大单+前置仓h24小时履约金额_剔除大单) / (订货单计划代采_剔除大单+前置仓发货_剔除大单),4) as 商家24小时履约率
        from (
            select 
                concat(
                    to_char(dateadd(to_date1({one_day_ago},'yyyymmdd'),-6,'dd'),'yyyymmdd'),
                    '-',
                    {one_day_ago}
                )as 需求周期
                ,供给部门_合并
                ,商运或买手
                ,供应商id_合并
                ,供应商名称
                ,sum(订货单计划代采) as 订货单计划代采
                ,sum(前置仓发货) as 前置仓发货
                ,sum(前置仓h24小时履约金额) as 前置仓h24小时履约金额
                ,sum(开单h24小时履约金额) as 开单h24小时履约金额
                ,sum(订货单计划代采_剔除大单) as 订货单计划代采_剔除大单
                ,sum(前置仓发货_剔除大单) as 前置仓发货_剔除大单
                ,sum(前置仓h24小时履约金额_剔除大单) as 前置仓h24小时履约金额_剔除大单
                ,sum(开单h24小时履约金额_剔除大单) as 开单h24小时履约金额_剔除大单
            from yishou_daily.temp_realtime_new_supply_allot_alarm
            where 供给部门_合并 = '1十三行-许愿' 
            and to_char(需求日期,'yyyymmdd') between to_char(dateadd(to_date1({one_day_ago},'yyyymmdd'),-6,'dd'),'yyyymmdd') and {one_day_ago}
            group by 1,2,3,4,5
        )
    )
    order by 商家采购金额 desc 
    limit 100
)
where 商家24小时履约率 < 0.6
;
'''

sql3 = f''' 
select 
    distinct a.nickname
    ,a.realname
    ,c.商运或买手 as flower_name
    ,a.user_id
from yishou_data.all_fmys_feishu_user_test a 
left join (
    select 
        供给部门
        ,花名
        ,`商运/买手` as 商运或买手
        , 组别
    from yishou_daily.ods_flower_name_list_excel
) b on a.nickname = b.花名
left join yishou_daily.temp_realtime_new_supply_allot_alarm c on c.商运或买手 = b.商运或买手
where b.花名 is not null and c.商运或买手 is not null
;
'''

# 设置全局字体路径
font_path = '/usr/share/fonts/wqy-microhei/wqy-microhei.ttc'
# 加载字体
custom_font = fm.FontProperties(fname=font_path)
# 设置全局字体
plt.rcParams['font.family'] = custom_font.get_name()


# 将数据转为表格图片
def made_tu_duanhuo(data_duanhuo):
    # 创建表格图片
    fig, ax = plt.subplots(figsize=(28, 20))
    ax.axis('off')  # 隐藏坐标轴
    # 处理列名换行
    columns = [textwrap.fill(col, width=11) for col in data_duanhuo.columns]
    table = ax.table(
        cellText=data_duanhuo.values, colLabels=columns,
        loc='center', cellLoc='center',
        colColours=['lightgray'] * len(columns)
    )

    print(f'列名:{columns}')
    # 调整表格样式
    table.auto_set_font_size(False)
    table.set_fontsize(18)
    table.scale(1, 2.4)
    # 调整单元格宽度
    for key, cell in table.get_celld().items():
        cell.set_width(0.24)
        cell.set_edgecolor('#dddddd')
    sum_jiankong_path = '/apps/data/project/tmp/ly_alarm_department_13hang_xuyuan_top100_allot24h_3day.png'
    # 如果图片文件已存在，删除旧文件
    if os.path.exists(sum_jiankong_path):
        print('图片已存在，清除旧文件')
        os.remove(sum_jiankong_path)
    # 保存表格图片
    plt.savefig(sum_jiankong_path)


# 将数据转为表格图片
def made_tu_ly(data_ly):
    # 创建表格图片
    fig, ax = plt.subplots(figsize=(28, 20))
    ax.axis('off')
    # 处理列名换行
    columns = [textwrap.fill(col, width=11) for col in data_ly.columns]
    table = ax.table(
        cellText=data_ly.values, colLabels=columns,
        loc='center', cellLoc='center',
        colColours=['lightgray'] * len(columns)
    )

    print(f'列名:{columns}')
    # 调整表格样式
    table.auto_set_font_size(False)
    table.set_fontsize(18)
    table.scale(1, 2.4)
    # 调整单元格宽度
    for key, cell in table.get_celld().items():
        cell.set_width(0.24)  # 设置单元格宽度
        cell.set_edgecolor('#dddddd')  # 设置边框颜色
    sum_jiankong_path = '/apps/data/project/tmp/ly_alarm_department_13hang_xuyuan_top100_allot24h_7day.png'
    # 如果图片文件已存在，删除旧文件
    if os.path.exists(sum_jiankong_path):
        print('图片已存在，清除旧文件')
        os.remove(sum_jiankong_path)
    # 保存表格图片
    plt.savefig(sum_jiankong_path)


if __name__ == '__main__':
    # 建立数据连接
    client = YsDliSQLCLient(queue='develop', database='yishou_daily')

    # 执行sql语句
    client.exec_sql(sql1)
    # 获取全部结果转为dataframe形式数据
    data_duanhuo = client.fetch_all_dataframe()
    # 将 NaN 替换成空字符串
    data_duanhuo = data_duanhuo.fillna('')

    # 执行sql语句
    client.exec_sql(sql2)
    # 获取全部结果转为dataframe形式数据
    data_ly = client.fetch_all_dataframe()
    # 将 NaN 替换成空字符串
    data_ly = data_ly.fillna('')

    # 执行sql3
    client.exec_sql(sql3)
    data_flower = client.fetch_all_dataframe()

    # 合并匹配
    merged_df = pd.merge(data_duanhuo, data_flower, left_on='商运或买手', right_on='flower_name', how='inner')
    # 提取并转换为逗号分隔，去重
    duanhuo_flower_ids = ','.join(set(merged_df['user_id'].tolist()))

    # 合并匹配
    merged_df_2 = pd.merge(data_ly, data_flower, left_on='商运或买手', right_on='flower_name', how='inner')
    # 提取并转换为逗号分隔，去重
    ly_flower_ids = ','.join(set(merged_df_2['user_id'].tolist()))

    # 合并输出
    duanhuo_flower_ids_set = set(duanhuo_flower_ids.split(','))
    ly_flower_ids_set = set(ly_flower_ids.split(','))
    merged_flower_ids_set = duanhuo_flower_ids_set.union(ly_flower_ids_set)
    merged_flower_ids = ','.join(merged_flower_ids_set)

    # 制作图片
    made_tu_ly(data_ly)
    made_tu_duanhuo(data_duanhuo)

    # 开启告警类，设置标题
    alarm = Alarm(alarm_title='商家24配预警')

    # 上传所有图片
    img_list = [
        '/apps/data/project/tmp/ly_alarm_department_13hang_xuyuan_top100_allot24h_3day.png',
        '/apps/data/project/tmp/ly_alarm_department_13hang_xuyuan_top100_allot24h_7day.png'
    ]
    image_keys = alarm.upload_img(img_list)
    alarm.set_alarm_img(image_keys[0], 'TOP100-近3天24配预警商家')
    alarm.set_alarm_img(image_keys[1], 'TOP100-近7天24配风险商家')
    alarm.set_at(merged_flower_ids, "相关业务")
    alarm.set_color('green')
    alarm.set_alarm_field('当前时间点', this_hour)
    alarm.set_alarm_field("上图名称", "TOP100近3天低于55%预警商家")
    alarm.set_alarm_field("下图名称", "TOP100近7天低于60%风险商家")
    alarm.set_alarm_field("部门", "十三行-许愿")

    # 测试
    # alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/294cf0df-3150-410e-b25d-6932f0cf38df')
    # alarm.send_to_feishu(alarm.build_alarm())
    # alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/3bfbc233-5947-4762-9dac-9834485e6e6d')
    # alarm.send_feishu(alarm.build_alarm())
    alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/98a9e702-3e89-4b09-bd3a-9f82dd6d0d03')
    alarm.send_feishu(alarm.build_alarm())
