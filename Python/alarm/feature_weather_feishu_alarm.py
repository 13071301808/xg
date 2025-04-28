import json
import requests
import urllib3
import warnings
import os
import time
from datetime import datetime, timedelta
from yssdk.dli.sql_client import YsDliSQLCLient
from pyecharts.charts import Line
from pyecharts import options as opts
from pyecharts.render import make_snapshot
from snapshot_selenium import snapshot as driver

# 全局配置参数
# 添加忽略网络安全提示
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", category=RuntimeWarning)
# 获取日期
one_day_ago = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
last_15day_ago = (datetime.now() - timedelta(days=15)).strftime("%Y%m%d")
client = YsDliSQLCLient(queue='analyst')

# 未来最高温sql
sql1 = f''' 
   SELECT 
        case 
            when city='沈阳市' then '沈阳'
            when city='北京市' then '北京'
            when city='郑州市' then '郑州'
            when city='广州市' then '广州'
            when city='昆明市' then '昆明'
            when city='杭州市' then '杭州'
        end as 城市, 
        d_high_1 as 当日气温, 
        d_high_2 as 未来1天, 
        d_high_3 as 未来2天, 
        d_high_4 as 未来3天, 
        d_high_5 as 未来4天, 
        d_high_6 as 未来5天, 
        d_high_7 as 未来6天, 
        d_high_8 as 未来7天, 
        d_high_9 as 未来8天, 
        d_high_10 as 未来9天, 
        d_high_11 as 未来10天, 
        d_high_12 as 未来11天, 
        d_high_13 as 未来12天, 
        d_high_14 as 未来13天, 
        d_high_15 as 未来14天
    from yishou_daily.dws_cross_origin_daily_new_weather_forecast_tb
    where dt = {one_day_ago} and city in ('沈阳市','北京市','郑州市','广州市','昆明市','杭州市')
    ;
'''
# 未来最低温sql
sql2 = f''' 
   SELECT   
        case 
            when city='沈阳市' then '沈阳'
            when city='北京市' then '北京'
            when city='郑州市' then '郑州'
            when city='广州市' then '广州'
            when city='昆明市' then '昆明'
            when city='杭州市' then '杭州'
        end as 城市, 
        d_low_1 as 当日气温, 
        d_low_2 as 未来1天, 
        d_low_3 as 未来2天, 
        d_low_4 as 未来3天, 
        d_low_5 as 未来4天, 
        d_low_6 as 未来5天, 
        d_low_7 as 未来6天, 
        d_low_8 as 未来7天, 
        d_low_9 as 未来8天, 
        d_low_10 as 未来9天, 
        d_low_11 as 未来10天, 
        d_low_12 as 未来11天, 
        d_low_13 as 未来12天, 
        d_low_14 as 未来13天, 
        d_low_15 as 未来14天
    from yishou_daily.dws_cross_origin_daily_new_weather_forecast_tb
    where dt = {one_day_ago} and city in ('沈阳市','北京市','郑州市','广州市','昆明市','杭州市')
'''


class Alarm_XG:
    def __init__(self, alarm_title):
        self.is_short = False
        self.tag = "lark_md"
        self.alarm_title = alarm_title
        self.headers = {
            'Authorization': 'Bearer ' + get_authorization(),
            'Content-Type': 'application/json'
        }
        self.alarm_content = []
        self.alarm_url = 'https://open.feishu.cn/open-apis/bot/v2/hook/294cf0df-3150-410e-b25d-6932f0cf38df'
        self.color = "red"

    # 告警json数据汇总
    def generate_alarm_message(self, alarm_field_message):
        return {
            "msg_type": "interactive",
            "card": {
                "config": {
                    "wide_screen_mode": True
                },
                "elements": [
                    {
                        "tag": "img",
                        "img_key": image_keys[0],
                        "alt": {
                            "tag": "plain_text",
                            "content": "未来最高温趋势图"
                        },
                        "mode": "fit_horizontal",
                        "preview": True
                    },
                    {
                        "tag": "img",
                        "img_key": image_keys[1],
                        "alt": {
                            "tag": "plain_text",
                            "content": "未来最低温趋势图"
                        },
                        "mode": "fit_horizontal",
                        "preview": True
                    },
                    {
                        "tag": "hr"
                    },
                    {
                        "fields": alarm_field_message,
                        "tag": "div"
                    }
                ],
                "header": {
                    "template": self.color,
                    "title": {
                        "content": self.alarm_title,
                        "tag": "plain_text"
                    }
                }
            }
        }

    # 设置飞书告警url
    def set_alarm_url(self, url):
        self.alarm_url = url

    # 设置飞书模板颜色
    def set_color(self, color):
        self.color = color

    # 生成告警字段的信息
    def get_alarm_field_info(self, field_name, field_value):
        field_info = {
            "is_short": self.is_short,
            "text": {
                "content": "**%s**: %s" % (field_name, field_value),
                "tag": self.tag
            }
        }
        return field_info

    # 设置告警字段的信息
    def set_alarm_field(self, field_name, field_value):
        field_info = self.get_alarm_field_info(field_name, field_value)
        self.alarm_content.append(field_info)

    # 生成告警信息
    def build_alarm(self):
        return self.generate_alarm_message(self.alarm_content)

    # 发送到飞书
    def send_feishu(self, alarm_message):
        print(str(alarm_message))
        response = requests.post(
            url=self.alarm_url,
            data=json.dumps(alarm_message),
            headers=self.headers,
            verify=False
        )
        return response


# 自动获取最新的token
def get_authorization():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = json.dumps({
        "app_id": "cli_a46f1952bb79d00c",
        "app_secret": "s9xm9RJ7KAdxlKMIzBZ0ofWYpsdG3Wao"
    })
    headers = {
        'Content-Type': 'application/json;charset=utf-8'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    response_data = json.loads(response.text)
    Authorization = response_data['tenant_access_token']
    return Authorization


# 制作城市维度的最高温图片
def made_tu_weather_max() -> Line:
    try:
        # 执行sql语句
        client.exec_sql(sql1)
        # 获取全部结果转为dataframe形式数据
        result_weather_max = client.fetch_all_dataframe()
        # 提取发货地为广州的数据
        line = Line(init_opts=opts.InitOpts(width="1600px", height="800px"))
        line.add_xaxis(result_weather_max.columns[1:].tolist())
        cities = result_weather_max['城市'].unique().tolist()
        # 定义多种颜色
        colors = ["purple", "blue", "green", "orange", "red", "black"]
        i = 0
        for city in cities:
            temperatures = result_weather_max[result_weather_max['城市'] == city].iloc[:, 1:].values.tolist()[0]
            line.add_yaxis(city, temperatures, itemstyle_opts=opts.ItemStyleOpts(color=colors[i]))
            i = i + 1
        # 折线图配置
        line.set_global_opts(
            xaxis_opts=opts.AxisOpts(
                name='日期',
                axislabel_opts=opts.LabelOpts(rotate=-45,font_family="微软雅黑")
            ),
            yaxis_opts=opts.AxisOpts(
                name='温度(℃)',
                axislabel_opts=opts.LabelOpts(rotate=-0,font_family="微软雅黑")
            )
        )
        line_render = line.render("line_weather_max.html")
        # 添加睡眠缓冲
        time.sleep(2)
        # 将网页转换为图片
        weather_max_path = "/data/project/test/weather/img/line_weather_max.png"
        # 如果图片文件已存在，删除旧文件
        if os.path.exists(weather_max_path):
            print('图片已存在，清除旧文件')
            os.remove(weather_max_path)
        make_snapshot(driver, line_render, weather_max_path)
        return line
    except Exception as e:
        print(e)


# 制作城市维度的最低温图片
def made_tu_weather_min(result_weather_min) -> Line:
    try:
        # 提取发货地为广州的数据
        line1 = Line(init_opts=opts.InitOpts(width="1600px", height="800px"))
        # 筛选满足条件的行
        line1.add_xaxis(result_weather_min.columns[1:].tolist())
        cities = result_weather_min['城市'].unique().tolist()
        # 定义多种颜色
        colors = ["purple", "blue", "green", "orange", "red", "black"]
        i = 0
        for city in cities:
            temperatures = result_weather_min[result_weather_min['城市'] == city].iloc[:, 1:].values.tolist()[0]
            line1.add_yaxis(city, temperatures,itemstyle_opts=opts.ItemStyleOpts(color=colors[i]))
            i = i + 1
        # 折线图配置
        line1.set_global_opts(
            xaxis_opts=opts.AxisOpts(
                name='日期',
                axislabel_opts=opts.LabelOpts(rotate=-45,font_family="微软雅黑")
            ),
            yaxis_opts=opts.AxisOpts(
                name='温度(℃)',
                axislabel_opts=opts.LabelOpts(rotate=-0,font_family="微软雅黑")
            )
        )
        line1_render = line1.render("line_weather_min.html")
        # 添加睡眠缓冲
        time.sleep(2)
        # 将图表保存为图片
        weather_min_path = "/data/project/test/weather/img/line_weather_min.png"
        # 如果图片文件已存在，删除旧文件
        if os.path.exists(weather_min_path):
            print('图片已存在，清除旧文件')
            os.remove(weather_min_path)
        make_snapshot(driver, line1_render, weather_min_path)
        return line1
    except Exception as e:
        print(e)


# 设置未来气温比较输出提示
def abs_maxmin(result_weather):
    result_lists = result_weather.values.tolist()
    question_name = []
    for result_list in result_lists:
        for i in range(2, 8):
            # 提取每一位与前一位的绝对差值
            difference = abs(float(result_list[i]) - float(result_list[i - 1]))
            if difference >= 5:
                question_name.append(result_list[0])
    questions = list(set(question_name))
    # 添加文字提示
    for q_city in questions:
        alarm.set_alarm_field(
            '温馨提示',
            f'\n未来15天,{q_city}未来7天最低气温单日降幅达≥5°,请前置仓团队注意备货品类和比例,履约团队注意配货率波动,采购团队注意市场变化'
        )


# 上传图片
def upload_img():
    url = "https://open.feishu.cn/open-apis/im/v1/images"
    headers = {
        'Authorization': 'Bearer ' + get_authorization()
    }
    payload = {'image_type': 'message'}
    image_list = ['line_weather_max.png', 'line_weather_min.png']
    image_keys = []
    for img in image_list:
        files = [
            ('image',
             (img, open(f'/data/project/test/weather/img/{img}', 'rb'), 'application/json'))
        ]
        response = requests.request("POST", url, headers=headers, data=payload, files=files)
        response_data = json.loads(response.text)
        image_key = response_data['data']['image_key']
        print(image_key)
        image_keys.append(image_key)
    return image_keys


if __name__ == '__main__':
    # 获取app权限
    get_authorization()
    line = made_tu_weather_max()
    # 执行sql语句
    client.exec_sql(sql2)
    # 获取全部结果转为dataframe形式数据
    result_weather_min = client.fetch_all_dataframe()
    # 制作气温图
    line1 = made_tu_weather_min(result_weather_min)
    # 上传所有图片
    image_keys = upload_img()
    # 开启告警类，设置标题
    alarm = Alarm_XG(alarm_title='未来气温监控')
    # 设置告警颜色
    alarm.set_color('green')
    # 添加文字提示
    alarm.set_alarm_field('注', '以上两张图分别是未来最高温趋势图和未来最低温趋势图')
    # 设置未来气温比较输出提示
    abs_maxmin(result_weather_min)
    alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/e49bcaa3-597c-452b-b803-cc7d62cf2276')
    alarm.send_feishu(alarm.build_alarm())
