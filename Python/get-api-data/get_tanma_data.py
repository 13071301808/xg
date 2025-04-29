# 导入依赖包
import requests
import json
import time
import hashlib
import pandas as pd
from datetime import datetime, timedelta
from obs import ObsClient
import argparse
import csv

# api全局配置
appId = "xxx"
appKey = "xxx"
http_url = "https://account.tanmarket.cn/apiServer"
# obs全局配置
ak = 'TWXI4V3FPVN8RGQY4BUO'
sk = 'hN2Fl8CLHknsi4zNhNkRosfIHx3O0cx3cNspIgJu'
my_server = 'https://obs.cn-south-1.myhuaweicloud.com'
obsClient = ObsClient(access_key_id=ak, secret_access_key=sk, server=my_server)

# pandas显示配置
pd.set_option('display.max_rows', None)  # 设置行数为无限制
pd.set_option('display.max_columns', None)  # 设置列数为无限制
pd.set_option('display.width', 1600)  # 设置列宽
pd.set_option('display.colheader_justify', 'left')

# 获取参数
parser = argparse.ArgumentParser(description='ArgUtils')
# 获取当前日期和时间
now_day = datetime.now().strftime("%Y-%m-%d 00:00:00")
parser.add_argument('-d', type=str, default=now_day, help="作业调度的时间")
args = parser.parse_args()
# 传入的结束
input_time = args.d
# 将日期字符串转换为datetime对象
end = datetime.strptime(input_time, '%Y%m%d')
# 格式化为目标日期字符串格式
end_str = end.strftime('%Y-%m-%d %H:%M:%S')
# 计算前一个月的时间
start = end - timedelta(days=30)
# 转换为字符串格式
start_str = start.strftime('%Y-%m-%d %H:%M:%S')
# obs存储时间
obs_time = end_str[0:7]

print("原始日期：", end_str)
print("前一个月的时间：", start_str)
print('存obs的时间：', obs_time)


# 查询字段一手用户ID的id
def yishou_fieldID():
    url = f"{http_url}/api/v3/profile-fields"
    payload = ""
    # 计算签名体
    # 调用配置参数
    timestamp = int(time.time() * 1000)
    # 将数据连接为待签名文本并使用UTF-8编码
    sign_text = f'{appId}{timestamp}{payload}{appKey}'.encode('utf-8')
    # 计算SHA256摘要值
    sign = hashlib.sha256(sign_text).hexdigest()
    headers = {
        'appId': appId,
        'timestamp': str(timestamp),
        'sign': sign,
        'appKey': appKey,
        'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    response_data = json.loads(response.text)
    yishou_field_id = response_data['data'][0]['id']
    print('字段一手用户ID的ID：', yishou_field_id)
    return yishou_field_id


# 获取总页数
def get_totalPage():
    # 调用配置参数
    timestamp = int(time.time() * 1000)
    url = f"{http_url}/api/v3/customer/search"
    payload = json.dumps({
        "conditions": {
            "conditionOp": 1,
            "conditionItems": [
                {
                    "name": str(yishou_field_id),
                    "op": 5,
                    "values": [
                        "一手用户ID"
                    ]
                }
            ]
        },
        "createTime": {
            "start": start_str,
            "end": end_str
        },
        "pageNo": 1,
        "pageSize": 100
    })
    sign_text = f'{appId}{timestamp}{payload}{appKey}'.encode('utf-8')
    sign = hashlib.sha256(sign_text).hexdigest()
    headers = {
        'appId': appId,
        'timestamp': str(timestamp),
        'sign': sign,
        'appKey': appKey,
        'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    response_data = json.loads(response.text)
    # 查询总页码
    totalPage = response_data['data']['totalPage']
    print('总页码：', totalPage)
    return totalPage


# 获取客户基本信息
def customer_all(yishou_field_id, totalPage):
    try:
        # 页码
        pageNo = 1
        # 每页显示的数量
        pageSize = 100
        # 存放数据的列表
        customer_all_list = []
        url = f"{http_url}/api/v3/customer/search"
        while pageNo <= int(totalPage):
            payload = json.dumps({
                "conditions": {
                    "conditionOp": 1,
                    "conditionItems": [
                        {
                            "name": str(yishou_field_id),
                            # 筛选条件
                            "op": 5,
                            "values": [
                                "一手用户ID"
                            ]
                        }
                    ]
                },
                "createTime": {
                    "start": start_str,
                    "end": end_str
                },
                # 页码
                "pageNo": pageNo,
                # 分页大小
                "pageSize": pageSize
            })
            # 调用配置参数
            timestamp = int(time.time() * 1000)
            sign_text = f'{appId}{timestamp}{payload}{appKey}'.encode('utf-8')
            sign = hashlib.sha256(sign_text).hexdigest()
            headers = {
                'appId': appId,
                'timestamp': str(timestamp),
                'sign': sign,
                'appKey': appKey,
                'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
                'Content-Type': 'application/json'
            }
            response = requests.request("POST", url, headers=headers, data=payload)
            response_data = json.loads(response.text)
            response_data = response_data['data']['data']
            for i in range(len(response_data)):
                # 将所需字段封装起来
                customer_base_info = {}
                # 客户微信名
                if 'wechatNickname' in response_data[i]:
                    customer_base_info['wechatNickname'] = response_data[i]['wechatNickname']
                else:
                    customer_base_info['wechatNickname'] = ''
                # 备注
                if 'qwRemark' in response_data[i]:
                    customer_base_info['qwRemark'] = response_data[i]['qwRemark']
                else:
                    customer_base_info['qwRemark'] = ''
                # 添加好友状态
                if 'isFriend' in response_data[i]:
                    if response_data[i]['isFriend']:
                        customer_base_info['isFriend'] = "已添加"
                    elif response_data[i]['deleteStatus'] == 1:
                        customer_base_info['isFriend'] = "已删除(客户主动删除)"
                    elif response_data[i]['deleteStatus'] == 2:
                        customer_base_info['isFriend'] = "已删除(员工主动删除)"
                    else:
                        customer_base_info['isFriend'] = "已删除"
                else:
                    customer_base_info['isFriend'] = ''
                # 身份类型new
                if '133180' in response_data[i]:
                    customer_base_info['identity_type_new'] = response_data[i]['133180']
                else:
                    customer_base_info['identity_type_new'] = ''
                # 每周的进货节点
                if '103916' in response_data[i]:
                    customer_base_info['week_get_node'] = response_data[i]['103916']
                else:
                    customer_base_info['week_get_node'] = ''
                # 拿大货渠道
                if '104173' in response_data[i]:
                    customer_base_info['get_bighuo_way'] = response_data[i]['104173']
                else:
                    customer_base_info['get_bighuo_way'] = ''
                # 生日
                if response_data[i]['birthday'] is not None:
                    customer_base_info['birthday'] = response_data[i]['birthday']
                elif response_data[i]['birthday'] is None:
                    customer_base_info['birthday'] = ''
                else:
                    customer_base_info['birthday'] = ''
                # 用户拿货风格
                if '108205' in response_data[i]:
                    customer_base_info['user_get_style'] = response_data[i]['108205']
                else:
                    customer_base_info['user_get_style'] = ''
                # 店主客户群体
                if '108204' in response_data[i]:
                    customer_base_info['shopuser_customer'] = response_data[i]['108204']
                else:
                    customer_base_info['shopuser_customer'] = ''
                # 用户经常去的市场
                if '133181' in response_data[i]:
                    customer_base_info['user_often_masket'] = response_data[i]['133181']
                else:
                    customer_base_info['user_often_masket'] = ''
                # 秋大货主要在哪拿
                if '132692' in response_data[i]:
                    customer_base_info['qiudahuo_main_whatget'] = response_data[i]['132692']
                else:
                    customer_base_info['qiudahuo_main_whatget'] = ''
                # 经营年限
                if '97668' in response_data[i]:
                    customer_base_info['operating_years'] = response_data[i]['97668']
                else:
                    customer_base_info['operating_years'] = ''
                # 进货频次
                if '103915' in response_data[i]:
                    customer_base_info['purchase_frequency'] = response_data[i]['103915']
                else:
                    customer_base_info['purchase_frequency'] = ''
                # 喜欢一手的权益？
                if '129809' in response_data[i]:
                    customer_base_info['like_yishou_what'] = response_data[i]['129809']
                else:
                    customer_base_info['like_yishou_what'] = ''
                # 不能对她做什么！
                if '132694' in response_data[i]:
                    customer_base_info['not_do'] = response_data[i]['132694']
                else:
                    customer_base_info['not_do'] = ''
                # 一手用户ID
                customer_base_info['yishou_field_id'] = response_data[i][str(yishou_field_id)]
                # 用户层级
                if '126299' in response_data[i]:
                    customer_base_info['user_lavel'] = response_data[i]['126299']
                else:
                    customer_base_info['user_lavel'] = ''
                # 员工发送消息数(1年内)
                if 'sendChatCount' in response_data[i]:
                    customer_base_info['yuangong_send_count'] = response_data[i]['sendChatCount']
                else:
                    customer_base_info['yuangong_send_count'] = 0
                # 客户回复消息数
                if 'replyChatCount' in response_data[i]:
                    customer_base_info['customer_reply_count'] = response_data[i]['replyChatCount']
                else:
                    customer_base_info['customer_reply_count'] = 0
                # 春季店铺进货金额(1年内)
                if '129507' in response_data[i]:
                    customer_base_info['cunji_shop_inhuo_money'] = response_data[i]['129507']
                else:
                    customer_base_info['cunji_shop_inhuo_money'] = ''
                # 一手渗透率2
                if '129743' in response_data[i]:
                    customer_base_info['yishou_shentou'] = response_data[i]['129743']
                else:
                    customer_base_info['yishou_shentou'] = ''
                # 协作人
                if response_data[i]['shareFollows']:
                    customer_base_info['salesName'] = response_data[i]['shareFollows']
                else:
                    customer_base_info['salesName'] = ''
                # 协作人所属部门
                if response_data[i]['shareFollows']:
                    customer_base_info['sales_followDeptNames'] = response_data[i]['shareFollows']
                else:
                    customer_base_info['sales_followDeptNames'] = ''
                # 客户标签
                if response_data[i]['tags']:
                    customer_base_info['tags'] = response_data[i]['tags']
                else:
                    customer_base_info['tags'] = ''
                # 内容标签
                if response_data[i]['contentTags']:
                    customer_base_info['contentTags'] = response_data[i]['contentTags']
                else:
                    customer_base_info['contentTags'] = ''
                # 创建时间
                if 'createTime' in response_data[i]:
                    customer_base_info['createtime'] = response_data[i]['createTime']
                else:
                    customer_base_info['createtime'] = ''
                # 分配时间
                if 'getCreateTime' in response_data[i]:
                    customer_base_info['fenpei_time'] = response_data[i]['getCreateTime']
                else:
                    customer_base_info['fenpei_time'] = ''
                # 负责人
                if 'mainFollows' in response_data[i]:
                    customer_base_info['mainFollowUserName'] = response_data[i]['mainFollows'][0]['salesName']
                else:
                    customer_base_info['mainFollowUserName'] = ''
                # 负责人所属部门
                if 'mainFollows' in response_data[i]:
                    customer_base_info['followDeptNames'] = response_data[i]['mainFollows'][0]['followDeptNames'][2:-2]
                else:
                    customer_base_info['followDeptNames'] = ''
                # 上次跟进时间
                if 'qwLastFollowTime' in response_data[i]:
                    customer_base_info['qwLastFollowTime'] = response_data[i]['qwLastFollowTime']
                else:
                    customer_base_info['qwLastFollowTime'] = ''
                # 添加渠道码
                if 'state' in response_data[i]:
                    if response_data[i]['state'] is not None:
                        customer_base_info['state'] = response_data[i]['state']
                    else:
                        customer_base_info['state'] = ''
                else:
                    customer_base_info['state'] = ''
                # 好友添加时间
                if 'contactList' in response_data[i]:
                    customer_base_info['qwaddtime'] = response_data[i]['contactList'][9]['qwCustomerTime']
                else:
                    customer_base_info['qwaddtime'] = ''
                # 所在群聊
                if len(response_data[i]['groupList']) >= 1:
                    customer_base_info['stay_qun'] = response_data[i]['groupList'][0]['chatName']
                else:
                    customer_base_info['stay_qun'] = 0
                # 签到拜访
                if 'signNum' in response_data[i]:
                    customer_base_info['signNum'] = response_data[i]['signNum']
                else:
                    customer_base_info['signNum'] = ''
                # 积分
                if response_data[i]['pointCredit']:
                    customer_base_info['pointCredit'] = response_data[i]['pointCredit']
                else:
                    customer_base_info['pointCredit'] = ''
                # 评分
                if response_data[i]['pointScore']:
                    customer_base_info['pointScore'] = response_data[i]['pointScore']
                else:
                    customer_base_info['pointScore'] = ''
                # 用户净值(id:97676)
                if '97676' in response_data[i]:
                    customer_base_info['user_net_worth'] = response_data[i]['97676']
                else:
                    customer_base_info['user_net_worth'] = ''
                # 用户敏感(id:108200)
                if '108200' in response_data[i]:
                    customer_base_info['user_tip'] = response_data[i]['108200']
                else:
                    customer_base_info['user_tip'] = ''
                # 特殊需求(id:108208)
                if '108208' in response_data[i]:
                    customer_base_info['tebie_need'] = response_data[i]['108208']
                else:
                    customer_base_info['tebie_need'] = ''
                # 本月实际拿货额(id:97675)
                if '97675' in response_data[i]:
                    customer_base_info['month_actual_expenses'] = response_data[i]['97675']
                else:
                    customer_base_info['month_actual_expenses'] = ''
                # 最后下单日期(id:97672)
                if '97672' in response_data[i]:
                    customer_base_info['last_order_time'] = response_data[i]['97672']
                else:
                    customer_base_info['last_order_time'] = ''
                # 享受的服务(id:97673)
                if '97673' in response_data[i]:
                    customer_base_info['user_have_service'] = response_data[i]['97673']
                else:
                    customer_base_info['user_have_service'] = ''
                # 注册一手时间(id:97670)
                if '97670' in response_data[i]:
                    customer_base_info['region_time'] = response_data[i]['97670']
                else:
                    customer_base_info['region_time'] = ''
                # 主要拿货渠道原因(id:104850)
                if '104850' in response_data[i]:
                    customer_base_info['main_getway_why'] = response_data[i]['104850']
                else:
                    customer_base_info['main_getway_why'] = ''
                # 描述(id:91637)
                if '91637' in response_data[i]:
                    customer_base_info['description'] = response_data[i]['91637']
                else:
                    customer_base_info['description'] = ''
                # 所在城市(id:97666)
                if '97666' in response_data[i]:
                    customer_base_info['city'] = response_data[i]['97666']
                else:
                    customer_base_info['city'] = ''
                # 客户意向(id:91639)
                if '91639' in response_data[i]:
                    customer_base_info['purchase_intention'] = response_data[i]['91639']
                else:
                    customer_base_info['purchase_intention'] = ''
                # 性别(id:91641)
                if '91641' in response_data[i]:
                    if response_data[i]['91641'] == 1:
                        customer_base_info['sex'] = "男"
                    elif response_data[i]['91641'] == 2:
                        customer_base_info['sex'] = "女"
                    else:
                        customer_base_info['sex'] = "其它"
                else:
                    customer_base_info['sex'] = '其它'
                # 电话(id:91635)
                if response_data[i]['91635']:
                    customer_base_info['mobile'] = response_data[i]['91635']
                else:
                    customer_base_info['mobile'] = ''
                # 客户阶段
                if 'salesStageName' in response_data[i]:
                    customer_base_info['salesStageName'] = response_data[i]['salesStageName']
                else:
                    customer_base_info['salesStageName'] = ''
                # 企微用户ID
                if 'externalUserId' in response_data[i]:
                    customer_base_info['externalUserId'] = response_data[i]['externalUserId']
                else:
                    customer_base_info['externalUserId'] = ''
                # 公众号授权unionId
                if 'unionId' in response_data[i]:
                    customer_base_info['unionId'] = response_data[i]['unionId']
                else:
                    customer_base_info['unionId'] = ''
                customer_all_list.append(customer_base_info)
                # 企微销售id
                if 'qwSalesId' in response_data[i]:
                    customer_base_info['qwSalesId'] = response_data[i]['qwSalesId']
                else:
                    customer_base_info['qwSalesId'] = ''
                customer_all_list.append(customer_base_info)
            pageNo = pageNo + 1
        print('当前页码：', pageNo - 1)
    except Exception as e:
        print(f"获取数据发生错误：{str(e)}")
    return customer_all_list


# 检测csv文件有无异常，进行修复
def fix_csv_file(file_path):
    # 修复 CSV 文件
    with open(file_path, 'r', encoding='utf-8-sig') as file:
        csv_reader = csv.reader(file)
        lines = [[cell.strip() for cell in row] for row in csv_reader]
    # 修复未关闭引号
    for i, line in enumerate(lines):
        if line[-1].startswith('[') and not line[-1].endswith(']'):
            next_line = lines[i + 1]
            if len(next_line) > 0:
                next_line[0] = line[-1] + next_line[0]
                del line[-1]
                lines[i + 1] = next_line
    # 将单元格中的换行符替换为斜杠
    for row in lines:
        for i in range(len(row)):
            row[i] = row[i].replace('\n', '/')
    # 将修复后的内容写回CSV文件
    with open(file_path, 'w', encoding='utf-8-sig', newline='') as file:
        csv_writer = csv.writer(file)
        csv_writer.writerows(lines)


# 上传文件到obs上
def save_result(customer_all_list):
    """
    结果输出csv，并上传到obs
    """
    # 将数据转为dataframe
    df = pd.DataFrame(customer_all_list, columns=list(customer_all_list[0].keys()))
    row_count = df.shape[0]
    print("行数：", row_count)
    # 对整个行进行去重
    customer_all_pd = df.drop_duplicates(
        ['wechatNickname', 'qwRemark', 'yishou_field_id', 'yuangong_send_count', 'customer_reply_count', 'createtime',
         'fenpei_time', 'mainFollowUserName', 'followDeptNames', 'qwLastFollowTime', 'state', 'salesStageName',
         'externalUserId', 'unionId', 'qwSalesId'])
    dup_row_count = customer_all_pd.shape[0]
    print("去重行数：", dup_row_count)
    # 堡垒机的路径
    file_path = fr"/data/project/test/tanma/{obs_time}tanma.csv"
    # 转csv文件到堡垒机
    try:
        customer_all_pd.to_csv(file_path, index=False, header=False, encoding='utf8')
    except Exception as e:
        print('寄:', e)
    # 检测csv文件有无异常，进行修复
    fix_csv_file(file_path)
    # 对象名，即上传后的文件名(日期目录+当日文件名)
    objectKey = "yishou_data.db/all_fmys_tanma_customer_mt/mt=%s/" % obs_time + obs_time + ".csv"
    # obsClient.putFile是追加上传而不是覆盖
    up_resp = obsClient.putFile("yishou-bigdata", objectKey, file_path)
    # 检查写入华为obs是否已成功
    if up_resp.status < 300:
        print('obs没问题')
    else:
        print('没写到obs')
    return file_path


if __name__ == '__main__':
    # 获取一手用户ID
    yishou_field_id = yishou_fieldID()
    # 获取总页码
    totalPage = get_totalPage()
    # 获取所有客户数据
    customer_all_list = customer_all(yishou_field_id, totalPage)
    # 存储到OBS
    save_result(customer_all_list)
