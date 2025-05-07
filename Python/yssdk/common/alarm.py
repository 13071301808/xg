# coding=utf-8
import json
import requests
from yssdk.common.printter import Printter
from yssdk.constant.alarm_url import url


class Alarm:
    """
    用于发送飞书告警信息
    """

    def __init__(self, alarm_title, alarm_group='prod'):
        self.is_short = False
        self.tag = "lark_md"
        self.alarm_title = alarm_title
        self.headers = {
            'Authorization': 'Bearer ' + self.get_authorization(),
            'Content-Type': 'application/json'
        }
        self.alarm_content = []
        self.alarm_url = url[alarm_group]
        self.color = "red"
        self.img_dict = {}
        self.at_dict = {}

    def set_color(self, color):
        self.color = color

    def set_alarm_desc(self, description):
        '''
        设置告警口径的描述 ,
        :param description: 告警口径的描述，string类型
        :return:
        '''
        description = self.get_alarm_desc(description)
        self.alarm_content.append(description)

    def set_alarm_url(self, url):
        '''
        设置飞书告警url
        :param url: 飞书告警url,string类型
        :return:
        '''
        self.alarm_url = url

    def set_alarm_field(self, field_name, field_value):
        '''
        设置告警字段的信息
        :param field_name: 告警字段名称
        :param field_value: 告警字段值
        :return:
        '''
        field_info = self.get_alarm_field_info(field_name, field_value)
        self.alarm_content.append(field_info)

    def build_alarm(self):
        '''
        生成告警信息
        :return: 字符串类型
        '''
        return self.generate_alarm_message(self.alarm_content)

    def get_alarm_desc(self, description):
        """
        生成告警描述信息
        :param description:告警描述信息 ,string类型
        :return: 告警描述信息, 字典类型
        """
        desc_info = {
            "is_short": self.is_short,
            "text": {
                "content": "**告警口径**:%s" % (description),
                "tag": self.tag
            }
        }

        return desc_info

    def get_alarm_field_info(self, field_name, field_value):
        """
        生成告警字段的信息
        :param field_name: 字段名, string类型
        :param field_value: 字段值, string类型
        :return: 告警字段的信息, 字典类型
        """

        field_info = {
            "is_short": self.is_short,
            "text": {
                "content": "**%s**: %s" % (field_name, field_value),
                "tag": self.tag
            }
        }

        return field_info

    def generate_alarm_message(self, alarm_field_message):
        """
        生成告警信息
        :param alarm_field_message: 告警信息
        :return: 告警信息, 字典类型
        """
        elements = []

        if self.img_dict:
            for k, v in self.img_dict.items():
                elements.append({
                    "tag": "img",
                    "img_key": k,
                    "alt": {
                        "tag": "plain_text",
                        "content": v
                    },
                    "mode": "fit_horizontal",
                    "preview": True
                })
            elements.append({
                "tag": "hr"
            })

        if self.at_dict:
            for k, v in self.at_dict.items():
                elements.append({
                    "tag": "markdown",
                    "content": f"{v}:<at ids=\"{k}\"></at>",
                    "text_align": "left",
                    "text_size": "normal"
                })
            elements.append({
                "tag": "hr"
            })

        elements.append({
            "fields": alarm_field_message,
            "tag": "div"
        })

        return {
            "msg_type": "interactive",
            "card": {
                "config": {
                    "wide_screen_mode": True
                },
                "elements": elements,
                "header": {
                    "template": self.color,
                    "title": {
                        "content": self.alarm_title,
                        "tag": "plain_text"
                    }
                }
            }}

    def send_to_feishu(self, alarm_message):
        """
        发送飞书告警信息
        :param alarm_message: 告警信息, 字典类型
        :return:None
        """
        Printter.info("Send alarm message:" + str(alarm_message))
        requests.post(self.alarm_url, data=json.dumps(alarm_message), headers=self.headers, verify=False)

    def init_content(self):
        self.alarm_content = []

    def get_authorization(self):
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        payload = json.dumps({
            "app_id": "cli_a46f1952bb79d00c",
            "app_secret": "s9xm9RJ7KAdxlKMIzBZ0ofWYpsdG3Wao",
            "chat_id": ""
        })
        headers = {
            'Content-Type': 'application/json;charset=utf-8'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        response_data = json.loads(response.text)
        Authorization = response_data['tenant_access_token']
        return Authorization

    def upload_img(self, img_path_list):
        url = "https://open.feishu.cn/open-apis/im/v1/images"
        headers = {
            'Authorization': 'Bearer ' + self.get_authorization()
        }
        payload = {'image_type': 'message'}
        image_keys = []
        for img in img_path_list:
            files = [
                ('image',
                 (img, open(img, 'rb'), 'application/json'))
            ]
            response = requests.request("POST", url, headers=headers, data=payload, files=files)
            response_data = json.loads(response.text)
            image_key = response_data['data']['image_key']
            image_keys.append(image_key)
        return image_keys

    def set_alarm_img(self, img_key, content):
        self.img_dict[img_key] = content

    def set_at(self, at_ids, content):
        self.at_dict[at_ids] = content


if __name__ == '__main__':
    """
    发送如下告警信息：
    测试告警信息
    告警口径：测试用途，请忽略
    监控指标：0
    """
    alarm_title = '测试告警信息'
    alarm_desc = '测试用途，请忽略'
    field_name = '监控指标'
    field_value = 0
    at_ids = "ou_1b7703a33d340f03129a9bce0c99ca66,ou_8bcbde88e472f485ee6ba95f417f2ff5"
    alarm = Alarm(alarm_title=alarm_title, alarm_group='dev')
    alarm.set_alarm_desc(alarm_desc)
    alarm.set_at(at_ids, "起飞名单")
    alarm.set_alarm_field(field_name, field_value)
    alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/294cf0df-3150-410e-b25d-6932f0cf38df')
    alarm.send_to_feishu(alarm.build_alarm())
