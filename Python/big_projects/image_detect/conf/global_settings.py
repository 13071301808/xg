""" configurations for this project

author sxh
"""
import os
from datetime import datetime
import platform
import torch

DEVICE = torch.device("cuda:0")
# mean and std of dataset
TRAIN_MEAN = (0.5070751592371323, 0.48654887331495095, 0.4409178433670343)
TRAIN_STD = (0.2673342858792401, 0.2564384629170883, 0.27615047132568404)

# total training epoches
EPOCH = 15
MILESTONES = [3, 7, 11, 13, 20]

DATE_FORMAT = '%A_%d_%B_%Y_%Hh_%Mm_%Ss'
# time of we run the script
TIME_NOW = datetime.now().strftime(DATE_FORMAT)

# tensorboard log dir
LOG_DIR = 'runs'

# save weights file per SAVE_EPOCH epoch
SAVE_EPOCH = 5

# fashionai_rootpath
# ROOT_PATH = '/pytorch_fashionai/'
# 不同环境配置
if platform.system().lower() == 'windows':
    # ROOT_PATH = 'g:/image_detect/'
    # 测试
    ROOT_PATH = 'G:/image_detect/'
else:
    ROOT_PATH = '/data/sxh/image_detect/'
    # 大数据测试机位置
    # ROOT_PATH = '/root/image_tag/'

RAWLESS_DATA_PATH = 'rawless_data'

# 测试
# CHECKPOINT_PATH = 'D:/image_detect/checkpoint'
# 大数据测试机位置
CHECKPOINT_PATH = '/root/image_tag/checkpoint'

# 品类字典
CATEGORY_DICT = {
    0.0: "upwear",
    1.0: "downwear",
    2.0: "skirt",
    3.0: "b_skirt",
    4.0: "shoe",
    5.0: "bag",
    6.0: "acc",
    7.0: "underwear"
}

# model
MODEL_DICT = {
    "b_skirt": {
        "length": {"model": ["real_length", "length"], "label": ["skirt_length", "waist_type"]},
        "last": {"model": ["last"], "label": ["silhouette", "stereotype"]}
    },
    "upwear": {
        "length": {"model": ["real_length", "length"], "label": ["coat_length", "sleeve_length"]},
        "top": {"model": ["top"], "label": ["neck", "lapel", "sleeve_type"]},
        "last": {"model": ["last"], "label": ["silhouette", "stereotype"]}
    },
    "downwear": {
        "length": {"model": ["real_length", "length"], "label": ["pant_length", "waist_type"]},
        "last": {"model": ["last"], "label": ["silhouette", "stereotype"]}
    },
    "op_dress": {
        "length": {"model": ["real_length", "length"], "label": ["skirt_length", "sleeve_length"]},
        "top": {"model": ["top"], "label": ["neck", "lapel", "sleeve_type"]},
        "last": {"model": ["last"], "label": ["silhouette", "stereotype"]}
    },
    "skirt": {
        "length": {"model": ["real_length", "length"], "label": ["skirt_length"]},
        "last": {"model": ["last"], "label": ["silhouette", "stereotype"]}
    }
}

MODEL_PATH = {
    "real_length": {"type": "cls", "path": ROOT_PATH + "checkpoint/length/real_length.pth", "size": 352,
                    "name": "inception_v3"},
    "length": {"type": "cls", "path": ROOT_PATH + "checkpoint/length/length.pth", "size": 448, "name": "inception_v3"},
    "neck_yolo": {"type": "yolo", "path": ROOT_PATH + "checkpoint/yolomodel/neck.pt", "size": 640},
    "realmodel": {"type": "cls", "path": ROOT_PATH + "checkpoint/realmodel/real_model.pth", "size": 224,
                  "name": "resnet50"},
    "clothing_yolo": {"type": "yolo", "path": ROOT_PATH + "checkpoint/yolomodel/clothing.pt", "size": 640},
    "top": {"type": "cls", "path": ROOT_PATH + "checkpoint/top/top.pth", "size": 416},
    "top_yolo": {"type": "yolo", "path": ROOT_PATH + "checkpoint/yolomodel/top.pt", "size": 640},
    "upwear_yolo": {"type": "yolo", "path": ROOT_PATH + "checkpoint/yolomodel/upwear.pt", "size": 640},
    "downwear_yolo": {"type": "yolo", "path": ROOT_PATH + "checkpoint/yolomodel/downwear.pt", "size": 640},
    "skirt_yolo": {"type": "yolo", "path": ROOT_PATH + "checkpoint/yolomodel/skirt.pt", "size": 640},
    "b_skirt_yolo": {"type": "yolo", "path": ROOT_PATH + "checkpoint/yolomodel/b_skirt.pt", "size": 640},
    "last": {"type": "cls", "path": ROOT_PATH + "checkpoint/last/last_model.pth", "size": 480},
    "cate_yolo": {"type": "yolo", "path": ROOT_PATH + "checkpoint/yolomodel/cate.pt", "size": 640},
    "yolov5s": {"type": "yolo", "path": ROOT_PATH + "checkpoint/yolomodel/yolov5s.pt", "size": 640}
}
YAML_PATH = {
    "clothing_yolo": ROOT_PATH + "/conf/clothing.yaml",
    "neck_yolo": ROOT_PATH + "/conf/neck.yaml",
    "top_yolo": ROOT_PATH + "/conf/top.yaml",
    "upwear_yolo": ROOT_PATH + "/conf/upwear.yaml",
    "downwear_yolo": ROOT_PATH + "/conf/downwear.yaml",
    "b_skirt_yolo": ROOT_PATH + "/conf/b_skirt.yaml",
    "skirt_yolo": ROOT_PATH + "/conf/skirt.yaml",
    "cate_yolo": ROOT_PATH + "/conf/cate.yaml",
    "yolov5s": ROOT_PATH + "/conf/coco128.yaml"
}
# task
TASK = {
    "last_task": ["pattern_crop", "craft_crop", "style_crop"],  # ["silhouette_crop","stereotype_crop"]
    "top_task": ["neck_crop", "lapel_crop", "sleeve_type_crop"],
    "length_task": ["pant_length", "skirt_length", "coat_length", "sleeve_length", "waist_type"],
    "crop_length_task": ["pant_length_real_crop", "skirt_length_real_crop", "coat_length_real_crop",
                         "sleeve_length_real_crop", "waist_type_real_crop"]
}
# label_dict
LABEL_DICT = {
    "real": {"实拍": 0, "模特": 1},
    "pant_length_real_crop": {"短裤": 0, "五分裤": 1, "七分裤": 2, "九分裤": 3, "长裤": 4},
    "skirt_length_real_crop": {"短裙": 0, "中裙": 1, "中长裙": 2, "长裙": 3},
    "coat_length_real_crop": {"短款": 0, "常规款": 1, "中长款": 2, "长款": 3},
    "sleeve_length_real_crop": {"无袖": 0, "短袖": 1, "中袖": 2, "七分袖": 3, "长袖": 4},
    "neck_crop": {"圆领": 0, "抹胸": 1, "V领": 2, "方领": 3, "单肩领": 4, "深V": 5, "桃型领": 6, "露肩领": 7,
                  "棒球服领": 8, "一字领": 9,
                  "无领": 10, "U领": 11, "立领": 12, "半高领": 13, "高领": 14, "堆堆领": 15, "娃娃领": 16, "衬衫领": 17,
                  "西装领": 18,
                  "POLO领": 19, "荷叶领": 20, "青果领": 21, "荷叶边高领": 22, "毛领": 23, "大翻领": 24, "海军领": 25,
                  "连帽": 26},
    "lapel_crop": {"拉链": 0, "一粒扣": 1, "单排扣": 2, "双排扣": 3, "盘扣": 4, "牛角扣": 5, "系带": 6, "暗门襟": 7,
                   "开襟": 8, "套头": 9},
    "waist_type_real_crop": {"无腰线": 0, "低腰": 1, "中腰": 2, "高腰": 3},
    "sleeve_type_crop": {"泡泡袖": 0, "堆堆袖": 1, "灯笼袖": 2, "蝙蝠袖": 3, "喇叭袖": 4, "插肩袖": 5, "落肩袖": 6,
                         "飞飞袖": 7
        , "荷叶袖": 8, "常规袖": 9, "衬衫袖": 10, "翻边袖": 11, "包袖": 12, "花瓣袖": 13},
    "silhouette": {"A": 0, "X": 1, "T": 2, "O": 3, "H": 4},
    "stereotype": {"紧身": 0, "合身": 1, "宽松": 2},
    "pattern": {"卡通": 0, "条纹": 1, "纯色": 2, "字母": 3, "动物纹": 4, "波点": 5, "拼色": 6, "格纹": 7, "迷彩": 8
        , "渐变": 9, "波浪": 10, "佩里斯纹": 11, "植物": 12, "扎染": 13, "图案": 14, "人物": 15},
    "craft": {"无": 0, "烫金": 1, "烫钻": 2, "印花": 3, "绣花": 4, "喷溅": 5, "植绒": 6, "印染": 7, "毛边": 8,
              "提花": 9, "水洗": 10, "口袋": 11, "磨损破洞": 12, "贴布贴片": 13, "钉珠柳钉": 14,
              "撞色线迹": 15, "蕾丝拼接": 16, "打揽": 17, "绗缝": 18, "亮片": 19, "粗针（粗线）": 20, "漏针（挑孔）": 21,
              "细针（细线）": 22, "压褶": 23, "木耳边": 24,
              "荷叶边": 25, "层叠": 26, "镂空": 27, "打结": 28, "系带": 29, "抽褶": 30},
    "style": {"淑女": 0, "小女人": 1, "田园浪漫": 2, "法式复古": 3, "网红辣妹": 4, "纯欲": 5, "大牌简约": 6, "高街": 7,
              "甜酷": 8, "街头潮流": 9, "中性休闲": 10, "鬼马": 11, "学院": 12, "日系极简": 13, "暗黑": 14,
              "休闲简约": 15, "日系简约": 16, "通勤简约": 17, "清新简约": 18, "基础百搭": 19}
}

CH_DICT = {
    "衣长": "coat_length_real_crop",
    "袖长": "sleeve_length_real_crop",
    "裤长": "pant_length_real_crop",
    "裙长": "skirt_length_real_crop",
    "领型": "neck_crop",
    "门襟": "lapel_crop",
    "腰型": "waist_type_real_crop",
    "袖型": "sleeve_type_crop",
    "廓形": "silhouette",
    "版型": "stereotype",
    "图案": "pattern",
    "工艺": "craft",
    "新风格": "style"
}

CROP_DICT = {
    "top": ["sleeve_type", "lapel", "neck"]
}

CLS_PRE_CROP = {
    "廓形": {"上装": "upwear_yolo", "裙装": {"半身裙": "b_skirt_yolo", "其他": "skirt_yolo"}},
    "版型": {"上装": "upwear_yolo", "裙装": {"半身裙": "b_skirt_yolo", "其他": "skirt_yolo"}, "下装": "downwear_yolo"},
    "衣长": {"上装": "upwear_yolo"},
    "袖长": {"上装": "upwear_yolo", "裙装": {"半身裙": "b_skirt_yolo", "其他": "skirt_yolo"}},
    "裤长": {"下装": "downwear_yolo"},
    "裙长": {"裙装": {"半身裙": "b_skirt_yolo", "其他": "skirt_yolo"}},
    "腰型": {"裙装": {"半身裙": "b_skirt_yolo", "其他": "skirt_yolo"}, "下装": "downwear_yolo"},
    "图案": {"上装": "upwear_yolo", "裙装": {"半身裙": "b_skirt_yolo", "其他": "skirt_yolo"}, "下装": "downwear_yolo",
             "套装": "upwear_yolo"},
    "工艺": {"上装": "upwear_yolo", "裙装": {"半身裙": "b_skirt_yolo", "其他": "skirt_yolo"}, "下装": "downwear_yolo",
             "套装": "upwear_yolo"},
    "新风格": {"上装": "upwear_yolo", "裙装": {"半身裙": "b_skirt_yolo", "其他": "skirt_yolo"}, "下装": "downwear_yolo",
               "套装": "upwear_yolo"}
}

REAL_PREDICT_LIST = ['衣长', '裙长', '裤长', '袖长', '腰型']

if __name__ == "__main__":
    import pandas as pd

    # print (os.path.isfile(MODEL_PATH['neck_yolo']['path']))
    print(CLS_PRE_CROP.get("廓形").get("装") != None)
