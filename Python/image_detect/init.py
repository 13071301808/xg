# encoding: utf-8

from conf import settings
from detect import box_detect

model_path = settings.MODEL_PATH['cate_yolo']['path']
model_data = settings.YAML_PATH['cate_yolo']

# 实例化接口
label_obj = box_detect(model_path, model_data)
