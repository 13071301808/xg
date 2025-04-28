# encoding: utf-8
""" labeled

author songxiaohang
"""
import numpy as np
import urllib.request
import cv2
from conf import settings
from utils_out import Logger, load_yolomodel, detect_box, api_img_process, load_realmodel, detect_crop
import torch
# 全局配置
# 结果列表
result = []

class box_detect:

    def __init__(self, model_path,model_data,conf_thres=0.3, iou_thres=0.5, max_det=6):
        self.yolo_model = load_yolomodel(model_path,model_data)
        self.conf_thres = conf_thres
        self.iou_thres = iou_thres
        self.max_det = max_det

    def detect(self, image):
        box_info = detect_box(image, self.yolo_model, detect_imgsz=640, conf_thres=self.conf_thres,iou_thres=self.iou_thres,max_det=self.max_det)
        return box_info


if __name__ == '__main__':
    # 所选图片
    url = 'http://img4.yishouapp.com/obs/ys/3bb2c180d6ba22f1ddde600f4190fa65.jpeg?imageView2/2/w/1000/format/jpg/q/50/'
    # url = 'https://image.baidu.com/search/detail?ct=503316480&z=0&ipn=d&word=%E5%A4%96%E5%A5%97&step_word=&hs=0&pn=5&spn=0&di=7264239678495129601&pi=0&rn=1&tn=baiduimagedetail&is=3159957060%2C26473499&istype=0&ie=utf-8&oe=utf-8&in=&cl=2&lm=-1&st=undefined&cs=808342631%2C2094643774&os=3159957060%2C26473499&simid=808342631%2C2094643774&adpicid=0&lpn=0&ln=1960&fr=&fmq=1702549048114_R&fm=&ic=undefined&s=undefined&hd=undefined&latest=undefined&copyright=undefined&se=&sme=&tab=0&width=undefined&height=undefined&face=undefined&ist=&jit=&cg=&bdtype=0&oriquery=&objurl=https%3A%2F%2Fgimg2.baidu.com%2Fimage_search%2Fsrc%3Dhttp%3A%2F%2Fsafe-img.xhscdn.com%2Fbw1%2Fb25543dc-957b-49b9-ac65-a61f6fc70723%3FimageView2%2F2%2Fw%2F1080%2Fformat%2Fjpg%26refer%3Dhttp%3A%2F%2Fsafe-img.xhscdn.com%26app%3D2002%26size%3Df9999%2C10000%26q%3Da80%26n%3D0%26g%3D0n%26fmt%3Dauto%3Fsec%3D1705141051%26t%3D9f06139b6d7a5e5b78de2dea4a0d836e&fromurl=ippr_z2C%24qAzdH3FAzdH3Fooo_z%26e3Bxtw5i5g2fi7_z%26e3Bv54AzdH3F1tfv5ej6yAzdH3Ftpj4AzdH3Fmnd9ncvbaaaaaaaaaba8kjml&gsm=1e&rpstart=0&rpnum=0&islist=&querylist=&nojc=undefined&dyTabStr=MCwyLDEsMyw2LDQsNSw4LDcsOQ%3D%3D&lid=11356861648094589835'
    resp = urllib.request.urlopen(url)
    image = np.asarray(bytearray(resp.read()), dtype="uint8")
    image = cv2.imdecode(image, cv2.IMREAD_COLOR)
    model_path = settings.MODEL_PATH['cate_yolo']['path']
    model_data = settings.YAML_PATH['cate_yolo']
    # 实例化接口
    label_obj = box_detect(model_path, model_data)

    # label_obj = box_detect()
    box_info = label_obj.detect(image)
    print(box_info)
