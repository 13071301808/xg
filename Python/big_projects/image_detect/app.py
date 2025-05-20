# encoding:utf-8
import logging
import os
from flask import Flask, render_template, request, jsonify
from werkzeug.utils import secure_filename
from conf import settings
from utils_out import load_yolomodel, detect_crop
from init import label_obj
from datetime import datetime
import numpy as np
import urllib.request
import cv2

app = Flask(__name__)

# 变量配置
# 上传原图地址
app.config['UPLOAD_FOLDER'] = 'static/images'
# 处理后的图片地址
app.config['NEW_UPLOAD_FOLDER'] = './static/model_images/'
model_images_path = app.config['NEW_UPLOAD_FOLDER']
# 允许上传的图片文件类型
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}
# 检测此路径下是否有这些文件夹
if not os.path.exists(app.config['UPLOAD_FOLDER']):
    # 创建文件夹
    os.makedirs(app.config['UPLOAD_FOLDER'])
if not os.path.exists(model_images_path):
    # 创建文件夹
    os.makedirs(model_images_path)

# 定义log文件路径
#log_file = f'/root/image_tag/log/{str(datetime.today())[:10]}_image_tag_log.txt'
log_file = f'/data/sxh/image_detect/logs/image_detect.log'
# 检测是否具有log文件
if not os.path.exists(os.path.dirname(log_file)):
    # 创建文件
    os.makedirs(os.path.dirname(log_file))
# 配置日志参数
app.logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(log_file)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
app.logger.addHandler(file_handler)


# 实例化方法封装类
# add_text = Add_Text()


class labeled:
    def __init__(self):
        self.yolo_model = load_yolomodel(
            settings.MODEL_PATH['cate_yolo']['path'],
            data=settings.YAML_PATH['cate_yolo']
        )

    def labeled(self, file_path, filename):
        write_path = os.path.join(model_images_path, filename)
        detect_crop(file_path, self.yolo_model, write_path=write_path, detect_imgsz=640)


# 对上传文件进行检测
def allowed_file(filename):
    return '.' in filename and \
        filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


# 默认访问
@app.route('/')
def index():
    return render_template('templates/index.html')


# 实现上传功能
@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # 获取文件
        file = request.files['file']
        # 识别上传文件
        if file and allowed_file(file.filename):
            # 提取文件名称
            filename = secure_filename(file.filename)
            # 打印上传文件名
            print(filename)
            # 上传原图地址
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            # 规范化路径分隔符
            file_path = file_path.replace('\\', '/')
            # 打印上传路径
            print(file_path)
            # 存储到本地
            file.save(file_path)
            # 打标方法
            label_obj = labeled()
            label_obj.labeled(file_path, filename)
            # 输出日志
            app.logger.info(f'>>>>> 识别图片成功，生成标注')
            return render_template('templates/index.html', message='上传成功', filename=filename)
        else:
            return render_template('templates/index.html', message='上传失败，只能上传图片')
    else:
        return render_template('templates/upload.html')


# 输入图片路径返回json数据
@app.route('/detect', methods=['GET', 'POST'])
def detect():
    d_t = datetime.now()
    object_dict = {}
    # 获取url参数
    url = request.args.get('url')
    try:
        image_s = datetime.now()
        resp = urllib.request.urlopen(url)
        image = np.asarray(bytearray(resp.read()), dtype="uint8")
        app.logger.info(f'图片长度：{len(image)}')
        image = cv2.imdecode(image, cv2.IMREAD_COLOR)
        app.logger.info(f'获取图片耗时：{datetime.now() - image_s}')
        app.logger.info(f'shape：{image.shape}')
    except Exception as e:
        app.logger.info(e)
        app.logger.info(f'获取图片报错url：' + url)
        app.logger.info(f'目标检测接口耗时：{datetime.now() - d_t}')
        return jsonify(object_dict)

    object_dict["shape"] = list(image.shape[:2])

    try:
        # 调用方法生成模型数据
        # 通过计算时间差检测超时
        start_time = datetime.now()
        # 调用方法生成模型数据
        box_info = label_obj.detect(image)
        end_time = datetime.now()
        timeout_check = (end_time - start_time).total_seconds()
        if timeout_check > 1:
            app.logger.info(f'生成模型数据超时，运行时间为：{timeout_check}')
        else:
            app.logger.info(f'生成模型数据正常，运行时间为：{timeout_check}')

        result = []
        # 循环遍历模型数据
        for i in box_info:
            # 定义解析数据
            # 品类类型
            category = int(i[5])
            # 品类类名
            category_name = settings.CATEGORY_DICT.get(category, '未知')
            # 置信度（固定小数点后两位）
            confidence = round(i[4], 2)
            # 生成坐标
            box_coordinates = [int(cood) for cood in i[:4]]
            # 将解析的数据封装起来
            result.append({
                "category": category,
                "category_name": category_name,
                "confidence": confidence,
                "box": box_coordinates
            })
        # # 最终的json数据
        # box_json = json.dumps(result, indent=4)
        object_dict["objects"] = result
        app.logger.info(f'目标检测接口耗时：{datetime.now() - d_t}：')
        return jsonify(object_dict)
    except Exception as e:
        app.logger.info(e)
        app.logger.info(f'报错url：' + url)
        app.logger.info(f'报错shape：{image.shape}')
        app.logger.info(f'目标检测接口耗时：{datetime.now() - d_t}：')
        return jsonify(object_dict)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5001)
