from PIL import Image, ImageDraw, ImageFont
import os


class Add_Text:
    # 添加文字到图片上
    @staticmethod
    def add_text(file_path, filename,model_images_path):
        # 添加文字到图片上
        image = Image.open(file_path)
        draw = ImageDraw.Draw(image)
        # 定义添加文字
        text = '原神启动'
        # 定义文字样式
        font_path = 'static/font/ch_jianti.ttf'  # 替换为你的Unicode字体文件的路径
        font = ImageFont.truetype(font_path, 20)
        draw.text((20, 20), text, fill='red', font=font)
        # 上传处理后图片地址
        model_file_path = os.path.join(model_images_path, filename)
        # 存储到处理后的文件夹下
        image.save(model_file_path)
