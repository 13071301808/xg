# encoding: utf-8
""" helper function

author songxiaohang
"""
import os
import sys
import re
import datetime
import cv2
import numpy
import numpy as np
import torch
from torch.optim.lr_scheduler import _LRScheduler
import torchvision
import torchvision.transforms as transforms
from torch.utils.data import DataLoader, Dataset
import albumentations as A
import torch
import torch.nn as nn
from  torchvision.models import inception_v3, resnet50
from utils.augmentations import letterbox
from utils.general import (scale_coords, non_max_suppression)
from utils.plots import Annotator, colors, save_one_box
from models.common import DetectMultiBackend
import logging
from logging import handlers
from tqdm import tqdm
from conf import settings

class union_model(nn.Module):

    def __init__(self, class_num_dict={}):

        super().__init__()
        self.base1 = nn.Sequential(*list(inception_v3(pretrained=True).children())[:15])
        self.base2 = nn.Sequential(*list(inception_v3(pretrained=True).children())[16:-1])
        self.base3 = nn.Sequential(*list(resnet50(pretrained=True).children())[:-1])
        #"""Dropout (keep 0.8)"""

        self.linears = nn.ModuleList([nn.Linear(4096,num_class[0]) for num_class in class_num_dict.values()])

    def forward(self, x):

        x1 = self.base1(x)
        x1 = self.base2(x1)
        x2 = self.base3(x)
        x=torch.cat([x1,x2],2)
        x = torch.flatten(x, 1)
        return self.linears[0](x),self.linears[1](x),self.linears[2](x),self.linears[3](x)

class multi_task_model(nn.Module):

    def __init__(self, class_num_dict={}):

        super(multi_task_model,self).__init__()
        self.class_num_dict = class_num_dict
        self.block1 = nn.Sequential(*list(inception_v3(pretrained=True).children())[:15])
        self.block2 = nn.Sequential(*list(inception_v3(pretrained=True).children())[16:-1])
        self.linears = nn.ModuleList([nn.Linear(2048,num_class[0]) for num_class in self.class_num_dict.values()])


    def forward(self, x):

        x = self.block1(x)
        x = self.block2(x)
        x=torch.flatten(x,1)
        return [self.linears[i](x) for i in range(len(self.class_num_dict))]
def load_yolomodel(weights='D:\pytorch_fashionai/checkpoint/yolomodel/neck.pt',data='D:\pytorch_fashionai\conf/neck.yaml'):
    if torch.cuda.is_available():
        model = DetectMultiBackend(weights, device=torch.device('cuda:0'), dnn=False, data=data)
    else:
        model = DetectMultiBackend(weights, device=torch.device('cpu'), dnn=False, data=data)
    return model

def load_realmodel(model_path=settings.MODEL_PATH['realmodel']['path']):
    net = torchvision.models.resnet18(pretrained=True)
    num_ftrs = net.fc.in_features
    net.fc = nn.Linear(num_ftrs, 2)
    net.load_state_dict(torch.load(model_path))
    net.eval()
    return net

def detect_crop(path,model,write_path='', detect_imgsz=640,conf_thres=0.3,iou_thres=0.5,max_det=6,anno=True,return_conf=False):
    if isinstance(path,str):
        img0 = cv2.imread(path)
        # print (img0)
    else:
        img0 = path
    img = letterbox(img0, detect_imgsz, stride=32, auto=True)[0]
    imc=img0.copy()
    # Convert
    img = img.transpose((2, 0, 1))[::-1]  # HWC to CHW, BGR to RGB
    img = np.ascontiguousarray(img)
    im = torch.from_numpy(img).to(torch.device('cpu'))
    im = im.float()  # uint8 to fp16/32
    im /= 255
    im=im[None]
    pred = model(im,augment=False)
    pred = non_max_suppression(pred, conf_thres, iou_thres, max_det=max_det)[0]
    annotator = Annotator(img0, line_width=2, example=str(model.names))
    if len(pred):
        pred[:, :4] = scale_coords(im.shape[2:], pred[:, :4], img0.shape).round()
        for *xyxy, conf, cls in reversed(pred):
            termi=save_one_box(xyxy, imc,save=False,BGR=True)
            conf = conf
            if anno:
                c = int(cls)  # integer class
                label = (f'{model.names[c]} {conf:.2f}')
                annotator.box_label(xyxy, label, color=colors(c, True))
                im0 = annotator.result()
                cv2.imwrite(write_path, im0)
        if write_path:
            pass
            # cv2.imwrite(write_path, termi)
        elif return_conf:
            return termi, conf.item()
        else:
           return termi
    else:
        im0 = annotator.result()
        if write_path:
            cv2.imwrite(write_path, im0)
        elif return_conf:
            return im0, 0.0
        else:
            return im0

def detect_box(img0,model,write_path='', detect_imgsz=640,conf_thres=0.3,iou_thres=0.5,max_det=6,anno=True,return_conf=False):

    model.model.half()
    img = letterbox(img0, detect_imgsz, stride=32, auto=True)[0]

    # Convert
    img = img.transpose((2, 0, 1))[::-1]  # HWC to CHW, BGR to RGB
    img = np.ascontiguousarray(img)

    if torch.cuda.is_available():
        im = torch.from_numpy(img).to('cuda:0')
        im = im.half()
    else:
        im = torch.from_numpy(img).to('cpu')
        im = im.float()
    im /= 255  # 0 - 255 to 0.0 - 1.0
    if len(im.shape) == 3:
        im = im[None]  # expand for batch dim

    #model.warmup(imgsz=(1, 3, detect_imgsz, detect_imgsz), half=False)  # warmup
    pred = model(im,augment=False)
    pred = non_max_suppression(pred, conf_thres, iou_thres, max_det=max_det)[0]

    if len(pred):
        pred[:, :4] = scale_coords(im.shape[2:], pred[:, :4], img0.shape).round()
        return pred.tolist()
    else:
        return []


class Albumentations:
    # YOLOv5 Albumentations class (optional, only used if package is installed)
    def __init__(self, h, w, crop_ratio):
        self.transform = A.Compose([
            A.OneOf([
                A.RandomGamma(gamma_limit=(60,120), p=0.5), #gamma_limit=(100,200), p=0.9
                A.RandomBrightnessContrast(brightness_limit=0.2, contrast_limit=0.2, p=0.5), #brightness_limit=0.4, contrast_limit=0.4, p=0.9
                A.CLAHE(clip_limit=4.0, tile_grid_size=(4, 4), p=0.5), #clip_limit=5.0, tile_grid_size=(2, 2), p=0.9
            ]),
            A.OneOf([
                A.Blur(blur_limit=4, p=1),
                A.MedianBlur(blur_limit=3, p=1)
            ], p=0.5),
            #A.VerticalFlip(always_apply=False, p=0.5),
            A.HorizontalFlip(p=0.5),
            A.RandomCrop(int(h * crop_ratio), int(w * crop_ratio), always_apply=False, p=0.2),
            A.Rotate(p=0.3,limit = 30)
        ])
        self.transform_inc = transforms.Compose([
                                                transforms.ToTensor(),
                                                transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                                                     std=[0.229, 0.224, 0.225])
        ])



    def __call__(self, image):
        if self.transform:
            new = self.transform(image=image)  # transformed
            im = new['image']
        return im

class LoadImagesAndLabels(Dataset):
    # YOLOv5 train_loader/val_loader, loads images and labels for training and validation
    cache_version = 0.6  # dataset labels *.cache version

    def __init__(self, data, imgsz, augment, if_detect_crop=False, detect_imgsz=512,if_resize=True, include_path=False,crop_ratio=1):
        self.imgsz = imgsz
        self.data = data
        self.if_resize = if_resize
        self.if_detect_crop = if_detect_crop
        self.detect_imgsz = detect_imgsz
        self.augment = augment
        self.include_path = include_path
        self.crop_raito = crop_ratio

    def __len__(self):
        return len(self.data)


    def __getitem__(self, index):
        img_path , label = self.data[index]  # linear, shuffled, or image_weights

        img = cv2.imread(img_path)
        h, w, c = img.shape

        if self.if_detect_crop:
            if len(detect_crop(img_path,detect_imgsz=self.detect_imgsz)):
                img=detect_crop(img_path,detect_imgsz=self.detect_imgsz)

        if self.augment:
            tsf = Albumentations(h,w,self.crop_raito)
            img = tsf(image=img)

        if self.if_resize:
            img = resize_image(img, self.imgsz)
            img = img.astype(np.uint8)


        img = img.transpose((2, 0, 1))[::-1]  # HWC to CHW, BGR to RGB

        img_tensor = torch.from_numpy(img/255.0)
        img_tensor = img_tensor.type(torch.FloatTensor)
        if self.include_path:
            return img_tensor, label, img_path
        else:
            return img_tensor, label

class LoadImagesAndLabels_multitask(Dataset):
    # YOLOv5 train_loader/val_loader, loads images and labels for training and validation
    cache_version = 0.6  # dataset labels *.cache version

    def __init__(self, data, imgsz, augment,label_info, if_resize=True,include_path=False,crop_ratio=1):
        self.imgsz = imgsz
        self.data = data
        self.if_resize = if_resize
        self.augment = augment
        self.label_info = label_info
        self.include_path = include_path
        self.crop_raito = crop_ratio

    def __len__(self):
        return len(self.data)

    # def __iter__(self):
    #     self.count = -1
    #     print('ran dataset iter')
    #     #self.shuffled_vector = np.random.permutation(self.nF) if self.augment else np.arange(self.nF)
    #     return self

    def __getitem__(self, index):
        img_path , tk , label = self.data[index]  # linear, shuffled, or image_weights
        img=cv2.imread(img_path)
        h, w, c = img.shape

        if self.augment:
            tsf = Albumentations(h,w,self.crop_raito)
            img = tsf(image=img)

        if self.if_resize:
            img = resize_image(img, self.imgsz)
            img = img.astype(np.uint8)

        img = img.transpose((2, 0, 1))[::-1]  # HWC to CHW, BGR to RGB

        img_tensor = torch.from_numpy(img/255.0)
        img_tensor = img_tensor.type(torch.FloatTensor)

        # 处理多任务label
        label_index = int(label)
        label=[torch.zeros(v[0]) for k, v in self.label_info.items()]
        info = self.label_info[tk]
        label[info[1]][label_index] = 1.0
        if self.include_path:
            return img_tensor, label, img_path
        else:
            return img_tensor, label

def api_img_process(img,size):
    img = resize_image(img, size)
    img = img.transpose((2, 0, 1))[::-1]  # HWC to CHW, BGR to RGB
    img_tensor = torch.from_numpy(img / 255.0)
    img_tensor = img_tensor[None].type(torch.FloatTensor)
    return img_tensor

def resize_image(img, target_sizes, keep_ratio=True):
    # Please Note： label style should be normalized xyxy, otherwise need modify
    # if keep_ratio is True, letterbox using padding
    if not isinstance(target_sizes, (list, set, tuple)):
        target_sizes = [target_sizes, target_sizes]
    target_h, target_w = target_sizes

    h, w, _ = img.shape
    scale = min(target_h / h, target_w / w)
    temp_h, temp_w = int(scale * h), int(scale * w)
    # if scale<=1:
    #     temp_h, temp_w = int(scale * h), int(scale * w)
    # else:
    #     temp_h,temp_w = h, w
    image_resize = cv2.resize(img, (temp_w, temp_h))

    if keep_ratio:
        image_new = np.full(shape=(target_h, target_w, 3), fill_value=0.0)
        delta_h, delta_w = (target_h - temp_h) // 2, (target_w - temp_w) // 2
        image_new[delta_h: delta_h + temp_h, delta_w: delta_w + temp_w, :] = image_resize
        return image_new
    else:
        return image_resize

def augment_hsv(im, hgain=0.0138, sgain=0.664, vgain=0.464):
    # HSV color-space augmentation
    if hgain or sgain or vgain:
        r = np.random.uniform(-1, 1, 3) * [hgain, sgain, vgain] + 1  # random gains
        #print (r)
        hue, sat, val = cv2.split(cv2.cvtColor(im, cv2.COLOR_BGR2HSV))
        # print (hue)
        dtype = im.dtype  # uint8

        x = np.arange(0, 256, dtype=r.dtype)
        lut_hue = ((x * r[0]) % 180).astype(dtype)
        #print (lut_hue)
        lut_sat = np.clip(x * r[1], 0, 255).astype(dtype)
        lut_val = np.clip(x * r[2], 0, 255).astype(dtype)

        im_hsv = cv2.merge((cv2.LUT(hue, lut_hue), cv2.LUT(sat, lut_sat), cv2.LUT(val, lut_val)))
        cv2.cvtColor(im_hsv, cv2.COLOR_HSV2BGR, dst=im)  # no return needed

def data_train_label(label_info,task,root_path,shape_stats=False):
    image_path=[]
    shape_list = []
    for key, value in label_info.items():
        sub_path = os.path.join(root_path, task, str(value))
        for file in tqdm(os.listdir(sub_path)):
            image_path.append((os.path.join(sub_path, file), task, value))
            if shape_stats:
                image = cv2.imread(os.path.join(sub_path, file))
                shape_list.append(image.shape)
    if shape_stats:
        item, count = np.unique(shape_list, axis=0, return_counts=True)
        item = [str(s) for s in item]
        import pandas as pd
        pd.DataFrame({'label': item, "count": count}).to_csv('./log/'+task+'_shape.csv', index=False)
    t = np.dtype([('path', str, 88), ('tk', str, 20), ('label', np.int64, 1)])
    return np.array(image_path,dtype= t)

def data_train_label_multi(label_info,root_path,task,shape_stats=False):

    image_path = []
    shape_list = []
    for key, value in label_info.items():
        task_path = os.path.join(root_path, key)
        for sub_path in os.listdir(task_path):
            file_list = os.listdir(os.path.join(task_path, sub_path))
            for file in file_list:
                image_path.append((os.path.join(task_path, sub_path, file), key, int(sub_path)))
            if shape_stats:
                image = cv2.imread(os.path.join(sub_path, file))
                shape_list.append(image.shape)
    if shape_stats:
        item, count = np.unique(shape_list, axis=0, return_counts=True)
        item = [str(s) for s in item]
        import pandas as pd
        pd.DataFrame({'label': item, "count": count}).to_csv('./log/'+task+'_shape.csv', index=False)
    t = np.dtype([('path', str, 128), ('tk', str, 32), ('label', np.int64, 1)])
    return np.array(image_path, dtype=t)

def process_image_ratio(image, ratio=0.88):

    h, w, c = image.shape
    crop_h=(1-ratio)*h
    crop_w=(1-ratio)*w
    image = image[int(crop_h):h, int(crop_w / 2):int(w - crop_w / 2)]

    return image

def get_network(args):
    """ return given network
    """

    if args.net == 'vgg16':
        from models.vgg import vgg16_bn
        net = vgg16_bn()
    elif args.net == 'vgg13':
        from models.vgg import vgg13_bn
        net = vgg13_bn()
    elif args.net == 'vgg11':
        from models.vgg import vgg11_bn
        net = vgg11_bn()
    elif args.net == 'vgg19':
        from models.vgg import vgg19_bn
        net = vgg19_bn()
    elif args.net == 'densenet121':
        from models.densenet import densenet121
        net = densenet121()
    elif args.net == 'densenet161':
        from models.densenet import densenet161
        net = densenet161()
    elif args.net == 'densenet169':
        from models.densenet import densenet169
        net = densenet169()
    elif args.net == 'densenet201':
        from models.densenet import densenet201
        net = densenet201()
    elif args.net == 'googlenet':
        from models.googlenet import googlenet
        net = googlenet()
    elif args.net == 'inceptionv3':
        from models.inceptionv3 import inceptionv3
        net = inceptionv3()
    elif args.net == 'inceptionv4':
        from models.inceptionv4 import inceptionv4
        net = inceptionv4()
    elif args.net == 'inceptionresnetv2':
        from models.inceptionv4 import inception_resnet_v2
        net = inception_resnet_v2()
    elif args.net == 'xception':
        from models.xception import xception
        net = xception()
    elif args.net == 'resnet18':
        from models.resnet import resnet18
        net = resnet18()
    elif args.net == 'resnet34':
        from models.resnet import resnet34
        net = resnet34()
    elif args.net == 'resnet50':
        from models.resnet import resnet50
        net = resnet50()
    elif args.net == 'resnet101':
        from models.resnet import resnet101
        net = resnet101()
    elif args.net == 'resnet152':
        from models.resnet import resnet152
        net = resnet152()
    elif args.net == 'preactresnet18':
        from models.preactresnet import preactresnet18
        net = preactresnet18()
    elif args.net == 'preactresnet34':
        from models.preactresnet import preactresnet34
        net = preactresnet34()
    elif args.net == 'preactresnet50':
        from models.preactresnet import preactresnet50
        net = preactresnet50()
    elif args.net == 'preactresnet101':
        from models.preactresnet import preactresnet101
        net = preactresnet101()
    elif args.net == 'preactresnet152':
        from models.preactresnet import preactresnet152
        net = preactresnet152()
    elif args.net == 'resnext50':
        from models.resnext import resnext50
        net = resnext50()
    elif args.net == 'resnext101':
        from models.resnext import resnext101
        net = resnext101()
    elif args.net == 'resnext152':
        from models.resnext import resnext152
        net = resnext152()
    elif args.net == 'shufflenet':
        from models.shufflenet import shufflenet
        net = shufflenet()
    elif args.net == 'shufflenetv2':
        from models.shufflenetv2 import shufflenetv2
        net = shufflenetv2()
    elif args.net == 'squeezenet':
        from models.squeezenet import squeezenet
        net = squeezenet()
    elif args.net == 'mobilenet':
        from models.mobilenet import mobilenet
        net = mobilenet()
    elif args.net == 'mobilenetv2':
        from models.mobilenetv2 import mobilenetv2
        net = mobilenetv2()
    elif args.net == 'nasnet':
        from models.nasnet import nasnet
        net = nasnet()
    elif args.net == 'attention56':
        from models.attention import attention56
        net = attention56()
    elif args.net == 'attention92':
        from models.attention import attention92
        net = attention92()
    elif args.net == 'seresnet18':
        from models.senet import seresnet18
        net = seresnet18()
    elif args.net == 'seresnet34':
        from models.senet import seresnet34
        net = seresnet34()
    elif args.net == 'seresnet50':
        from models.senet import seresnet50
        net = seresnet50()
    elif args.net == 'seresnet101':
        from models.senet import seresnet101
        net = seresnet101()
    elif args.net == 'seresnet152':
        from models.senet import seresnet152
        net = seresnet152()
    elif args.net == 'wideresnet':
        from models.wideresidual import wideresnet
        net = wideresnet()
    elif args.net == 'stochasticdepth18':
        from models.stochasticdepth import stochastic_depth_resnet18
        net = stochastic_depth_resnet18()
    elif args.net == 'stochasticdepth34':
        from models.stochasticdepth import stochastic_depth_resnet34
        net = stochastic_depth_resnet34()
    elif args.net == 'stochasticdepth50':
        from models.stochasticdepth import stochastic_depth_resnet50
        net = stochastic_depth_resnet50()
    elif args.net == 'stochasticdepth101':
        from models.stochasticdepth import stochastic_depth_resnet101
        net = stochastic_depth_resnet101()

    else:
        print('the network name you have entered is not supported yet')
        sys.exit()

    if args.gpu: #use_gpu
        net = net.cuda()

    return net


def get_training_dataloader(mean, std, batch_size=16, num_workers=2, shuffle=True):
    """ return training dataloader
    Args:
        mean: mean of cifar100 training dataset
        std: std of cifar100 training dataset
        path: path to cifar100 training python dataset
        batch_size: dataloader batchsize
        num_workers: dataloader num_works
        shuffle: whether to shuffle
    Returns: train_data_loader:torch dataloader object
    """

    transform_train = transforms.Compose([
        #transforms.ToPILImage(),
        transforms.RandomCrop(32, padding=4),
        transforms.RandomHorizontalFlip(),
        transforms.RandomRotation(15),
        transforms.Resize((224,224)),
        transforms.ToTensor(),
        transforms.Normalize(mean, std)
    ])
    #cifar100_training = CIFAR100Train(path, transform=transform_train)
    cifar100_training = torchvision.datasets.CIFAR100(root='./data', train=True, download=True, transform=transform_train)
    cifar100_training_loader = DataLoader(
        cifar100_training, shuffle=shuffle, num_workers=num_workers, batch_size=batch_size)

    return cifar100_training_loader

def get_test_dataloader(mean, std, batch_size=16, num_workers=2, shuffle=True):
    """ return training dataloader
    Args:
        mean: mean of cifar100 test dataset
        std: std of cifar100 test dataset
        path: path to cifar100 test python dataset
        batch_size: dataloader batchsize
        num_workers: dataloader num_works
        shuffle: whether to shuffle
    Returns: cifar100_test_loader:torch dataloader object
    """

    transform_test = transforms.Compose([
        transforms.Resize((224, 224)),
        transforms.ToTensor(),
        transforms.Normalize(mean, std)
    ])
    #cifar100_test = CIFAR100Test(path, transform=transform_test)
    cifar100_test = torchvision.datasets.CIFAR100(root='./data', train=False, download=True, transform=transform_test)
    cifar100_test_loader = DataLoader(
        cifar100_test, shuffle=shuffle, num_workers=num_workers, batch_size=batch_size)

    return cifar100_test_loader

def compute_mean_std(cifar100_dataset):
    """compute the mean and std of cifar100 dataset
    Args:
        cifar100_training_dataset or cifar100_test_dataset
        witch derived from class torch.utils.data

    Returns:
        a tuple contains mean, std value of entire dataset
    """

    data_r = numpy.dstack([cifar100_dataset[i][1][:, :, 0] for i in range(len(cifar100_dataset))])
    data_g = numpy.dstack([cifar100_dataset[i][1][:, :, 1] for i in range(len(cifar100_dataset))])
    data_b = numpy.dstack([cifar100_dataset[i][1][:, :, 2] for i in range(len(cifar100_dataset))])
    mean = numpy.mean(data_r), numpy.mean(data_g), numpy.mean(data_b)
    std = numpy.std(data_r), numpy.std(data_g), numpy.std(data_b)

    return mean, std

class WarmUpLR(_LRScheduler):
    """warmup_training learning rate scheduler
    Args:
        optimizer: optimzier(e.g. SGD)
        total_iters: totoal_iters of warmup phase
    """
    def __init__(self, optimizer, total_iters, last_epoch=-1):

        self.total_iters = total_iters
        super().__init__(optimizer, last_epoch)

    def get_lr(self):
        """we will use the first m batches, and set the learning
        rate to base_lr * m / total_iters
        """
        return [base_lr * self.last_epoch / (self.total_iters + 1e-8) for base_lr in self.base_lrs]


def most_recent_folder(net_weights, fmt):
    """
        return most recent created folder under net_weights
        if no none-empty folder were found, return empty folder
    """
    # get subfolders in net_weights
    folders = os.listdir(net_weights)

    # filter out empty folders
    folders = [f for f in folders if len(os.listdir(os.path.join(net_weights, f)))]
    if len(folders) == 0:
        return ''

    # sort folders by folder created time
    folders = sorted(folders, key=lambda f: datetime.datetime.strptime(f, fmt))
    return folders[-1]

def most_recent_weights(weights_folder):
    """
        return most recent created weights file
        if folder is empty return empty string
    """
    weight_files = os.listdir(weights_folder)
    if len(weights_folder) == 0:
        return ''

    regex_str = r'([A-Za-z0-9]+)-([0-9]+)-(regular|best)'

    # sort files by epoch
    weight_files = sorted(weight_files, key=lambda w: int(re.search(regex_str, w).groups()[1]))

    return weight_files[-1]

def last_epoch(weights_folder):
    weight_file = most_recent_weights(weights_folder)
    if not weight_file:
       raise Exception('no recent weights were found')
    resume_epoch = int(weight_file.split('-')[1])

    return resume_epoch

def best_acc_weights(weights_folder):
    """
        return the best acc .pth file in given folder, if no
        best acc weights file were found, return empty string
    """
    files = os.listdir(weights_folder)
    if len(files) == 0:
        return ''

    regex_str = r'([A-Za-z0-9]+)-([0-9]+)-(regular|best)'
    best_files = [w for w in files if re.search(regex_str, w).groups()[2] == 'best']
    if len(best_files) == 0:
        return ''

    best_files = sorted(best_files, key=lambda w: int(re.search(regex_str, w).groups()[1]))
    return best_files[-1]


def create_dataloader(data, imgsz=224, if_detect_crop=False,detect_imgsz=512,augment=True, batch_size=16, num_workers=8, shuffle=False,include_path=False,crop_ratio=0.95):

    dataset = LoadImagesAndLabels(data, imgsz, augment, if_detect_crop, detect_imgsz, if_resize=True, include_path=include_path,crop_ratio=crop_ratio)

    return DataLoader(dataset,
                  batch_size=batch_size,
                  shuffle=shuffle,
                  num_workers=num_workers
                  )

def create_dataloader_multitask(data, imgsz=224, augment=True, label_info={}, batch_size=16, num_workers=8, shuffle=False,include_path=False,crop_ratio=1):

    dataset = LoadImagesAndLabels_multitask(data, imgsz, augment, label_info, if_resize=True,include_path=include_path, crop_ratio=crop_ratio)

    return DataLoader(dataset,
                  batch_size=batch_size,
                  shuffle=shuffle,
                  num_workers=num_workers
                  )

def mkdir_if_not_exist(path):
    if not os.path.exists(os.path.join(*path)):
        os.makedirs(os.path.join(*path))

# 日志处理
class Logger(object):
    level_relations = {
        'debug':logging.DEBUG,
        'info':logging.INFO
    }

    def __init__(self,filename,level='info',when='D',backCount=3,fmt='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'):
        self.logger = logging.getLogger(filename)
        format_str = logging.Formatter(fmt)
        self.logger.setLevel(self.level_relations.get(level))
        sh = logging.StreamHandler()
        sh.setFormatter(format_str)
        th = handlers.TimedRotatingFileHandler(filename=filename,when=when,backupCount=backCount,encoding='utf-8')
        th.setFormatter(format_str)
        self.logger.addHandler(sh)
        self.logger.addHandler(th)

if __name__=="__main__":
    #path='./data/fashionAI_attributes_train1/Images/skirt_length_labels/000c8ed315fb0b2d931d5c348a27e5c7.jpg'
    net = torchvision.models.resnet18(pretrained=True)
    num_ftrs = net.fc.in_features
    net.fc = nn.Linear(num_ftrs, 2)
    print (torch.load(settings.MODEL_PATH['realmodel']))
    # net.load_state_dict(torch.load(settings.MODEL_PATH['realmodel']))
    # net.eval()


