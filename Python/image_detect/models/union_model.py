# -*- coding: UTF-8 -*-
""" inceptionv4 in pytorch


[1] Christian Szegedy, Sergey Ioffe, Vincent Vanhoucke, Alex Alemi

    Inception-v4, Inception-ResNet and the Impact of Residual Connections on Learning
    https://arxiv.org/abs/1602.07261
"""

import torch
import torch.nn as nn
from  torchvision.models import inception_v3
from inceptionv4 import inceptionv4,inception_resnet_v2

class union_model(nn.Module):

    def __init__(self, class_nums=6):

        super().__init__()
        self.inception_a = inception_v3(pretrained=True,init_weights=True)
        self.inception_b = inceptionv4()
        self.inception_c = inception_resnet_v2()
        self.base1 = nn.Sequential(*list(self.inception_a.children())[:-1])
        self.base2 = nn.Sequential(*list(self.inception_b.children())[:-1])
        self.base3 = nn.Sequential(*list(self.inception_c.children())[:-1])
        #"""Dropout (keep 0.8)"""

        self.linear = nn.Linear(5632, class_nums)

    def forward(self, x):

        x1 = self.base1(x)
        x2 = self.base2(x)
        x3 = self.base3(x)
        x = torch.cat([x1,x2,x3])
        x = self.linear(x)
        return x


y = union_model(6)

print (y.inception_a.parameters())