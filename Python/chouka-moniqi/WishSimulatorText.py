"""
要求：180抽大保底，90抽小保底
10抽必出4星
90抽必出5星  抽到的时候up和常驻各占50%
P = {
0.006                       i<=73
0.006 + 0.06(i - 73)        74<=i<=89
1                           i=90
}
五星中奖概率为0.6%
四星角色概率为2.55%
其余的三星概率是96.85%
大保底小报底各占50%
"""
import os
import random


# 初始化抽卡的概率
def rate_initialization():
    rate_list.extend(['蓝' for temp in range(9685)])
    rate_list.extend(['紫' for temp in range(255)])
    rate_list.extend(['蓝' for temp in range(60)])
    random.shuffle(rate_list)


# 74抽之后增加概率
def rate_add():
    try:
        for _ in range(60):
            rate_list.remove('蓝')
            rate_list.insert(random.randint(0, len(rate_list) - 1), '金')
    except ValueError:
        print('程序异常，概率增加异常')
        exit()


# 大保底还是小保底
def guarantees():
    try:
        with open('recording/isGuarantees.txt', 'r', encoding='utf-8') as file:
            if file.read() == 'true':
                certainly = True
            else:
                certainly = False
    except BaseException:
        with open('recording/isGuarantees.txt', 'w+', encoding='utf-8') as file:
            file.write('false')
        with open('recording/isGuarantees.txt', 'r', encoding='utf-8') as file:
            if file.read() == 'true':
                certainly = True
            else:
                certainly = False
    if certainly:
        with open('recording/isGuarantees.txt', 'w+', encoding='utf-8') as file:
            file.write('false')
        return True
    else:
        res = random.choice(['up', 'resident'])
        if res == 'up':
            with open('recording/isGuarantees.txt', 'w+', encoding='utf-8') as file:
                file.write('false')
            return True
        else:
            with open('recording/isGuarantees.txt', 'w+', encoding='utf-8') as file:
                file.write('true')
            return False


# 创建一个文件夹在同一个目录下
def mkdir(path):
    folder = os.path.exists(path)
    if not folder:  # 判断文件夹是否存在
        os.makedirs(path)  # 不存在就新建一个


# 抽卡次数记录
def record_times(number):
    with open('recording/times.txt', 'w+', encoding='utf-8') as file:
        file.write(str(number))


# 抽卡次数读取
def read_times():
    try:
        with open('recording/times.txt', 'r', encoding='utf-8') as file:
            number = int(file.read())
            return number
    except BaseException:
        with open('recording/times.txt', 'w+', encoding='utf-8') as file:
            file.write('0')
        return 0


# 出金了
def gold():
    rate_initialization()
    record_times(0)
    if guarantees():
        res_list.append(up)
        return '出货了！！！！！'
    else:
        res_list.append(random.choice(fiveStar))
        return '哇！金色传。。。歪了。。。'


# 记录紫色保底，10抽必出紫色
def record_rare(number):
    with open('recording/rare.txt', 'w+', encoding='utf-8') as file:
        file.write(str(number))


# 读取紫色保底
def read_rare():
    try:
        with open('recording/rare.txt', 'r', encoding='utf-8') as file:
            return int(file.read())
    except BaseException:
        with open('recording/rare.txt', 'w+', encoding='utf-8') as file:
            file.write('0')
            return 0


# 写入抽卡记录
def recode_gacha():
    with open('抽卡记录.txt', 'r', encoding='utf-8') as file:
        record_list = file.read()
        if len(record_list) >= 300:
            with open('抽卡记录.txt', 'a+', encoding='utf-8') as writeFile:
                pass
            file.write(' '.join(res_list))
        else:
            with open('抽卡记录.txt', 'a+', encoding='utf-8') as writeFile:
                pass


mkdir('卡池')
mkdir('recording')
while True:
    try:
        with open('卡池/up.txt', 'r', encoding='utf-8') as tempFile:
            up = tempFile.read()
        with open('卡池/fiveStar.txt', 'r', encoding='utf-8') as tempFile:
            fiveStar = tempFile.read().split(sep=' ')
        with open('卡池/fourStar.txt', 'r', encoding='utf-8') as tempFile:
            fourStar = tempFile.read().split(sep=' ')
        with open('卡池/threeStar.txt', 'r', encoding='utf-8') as tempFile:
            threeStar = tempFile.read().split(sep=' ')
        break
    except BaseException:
        print('你好，欢迎使用本模拟器，我在上面帮你创建了几文件夹，在里面填入卡池的东西就好了，用空格隔开')
        with open('卡池/up.txt', 'w+', encoding='utf-8'):
            pass
        with open('卡池/fiveStar.txt', 'w+', encoding='utf-8'):
            pass
        with open('卡池/fourStar.txt', 'w+', encoding='utf-8'):
            pass
        with open('卡池/threeStar.txt', 'w+', encoding='utf-8'):
            pass
        input('填好了就回个车')
rate_list = list()
rate_initialization()
while True:
    try:
        wish = int(input('1、单抽 2、十连抽 3、退出\n请输入：'))
    except ValueError:
        print('你这输入的啥玩意')
        continue
    if wish == 1:
        count = 1
    elif wish == 2:
        count = 10
    elif wish == 3:
        break
    else:
        print('奇奇怪怪的数字')
        continue
    res_list = []
    result_report = '蓝天白云'
    temp_list = [random.choice(rate_list) for _ in range(count)]
    flag = False
    for character_rank in temp_list:
        if 73 <= read_times() <= 88:
            rate_add()
        elif read_times() >= 89:
            result_report = gold()
            continue
        record_times(read_times() + 1)
        if character_rank == '蓝':
            record_rare(read_rare() + 1)
            if read_rare() >= 10:
                if not flag:
                    result_report = '出了个紫色'
                record_rare(0)
                res_list.append(random.choice(fourStar))
                continue
            res_list.append(random.choice(threeStar))
        elif character_rank == '紫':
            if not flag:
                result_report = '出了个紫色'
            record_rare(0)
            res_list.append(random.choice(fourStar))
        elif character_rank == '金':
            flag = True
            result_report = gold()
    print(result_report)
    print(' '.join(res_list))
    # recode_gacha()
    print('==================================================')
