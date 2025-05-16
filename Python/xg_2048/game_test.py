# 导入依赖包
from tkinter import *
import tkinter as tk
from random import *
from tkinter.messagebox import *


def base():
    numdict = {1: {}, 2: {}, 3: {}, 4: {}}
    for key in numdict.keys(): numdict[key] = {1: '', 2: '', 3: '', 4: ''}
    while 1:
        x1, x2, y1, y2 = randint(1, 4), randint(1, 4), randint(1, 4), randint(1, 4)
        if x1 != x2 or y1 != y2:
            numdict[x1][y1], numdict[x2][y2] = 2, 2
            break

    # 主要弹窗
    frame = Frame(game, bg='#BBADA0').place(width=205, height=205)
    frame2 = Frame(game, bg='#D3D3D3').place(width=100, height=205, x=205)
    frame3 = Frame(frame2, bg='#FAF8EF').place(x=210, y=5, width=90, height=195)
    frame4 = Frame(game, bg='#D3D3D3').place(width=205, height=205)

    # 分数递增计数（默认0）
    (score := StringVar()).set('计算数值\n\n0')
    (score_value := StringVar()).set('0')
    # 计分板
    Label(frame3, textvariable=score, font=('consolas', 15), bg='#D3D3D3').place(x=215, y=10, width=80, height=95)
    # 开始按钮布局
    play_button = Button(frame2, text='开始', font=('consolas', 15), bd=0, bg='#D3D3D3',
                         command=lambda: game_start())
    play_button.place(x=215, y=110, width=80, height=25)
    # 结束按钮布局
    end_button = Button(frame2, text='结束', font=('consolas', 15), bd=0, bg='#D3D3D3', command=lambda: game_over())
    end_button.place(x=215, y=140, width=80, height=25)
    # 帮助按钮布局
    backup_play_button = Button(frame2, text='退一步', font=('consolas', 15), bd=0, bg='#D3D3D3',
                                command=lambda: backup_play())
    backup_play_button.place(x=215, y=170, width=80, height=25)
    # 数值标签（4 * 4）
    n14 = StringVar()
    n24 = StringVar()
    n34 = StringVar()
    n44 = StringVar()
    n13 = StringVar()
    n23 = StringVar()
    n33 = StringVar()
    n43 = StringVar()
    n12 = StringVar()
    n22 = StringVar()
    n32 = StringVar()
    n42 = StringVar()
    n11 = StringVar()
    n21 = StringVar()
    n31 = StringVar()
    n41 = StringVar()
    # 放置格子
    for sy in [5, 55, 105, 155]:
        for sx, i in zip([5, 55, 105, 155],
                         [n14, n24, n34, n44, n13, n23, n33, n43, n12, n22, n32, n42, n11, n21, n31, n41][
                         4 * (sy - 5) // 50:4 * (sy + 45) // 50]):
            # 单元格
            Label(frame, bg='#CDC1B4', textvariable=i, font=('consolas', 15)).place(width=45, height=45, y=sy, x=sx)

    # 初始化单元格
    def initialization():
        for x in range(1, 5):
            for y, i in zip(
                    range(1, 5),
                    [n11, n12, n13, n14, n21, n22, n23, n24, n31, n32, n33, n34, n41, n42, n43, n44][4 * (x - 1):4 * x]
            ): i.set(numdict[x][y])

    # tip备注
    Label(
        frame4, text='Thanks of your use my software', font=('consolas', 10), fg='black', bg='#D3D3D3'
    ).place(x=0, y=205, width=305, height=18)

    # 游戏胜利
    def game_win():
        for value in numdict.values():
            # 检测是否有2048在单元格中
            if 2048 in value:
                # 胜利弹窗
                frame_win = Frame(game, bg='yellow').place(width=305, height=205)
                # 胜利语
                Label(
                    frame_win, text='恭喜您获得胜利^-^', font=('consolas', 30), fg='red', bg='yellow'
                ).place(width=305, height=60)
                # 重来键
                Button(
                    frame_win, bd=0, bg='#D3D3D3', font=('consolas', 15), text='再来一次', command=lambda: base()
                ).place(width=80, height=30, y=150, x=45)
                # 结束键
                Button(
                    frame_win, bd=0, bg='#D3D3D3', font=('consolas', 15), text='结束游戏', command=lambda: quit()
                ).place(width=80, height=30, y=150, x=180)
                Label(
                    frame, font=('consolas', 15), text='You have got to\n2048!', bg='yellow'
                ).place(width=205, height=60, y=60, x=50)
        # 设置100ms的缓冲
        game.after(100, game_win)

    # 游戏结束功能
    def game_over():
        # 结束弹窗
        frame_over = Frame(game, bg='yellow').place(width=305, height=205)
        # 结束语
        Label(frame_over, text='寄了呀!!!', font=('consolas', 30), fg='red', bg='yellow').place(width=305, height=60)
        # 重来键
        Button(
            frame_over, bd=0, bg='#D3D3D3', font=('consolas', 15), text='再来一次', command=lambda: base()
        ).place(width=80, height=30, y=150, x=45)
        # 结束键
        Button(
            frame_over, bd=0, bg='#D3D3D3', font=('consolas', 15), text='结束使用', command=lambda: quit()
        ).place(width=80, height=30, y=150, x=180)
        # 显示最终分数
        Label(
            frame, font=('consolas', 50), textvariable=score_value, bg='yellow'
        ).place(width=205, height=60, y=60, x=50)
        # 记录最高分到文件上
        with open('log/score.txt', 'r') as f:
            # 读取文件中的最高分数（如果存在）
            highest_score = f.read()
            # print(highest_score)
            f.close()
        if int(highest_score) < int(score_value.get()):
            # 记录到文件中
            with open('log/score.txt', 'w+') as f1:
                # 将数据写入文件
                f1.write(str(score_value.get()))
                f1.seek(0)  # 将文件指针移回文件开头，以便下次读取时从正确的位置开始
                f1.close()

    # 操作函数
    def move(way, count=0):
        # 判断是否为正确的操作
        if way in ['w', 's', 'a', 'd']:
            # 操作方向键w
            if way == 'w':
                for x in range(1, 5):
                    numdict[x][5] = 0
                    for y in range(1, 5):
                        if numdict[x][y] == numdict[x][y + 1] and numdict[x][y] != '':
                            numdict[x][y] = ''
                            numdict[x][y + 1] *= 2
                        elif numdict[x][y] != '' and numdict[x][y + 1] == '':
                            numdict[x][y], numdict[x][y + 1] = numdict[x][y + 1], numdict[x][y]
                    del numdict[x][5]
            # 操作方向键s
            if way == 's':
                for x in range(1, 5):
                    numdict[x][0] = 0
                    for y in range(4, 0, -1):
                        if numdict[x][y] == numdict[x][y - 1] and numdict[x][y] != '':
                            numdict[x][y] = ''
                            numdict[x][y - 1] *= 2
                        elif numdict[x][y] != '' and numdict[x][y - 1] == '':
                            numdict[x][y], numdict[x][y - 1] = numdict[x][y - 1], numdict[x][y]
                    del numdict[x][0]
            # 操作方向键d
            if way == 'd':
                numdict[5] = {1: 0, 2: 0, 3: 0, 4: 0}
                for y in range(1, 5):
                    for x in range(1, 5):
                        if numdict[x][y] == numdict[x + 1][y] and numdict[x][y] != '':
                            numdict[x][y] = ''
                            numdict[x + 1][y] *= 2
                        elif numdict[x][y] != '' and numdict[x + 1][y] == '':
                            numdict[x][y], numdict[x + 1][y] = numdict[x + 1][y], numdict[x][y]
                del numdict[5]
            # 操作方向键a
            if way == 'a':
                numdict[0] = {1: 0, 2: 0, 3: 0, 4: 0}
                for y in range(1, 5):
                    for x in range(4, 0, -1):
                        if numdict[x][y] == numdict[x - 1][y] and numdict[x][y] != '':
                            numdict[x][y] = ''
                            numdict[x - 1][y] *= 2
                        elif numdict[x][y] != '' and numdict[x - 1][y] == '':
                            numdict[x][y], numdict[x - 1][y] = numdict[x - 1][y], numdict[x][y]
                del numdict[0]

            for x in range(1, 5):
                for y, i in zip(range(1, 5),
                                [n11, n12, n13, n14, n21, n22, n23, n24, n31, n32, n33, n34, n41, n42, n43, n44][
                                4 * (x - 1):4 * x]):
                    i.set(numdict[x][y])
                    if numdict[x][y] == '':
                        count = 1
            # 如果单元格都填满数
            if count == 0:
                # 结束游戏功能
                game_over()
                # 决定是否结束游戏
                return None
            # 随机再产生一个数
            while 1:
                x, y = randint(1, 4), randint(1, 4)
                if numdict[x][y] == '':
                    numdict[x][y] = choice([2, 4])
                    break
            [n11, n12, n13, n14, n21, n22, n23, n24, n31, n32, n33, n34, n41, n42, n43, n44][4 * x + y - 5].set(
                numdict[x][y])

    # 悔棋功能
    def backup_play():
        showinfo(
            title='帮助界面指引',
            message="点击w为上,点击s为下,点击d为右,点击a为左\n如需结束游戏直接点击关闭\n快捷键：\n\tCtrl+N 关于我们\n\tCtrl+M 历史数据"
        )

    # 计分板
    def scorevalue(value=0):
        for x in range(1, 5):
            for y in range(1, 5):
                if numdict[x][y] != '': value += numdict[x][y]
        score.set('分数\n\n%s' % value)
        score_value.set(str(value))
        # 设置10ms缓冲
        game.after(10, scorevalue)

    # 游戏开始
    def game_start():
        # 键盘关联
        game.bind_all('<Any-KeyPress>', lambda event: move(event.char))
        # 初始化
        initialization()
        # 开始计分
        scorevalue()
        # 检测胜利
        game_win()

    def enter(event):
        event.widget['bg'] = '#333333'

    def leave(event):
        event.widget['bg'] = '#D3D3D3'

    for i in [play_button, end_button, backup_play_button]:
        i.bind('<Enter>', lambda event: enter(event))
        i.bind('<Leave>', lambda event: leave(event))

    return score_value


# 最高分
def max_score():
    # 记录结束的分数
    with open('log/score.txt', 'r') as f:
        for line in f:
            data = line.strip()  # 去除行末尾的换行符和空格
    # 设置自定义背景弹窗
    frame_maxscore = Frame(game, bg='white').place(width=305, height=205)
    # 标题语
    Label(
        frame_maxscore, text="最高分", font=('consolas', 12), fg='black', bg='#D3D3D3'
    ).place(width=305, height=30)
    # 分数
    Label(
        frame_maxscore, text=str(data), font=('consolas', 30), bg='#D3D3D3'
    ).place(width=205, height=60, y=60, x=50)
    # 确认按钮
    Button(
        frame_maxscore, bd=0, bg='#D3D3D3', font=('consolas', 15), text='确定', command=lambda: base()
    ).place(width=100, height=30, y=160, x=105)


def change_background_color(event):
    selected_color = event.widget.cget("color")  # 获取选中的颜色
    game.configure(bg=selected_color)  # 更改弹窗背景颜色


# 自定义弹窗背景
def background_style():
    # 设置自定义弹窗背景
    frame_bgstyle = Frame(game, bg='white').place(width=305, height=205)
    # 标题语
    Label(
        frame_bgstyle, text='自定义单元格', font=('consolas', 12), fg='black', bg='#D3D3D3'
    ).place(width=305, height=30)
    # 创建单选按钮组
    colors = ["red", "green", "blue"]
    var = tk.StringVar(game)
    # 默认选中第一个颜色
    var.set(colors[0])
    #
    for index, color in enumerate(colors):
        radiobutton = tk.Radiobutton(
            game, text=color, value=color, variable=var, command=lambda: change_background_color
        )
        radiobutton.place(y=80, x=40 + (index * 80))

    # 确认按钮
    Button(
        frame_bgstyle, bd=0, bg='#D3D3D3', font=('consolas', 15), text='确定设置', command=lambda: base()
    ).place(width=100, height=30, y=160, x=105)


# 游戏帮助功能
def game_help():
    showinfo(
        title='帮助界面指引',
        message="点击w为上,点击s为下,点击d为右,点击a为左\n每局只能点一次退一步\n快捷键：\n\tCtrl+M 最高分\n\tCtrl+L 自定义单元格\n\tCtrl+P 帮助\n\tCtrl+N 关于我们"
    )


# 关于我们
def about():
    # 显示弹窗
    showinfo(
        title='关于我们',
        message="作者：xg\n制作不易，希望大佬们多多支持哦~"
    )


# 执行程序
if __name__ == '__main__':
    # 基本框架
    game = tk.Tk()
    # 游戏标题
    game.title('jntm计算器-v1.1')
    # 游戏布局
    game.geometry('305x225+500+250')
    # 后端函数
    base()
    # 顶部下拉菜单
    top = tk.Menu(game, bg='#D3D3D3')
    # 二级菜单
    option_menu = tk.Menu(top, bg='#D3D3D3')
    top.add_cascade(label='选项', menu=option_menu)
    option_menu.add_command(label='最高分', command=lambda: max_score(), accelerator='Ctrl+M')
    option_menu.add_command(label='自定义背景', command=lambda: background_style(), accelerator='Ctrl+L')
    option_menu.add_command(label='帮助', command=lambda: game_help(), accelerator='Ctrl+P')
    option_menu.add_command(label='关于我们', command=lambda: about(), accelerator='Ctrl+N')
    # 退出键
    top.add_command(label='退出', command=game.destroy)
    # 添加分割线
    top.add_separator()

    # 如果确实此句，则不会显示菜单分组
    game.config(menu=top)
    # 设置禁止拉动窗口大小
    game.resizable(False, False)
    game.mainloop()
