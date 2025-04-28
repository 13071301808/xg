import logging
import time
from utils.chatmodel import ChatModel
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
from config.game_config import GameConfig
from utils.player_data_manager import PlayerDataManager
from models.player import Player

app = Flask(__name__)
# 全局配置
# 设置session密钥
app.secret_key = GameConfig.SESSION_SECRET_KEY
# 修仙境界定义
CULTIVATION_STAGES = GameConfig.CULTIVATION_STAGES
# 修改门派定义
SECTS = GameConfig.SECTS
# api 调用
chat_api = ChatModel()
config = GameConfig()
player_data_manager_api = PlayerDataManager(config)


@app.route('/')
def index():
    """ 根目录 """
    return redirect(url_for('login'))


def validate_player_name(name):
    """ 验证玩家名称是否合法 """
    if not name or len(name) < 2:
        return False, "道号必须至少 2 个字符"
    if len(name) > 20:
        return False, "道号不能超过 20 个字符"
    # 检查用户名是否已被占用
    player = player_data_manager_api.get_player(name)
    if player is not None:
        return False, "此道号已被占用"
    return True, None


@app.route('/register', methods=['GET', 'POST'])
def register():
    """ 玩家注册模块 """
    if request.method == 'POST':
        username = request.form['username'].strip()
        password = request.form['password']
        confirm_password = request.form['confirm_password']

        # 验证输入
        if password != confirm_password:
            flash('两次输入的密码不一致', 'error')
            return render_template('register.html')

        is_valid, error_msg = validate_player_name(username)
        if not is_valid:
            flash(error_msg, 'error')
            return render_template('register.html')

        # 创建新玩家
        player = Player(username)
        player.set_password(password)
        # 添加用户数据
        player_data_manager_api.add_player(player)
        flash('注册成功！请登录', 'success')
        return redirect(url_for('login'))

    return render_template('register.html')


@app.route('/login', methods=['GET', 'POST'])
def login():
    """ 登录模块 """
    if request.method == 'POST':
        username = request.form['username'].strip()
        password = request.form['password']

        player = player_data_manager_api.get_player(username)

        if player is None:
            flash('道号不存在', 'error')
            return render_template('login.html')

        if not player.check_password(password):
            flash('密码错误', 'error')
            return render_template('login.html')

        session['player_name'] = username
        flash(f'欢迎回来，{username}道友！', 'success')
        return redirect(url_for('game'))

    return render_template('login.html')


@app.route('/game')
def game():
    """ 游戏主页 """
    if 'player_name' not in session:
        return redirect(url_for('login'))

    player = player_data_manager_api.get_player(session['player_name'])

    # 计算修炼进度百分比
    progress = (player.exp / (player.level * 50)) * 100  # 改为 50

    # 获取当前境界描述
    stage_description = CULTIVATION_STAGES[player.cultivation_stage]["description"]

    # 计算下一个境界
    next_stage = None
    for stage, reqs in CULTIVATION_STAGES.items():
        if reqs["min_level"] > player.level:
            next_stage = stage
            break

    return render_template('game.html',
                           player=player,
                           stages=CULTIVATION_STAGES,
                           next_level_exp=player.level * 50,
                           progress=progress,
                           stage_description=stage_description,
                           next_stage=next_stage,
                           SECTS=SECTS)


@app.route('/cultivate')
def cultivate():
    """ 玩家修炼功能 """
    if 'player_name' not in session:
        return redirect(url_for('login'))

    player = player_data_manager_api.get_player(session['player_name'])
    current_time = time.time()
    time_diff = current_time - player.last_cultivation_time

    # 计算修炼获得的经验
    exp_time = CULTIVATION_STAGES[player.cultivation_stage]["exp_time"]
    exp_multiplier = player.calculate_exp_multiplier()
    base_exp = max(1, int(time_diff / exp_time))
    exp_gain = max(1, int(base_exp * exp_multiplier))
    player.exp += exp_gain
    player.cultivation_times += 1
    # 降低升级所需经验值
    exp_required = player.level * GameConfig.EXP_REQUIRED_BASE
    # 升级判定
    level_up_count = 0
    total_spiritual_power_gain = 0
    while player.exp >= exp_required:
        player.exp -= exp_required
        player.level += 1
        spiritual_power_gain = 50
        player.spiritual_power += spiritual_power_gain
        total_spiritual_power_gain += spiritual_power_gain
        level_up_count += 1
        exp_required = player.level * 50  # 更新下一级所需经验

    # 尝试突破
    breakthrough_success, breakthrough_msg = player.try_breakthrough()
    # 更新最后修炼时间并保存数据
    player.last_cultivation_time = current_time
    # 保存玩家数据（注意要传入字典格式的玩家数据）
    player_data_manager_api.save_players()
    if breakthrough_success:
        flash(f'突破至{player.cultivation_stage}境界', 'success')
    return redirect(url_for('game'))


@app.route('/game/go_chat')
def go_chat():
    """ 跳转聊天页 """
    if 'player_name' not in session:
        return redirect(url_for('login'))
    return render_template('chat.html')


@app.route('/game/chat', methods=['POST', 'GET'])
def chat():
    """ 与模型聊天功能 """
    if 'player_name' not in session:
        return redirect(url_for('login'))

    if request.method == 'POST':
        # 获取输入的信息
        message = request.form['message']
        logging.info(f'>>>>> 问的问题:{message}')
        if message:
            # 提问问题
            response = chat_api.chat(message)
            # 返回 JSON 响应
            return str(response)
        else:
            logging.warning('>>>>> 提交了空消息')
            return jsonify({'error': '请输入问题'}), 400


@app.route('/choose_sect')
def choose_sect():
    """ 跳转门派选择页 """
    if 'player_name' not in session:
        return redirect(url_for('login'))

    player = player_data_manager_api.get_player(session['player_name'])
    if player.cultivation_stage != "筑基期" or player.sect is not None:
        flash('只有筑基期的修士才能选择门派，且只能选择一次！', 'error')
        return redirect(url_for('game'))

    return render_template('choose_sect.html', sects=SECTS)


@app.route('/join_sect/<sect_name>')
def join_sect(sect_name):
    """ 选择门派功能 """
    if 'player_name' not in session:
        return redirect(url_for('login'))

    if sect_name not in SECTS:
        flash('无效的门派选择！', 'error')
        return redirect(url_for('game'))

    player = player_data_manager_api.get_player(session['player_name'])
    if player.cultivation_stage != "筑基期" or player.sect is not None:
        flash('只有筑基期的修士才能选择门派，且只能选择一次！', 'error')
        return redirect(url_for('game'))

    player.join_sect(sect_name)
    flash(f'恭喜加入{sect_name}！{SECTS[sect_name]["description"]}', 'success')
    if SECTS[sect_name]['skills']:
        flash(f'获得门派功法：{", ".join(SECTS[sect_name]["skills"])}', 'success')
    # 保存玩家数据（注意要传入字典格式的玩家数据）
    player_data_manager_api.save_players()

    return redirect(url_for('game'))


@app.route('/game/choose_daolu')
def choose_daolu():
    """ 跳转道侣选择页 """
    if 'player_name' not in session:
        return redirect(url_for('login'))

    player = player_data_manager_api.get_player(session['player_name'])
    return render_template('choose_daolu.html', player=player)


@app.route('/logout')
def logout():
    """ 登出功能 """
    if 'player_name' in session:
        session.pop('player_name', None)
        flash('已成功退出修仙界', 'info')
    return redirect(url_for('index'))


if __name__ == '__main__':
    # 应用启动时自动加载数据
    player_data_manager_api.load_players()
    app.run(debug=True)
