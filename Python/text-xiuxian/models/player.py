import time
from datetime import datetime
from config.game_config import GameConfig


class Player:
    def __init__(self, name):
        self.name = name
        # 密码
        self.password = None
        # 等级
        self.level = 1
        # 经验
        self.exp = 0
        # 境界
        self.cultivation_stage = "练气期"
        # 灵力
        self.spiritual_power = 10
        # 最后登录时间
        self.last_cultivation_time = time.time()
        # 修炼次数
        self.cultivation_times = 0
        # 创建时间
        self.creation_time = datetime.now()
        # 突破历史记录
        self.breakthrough_history = []
        # 玩家称号
        self.title = "凡人"
        # 玩家头像
        self.avatar = "images/avatars/初始.png"
        # 已学功法
        self.skills = []
        # 拥有的法宝
        self.items = []
        # 门派属性
        self.sect = None
        # 道侣
        self.partner = None


    def calculate_exp_multiplier(self):
        """计算经验倍率"""
        multiplier = GameConfig.CULTIVATION_STAGES[self.cultivation_stage]["exp_rate"]
        level_bonus = GameConfig.get_level_bonus(self.level)
        multiplier *= level_bonus
        # 灵力加成，根据门派不同有所改变
        spiritual_bonus = GameConfig.get_spiritual_bonus(
            self.spiritual_power,
            self.sect
        )
        # 随机波动
        multiplier *= spiritual_bonus
        random_factor = GameConfig.get_random_factor()
        multiplier *= random_factor
        return multiplier

    def try_breakthrough(self):
        """尝试突破境界"""
        for stage, requirements in GameConfig.CULTIVATION_STAGES.items():
            if (self.level >= requirements["min_level"] > GameConfig.CULTIVATION_STAGES[self.cultivation_stage][
                "min_level"] and
                    self.cultivation_stage != stage):
                old_stage = self.cultivation_stage
                self.cultivation_stage = stage
                self.spiritual_power = max(self.spiritual_power, requirements["spiritual_power"])
                self.breakthrough_history.append({
                    'time': datetime.now(),
                    'from': old_stage,
                    'to': stage,
                    'description': GameConfig.CULTIVATION_STAGES[stage]["description"]
                })
                return True, f"恭喜突破到{stage}！{GameConfig.CULTIVATION_STAGES[stage]['description']}"
        return False, None

    def join_sect(self, sect_name):
        """加入门派时获得门派特有功法"""
        self.sect = sect_name
        self.avatar = GameConfig.SECTS[sect_name]['avatar']
        # 获得门派特有功法
        self.skills.extend(GameConfig.SECTS[sect_name]['skills'])

    def set_password(self, password):
        """设置密码"""
        self.password = password

    def check_password(self, password):
        """验证密码"""
        return self.password == password
