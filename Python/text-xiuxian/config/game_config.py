# -*- coding: utf-8 -*-
import random


class GameConfig:
    # 修仙境界配置
    CULTIVATION_STAGES = {
        "练气期": {"min_level": 1, "spiritual_power": 10, "exp_rate": 10.0, "exp_time": 1,
                   "description": "修行之始,以炼精化气为主"},  # 1 秒 10 点经验
        "筑基期": {"min_level": 10, "spiritual_power": 100, "exp_rate": 20.0, "exp_time": 5,
                   "description": "打基础之境,为金丹打下根基"},  # 5 秒 10 点经验
        "金丹期": {"min_level": 20, "spiritual_power": 1000, "exp_rate": 50.0, "exp_time": 10,
                   "description": "结丹成婴,法力大增"},  # 10 秒 20 点经验
        "元婴期": {"min_level": 30, "spiritual_power": 2000, "exp_rate": 100.0, "exp_time": 15,
                   "description": "元婴出窍,神识可游"},  # 15 秒 50 点经验
        "化神期": {"min_level": 40, "spiritual_power": 5000, "exp_rate": 200.0, "exp_time": 20,
                   "description": "化虚为实,法力通玄"},  # 20 秒 100 点经验
        "大乘期": {"min_level": 50, "spiritual_power": 10000, "exp_rate": 1000.0, "exp_time": 25,
                   "description": "道法自然,位列仙班"}  # 25 秒 500 点经验
    }

    # 门派配置
    SECTS = {
        "剑宗": {
            "description": "剑修一脉，以剑入道，追求极致锋芒",
            "avatar": "images/avatars/剑宗.png",
            "bonus": "剑道加成：修炼速度提升 20%",
            "skills": []
        },
        "佛宗": {
            "description": "佛法无边，以禅入道，追求内心平静",
            "avatar": "images/avatars/佛宗.png",
            "bonus": "佛法加成：灵力提升 20%",
            "skills": []
        },
        "钓鱼宗": {
            "description": "以钓入道，万物皆可钓，道亦可钓",
            "avatar": "images/avatars/钓鱼宗.png",
            "bonus": "钓鱼加成：钓鱼速度提升 20%，稀有物品概率提升",
            "skills": ["愿者上钩"]
        }
    }

    """常量配置"""
    # 每级所需基础经验值
    EXP_REQUIRED_BASE = 50
    # 每次升级获得的灵力值
    LEVEL_UP_SPIRIT_GAIN = 50
    LOGIN_COOKIE_NAME = 'player_name'
    SESSION_SECRET_KEY = 'xiuxian_secret_key'
    # 数据存储相关配置
    PLAYER_DATA_FILE = 'players.json'
    PLAYER_DATA_ENCODING = 'utf-8'
    # 开发时显示加载日志
    DEBUG_LOGGING = True
    # 自动保存间隔(秒)
    SAVE_INTERVAL = 300
    # 数据修改标志
    MODIFY_FLAG = False
    # 添加线程锁
    SAVE_LOCK = None
    # 上次保存时间戳
    LAST_SAVE_TIME = 0
    # 上次保存时间戳(秒)
    LAST_SAVE_TIMESTAMP = 0
    # 数据版本
    DATA_VERSION = '1.0'
    # 缓存最新玩家数据
    PLAYER_CACHE = {}

    @staticmethod
    def get_random_factor():
        return 0.9 + random.random() * 0.2

    @staticmethod
    def get_level_bonus(level: int) -> float:
        return 1.0 + (level - 1) * 0.1

    @staticmethod
    def get_spiritual_bonus(spiritual_power: int, sect: str) -> float:
        base = 1.0 + spiritual_power / 1000
        if sect == '佛宗':
            base *= 1.2
        return base
