# -*- coding: utf-8 -*-
import json
from datetime import datetime
from config.game_config import GameConfig
from models.player import Player


class PlayerDataManager:
    def __init__(self, config=None):
        # 存储玩家数据
        self.players = {}
        self.config = config or GameConfig()
        # 从配置中读取数据文件路径
        self.file_path = self.config.PLAYER_DATA_FILE

    def add_player(self, player):
        """添加新玩家"""
        if player.name in self.players:
            raise ValueError(f"玩家 {player.name} 已存在")
        self.players[player.name] = player

    def remove_player(self, name):
        """删除玩家"""
        if name in self.players:
            del self.players[name]

    def get_player(self, name):
        """获取玩家对象"""
        return self.players.get(name)

    def save_players(self):
        """保存玩家数据到文件"""
        try:
            data = []
            for player in self.players.values():
                data.append(self.player_to_dict(player))
            with open(self.file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[PlayerDataManager] 保存失败: {e}")

    def load_players(self):
        """从文件加载玩家数据"""
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for entry in data:
                    player = self.dict_to_player(entry)
                    self.players[player.name] = player
        except FileNotFoundError:
            print(f"[PlayerDataManager] 数据文件未找到 ({self.file_path})")
        except json.JSONDecodeError as e:
            print(f"[PlayerDataManager] JSON解析错误: {e}")

    @staticmethod
    def player_to_dict(player):
        return {
            'name': player.name,
            'password': player.password,
            'level': player.level,
            'exp': player.exp,
            'cultivation_stage': player.cultivation_stage,
            'spiritual_power': player.spiritual_power,
            'cultivation_times': player.cultivation_times,
            'creation_time': player.creation_time.isoformat(),
            'breakthrough_history': [
                {'time': hist['time'].isoformat(),
                 'from': hist['from'],
                 'to': hist['to'],
                 'description': hist['description']}
                for hist in player.breakthrough_history
            ],
            'title': player.title,
            'avatar': player.avatar,
            'skills': player.skills.copy(),
            'items': player.items.copy(),
            'sect': player.sect,
            'partner': player.partner

        }

    @staticmethod
    def dict_to_player(entry):
        name = entry['name']
        player = Player(name)
        player.password = entry.get('password')
        player.level = entry['level']
        player.exp = entry['exp']
        player.cultivation_stage = entry['cultivation_stage']
        player.spiritual_power = entry['spiritual_power']
        player.cultivation_times = entry['cultivation_times']
        player.creation_time = datetime.fromisoformat(entry['creation_time'])
        player.breakthrough_history = entry['breakthrough_history']
        player.title = entry.get('title', '凡人')
        player.avatar = entry.get('avatar', 'images/avatars/初始.png')
        player.skills = entry.get('skills', [])
        player.items = entry.get('items', [])
        player.sect = entry.get('sect')
        player.partner = entry.get('partner')
        return player
