# -*-coding: Utf-8 -*-
# @File : date_generator .py
# author: ShanFeng
# Time：2024/3/28/0028

from datetime import datetime, timedelta


def print_info(*message):
    print('>>>>> %s [INFO]: %s' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), ",".join(message)))


class DateGenerator(object):
    def __init__(self, start_date, end_date, date_format='%Y%m%d', out=True):
        """
        :param start_date: 开始日期, 传入yyyymmdd格式
        :param end_date: 结束日期, 传入yyyymmdd格式
        :param date_format: 日期格式, 默认生成yyyymmdd格式
        """
        self.__start_date = str(start_date)
        self.__end_date = str(end_date)
        self.__date_format = date_format
        self.__out = out
        if self.__out:
            print_info(f'日期生成器构建中, 开始日期:{start_date}, 结束日期:{end_date}, 输出日期格式为:{date_format}')

    def generate_range_date(self):
        if self.__out:
            print_info(f'生成开始至结束的日期列表, 适用于单个参数情况')

        date_list = []

        start_date_obj = datetime.strptime(self.__start_date, '%Y%m%d')
        end_date_obj = datetime.strptime(self.__end_date, '%Y%m%d')

        while start_date_obj <= end_date_obj:
            date_list.append(start_date_obj.strftime(self.__date_format))
            start_date_obj += timedelta(days=1)

        return date_list

    def generate_month_ranges_date(self):
        if self.__out:
            print_info(f'生成含多个子列表(结尾为下月1号)的日期列表, 适用于按月补数, 且结尾日期不为补数日期的情况')
        date_list = []

        start_date_obj = datetime.strptime(self.__start_date, '%Y%m%d')
        end_date_obj = datetime.strptime(self.__end_date, '%Y%m%d')

        while start_date_obj < end_date_obj:
            next_month_start = start_date_obj.replace(day=1) + timedelta(days=32)
            next_month_start = next_month_start.replace(day=1)  # 获取下个月的第一天
            if next_month_start > end_date_obj:
                next_month_start = end_date_obj + timedelta(days=1)
            date_list.append(
                [start_date_obj.strftime(self.__date_format), next_month_start.strftime(self.__date_format)])
            start_date_obj = next_month_start
        return date_list

    def generate_month_ranges_date_without_end(self):
        if self.__out:
            print_info(f'生成含多个子列表(结尾为本月)的日期列表, 适用于按月补数, 且结尾日期为补数日期的情况')
        date_list = []

        start_date_obj = datetime.strptime(self.__start_date, '%Y%m%d')
        end_date_obj = datetime.strptime(self.__end_date, '%Y%m%d')

        while start_date_obj < end_date_obj:
            month_end = start_date_obj.replace(day=1) + timedelta(days=32)
            month_end = month_end.replace(day=1) - timedelta(days=1)  # 获取月末日期
            if month_end > end_date_obj:
                month_end = end_date_obj
            date_list.append([start_date_obj.strftime(self.__date_format), month_end.strftime(self.__date_format)])
            start_date_obj = month_end + timedelta(days=1)
        return date_list

    def generate_custom_ranges_date(self, interval):
        if self.__out:
            print_info(f'生成含多个子列表(结尾为下一个日期开始)的日期列表, 适用于自定义天数补数, 且结尾日期不为补数日期的情况')
        date_list = []

        start_date_obj = datetime.strptime(self.__start_date, '%Y%m%d')
        end_date_obj = datetime.strptime(self.__end_date, '%Y%m%d')

        while start_date_obj < end_date_obj:
            next_month_start = start_date_obj + timedelta(days=interval)
            if next_month_start > end_date_obj:
                next_month_start = end_date_obj + timedelta(days=1)
            date_list.append(
                [start_date_obj.strftime(self.__date_format), next_month_start.strftime(self.__date_format)])
            start_date_obj = next_month_start
        return date_list

    def generate_custom_ranges_date_without_end(self, interval):
        if self.__out:
            print_info(f'生成含多个子列表(结尾为当前日期段末)的日期列表, 适用于自定义天数补数, 且结尾日期为补数日期的情况')
        date_list = []

        start_date_obj = datetime.strptime(self.__start_date, '%Y%m%d')
        end_date_obj = datetime.strptime(self.__end_date, '%Y%m%d')

        while start_date_obj < end_date_obj:
            next_month_start = start_date_obj + timedelta(days=interval)
            if next_month_start > end_date_obj:
                next_month_start = end_date_obj
            date_list.append(
                [start_date_obj.strftime(self.__date_format), next_month_start.strftime(self.__date_format)])
            start_date_obj = next_month_start + timedelta(days=1)
        return date_list

    @staticmethod
    def one_day_ago(date_format='%Y%m%d'):
        today = datetime.now()
        one_day_ago = (today - timedelta(days=1)).strftime(date_format)
        print_info(f"获取前一天日期: {one_day_ago}")
        return one_day_ago

    @staticmethod
    def today(date_format='%Y%m%d'):
        today = datetime.now().strftime(date_format)
        print_info(f"获取当天日期: {today}")

        return today

    @staticmethod
    def interval_day(interval, date_format='%Y%m%d'):
        today = datetime.now()
        interval_day = (today + timedelta(days=interval)).strftime(date_format)
        print_info(f"获取{interval}天后日期: {interval_day}") if interval > 0 else print_info(f"获取{abs(interval)}天前日期: {interval_day}")

        return interval_day

    def init_format(self, date_format='%Y%m%d'):
        self.__date_format = date_format
        if self.__out:
            print_info(f'初始化输出日期格式为{date_format}')

    def generate_month_ranges_date_3(self):
        if self.__out:
            print_info(f'生成含多个子列表(3参数,结尾为下月1号)的日期列表, 适用于按月补数, 且结尾日期不为补数日期的情况')
        date_list = []

        start_date_obj = datetime.strptime(self.__start_date, '%Y%m%d')
        end_date_obj = datetime.strptime(self.__end_date, '%Y%m%d')

        while start_date_obj < end_date_obj:
            next_month_start = start_date_obj.replace(day=1) + timedelta(days=32)
            next_month_start = next_month_start.replace(day=1)  # 获取下个月的第一天
            if next_month_start > end_date_obj:
                next_month_start = end_date_obj + timedelta(days=1)
            middle_date = next_month_start - timedelta(days=1)

            date_list.append(
                [start_date_obj.strftime(self.__date_format), middle_date.strftime(self.__date_format), next_month_start.strftime(self.__date_format)])
            start_date_obj = next_month_start
        return date_list

    def generate_custom_ranges_date_3(self, interval):
        if self.__out:
            print_info(f'生成含多个子列表(结尾为下一个日期开始)的日期列表, 适用于自定义天数补数, 且结尾日期不为补数日期的情况')
        date_list = []

        start_date_obj = datetime.strptime(self.__start_date, '%Y%m%d')
        end_date_obj = datetime.strptime(self.__end_date, '%Y%m%d')

        while start_date_obj < end_date_obj:
            next_month_start = start_date_obj + timedelta(days=interval)
            if next_month_start > end_date_obj:
                next_month_start = end_date_obj + timedelta(days=1)
            middle_date = next_month_start - timedelta(days=1)
            date_list.append(
                [start_date_obj.strftime(self.__date_format), middle_date.strftime(self.__date_format), next_month_start.strftime(self.__date_format)])
            start_date_obj = next_month_start
        return date_list
