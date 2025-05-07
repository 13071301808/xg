# coding=utf-8
import sys

from yssdk.dgc.history_runner import HistoryRunner

if __name__ == '__main__':

    """
    案例说明：需要重跑DGC脚本 20210101 - 20210105 的数据，即bdp.system.bizdate的范围是20210101 - 20210105
    脚本名是concurrent_run_history
    """
    #创建HistoryRunner对象
    runner = HistoryRunner(thread_num=5,
                           start_date='20210101',
                           end_date='20210105',
                           workspace_name='yishou_data',
                           script_name='concurrent_run_history',
                           conf_mode='dev')
    # 开始补数据
    runner.rerun()