# coding=utf-8
conf_dir = '/home/data_user/binlog_config/binlog_conf_prod.ini'

database = {'prod':
                 {
                     'yishou_daily_dbname':'yishou_daily',
                     'yishou_data_dbname':'yishou_data',
                     'yishou_apex_dbname':'yishou_apex',
                     'yishou_pay_dbname':'yishou_pay',
                     'yishou_dws_finebi_dbname':'finebi',
                     'yishou_dws_cross_origin_dbname':'cross_origin',
                     'bdp.system.bizdate':'20220905',
                     'bizdate':'20220905'
                 },
             'test':
                 {
                     'yishou_daily_dbname':'yishou_daily_test',
                     'yishou_data_dbname':'yishou_data_qa_test',
                     'yishou_apex_dbname':'yishou_apex_test',
                     'yishou_pay_dbname':'yishou_pay_test',
                     'yishou_dws_finebi_dbname':'finebi_test',
                     'yishou_dws_cross_origin_dbname':'cross_origin_test'
                 }
}