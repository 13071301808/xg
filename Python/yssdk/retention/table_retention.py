from yssdk.mysql.client import YsMysqlClient
from yssdk.obs.client import YsObsClient
from yssdk.dli.sql_client import YsDliSQLCLient
import pandas as pd
from yssdk.common.printter import Printter

class TableRetention:
    def __init__(self, conf_mode = 'prod'):
        self.__mysql_client = YsMysqlClient(conf_mode = conf_mode)
        self.__mysql_client.get_zzconnect()
        self.__obs_client = YsObsClient(conf_mode = conf_mode)
        self.__dli_client = YsDliSQLCLient(conf_mode = conf_mode)

    def get_table_retention_conf(self):
        """
        获取所有表的保留天数配置
        :return:
        """
        Printter.info("获取所有表的保留天数配置")
        data = self.__mysql_client.select('select * from data_center.table_retention_conf')
        column_list = self.__mysql_client.get_columns()
        df = pd.DataFrame(list(data), columns=column_list)
        table_retention_confs = df.to_dict(orient='records')
        return table_retention_confs

    def get_single_table_retention_conf(self, db_name, table_name):
        """
        获取单表的保留天数配置
        :param db_name:
        :param table_name:
        :return:
        """
        Printter.info("获取单表的保留天数配置")
        data = self.__mysql_client.select("select * from data_center.table_retention_conf where db_name = '%s' and table_name = '%s'"%(db_name, table_name))
        column_list = self.__mysql_client.get_columns()
        df = pd.DataFrame(list(data), columns=column_list)
        table_retention_conf = df.to_dict(orient='records')
        return table_retention_conf

    def table_retention(self):
        """
        获取表的保留天数配置，清理所有表的历史数据
        :return:
        """
        Printter.info("获取表的保留天数配置，清理所有表的历史数据")
        table_retention_confs = self.get_table_retention_conf()
        for conf in table_retention_confs:
            self.single_table_retention(conf['db_name'], conf['table_name'], conf['retention_days'])

    def single_table_retention(self, db_name, table_name, retention_days):
        """
        根据表的保留天数配置，清理单表的历史数据
        :param db_name:
        :param table_name:
        :param retention_days:
        :return:
        """
        Printter.info("根据%s.%s的保留天数配置，清理单表的历史数据"%(db_name, table_name))
        obs_location = self.__dli_client.get_table_obs_location(db_name, table_name)
        obs_bucket = obs_location.replace('obs://','')[1:15]
        obs_dir = obs_location.replace('obs://','')[16:-1]
        self.__obs_client.clean_date_partition_table_obs_data(obs_bucket, obs_dir, retention_days)
        self.__dli_client.clean_partitions(db_name, table_name, retention_days)

    def set_table_retention_conf(self,db_name, table_name, retention_days):
        """
        设置表的保留天数，设置已存在则更新
        :param db_name:
        :param table_name:
        :param retention_days:
        :return:
        """
        Printter.info("设置%s.%s的保留天数"%(db_name, table_name))
        sql = "insert into data_center.table_retention_conf(db_name,table_name,retention_days) values ('%s','%s',%d)" \
              "ON DUPLICATE KEY UPDATE retention_days = %d"%(db_name, table_name, retention_days, retention_days)
        self.__mysql_client.update(sql)

    def remove_table_retention_conf(self, db_name, table_name):
        """
        删除表的保留天数配置
        :param db_name:
        :param table_name:
        :return:
        """
        Printter.info("删除%s.%s的保留天数配置"%(db_name, table_name))
        sql = "delete from data_center.table_retention_conf where db_name = '%s' and table_name = '%s'"%(db_name, table_name)
        self.__mysql_client.update(sql)

if __name__ == '__main__':


    #table_retention = TableRetention(conf_mode='dev')
    #table_retention.set_table_retention('yishou_data_qa_test','test_table_1',4)
    #table_retention.remove_table_retention_conf('yishou_data_qa_test','test_table_1')

    table_retention = TableRetention(conf_mode='dev')
    table_list = ['ods_fmys_card_detail_dt'
        ,'ods_fmys_card_detail_record_dt'
        ,'ods_fmys_category_dt'
        ,'ods_fmys_category_record_dt'
        ,'ods_fmys_goods_dt'
        ,'ods_fmys_goods_record_dt'
        ,'ods_fmys_goods_ext_dt'
        ,'ods_fmys_goods_ext_record_dt'
        ,'ods_fmys_goods_lib_dt'
        ,'ods_fmys_goods_lib_record_dt'
        ,'ods_fmys_goods_lib_ext_dt'
        ,'ods_fmys_goods_lib_ext_record_dt'
        ,'ods_fmys_goods_sku_dt'
        ,'ods_fmys_goods_sku_record_dt'
        ,'ods_fmys_order_dt'
        ,'ods_fmys_order_record_dt'
        ,'ods_fmys_order_cancel_record_dt'
        ,'ods_fmys_order_cancel_record_record_dt'
        ,'ods_fmys_order_infos_dt'
        ,'ods_fmys_order_infos_record_dt'
        ,'ods_fmys_region_dt'
        ,'ods_fmys_region_record_dt'
        ,'ods_fmys_special_dt'
        ,'ods_fmys_special_record_dt'
        ,'ods_fmys_stock_in_record_dt'
        ,'ods_fmys_stock_in_record_record_dt'
        ,'ods_fmys_users_dt'
        ,'ods_fmys_users_record_dt'
        ,'ods_fmys_cart_dt'
        ,'ods_fmys_cart_record_dt'
        ,'ods_fmys_new_user_pre_stock_goods_dt'
        ,'ods_fmys_new_user_pre_stock_goods_record_dt'
        ,'ods_fmys_new_user_pre_stock_supply_dt'
        ,'ods_fmys_new_user_pre_stock_supply_record_dt'
        ,'ods_fmys_coupon_freight_dt'
        ,'ods_fmys_coupon_freight_record_dt'
        ,'ods_fmys_return_profit_bill_dt'
        ,'ods_fmys_return_profit_bill_record_dt'
        ,'ods_fmys_return_profit_bill_detail_dt'
        ,'ods_fmys_return_profit_bill_detail_record_dt'
        ,'ods_fmys_return_profit_bill_log_dt'
        ,'ods_fmys_return_profit_bill_log_record_dt'
        ,'ods_fmys_return_profit_roll_back_dt'
        ,'ods_fmys_return_profit_roll_back_record_dt'
        ,'ods_fmys_return_profit_stats_dt'
        ,'ods_fmys_return_profit_stats_record_dt'
        ,'ods_fmys_return_profit_stats_detail_dt'
        ,'ods_fmys_return_profit_stats_detail_record_dt'
        ,'ods_fmys_aso_channel_info_dt'
        ,'ods_fmys_utm_users_dt'
        ,'ods_fmys_aso_idfa_dt'
        ,'ods_fmys_vip_recall_dt'
        ,'ods_fmys_association_user_dt'
        ,'ods_fmys_aso_channel_user_dt'
        ,'ods_fmys_temp_stock_dbid_dt'
        ,'ods_fmys_reject_detail_dt'
        ,'ods_fmys_reject_detail_record_dt'
        ,'ods_fmys_wcg_order_plan_infos_dt'
        ,'ods_fmys_wcg_order_plan_infos_record_dt'
        ,'ods_fmys_wcg_plan_info_dt'
        ,'ods_fmys_wcg_plan_info_record_dt'
        ,'ods_fmys_wcg_plan_dt'
        ,'ods_fmys_wcg_plan_record_dt'
        ,'ods_fmys_wcg_plan_goods_label_dt'
        ,'ods_fmys_wcg_plan_goods_label_record_dt'
        ,'ods_fmys_order_plan_all_detail_dt'
        ,'ods_fmys_order_plan_all_detail_record_dt'
        ,'ods_fmys_wcg_order_info_dt'
        ,'ods_fmys_wcg_order_info_record_dt'
        ,'ods_fmys_wcg_order_dt'
        ,'ods_fmys_wcg_order_record_dt'
        ,'ods_fmys_wcg_order_ext_dt'
        ,'ods_fmys_wcg_order_ext_record_dt'
        ,'ods_fmys_supply_activity_fee_dt'
        ,'ods_fmys_supply_activity_fee_record_dt'
        ,'ods_fmys_wcg_owe_record_dt'
        ,'ods_fmys_wcg_owe_record_record_dt'
        ,'ods_fmys_supply_settle_bill_info_dt'
        ,'ods_fmys_supply_settle_bill_info_record_dt'
        ,'ods_fmys_supply_settle_bill_dt'
        ,'ods_fmys_supply_settle_bill_record_dt'
        ,'ods_fmys_da_biao_record_dt'
        ,'ods_fmys_da_biao_record_record_dt'
        ,'ods_fmys_wcg_refund_detail_dt'
        ,'ods_fmys_wcg_refund_detail_record_dt'
        ,'ods_fmys_wcg_refund_dt'
        ,'ods_fmys_wcg_refund_record_dt'
        ,'ods_fmys_wcg_ded_dt'
        ,'ods_fmys_wcg_ded_record_dt'
        ,'ods_fmys_wcg_ded_detail_dt'
        ,'ods_fmys_wcg_ded_detail_record_dt'
        ,'ods_fmys_stagnant_debts_record_dt'
        ,'ods_fmys_stagnant_debts_record_record_dt'
        ,'ods_fmys_broken_debts_record_dt'
        ,'ods_fmys_broken_debts_record_record_dt'
        ,'ods_fmys_wcg_owe_all_price_dt'
        ,'ods_fmys_wcg_owe_all_price_record_dt'
        ,'ods_fmys_dbid_dt'
        ,'ods_fmys_dbid_record_dt'
        ,'ods_fmys_work_dbid_dt'
        ,'ods_fmys_work_dbid_record_dt'
        ,'ods_fmys_supply_activity_fee_ade_dt'
        ,'ods_fmys_supply_activity_fee_ade_record_dt'
        ,'ods_fmys_shipping_space_dt'
        ,'ods_fmys_shipping_space_record_dt'
        ,'ods_fmys_stock_out_record_dt'
        ,'ods_fmys_stock_out_record_record_dt'
        ,'ods_fmys_wave_picking_dbid_dt'
        ,'ods_fmys_wave_picking_dbid_record_dt'
        ,'ods_fmys_wave_picking_info_dt'
        ,'ods_fmys_wave_picking_info_record_dt'
        ,'ods_fmys_wave_picking_dt'
        ,'ods_fmys_wave_picking_record_dt'
        ,'ods_fmys_shipping_detail_dt'
        ,'ods_fmys_shipping_detail_record_dt'
        ,'ods_fmys_service_order_dt'
        ,'ods_fmys_service_order_record_dt'
        ,'ods_fmys_problem_type_dt'
        ,'ods_fmys_problem_type_record_dt'
        ,'ods_fmys_service_order_ext_dt'
        ,'ods_fmys_service_order_ext_record_dt'
        ,'ods_fmys_order_infos_subsidiary_dt'
        ,'ods_fmys_order_refund_record_dt'
        ,'ods_fmys_refund_abtest_user_dt'
        ,'ods_fmys_reject_dt'
        ,'ods_fmys_reject_record_dt'
        ,'ods_fmys_express_cost_detail_dt'
        ,'ods_fmys_express_cost_detail_record_dt'
        ,'ods_fmys_express_cost_dt'
        ,'ods_fmys_express_cost_record_dt'
        ,'ods_fmys_order_infos_cancel_dt'
        ,'ods_fmys_order_infos_cancel_record_dt'
        ,'ods_fmys_order_back_record_dt'
        ,'ods_fmys_order_back_record_record_dt'
        ,'ods_fmys_no_reason_order_dt'
        ,'ods_fmys_no_reason_order_record_dt'
        ,'ods_fmys_seckill_goods_dt'
        ,'ods_fmys_seckill_goods_record_dt'
        ,'ods_fmys_seckill_dt'
        ,'ods_fmys_seckill_record_dt'
        ,'ods_fmys_admin_dt'
        ,'ods_fmys_admin_record_dt'
        ,'ods_fmys_department_group_dt'
        ,'ods_fmys_department_group_record_dt'
        ,'ods_fmys_goods_in_stock_dt'
        ,'ods_fmys_goods_in_stock_record_dt'
        ,'ods_fmys_tail_goods_discount_detail_dt'
        ,'ods_fmys_tail_goods_discount_detail_record_dt'
        ,'ods_fmys_pinkage_order_dt'
        ,'ods_fmys_pinkage_order_record_dt'
        ,'ods_fmys_service_order_by_app_dt'
        ,'ods_fmys_service_order_by_app_record_dt'
        ,'ods_fmys_goods_fifteen_data_dt'
        ,'ods_fmys_goods_fifteen_data_record_dt']

    table_list = ['ods_fmys_goods_fifteen_data_record_dt']

    for table in table_list:
        table_retention.single_table_retention('yishou_data',table,7)

