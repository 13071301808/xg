-- DLI sql
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/05/23 14:07:51 GMT+08:00
-- ******************************************************************** --
-- 用户主题大宽表
INSERT OVERWRITE TABLE yishou_data.dws_ys_sale_log_sp_user_info_log_detail_dt PARTITION (dt)
SELECT
    coalesce(log.special_date, main.special_date) special_date
    , coalesce(log.user_id,main.user_id) user_id
    , log.user_id as log_user_id
    , log.is_login
    , log.is_app_login
    , log.is_mini_login
    , log.goods_exposure_uv
    , log.goods_no_exposure_uv
    , log.goods_exposure_pv
    , log.goods_click_uv
    , log.goods_no_click_uv
    , log.goods_click_pv
    , log.goods_add_cart_uv
    , log.goods_no_add_cart_uv
    , log.goods_add_cart_pv
    , log.goods_add_cart_amount
    , log.goods_add_cart_num
    , main.sale_user_id
    , main.buy_num
    , main.buy_amount
    , main.pay_order_num
    , main.buy_goods_num
    , main.buy_goods_no_num
    , main.real_buy_num
    , main.real_buy_amount
    , main.real_pay_order_num
    , main.real_buy_goods_num
    , main.real_buy_goods_no_num
    , main.tail
    , main.reg_time
    , main.dianzhu_level
    , main.is_check
    , main.identity_type
    , main.is_b_port
    , main.is_active
    , main.first_half_order_amount
    , main.second_half_order_amount
    , main.business_age
    , main.shop_num
    , main.closing_special_date
    , main.user_name
    , main.sex
    , main.user_money
    , main.frozen_money
    , main.last_login_time
    , main.reg_ip
    , main.reg_ip_source
    , main.last_ip
    , main.login_count
    , main.level
    , main.referral
    , main.introduction_code
    , main.phone_markting_group
    , main.vip
    , main.channel
    , main.sub_channel
    , main.partner_uid
    , main.is_vip
    , main.vip_create_time
    , main.is_association
    , main.association_create_time
    , main.one_channel
    , main.two_channel
    , main.one_source
    , main.two_source
    , main.mon_time
    , main.mon_str
    , main.new_old_user
    , main.is_new
    , main.user_level
    , main.size_customer
    , main.user_fine_level
    , main.last_mon_pay_type
    , main.last_mon_pay
    , main.is_six_order_type
    , main.is_six_order
    , main.six_order_type
    , main.first_time
    , main.fifth_time
    , main.first_amount
    , main.first_buy_num
    , main.fifth_amount
    , main.fifth_buy_num
    , main.fifth_down_amount
    , main.fifth_down_buy_num
    , main.sixth_time
    , main.sixth_buy_amount
    , main.sixth_buy_num
    , main.source_id
    , main.is_b_port_v1
    , main.is_important_user
    , main.is_hight_potential_user
    , main.is_child_user
    , main.prognosis_child_user_type
    , main.province
    , main.city
    , main.province_region
    , main.has_search
    , main.real_new_old_user
    , main.real_is_new
    , main.real_user_level
    , main.real_size_customer
    , main.real_user_fine_level
    , main.real_last_mon_pay_type
    , main.real_last_mon_pay
    , main.real_is_six_order_type
    , main.real_is_six_order
    , main.real_six_order_type
    , main.real_first_time
    , main.real_fifth_time
    , main.real_first_amount
    , main.real_first_buy_num
    , main.real_fifth_amount
    , main.real_fifth_buy_num
    , main.real_fifth_down_amount
    , main.real_fifth_down_buy_num
    , main.real_sixth_time
    , main.real_sixth_buy_amount
    , main.real_sixth_buy_num
    , main.sixth_order_id
    , main.sixth_order_sn
    , main.sixth_up_buy_amount
    , main.sixth_up_buy_num
    , main.sixth_up_pay_order_num
    , main.real_sixth_order_id
    , main.real_sixth_order_sn
    , main.real_sixth_up_buy_amount
    , main.real_sixth_up_buy_num
    , main.real_sixth_up_pay_order_num
    , main.seventh_order_id
    , main.seventh_order_sn
    , main.seventh_time
    , main.seventh_buy_amount
    , main.seventh_buy_num
    , main.seventh_up_buy_amount
    , main.seventh_up_buy_num
    , main.seventh_up_pay_order_num
    , main.real_seventh_order_id
    , main.real_seventh_order_sn
    , main.real_seventh_time
    , main.real_seventh_buy_amount
    , main.real_seventh_buy_num
    , main.real_seventh_up_buy_amount
    , main.real_seventh_up_buy_num
    , main.real_seventh_up_pay_order_num
    , main.eighth_order_id
    , main.eighth_order_sn
    , main.eighth_time
    , main.eighth_buy_amount
    , main.eighth_buy_num
    , main.eighth_up_buy_amount
    , main.eighth_up_buy_num
    , main.eighth_up_pay_order_num
    , main.real_eighth_order_id
    , main.real_eighth_order_sn
    , main.real_eighth_time
    , main.real_eighth_buy_amount
    , main.real_eighth_buy_num
    , main.real_eighth_up_buy_amount
    , main.real_eighth_up_buy_num
    , main.real_eighth_up_pay_order_num
    ,to_char(coalesce(log.special_date,main.special_date),'yyyymmdd') as dt
FROM yishou_data.dws_ys_sale_log_sp_user_info_log_detail_dt_backup main
full join (
    select
        special_date,
        user_id,
        is_login,
        is_app_login,
        is_mini_login,
        goods_exposure_uv,
        goods_no_exposure_uv,
        goods_exposure_pv,
        goods_click_uv,
        goods_no_click_uv,
        goods_click_pv,
        goods_add_cart_uv,
        goods_no_add_cart_uv,
        goods_add_cart_pv,
        goods_add_cart_amount,
        goods_add_cart_num
  from yishou_data.dws_ys_log_sp_user_log_dt
  where dt between '${start}' and '${end}'
) log
on main.user_id = log.user_id and to_char(log.special_date,'yyyymmdd') = to_char(main.special_date,'yyyymmdd')
DISTRIBUTE BY dt
;