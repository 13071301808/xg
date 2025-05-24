-- DLI sql
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/05/23 11:01:00 GMT+08:00
-- ******************************************************************** --
-- 一二三级页面常用用户指标
with ex as (
    select
        CASE
            WHEN special_date LIKE '% %' THEN special_date
            ELSE to_date1(regexp_replace(ex.special_date,'-',''),'yyyymmdd')
        END AS special_date
        ,ex.user_id ex_user_id
        ,ex.first_page_name
        ,ex.second_page_name
        ,ex.third_page_name
        ,ex.dt
        ,count(distinct ex.goods_id) goods_exposure_uv
    from yishou_daily.route_all_event_exposure_detail_v2 ex
    where ex.dt between '${start}' and '${end}'
    group by 1,2,3,4,5,6
)
INSERT OVERWRITE TABLE yishou_daily.third_page_name_users_dt partition(dt)
SELECT
    main.special_date
    , main.user_id
    , main.user_id_tail
    , main.ex_user_id
    , main.cl_user_id
    , main.ad_user_id
    , main.pa_user_id
    , main.first_page_name
    , main.second_page_name
    , main.third_page_name
    , main.country
    , main.area
    , main.province
    , main.province_region
    , main.city
    , main.reg_time
    , main.order_num
    , main.dianzhu_level
    , main.first_time
    , main.real_first_time
    , main.first_month
    , main.real_first_month
    , main.is_b_port
    , main.crm_identity_type
    , main.head_number
    , main.user_md5_tail
    , main.is_new
    , main.real_is_new
    , main.size_customer
    , main.real_size_customer
    , main.primary_channel_name
    , main.second_channel_name
    , main.is_private_user
    , main.strategy_type
    , ex.goods_exposure_uv
    , main.goods_click_uv
    , main.goods_add_uv
    , main.goods_pay_uv
    , main.buy_amount
    , main.buy_num
    , main.max_app_version
    , main.join_goods_add_uv
    , main.add_cart_num
    , main.add_cart_amount
    , main.real_gmv
    , main.real_buy_num
    , main.add_cart_pv
    , main.real_goods_pay_uv
    , main.dt
from yishou_daily.third_page_name_users_dt_backup main
left join ex
on ex.special_date = main.special_date and ex.ex_user_id = main.user_id
and ex.first_page_name = main.first_page_name and ex.second_page_name = main.second_page_name
and ex.third_page_name = main.third_page_name and ex.dt = main.dt
where main.dt between '${start}' and '${end}'
DISTRIBUTE BY dt
;

