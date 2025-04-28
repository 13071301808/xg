-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/04/24 11:44:40 GMT+08:00
-- ******************************************************************** --
-- CREATE EXTERNAL TABLE yishou_daily.temp_route_all_event_flow_detail_h5_dt (
--     special_date string comment ''
--     ,user_id string comment ''
--     ,first_page_name  string comment ''
--     ,second_page_name string comment '' 
--     ,third_page_name string comment '' 
--     ,four_page_name  string comment ''
--     ,goods_exposure_uv string comment ''
--     ,goods_click_uv string comment ''
--     ,goods_add_uv string comment ''
--     ,goods_pay_uv string comment ''
--     ,add_cart_num string comment ''
--     ,add_cart_amount string comment ''
--     ,real_gmv string comment ''
--     ,real_buy_num string comment '' 
-- )
-- comment '首页h5路径归因4级页面整合'
-- partitioned by (dt string)
-- STORED AS ORC LOCATION 'obs://yishou-bigdata/yishou_daily.db/temp_route_all_event_flow_detail_h5_dt'
-- ;

with ex as (
    select 
        to_char(ex.special_date,'yyyymmdd') special_date  
        ,ex.user_id ex_user_id 
        ,ex.first_page_name
        ,ex.second_page_name
        ,ex.third_page_name
        ,ex.four_page_name
        ,ex.dt
        ,count(distinct ex.goods_id) goods_exposure_uv
    from yishou_daily.temp_route_all_event_exposure_detail_h5_dt ex 
    where ex.dt between '${one_day_ago}' and '${gmtdate}'
    and to_char(ex.special_date,'yyyymmdd') between '${one_day_ago}' and '${gmtdate}'
    group by 1,2,3,4,5,6,7
),
cl as (
    select 
        to_char(cl.special_date,'yyyymmdd') special_date  
        ,cl.user_id cl_user_id
        ,cl.first_page_name
        ,cl.second_page_name
        ,cl.third_page_name
        ,cl.four_page_name
        ,cl.dt 
        ,count(distinct cl.goods_id) goods_click_uv
    from yishou_daily.temp_route_all_event_clickaddcart_detail_h5_dt cl 
    where cl.dt between '${one_day_ago}' and '${gmtdate}'
    and to_char(cl.special_date,'yyyymmdd') between '${one_day_ago}' and '${gmtdate}'
    and page_name = '商品详情'
    group by 1,2,3,4,5,6,7
),
ad as (
    select 
        coalesce(cart.special_date,pay.special_date) special_date 
        ,cart.user_id ad_user_id
        ,coalesce(cart.first_page_name,pay.first_page_name) first_page_name
        ,coalesce(cart.second_page_name,pay.second_page_name) second_page_name
        ,coalesce(cart.third_page_name,pay.third_page_name) third_page_name
        ,coalesce(cart.four_page_name,pay.four_page_name) four_page_name
        ,coalesce(cart.dt,pay.dt) dt
        ,count(DISTINCT case when cart.user_id is not null then cart.goods_id end) goods_add_uv
        ,sum(cart.add_cart_num) add_cart_num
        ,sum(cart.add_cart_amount) add_cart_amount
        ,count(DISTINCT case when buy_num > 0 then pay.goods_id end) goods_pay_uv
        ,sum(pay.gmv) gmv
        ,sum(pay.buy_num) buy_num  
        ,sum(pay.real_gmv) real_gmv
        ,sum(real_buy_num) real_buy_num  
        ,count(distinct case when real_buy_num > 0 then pay.goods_id end) real_goods_pay_uv
    from (
        select 
            to_char(ad.special_date,'yyyymmdd') special_date  
            ,ad.user_id 
            ,ad.goods_id
            ,ad.first_page_name
            ,ad.second_page_name
            ,ad.third_page_name
            ,ad.four_page_name
            ,ad.dt
            ,sum(add_cart_num) add_cart_num
            ,sum(add_cart_amount) add_cart_amount
        from yishou_daily.temp_route_all_event_clickaddcart_detail_h5_dt ad 
        where ad.dt between '${one_day_ago}' and '${gmtdate}'
        and to_char(ad.special_date,'yyyymmdd') between '${one_day_ago}' and '${gmtdate}'
        and page_name = '加购'
        group by 1,2,3,4,5,6,7,8
    ) cart 
    full join (
        select 
            to_char(pa.special_date,'yyyymmdd') special_date  
            ,pa.user_id
            ,pa.goods_id
            ,pa.first_page_name
            ,pa.second_page_name
            ,pa.third_page_name
            ,pa.four_page_name
            ,pa.dt 
            ,sum(gmv) gmv
            ,sum(buy_num) buy_num  
            ,sum(real_gmv) real_gmv
            ,sum(real_buy_num) real_buy_num  
        from yishou_daily.temp_route_all_event_gmv_detail_h5_dt pa 
        where pa.dt between '${one_day_ago}' and '${gmtdate}'
        and to_char(pa.special_date,'yyyymmdd') between '${one_day_ago}' and '${gmtdate}'
        group by 1,2,3,4,5,6,7,8
    )pay 
    on cart.special_date = pay.special_date and cart.user_id = pay.user_id 
    and cart.goods_id = pay.goods_id
    and pay.first_page_name = cart.first_page_name and pay.second_page_name = cart.second_page_name
    and pay.third_page_name = cart.third_page_name and pay.four_page_name = cart.four_page_name
    and cart.dt = pay.dt
    where cart.user_id is not null
    group by 1,2,3,4,5,6,7
)
insert overwrite TABLE yishou_daily.temp_route_all_event_flow_detail_h5_dt PARTITION(dt)
select 
    coalesce(ex.special_date,cl.special_date,ad.special_date) special_date
    ,coalesce(ex.ex_user_id,cl.cl_user_id,ad.ad_user_id) user_id
    ,coalesce(ex.first_page_name,cl.first_page_name,ad.first_page_name) first_page_name 
    ,coalesce(ex.second_page_name,cl.second_page_name,ad.second_page_name) second_page_name 
    ,coalesce(ex.third_page_name,cl.third_page_name,ad.third_page_name) third_page_name 
    ,coalesce(ex.four_page_name,cl.four_page_name,ad.four_page_name) four_page_name 
    ,sum(goods_exposure_uv) goods_exposure_uv
    ,sum(goods_click_uv) goods_click_uv
    ,sum(goods_add_uv) goods_add_uv
    ,sum(goods_pay_uv) goods_pay_uv
    ,sum(add_cart_num) add_cart_num
    ,sum(add_cart_amount) add_cart_amount
    ,sum(real_gmv) real_gmv
    ,sum(real_buy_num) real_buy_num  
    ,coalesce(ex.dt,cl.dt,ad.dt) dt
from ex 
full join cl 
on ex.ex_user_id = cl.cl_user_id 
and ex.special_date = cl.special_date 
and ex.first_page_name =cl.first_page_name 
and ex.second_page_name = cl.second_page_name 
and ex.third_page_name = cl.third_page_name 
and ex.four_page_name = cl.four_page_name 
and ex.dt = cl.dt 
full join ad 
on coalesce(ex.ex_user_id,cl.cl_user_id) = ad.ad_user_id 
and coalesce(ex.special_date,cl.special_date) = ad.special_date 
and coalesce(ex.first_page_name,cl.first_page_name) =ad.first_page_name 
and coalesce(ex.second_page_name,cl.second_page_name) = ad.second_page_name 
and coalesce(ex.third_page_name,cl.third_page_name) = ad.third_page_name 
and coalesce(ex.four_page_name,cl.four_page_name) = ad.four_page_name 
and coalesce(ex.dt,cl.dt) = ad.dt 
group by 1,2,3,4,5,6,coalesce(ex.dt,cl.dt,ad.dt)
DISTRIBUTE BY dt 
;


