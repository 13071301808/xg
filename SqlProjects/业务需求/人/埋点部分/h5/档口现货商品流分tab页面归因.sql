-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/04/24 15:50:20 GMT+08:00
-- ******************************************************************** --
-- select 
--     special_date
--     ,case 
--         when four_page_name = '档口现货搜索结果' then '现货频道-搜索栏'
--         when four_page_name like '档口现货轮播图%' then '现货频道-轮播banner'
--         when four_page_name in ('档口现货快速补货','档口现货大牌联合入仓') then '现货频道-轮播下两坑运营位'
--         when four_page_name in ('档口现货市场特价','档口现货市场新款','档口现货返单爆款') then '现货频道-三坑运营位'
--         when four_page_name like '档口现货品类%' then '现货频道-品类八坑运营位'
--     end as model_name
--     ,'现有埋点暂时无法识别' as second_model_name
--     ,four_page_name as page_name
--     ,coalesce(count(DISTINCT case when goods_exposure_uv > 0 then user_id else null end),0) as exposure_uv
--     ,coalesce(count(DISTINCT case when goods_click_uv > 0 then user_id else null end),0) as click_uv
--     ,coalesce(count(DISTINCT case when goods_add_uv > 0 then user_id else null end),0) as add_cart_uv
--     ,coalesce(count(DISTINCT case when goods_pay_uv > 0 then user_id else null end),0) as pay_uv
--     ,coalesce(sum(goods_exposure_uv),0) as goods_exposure_uv
--     ,coalesce(sum(goods_click_uv),0) as goods_click_uv
--     ,coalesce(sum(goods_add_uv),0) AS goods_add_uv
--     ,coalesce(round(sum(real_gmv),2),0) as real_gmv
--     ,coalesce(sum(real_buy_num),0) as real_buy_num
-- from yishou_daily.temp_route_all_event_flow_detail_h5_dt
-- where dt between '${start}' and '${end}' and four_page_name is not null
-- group by 1,2,3,4
-- union all 
-- select 
--     special_date
--     , model_name
--     , '现有埋点暂时无法识别'  as second_model_name
--     , page_name
--     , coalesce(sum(exposure_uv),0) as exposure_uv
--     , coalesce(sum(click_uv),0) as click_uv
--     , coalesce(sum(add_cart_uv),0) as add_cart_uv
--     , coalesce(sum(pay_uv),0) as pay_uv
--     , coalesce(sum(goods_exposure_uv),0) as goods_exposure_uv
--     , coalesce(sum(goods_click_uv),0) as goods_click_uv
--     , coalesce(sum(goods_add_uv),0) as goods_add_uv
--     , coalesce(round(sum(real_gmv),2),0) as real_gmv
--     , coalesce(sum(real_buy_num),0) as real_buy_num
-- from yishou_daily.temp_route_supply_goods_flow_detail_h5_dt
-- where dt between '${start}' and '${end}' and page_name is not null
-- group by 1,2,3,4
-- ;

-- 验数
-- select 
--     dt
--     ,count(DISTINCT case when h5_exposure_pv > 0 then user_id end) as h5_exposure_uv
--     ,count(DISTINCT case when click_pv > 0 then user_id end) as click_uv
--     ,count(DISTINCT case when addcart_pv > 0 then user_id end) addcart_uv
--     ,sum(real_gmv) as real_gmv
--     ,sum(real_pay_num) as real_pay_num
-- from yishou_data.dwm_page_name_user_goods_incr_dt
-- where dt between '20250401' and '20250425'
-- and source_code like '%档口有现货%'
-- group by dt 
-- ;

-- drop table yishou_daily.temp_route_supply_goods_flow_detail_h5_dt;
-- CREATE EXTERNAL TABLE yishou_daily.temp_route_supply_goods_flow_detail_h5_dt (
--     special_date string comment ''
--     ,model_name string comment ''
--     ,second_model_name string comment ''
--     ,page_name string comment ''
--     ,exposure_uv string comment ''
--     ,click_uv string comment ''
--     ,add_cart_uv string comment ''
--     ,pay_uv string comment ''
--     ,goods_exposure_uv string comment ''
--     ,goods_click_uv string comment ''
--     ,goods_add_uv string comment ''
--     ,real_gmv string comment ''
--     ,real_buy_num string comment ''
-- )
-- comment '档口现货商品流tab流量数据'
-- partitioned by (dt string)
-- STORED AS ORC LOCATION 'obs://yishou-bigdata/yishou_daily.db/temp_route_supply_goods_flow_detail_h5_dt'
-- ;

-- 档口现货h5页面tab商品流量
with ex as (
    select 
        to_char(datetrunc(from_unixtime(log_time-25200),'dd'),'yyyymmdd') as special_date
        ,tab_name as page_name
        ,user_id
        ,dt
        ,count(DISTINCT goods_id) as goods_exposure_uv
    from yishou_data.dcl_h5_ys_h5_goods_exposure 
    where dt between '${one_day_ago}' and '${gmtdate}'
    and to_char(datetrunc(from_unixtime(log_time-25200),'dd'),'yyyymmdd') = '${one_day_ago}'
    and title like '%档口有现货%'
    group by 1,2,3,4
)
,cl as (
    select
        to_char(special_date,'yyyymmdd') as special_date
        ,user_id
        ,dt
        ,count(DISTINCT goods_id) as goods_click_uv
    from yishou_data.dwd_log_goods_detail_page_dt
    where dt between '${one_day_ago}' and '${gmtdate}'
    and to_char(special_date,'yyyymmdd') = '${one_day_ago}'
    and source = '115974'
    group by 1,2,3
)
,ad as (
    select 
        coalesce(cart.special_date,pay.special_date) special_date
        ,cart.user_id
        ,coalesce(cart.dt,pay.dt) as dt
        ,max(case when pay.user_id is not null then 1 else 0 end) as is_pay
        ,count(DISTINCT case when cart.user_id is not null then cart.user_id end,cart.goods_id) as goods_add_uv
        ,sum(pay.real_gmv) as real_gmv
        ,sum(pay.real_buy_num) as real_buy_num
    from (
        select 
            to_char(special_date,'yyyymmdd') as special_date
            ,user_id
            ,goods_id
            ,dt
        from yishou_data.dwd_log_add_cart_dt
        where dt between '${one_day_ago}' and '${gmtdate}'
        and to_char(special_date,'yyyymmdd') = '${one_day_ago}'
        and source = '115974'
        group by 1,2,3,4
    ) cart
    full join (
        select 
            to_char(dateadd(add_time,-7,'hh'),'yyyymmdd') special_date
            ,user_id
            ,goods_id
            ,dt
            ,sum(case when is_real_pay = 1 then buy_num * shop_price else 0 end) real_gmv
    		,sum(case when is_real_pay = 1 then buy_num else 0 end) real_buy_num
        from yishou_data.dwd_sale_order_info_dt
        where dt between '${one_day_ago}' and '${gmtdate}'
        and to_char(dateadd(add_time,-7,'hh'),'yyyymmdd') = '${one_day_ago}'
        and sa_source = '115974'
        and pay_status = 1
        group by 1,2,3,4
    )pay on cart.goods_id = pay.goods_id and cart.special_date = pay.special_date and cart.user_id = pay.user_id and cart.dt = pay.dt
    where cart.user_id is not null
    group by 1,2,3
)
insert overwrite table yishou_daily.temp_route_supply_goods_flow_detail_h5_dt PARTITION(dt)
select 
    coalesce(ex.special_date,cl.special_date,ad.special_date) special_date
    ,'现货频道-商品流' as model_name
    ,'现货频道-商品流-tab' as second_model_name
    ,ex.page_name
    ,count(DISTINCT ex.user_id) as exposure_uv
    ,count(DISTINCT cl.user_id) as click_uv
    ,count(DISTINCT ad.user_id) as add_cart_uv
    ,count(DISTINCT case when ad.is_pay = 1 then ad.user_id end) as pay_uv
    ,sum(ex.goods_exposure_uv) as goods_exposure_uv
    ,sum(cl.goods_click_uv) as goods_click_uv
    ,sum(ad.goods_add_uv) as goods_add_uv
    ,sum(ad.real_gmv) as real_gmv
    ,sum(ad.real_buy_num) as real_buy_num
    ,coalesce(ex.dt,cl.dt,ad.dt) dt
from ex 
full join cl 
on ex.user_id = cl.user_id 
and ex.special_date = cl.special_date 
and ex.dt = cl.dt 
full join ad 
on coalesce(ex.user_id,cl.user_id) = ad.user_id 
and coalesce(ex.special_date,cl.special_date) = ad.special_date 
and coalesce(ex.dt,cl.dt) = ad.dt 
where ex.page_name is not null
and coalesce(ex.special_date,cl.special_date,ad.special_date) = '${one_day_ago}'
group by 1,2,3,4,coalesce(ex.dt,cl.dt,ad.dt)
DISTRIBUTE BY dt 
;