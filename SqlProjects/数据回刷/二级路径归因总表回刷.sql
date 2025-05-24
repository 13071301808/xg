-- DLI sql
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/05/21 17:05:21 GMT+08:00
-- ******************************************************************** --
insert OVERWRITE table yishou_daily.route_12page_exposure_detail_dt partition (dt)
SELECT
    to_date1(regexp_replace(special_date,'-',''),'yyyymmdd') special_date
    ,goods_id
    ,user_id
    ,first_page_name
    ,second_page_name
    ,max(goods_exposure_uv) goods_exposure_uv
    ,max(goods_click_uv) goods_click_uv
    ,max(goods_add_uv) goods_add_uv
    ,max(buy_user_num) buy_user_num
    ,max(gmv) buy_amount
    ,max(buy_num) buy_num
    ,dt
FROM (
    select
        dt,
        ex.special_date
        ,ex.goods_id
        ,ex.user_id
        ,ex.first_page_name
        ,ex.second_page_name
        ,count(distinct ex.user_id,ex.goods_id) goods_exposure_uv
        ,NULL goods_click_uv
        ,NULL goods_add_uv
        ,NULL buy_user_num
        ,NULL gmv
        ,null buy_num
    from yishou_daily.route_all_event_exposure_detail_v2 ex
    where ex.dt between '${start}' and '${end}'
    group by 1,2,3,4,5,6
    UNION ALL
    select
        dt,
        cl.special_date
        ,cl.goods_id
        ,cl.user_id
        ,cl.first_page_name
        ,cl.second_page_name
        ,null goods_exposure_uv
        ,count(distinct cl.user_id,cl.goods_id) goods_click_uv
        ,NULL goods_add_uv
        ,NULL buy_user_num
        ,NULL gmv
        ,null buy_num
    from yishou_daily.route_all_event_click_add_cart_detail_v2 cl
    where cl.dt between '${start}' and '${end}' --OR cl.dt = '${bdp.system.last_year_bizdate}'
    and page_name = '商品详情'
    group by 1,2,3,4,5,6
    UNION ALL
    select
        dt,
        ad.special_date
        ,ad.goods_id
        ,ad.user_id
        ,ad.first_page_name
        ,ad.second_page_name
        ,null goods_exposure_uv
        ,NULL goods_click_uv
        ,count(distinct ad.user_id,ad.goods_id) goods_add_uv
        ,NULL buy_user_num
        ,NULL gmv
        ,null buy_num
    from yishou_daily.route_all_event_click_add_cart_detail_v2 ad
    where ad.dt between '${start}' and '${end}' --OR ad.dt = '${bdp.system.last_year_bizdate}'
    and page_name = '加购'
    group by 1,2,3,4,5,6
    UNION ALL
    select
        dt,
        pa.special_date
        ,pa.goods_id
        ,pa.user_id
        ,pa.first_page_name
        ,pa.second_page_name
        ,null goods_exposure_uv
        ,NULL goods_click_uv
        ,NULL goods_add_uv
        ,count(distinct pa.user_id) buy_user_num
        ,sum(real_gmv) gmv
        ,sum(buy_num) buy_num
    from yishou_daily.route_all_event_gmv_detail_v2 pa
    where pa.dt between '${start}' and '${end}' --or pa.dt = '${bdp.system.last_year_bizdate}'
    group by 1,2,3,4,5,6
)
GROUP BY 1,2,3,4,5,dt