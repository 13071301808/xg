-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/05/13 16:06:04 GMT+08:00
-- ******************************************************************** --
-- https://yb7ao262ru.feishu.cn/wiki/RJeHwwLYhiZs09kfvkTcnuKLndI
select 
    coalesce(ck.dt,ad.dt) dt 
    ,coalesce(ck.promotion_id,ad.promotion_id) promotion_id
    ,coalesce(ck.point,ad.point) point
    ,coalesce(sum(ck.click_uv),0) click_uv
    ,coalesce(sum(ad.add_uv),0) add_uv
    ,coalesce(sum(ad.pay_uv),0) pay_uv
    ,coalesce(sum(ad.gmv),0) gmv
    ,coalesce(sum(ad.real_gmv),0) real_gmv
from (
    -- 点击
    select 
        dt
        ,coalesce(promotion_id,split(split(regexp_replace(h5_url, '%3D', '='),'[&\|?]promotion_id=')[1],'&')[0]) promotion_id
        ,case 
            when source = 10 then '专场页'
            when source = 11 then '档口页'
            when source > 1000 then 'h5活动模版'
            when source is null or source = 0 then '商详页'
        end as point
        ,count(DISTINCT user_id) click_uv
    from yishou_data.dcl_h5_good_detail_page_d
    where dt between '20250512' and '20250513'
    and coalesce(
        promotion_id,
        split(split(regexp_replace(h5_url, '%3D', '='),'[&\|?]promotion_id=')[1],'&')[0]
    ) <> ''
    group by 1,2,3
) ck 
left join (
    -- 加购支付
    select 
        coalesce(ad.dt,pay.dt) dt
        ,ad.promotion_id
        ,ad.point
        ,count(DISTINCT ad.user_id) add_uv
        ,count(DISTINCT pay.user_id) pay_uv
        ,round(sum(pay.gmv),2) gmv
        ,round(sum(pay.real_gmv),2) real_gmv
    from (
        -- 加购
        select 
            dt
            ,coalesce(promotion_id,split(split(regexp_replace(h5Url, '%3D', '='),'[&\|?]promotion_id=')[1],'&')[0]) promotion_id
            ,case 
                when source = 10 then '专场页'
                when source = 11 then '档口页'
                when source > 1000 then 'h5活动模版'
                when source is null or source = 0 then '商详页'
            end as point
            ,user_id
            ,goods_id
            ,to_char(from_unixtime(time/1000),'yyyymmddhhmm') add_cart_time
        from yishou_data.dcl_wx_add_cart_click_d
        where dt between '20250512' and '20250513'
        and coalesce(promotion_id,split(split(regexp_replace(h5Url, '%3D', '='),'[&\|?]promotion_id=')[1],'&')[0]) <> ''
        group by 1,2,3,4,5,6
        -- limit 10
    ) ad 
    left join (
        -- 支付
        select 
            dt
            ,user_id
            ,goods_id
            ,to_char(sa_add_cart_time,'yyyymmddhhmm') add_cart_time
            ,sum(shop_price * buy_num) gmv
            ,sum(case when is_real_pay = 1 and pay_status = 1 then shop_price * buy_num else null end) real_gmv
        from yishou_data.dwd_sale_order_info_dt
        where dt between '20250512' and '20250513'
        -- and pay_status = 1
        and sa_os = 'wx'
        group by 1,2,3,4
    )pay on ad.dt = pay.dt and ad.user_id = pay.user_id and ad.goods_id = pay.goods_id and pay.add_cart_time = ad.add_cart_time
    group by 1,2,3
) ad on ck.dt = ad.dt and ck.promotion_id = ad.promotion_id and ck.point = ad.point
where coalesce(ck.point,ad.point) is not null
group by 1,2,3
;
