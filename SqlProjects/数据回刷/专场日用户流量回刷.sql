-- DLI sql
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/05/23 13:35:30 GMT+08:00
-- ******************************************************************** --
-- 专场日用户流量数据
insert overwrite table yishou_data.dws_ys_log_sp_user_log_dt partition (dt)
select
    coalesce(a.special_date,b.special_date,t2.special_date) as special_date
    ,coalesce(a.user_id,b.user_id,t2.user_id) as user_id
    ,coalesce(is_login,0) as is_login
    ,coalesce(is_app_login,0) as is_app_login
    ,coalesce(is_mini_login,0) as is_mini_login
    ,coalesce(a.goods_exposure_uv,0) + coalesce(t2.h5_goods_exposure_uv,0) as exposure_uv
    ,coalesce(goods_no_exposure_uv,0)  as goods_no_exposure_uv  --曝光商品款数
    ,coalesce(goods_exposure_pv,0) as goods_exposure_pv     --曝光pv
    ,coalesce(goods_click_uv,0) as goods_click_uv  --点击商品id数
    ,coalesce(goods_no_click_uv,0) as goods_no_click_uv  --点击商品款数
    ,coalesce(goods_click_pv,0) as goods_click_pv    --点击pv
    ,coalesce(goods_add_cart_uv,0) as goods_add_cart_uv  --加购商品id数
    ,coalesce(goods_no_add_cart_uv,0) as goods_no_add_cart_uv  --加购商品款数
    ,coalesce(goods_add_cart_pv,0) as goods_add_cart_pv --加购pv
    ,coalesce(goods_add_cart_amount,0) as goods_add_cart_amount --加购金额
    ,coalesce(goods_add_cart_num,0) as goods_add_cart_num    --加购件数
    ,coalesce(to_char(a.special_date,'yyyymmdd'),to_char(b.special_date,'yyyymmdd'),to_char(t2.special_date,'yyyymmdd')) as dt
from  (
    select
        special_date
        ,user_id
        ,count(distinct  case when goods_exposure_pv>0 then goods_id end ) as goods_exposure_uv --曝光商品id数
        ,count(distinct  case when goods_exposure_pv>0 then goods_no end ) as goods_no_exposure_uv --曝光商品款数
        ,sum(goods_exposure_pv) as goods_exposure_pv --曝光pv
        ,count(distinct  case when goods_click_pv>0 then goods_id end ) as goods_click_uv --点击商品id数
        ,count(distinct  case when goods_click_pv>0 then goods_no end ) as goods_no_click_uv --点击商品款数
        ,sum(goods_click_pv)  as goods_click_pv --点击pv
        ,count(distinct  case when goods_add_cart_pv>0 then goods_id end ) as goods_add_cart_uv --加购商品id数
        ,count(distinct  case when goods_add_cart_pv>0 then goods_no end ) as goods_no_add_cart_uv --加购商品款数
        ,sum(goods_add_cart_pv) as goods_add_cart_pv --加购pv
        ,sum(goods_add_cart_amount) as goods_add_cart_amount --加购金额
        ,sum(goods_add_cart_num) as  goods_add_cart_num --加购件数
     from yishou_data.dwm_ys_log_sp_user_goods_log_dt
     where dt between '${start}' and '${end}'
     group by special_date,user_id
) a
full join (
    select
        special_date
        ,user_id
        ,max(case when user_id > 0 then 1 else 0 end) as is_login
        ,max(case when user_id > 0 and type='app' then 1 else 0 end) as is_app_login
        ,max(case when user_id > 0 and type='小程序' then 1 else 0 end) as is_mini_login
    from yishou_data.dwm_log_sp_user_login_stats_dt
    where dt between '${start}' and '${end}'
    and user_id > 0
    group by special_date,user_id
)b
on to_char(a.special_date,'yyyymmdd')=to_char(b.special_date,'yyyymmdd')
and a.user_id=b.user_id
full join (
    select
        user_id
        ,special_date
        ,count(distinct goods_id) as h5_goods_exposure_uv
    from yishou_data.dwd_log_h5_goods_list_exposure_dt
    where dt between '${start}' and to_char(dateadd(to_date1('${end}','yyyymmdd'),1,'dd'),'yyyymmdd')
    and to_char(special_date,'yyyymmdd') between '${start}' and '${end}'
    and user_id is not null
    group by user_id,special_date
) t2
on coalesce(a.user_id,b.user_id) = t2.user_id
and to_char(t2.special_date,'yyyymmdd') = to_char(a.special_date,'yyyymmdd')
;

