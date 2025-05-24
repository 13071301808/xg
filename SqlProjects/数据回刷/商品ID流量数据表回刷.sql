-- DLI sql
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/05/23 10:08:14 GMT+08:00
-- ******************************************************************** --
-- 商品ID流量数据表
INSERT OVERWRITE TABLE yishou_data.dwm_goods_flow_incr_dt PARTITION (dt)
select
    goods_id
    ,coalesce(count(distinct case when app_exposure_pv>0 then user_id end),0) goods_exposure_uv
    ,coalesce(sum(app_exposure_pv),0)   goods_exposure_pv
    ,coalesce(count(click_pv),0)    goods_click_pv
    ,coalesce(count(distinct case when click_pv>0 then user_id end),0) goods_click_uv
    ,coalesce(count(distinct case when addcart_pv>0 then user_id end),0) goods_add_cart_uv
    ,coalesce(sum(addcart_pv),0)  goods_add_cart_pv
    ,coalesce(sum(addcart_num),0)  goods_add_cart_num
    ,coalesce(sum(addcart_amount),0)    goods_add_cart_amount
    ,dt    dt
from yishou_data.dwm_page_name_user_goods_incr_dt
where dt between '${start}' and '${end}'
group by goods_id,dt
DISTRIBUTE BY dt
;

-- show create table yishou_data.dwm_goods_flow_incr_dt;