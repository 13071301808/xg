-- DLI sql
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/05/23 12:49:30 GMT+08:00
-- ******************************************************************** --
-- 专场日用户商品流量数据
insert overwrite table yishou_data.dwm_ys_log_sp_user_goods_log_dt partition (dt)
SELECT
    to_date1(dt, 'yyyymmdd') special_date,
    cast(user_id AS BIGINT) AS user_id,
    cast(goods_id AS BIGINT) AS goods_id,
    cast(goods_no AS BIGINT) AS goods_no,
    sum(coalesce(app_exposure_pv,0)) AS goods_exposure_pv,
    sum(coalesce(click_pv,0)) AS goods_click_pv,
    sum(coalesce(addcart_pv,0)) AS goods_add_cart_pv,
    sum(coalesce(addcart_amount,0)) AS goods_add_cart_amount,
    sum(coalesce(addcart_num,0)) AS goods_add_cart_num,
    sum(coalesce(h5_exposure_pv,0)) AS h5_goods_exposure_pv,
    dt
FROM yishou_data.dwm_page_name_user_goods_incr_dt
WHERE dt between '${start}' and '${end}'
and user_id != 0
and coalesce(app_exposure_pv, 0) + coalesce(click_pv, 0) + coalesce(addcart_pv, 0) + coalesce(addcart_amount, 0) + coalesce(addcart_num, 0) + coalesce(h5_exposure_pv, 0) != 0
and goods_id != 0
GROUP BY to_date1(dt, 'yyyymmdd'),cast(user_id AS BIGINT),cast(goods_id AS BIGINT),cast(goods_no AS BIGINT),dt
;