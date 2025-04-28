-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/02/28 10:20:24 GMT+08:00
-- ******************************************************************** --
-- 需求1
select 
    mt,sum(exposure_uv) as 曝光UV,sum(click_uv) as 点击UV
from (
    select 
        substr(dt,1,6) as mt, count(DISTINCT user_id) as exposure_uv,0 as click_uv
    from yishou_data.dcl_event_goods_detail_page_exposure_d 
    where dt between '20240101' and '20241231' 
    -- and exposure_models like '%档口信息%'
    and is_stall_question = 1
    group by 1
    union all
    select 
        substr(dt,1,6) as mt,0 as exposure_uv, count(DISTINCT user_id) as click_uv
    from yishou_data.dcl_event_stall_request_page_d 
    where dt between '20240101' and '20241231' 
    and source = 1
    group by 1
)
group by mt
;

-- 需求2
select 
    a.dt
    ,case when b.goods_no is not null then '是' else '否' end as is_appraise
    ,count(DISTINCT a.goods_no) as goods_num
    ,sum(a.goods_exposure_uv) as goods_exposure_uv
    ,sum(a.goods_click_uv) as goods_click_uv
    ,sum(a.goods_add_cart_uv) as goods_add_cart_uv
    ,sum(a.pay_uv) as pay_uv
    ,sum(a.real_buy_num) as real_buy_num
    ,round(sum(a.real_buy_amount),4) as real_buy_amount
from (
    select 
        dt
        ,goods_no
        ,sum(goods_exposure_uv) as goods_exposure_uv
        ,sum(goods_click_uv) as goods_click_uv
        ,sum(goods_add_cart_uv) as goods_add_cart_uv
        ,sum(real_buy_user_num) as pay_uv
        ,sum(real_buy_num) as real_buy_num
        ,sum(real_buy_amount) as real_buy_amount
    from yishou_data.dws_goods_id_action_statistics_incr_dt
    where dt between '${start}' and '${end}'
    and to_char(special_date,'yyyymmdd') between '${start}' and '${end}'
    group by 1,2
) a
left join (
    -- 评价
    select 
        to_char(from_unixtime(create_time),'yyyymmdd') as dt
        ,goods_no
    from yishou_data.ods_fmys_goods_appraise_dt 
    where dt = '20250227'
    group by 1,2
) b on a.dt = b.dt and a.goods_no = b.goods_no
group by 1,2
