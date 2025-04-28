select
    dt 
    ,sum(exposure_uv) as exposure_uv
    ,sum(click_uv) as click_uv
from (
    select 
        dt
        ,0 as click_uv
        ,count(DISTINCT user_id) as exposure_uv
    from yishou_data.dwd_log_h5_goods_list_exposure_dt
    where dt between '20250315' and '20250319'
    and title = '网商店主权益导览'
    group by dt
    union ALL 
    select  
        dt
        ,count(DISTINCT user_id) as click_uv
        ,0 as exposure_uv
    from yishou_data.dwd_log_h5_goods_list_click_dt
    where dt between '20250315' and '20250319'
    and title = '网商店主权益导览'
    group by dt
)
group by dt
order by dt asc