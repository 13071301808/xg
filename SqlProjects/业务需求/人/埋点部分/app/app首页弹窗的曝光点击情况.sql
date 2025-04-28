-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/04/17 11:59:19 GMT+08:00
-- ******************************************************************** --
select
    dt 
    ,sum(exposure_uv) as exposure_uv
    ,sum(click_uv) as click_uv
from (
    select  
        dt
        ,count(DISTINCT user_id) as exposure_uv
        ,0 as click_uv
    from yishou_data.dcl_event_business_banner_show_d
    where dt between '20250401' and '20250416' 
    and (source in ('1','3','5','6') or source is null)
    group by dt
    union all 
    select 
        dt
        ,0 as exposure_uv
        ,count(DISTINCT user_id) as click_uv
    from yishou_data.dwd_log_app_route_dt
    where dt between '20250401' and '20250416' 
    and first_home_index = 'AA'
    and route = '首页_H5'
    group by dt
)
group by dt 
;



