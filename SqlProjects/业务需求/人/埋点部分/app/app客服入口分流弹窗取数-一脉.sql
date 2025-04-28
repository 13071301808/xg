-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/11/01 10:29:37 GMT+08:00
-- ******************************************************************** --
-- -- serviceEntranceDiversionWindow
select
    dt as 日期
    ,banner_id as 运营位ID
    ,sum(点击uv) as 点击uv
    ,sum(曝光pv) as 曝光pv
    ,sum(曝光uv) as 曝光uv
from (
    select 
        dt,banner_id
        ,count(DISTINCT user_id) as 点击uv,0 as 曝光pv,0 as 曝光uv
    from dw_yishou_data.dcl_event_service_entrance_diversion_window_d
    where dt between '${开始时间}' and '${结束时间}' 
    and click_source != '曝光' 
    group by dt,banner_id
    union ALL 
    select  
        dt,banner_id
        ,0 as 点击uv,count(user_id) as 曝光pv,count(DISTINCT user_id) as 曝光uv
    from dw_yishou_data.dcl_event_service_entrance_diversion_window_d
    where dt between '${开始时间}' and '${结束时间}' 
    and click_source = '曝光' 
    group by dt,banner_id
)
group by dt,banner_id