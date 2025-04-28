-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/03/06 09:24:16 GMT+08:00
-- ******************************************************************** --
select 
    a.dt
    ,count(DISTINCT if(b.user_id is not null,a.user_id,null)) as exposure_click_num
    ,count(DISTINCT a.user_id) as exposure_num
    ,count(DISTINCT if(b.user_id is not null,a.user_id,null)) / count(DISTINCT a.user_id) as exposure_click_ratio
from (
    select 
        dt,user_id,count(1) 
    from yishou_data.dwd_special_list_exposure_view
    where dt = '20250306' group by 1,2
    having count(1) = 1
) a 
left join (
    select 
        dt,user_id 
    from yishou_data.dwd_app_enter_special_dt_view
    where dt = '20250306' and special_source = '1'
    group by 1,2
) b on a.dt = b.dt and a.user_id = b.user_id
group by a.dt