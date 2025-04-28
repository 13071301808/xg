-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/09/03 19:36:42 GMT+08:00
-- ******************************************************************** --
-- 点击
select 
    click_source
    ,banner_name
    ,count(user_id) as user_pv
    ,count(DISTINCT user_id) as user_uv
from yishou_data.dcl_event_home_click_d
where dt >= '20240901' and click_source in ('新人限时奖励banner','新人限时奖励弹窗')
group by 1,2,3;

-- 曝光
select 
    case 
        when get_json_object(new_user_limited_time_reward_banner,'$.h5_url') = 'https://h5.yishouapp.com/users/newcomer-award?spring=0&immersive=1'
        then '新人限时奖励banner'
    end as 曝光位置
    ,count(user_id) as user_pv
    ,count(DISTINCT user_id) as user_uv
from yishou_data.dcl_event_home_exposure_d
where dt >= '20240901' 
and get_json_object(new_user_limited_time_reward_banner,'$.banner_name') is not null
and get_json_object(new_user_limited_time_reward_banner,'$.banner_name') <> ''
group by 1
union ALL 
select 
    '新人限时奖励弹窗' as 曝光位置
    ,count(user_id) as user_pv
    ,count(DISTINCT user_id) as user_uv
from yishou_data.dcl_event_business_banner_show_d
where source = '6' and dt >= '20240901'
;

-- select * from yishou_data.dcl_event_business_banner_show_d where source = '6' and dt >= '20240901';
