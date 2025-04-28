select 
    dt,位置,sum(曝光PV) as 曝光PV,sum(曝光UV) as 曝光UV,sum(点击UV) as 点击UV
from (
    -- 首页
    select dt,'首页' as 位置,count(user_id) as 曝光PV,count(DISTINCT user_id) as 曝光UV,0 as 点击UV 
    from yishou_data.dcl_wx_home_exposure_d 
    where dt between '${start}' and '${end}' group by dt
    union all
    -- 我的页面
    select dt,'我的页面' as 位置,count(user_id) as 曝光PV,count(DISTINCT user_id) as 曝光UV,0 as 点击UV 
    from yishou_data.dcl_wx_user_center_exposure_d
    where dt between '${start}' and '${end}' group by dt
    union ALL 
    -- 商详页助力价横条
    select dt,'商详页助力价横条' as 位置,0 as 曝光PV,0 as 曝光UV,count(DISTINCT user_id) as 点击UV 
    from yishou_data.dcl_wx_good_detail_page_click_d
    where dt between '${start}' and '${end}' and click_source = '邀新助力价横条' group by dt 
    union ALL 
    select dt,'商详页助力价横条' as 位置,count(user_id) as 曝光PV,count(DISTINCT user_id) as 曝光UV,0 as 点击UV 
    from yishou_data.dcl_wx_good_detail_page_exposure_d
    where dt between '${start}' and '${end}' and assistance_banner_arr is not null group by dt
    union ALL 
    -- 助力活动页-发起人视角
    select dt,'助力活动页-发起人视角' as 位置,0 as 曝光PV,0 as 曝光UV,count(DISTINCT user_id) as 点击UV 
    FROM yishou_data.dcl_wx_assistance_activity_page_organiser_click_d
    where dt between '${start}' and '${end}' and click_source = '邀请新店主助力'
    group by dt
    union all 
    select dt,'助力活动页-发起人视角' as 位置,count(get_json_object(data, '$.userid')) as 曝光PV,count(DISTINCT get_json_object(data, '$.userid')) as 曝光UV,0 as 点击UV
    FROM yishou_data.ods_h5_event_action_d
    where event= 'assistanceactivitypageorganiserexposure' 
    and dt between '${start}' and '${end}' and get_json_object(data, '$.copywriting') is not null
    group by dt
    union all
    -- 助力活动页点击-助力人视角
    select dt,'助力活动页-助力人视角' as 位置,0 as 曝光PV,0 as 曝光UV,count(DISTINCT user_id) as 点击UV 
    FROM yishou_data.dcl_wx_assistance_activity_page_sendee_click_d
    where dt between '${start}' and '${end}' and click_source = '帮Ta助力'
    group by dt
    union all 
    select dt,'助力活动页-助力人视角' as 位置,count(get_json_object(data, '$.userid')) as 曝光PV,count(DISTINCT get_json_object(data, '$.userid')) as 曝光UV,0 as 点击UV
    FROM yishou_data.ods_h5_event_action_d
    where event= 'assistanceactivitypagesendeeexposure' 
    and dt between '${start}' and '${end}' and get_json_object(data, '$.exposure_arr') is not null
    group by dt
)
group by 1,2


