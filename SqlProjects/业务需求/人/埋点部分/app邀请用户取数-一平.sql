-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/07/15 11:09:26 GMT+08:00
-- ******************************************************************** --
-- app
select
	dt,
	'APP' as `平台`,
	'邀请店主赚钱点击' as source,
	count(DISTINCT user_id) as user_uv,
	count(user_id) as user_pv
from
	yishou_data.dcl_event_user_center_click_d
where
	click_source = '我的活动'
	and banner_name = '邀请店主赚钱'
	and dt between '20230101' and '20240711'
group by 1,2,3
union ALL
select
	dt,
	'APP' as `平台`,
	'邀请店主赚钱曝光' as source,
	count(DISTINCT user_id) as user_uv,
	count(user_id) as user_pv
from
	yishou_data.dcl_event_user_center_exposure_d
where
	get_json_object(user_center_banner, '$.banner_id') = '35691'
	and dt between '20230101' and '20240711'
group by 1,2,3
union all
SELECT
	dt,
	'APP' as `平台`,
	'邀请好友点击' as source,
	count(
		DISTINCT get_json_object(action, '$.properties.userid')
	) as user_uv,
	count(get_json_object(action, '$.properties.userid')) as user_pv
from
	yishou_data.ods_app_event_log_d
where
	event = 'shareinvitationclick'
	and get_json_object(action, '$.properties.clickSource') = '网页内分享'
	and dt between '20230101' and '20240711'
group by
	dt;

-- 小程序
select 
    dt
    ,'微信小程序' as `平台`
    ,'邀请店主赚钱曝光' as source
    ,count(DISTINCT user_id) as user_uv  
    ,count(user_id) as user_pv 
from yishou_data.dcl_wx_user_center_exposure_d
where click_source = '曝光' 
and systemOs = '微信小程序'
-- 新客红包
and banner_id = '35691'
and dt between '20230101' and '20240711'
group by dt
union ALL 
select 
    dt
    ,'微信小程序' as `平台`
    ,'邀请店主赚钱点击' as source
    ,count(DISTINCT user_id) as user_uv 
    ,count(user_id) as user_pv 
from yishou_data.dcl_wx_user_center_click_d
where click_source = '我的活动' 
and systemOs = '微信小程序'
and banner_name = '邀请店主赚钱' 
and dt between '20230101' and '20240711'
group by dt
;

