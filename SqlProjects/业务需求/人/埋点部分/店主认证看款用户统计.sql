-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/10/18 10:27:21 GMT+08:00
select 
    to_char(dyubsi.first_start_time,'yyyymmdd') as 首次认证店主日期
    ,coalesce(h5.system_type,windows.os) as 手机类型
    ,case 
        when log.is_app_login=1 then 'APP' 
        when log.is_mini_login=1 then '微信小程序'
    end as 认证路径
    ,count(DISTINCT afcac.user_id) as 当天认证成功用户数
    ,count(DISTINCT h5.user_id) as 对应用户新人专享货盘曝光用户数
    ,count(DISTINCT windows.user_id) as 对应用户新人专享弹窗成功的用户数
    ,count(DISTINCT windows.user_id) - count(DISTINCT h5.user_id) as 没看款用户数
FROM yishou_data.all_fmys_users afu
LEFT OUTER JOIN (
    -- 店主认证成功
    SELECT distinct user_id FROM yishou_data.all_fmys_check_account_channel WHERE status = 1 
) afcac
ON afu.user_id=afcac.user_id
LEFT OUTER JOIN yishou_data.dim_ys_user_basic_sale_info dyubsi ON dyubsi.user_id=afu.user_id
left join (
    select 
        dt
        ,case 
            when url like '%Android%' then 'Android' 
            when url like '%iOS%' then 'iOS'
        end as system_type
        ,user_id 
    from yishou_data.dcl_h5_ys_h5_goods_exposure
    where dt between '20240917' and '20241017' and title = '新人专享2022版'
) h5 
on h5.user_id = afu.user_id and afu.pt = h5.dt
left join (
    select user_id,is_app_login,is_mini_login,dt from yishou_data.dws_ys_log_sp_user_log_dt 
    where dt between '20240917' and '20241017'
) log 
on afu.user_id = log.user_id and afu.pt = log.dt
left join (
    select 
        dt
        ,case 
            when os='Android' then 'Android' 
            when os='iOS' then 'iOS'
        end as os
        ,'APP' as auto_channel
        ,user_id 
    from yishou_data.dcl_event_business_banner_show_d
    where dt between '20240917' and '20241017' and source = 1
) windows 
on windows.user_id = afu.user_id and afu.pt = windows.dt
left join (
    select dt,systemOs,user_id from yishou_data.dcl_wx_home_windows_exposure_d 
    where banner_id = '20080' and dt between '20240917' and '20241017' and systemOs='微信小程序'
) wx_windows 
on wx_windows.user_id = afu.user_id and afu.pt = wx_windows.dt
where afu.pt between '20240917' and '20241017'
and afcac.user_id is not null
and coalesce(h5.system_type,windows.os) is not null
and to_char(dyubsi.first_start_time,'yyyymmdd') between '20240917' and '20241017'
and (case when log.is_app_login=1 then 'APP' when log.is_mini_login=1 then '微信小程序' end) is not null
group by 1,2,3
;

-- 看板逻辑
-- select 
--     to_char(first_start_time,'yyyymmdd') as first_start_time
--     ,count(distinct if(goods_no_exposure_uv_1d=0,user_id,null)) as 没看款用户数
--     ,count(distinct if(goods_no_exposure_uv_1d>0,user_id,null)) as 看款用户数
-- from yishou_data.dws_user_label_summary_dt 
-- where dt between '20240917' and '20241017' 
-- and to_char(first_start_time,'yyyymmdd') between '20240917' and '20241017'
-- and is_authenticated=1
-- group by to_char(first_start_time,'yyyymmdd');


