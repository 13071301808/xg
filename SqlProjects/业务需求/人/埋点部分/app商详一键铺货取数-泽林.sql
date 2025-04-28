select 
    a.pt as 日期,
    a.user_id,
    from_unixtime(a.reg_time) as 注册时间,
    fau.user_type as 身份标签,
    click.平台名,
    click.点击pv,
    click.点击uv,
    exposure.曝光pv,
    exposure.曝光uv,
    fup.allamount as 用户成交GMV
from yishou_data.all_fmys_users a
left join (
    select 
        dt,
        user_id,
        count(distinct user_id) 曝光uv,
        count(user_id) 曝光pv
    from yishou_data.dcl_event_goods_detail_page_exposure_d 
    where dt between '20240918' and '20241018'
    and exposure_models like '%铺货入口%'
    group by 1,2
)exposure on a.user_id = exposure.user_id and a.pt = exposure.dt
left join (
    select 
        dt
        ,user_id
        ,copywriting as 平台名
        ,count(distinct user_id) 点击uv
        ,count(user_id) 点击pv
    from yishou_data.dcl_event_goods_detail_page_click_d
    where dt between '20240918' and '20241018'
    and copywriting is not null
    and click_source in ('铺货入口','铺货平台')
    group by 1,2,3
)click on click.dt = a.pt and click.user_id = a.user_id and click.user_id = exposure.user_id
left join (select user_id,allamount from yishou_data.ods_fmys_user_profile_dt where dt = '${bdp.system.bizdate}') fup on fup.user_id = a.user_id
left join (select user_id,user_type from yishou_data.ods_fmys_agg_user_tag_dt where dt = '${bdp.system.bizdate}') fau on fau.user_id = a.user_id
where click.平台名 is not null and a.user_id is not null
;


-- select exposure_models,count(1) from yishou_data.dcl_event_goods_detail_page_exposure_d where exposure_models like '%铺货平台%' and dt = '20240924' group by 1; 