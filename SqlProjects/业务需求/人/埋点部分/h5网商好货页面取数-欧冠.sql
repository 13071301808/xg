-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/10/21 17:46:26 GMT+08:00
-- ******************************************************************** --
select DISTINCT
    click.dt as 点击日期
    ,from_unixtime(fu.reg_time) as 注册时间
    ,case
        when fau.user_type like '%,%' or fau.user_type is null then dyusl.identity_type
        when fau.user_type in ('其他','其它') or dyusl.identity_type in ('其他','其它') then '其他'
        else fau.user_type
    end as `身份类型-2024`
    ,click.user_id
from (select * from yishou_data.all_fmys_users where pt = '${bdp.system.bizdate}') fu
left join (
    select 
        a.dt
        ,a.user_id
    from (
        select dt,get_json_object(data, '$.uid') as user_id from yishou_data.ods_h5_event_action_d
        where event = 'h5click'
        and dt between '20241008' and '20241021'
        and get_json_object(data, '$.source') like '%https://static.yishouapp.com/app/h5/production/screening-operate/index.html#/?id=117875%'
        and get_json_object(data, '$.uid') is not null and get_json_object(data, '$.uid') <> ''
    ) a
)click on click.user_id = fu.user_id
left join (select user_id,user_type from yishou_data.ods_fmys_agg_user_tag_dt where dt = '${bdp.system.bizdate}') fau on fau.user_id = fu.user_id
LEFT JOIN (select user_id,identity_type from yishou_data.dws_ys_user_sp_label_dt where dt between '20241008' and '20241021') dyusl ON dyusl.user_id=fu.user_id
where click.user_id is not null
;