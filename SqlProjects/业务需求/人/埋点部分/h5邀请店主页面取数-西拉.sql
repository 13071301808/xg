select
    dt
    ,case 
        when get_json_object(data,'$.source') like '%https://static4.yishouapp.com/app/h5/production/invitation/index.html#/%'
        then '邀请店主得红包页'
        when get_json_object(data,'$.source') like '%https://static4.yishouapp.com/app/h5/production/invitation-new/index.html#/%'
        then '邀请店主返红包页'
    end as 点击位置
    ,get_json_object(data,'$.uid') as user_id 
from yishou_data.ods_h5_event_action_d
where event= 'h5click' and activity = '邀请有奖'
and ((dt between '20231001' and '20231130') or (dt between '20241001' and '20241118'))
and get_json_object(data,'$.uid') is not null 
and get_json_object(data,'$.uid') <> ''
and (
    case 
        when get_json_object(data,'$.source') like '%https://static4.yishouapp.com/app/h5/production/invitation/index.html#/%'
        then '邀请店主得红包页'
        when get_json_object(data,'$.source') like '%https://static4.yishouapp.com/app/h5/production/invitation-new/index.html#/%'
        then '邀请店主返红包页'
    end
) is not null
;