-- changeUserExposure
select 
    count(user_id) as duo_user
from (
    select
        get_json_object(scdata, '$.user_id') as user_id 
        ,count(change_user_id) as change_num
    from yishou_data.ods_app_event_log_exposure_d
    LATERAL VIEW explode(split(regexp_replace(get_json_object(scdata, '$.user_arr'), '[\\[\\]{}"]', ''), '},')) exploded_table AS change_user_id
    where dt between '20250120' and '20250214'
    and event = 'changeuserexposure'
    group by get_json_object(scdata, '$.user_id')
    having change_num >= 2
)
;
-- changeuser
select
    dt
    ,count(DISTINCT json_parse(action,'$.properties.change_user_id')) change_user_uv
from yishou_data.ods_app_event_log_d
where event = 'changeuser' and dt between '20250206' and '20250214'
and json_parse(action,'$.properties.change_user_id') != json_parse(action,'$.properties.userid')
group by dt
;