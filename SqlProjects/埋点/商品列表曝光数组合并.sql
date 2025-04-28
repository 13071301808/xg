-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/10/21 14:01:53 GMT+08:00
-- ******************************************************************** --
-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/10/21 14:42:55 GMT+08:00
-- ******************************************************************** --
drop table if exists yishou_data.temp_goods_exposure_add_1009;
create table yishou_data.temp_goods_exposure_add_1009 as
with temp_1021 as (
    SELECT 
        coalesce(get_json_object(scdata, '$.user_id'),get_json_object(scdata, '$.userid')) AS user_id 
        , regexp_replace1(regexp_replace1(regexp_replace1(get_json_object(scdata, '$.goods_arr'), '^\\[',''),'\\]$',''),'},\\{','}|-|{') AS goods
        , regexp_replace1(regexp_replace1(regexp_replace1(get_json_object(scdata, '$.banner_arr'), '^\\[',''),'\\]$',''),'},\\{','}|-|{') AS banner     
        , get_json_object(scdata, '$.special_id') AS special_id
        , get_json_object(scdata,'$.os') AS os      
        , get_json_object(scdata,'$.pid') AS pid
        , get_json_object(scdata,'$.ptime') AS ptime
        , get_json_object(scdata,'$.source') AS source
        , time * 1000 AS report_time
        , get_json_object(scdata,'$.event_id') AS event_id
        , get_json_object(scdata,'$.search_event_id') AS search_event_id
        , regexp_replace1(regexp_replace1(regexp_replace1(get_json_object(scdata, '$.click_goods_arr'), '^\\[',''),'\\]$',''),'},\\{','}|-|{') AS click_goods
        , regexp_replace1(regexp_replace1(regexp_replace1(get_json_object(scdata, '$.click_banner_arr'), '^\\[',''),'\\]$',''),'},\\{','}|-|{') AS click_banner 
        , get_json_object(scdata,'$.keyword') AS keyword     
        , get_json_object(scdata,'$.app_version') AS app_version
        , get_json_object(scdata,'$.recall_num') AS recall_num
        , get_json_object(scdata,'$.isPro') AS is_pro
        , get_json_object(scdata,'$.cat_id') AS cat_id
        , get_json_object(scdata,'$.cat_name') AS cat_name
        , get_json_object(scdata,'$.stall_id') AS stall_id
        , get_json_object(scdata, '$.special_index') AS special_index
        , get_json_object(scdata, '$.page_name') AS page_name
        , get_json_object(scdata,'$.pgm_code') AS pgm_code
        , get_json_object(scdata,'$.activity_id') AS activity_id
        , get_json_object(scdata,'$.activity_name') AS activity_name
        , 'new' AS type  
        , get_json_object(scdata,'$.secondsource') AS secondsource
        , regexp_replace1(regexp_replace1(regexp_replace1(get_json_object(scdata, '$.recommend_stall_arr'), '^\\[',''),'\\]$',''),'},\\{','}|-|{') AS recommend_stall_arr
        , regexp_replace1(regexp_replace1(regexp_replace1(get_json_object(scdata, '$.click_recommend_stall_arr'), '^\\[',''),'\\]$',''),'},\\{','}|-|{') AS click_recommend_stall_arr  
        , regexp_replace1(regexp_replace1(regexp_replace1(get_json_object(scdata, '$.goods_detail_stall_arr'), '^\\[',''),'\\]$',''),'},\\{','}|-|{') AS goods_detail_stall_arr  
        , dt 
    FROM yishou_data.ods_app_event_log_exposure_d
    WHERE dt BETWEEN '20241009' and '20241023'
    AND event = 'goodsexposure'
)
-- insert OVERWRITE table yishou_data.dcl_event_big_goods_exposure_d PARTITION(dt)
SELECT 
    DISTINCT 
    user_id,
    goods_id,
    is_rec,
    special_id,
    os,
    goods_no,
    pid,
    ptime,
    source,
    report_time,
    event_id,
    search_event_id,
    keyword,
    app_version,
    log_type,
    exposure_index,
    strategy_id,
    is_default,
    is_operat,
    recall_num,
    is_pro,
    goods_seat_id,
    tab_name,
    cat_id,
    cat_name,
    stall_id,
    special_index,
    pgm_code,
    activity_id,
    activity_name,
    search_score,
    secondsource,
    dt
FROM yishou_data.dwd_event_goods_exposure_incr_dt
WHERE dt  BETWEEN '20241009' and '20241023'
union ALL 
select
    DISTINCT 
    user_id,
    regexp_replace(goods_id, '"', '') as goods_id,
    null as is_rec,
    special_id,
    os,
    null as goods_no,
    pid,
    ptime,
    source,
    report_time,
    event_id,
    search_event_id,
    keyword,
    app_version,
    null as log_type,
    null as exposure_index,
    null as strategy_id,
    null as is_default,
    null as is_operat,
    recall_num,
    is_pro,
    null as goods_seat_id,
    null as tab_name,
    cat_id,
    cat_name,
    stall_id,
    special_index,
    pgm_code,
    activity_id,
    activity_name,
    null as search_score,
    secondsource,
    dt
FROM temp_1021
lateral view outer explode (split(goods_detail_stall_arr, '\\|-\\|'))t as goods_detail_stall_arr_dict
LATERAL VIEW explode(split(regexp_replace(get_json_object(goods_detail_stall_arr, '$.goods_id_lists'), '\\[|\\]', ''), ',')) exploded_table AS goods_id
WHERE dt BETWEEN '20241009' and '20241023' 
and goods_detail_stall_arr is not null 
and goods_detail_stall_arr <> ''
and user_id is not null
;
