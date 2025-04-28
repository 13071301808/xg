--odps sql
--********************************************************************--
--author:于洋洋
--create time:2020-11-25 10:30:31
--********************************************************************--
-- 6.0.0版本以后商品曝光放在大曝光（yishou_log_exposure，增量同步）
-- 解析商品曝光
--商品列表曝光 dcl_event_big_goods_exposure_d;
--商品列表点击 dcl_event_big_goods_exposure_click_d

-- select * from yishou_data.temp_log_goods_exposure_20201125 limit 1000;

-- alter table dcl_event_big_goods_exposure_d add columns(goods_seat_id string comment'坑位策略id');
-- alter table dcl_event_big_goods_exposure_click_d add columns(tab_name string comment'tab名称');
-- select * from  dcl_event_big_goods_exposure_d where dt=20220726 limit 1000;
-- 曝光商品明细
insert overwrite  table dcl_event_big_goods_exposure_d  partition (dt)
select
    distinct user_id
    , coalesce(get_json_object(goods_dict, '$.goods_id'), get_json_object(goods_dict, '$.good_id')) as goods_id
    , get_json_object(goods_dict, '$.is_rec') as is_rec
    , special_id
    , os
    , get_json_object(goods_dict, '$.goods_no') as goods_no
    , pid
    , coalesce(ptime,0)
    , source
    , cast(report_time as string) as report_time
    , event_id
    , search_event_id
    , keyword
    , app_version
    , type as log_type
    , get_json_object(goods_dict, '$.index') as exposure_index
    , get_json_object(goods_dict, '$.strategy_id') as strategy_id
    , get_json_object(goods_dict, '$.is_default') as is_default
    , get_json_object(goods_dict, '$.is_operat') as is_operat
    -- , get_json_object(banner_dict, '$.home_index') as banner_home_index
    -- , get_json_object(banner_dict, '$.index') as banner_index
    -- , get_json_object(banner_dict, '$.banner_id') as banner_id
    , recall_num
    , is_pro
    , get_json_object(goods_dict, '$.goods_seat_id') as goods_seat_id
    , get_json_object(goods_dict, '$.tab_name') as tab_name
    ,cat_id
    ,cat_name
    ,stall_id
    ,special_index
    ,pgm_code
    ,activity_id
    ,activity_name
    ,get_json_object(goods_dict, '$.search_score') as  search_score
    , dt
from temp_log_goods_exposure_20201125
lateral view outer explode (split(goods, '\\|-\\|'))t as goods_dict
-- lateral view outer explode (split(banner, '\\|-\\|'))t as banner_dict
where goods_dict is not null
and goods_dict <> ''
;

-- alter table dcl_event_big_goods_exposure_click_d add columns(goods_seat_id string comment'坑位策略id');
-- select * from  dcl_event_big_goods_exposure_click_d where dt=20220726 limit 1000;
-- 曝光点击商品明细
insert overwrite  table dcl_event_big_goods_exposure_click_d  partition (dt)
select
    distinct user_id
    , coalesce(get_json_object(goods_dict, '$.goods_id'), get_json_object(goods_dict, '$.good_id')) as goods_id
    , get_json_object(goods_dict, '$.is_rec') as is_rec
    , special_id
    , os
    , get_json_object(goods_dict, '$.goods_no') as goods_no
    , pid
    , ptime
    , source
    , report_time
    , event_id
    , search_event_id
    , keyword
    , app_version
    , type as log_type
    , get_json_object(goods_dict, '$.index') as click_index
    , get_json_object(goods_dict, '$.strategy_id') as strategy_id
    , get_json_object(goods_dict, '$.is_default') as is_default
    , get_json_object(goods_dict, '$.is_operat') as is_operat
    -- , get_json_object(banner_dict, '$.home_index') as banner_home_index
    -- , get_json_object(banner_dict, '$.index') as banner_index
    -- , get_json_object(banner_dict, '$.banner_id') as banner_id
    , recall_num
    , is_pro
    , get_json_object(goods_dict, '$.goods_seat_id') as goods_seat_id
    , get_json_object(goods_dict, '$.tab_name') as tab_name
    ,cat_id
    ,cat_name
    ,stall_id
    ,special_index
    ,pgm_code
    ,activity_id
    ,activity_name
    ,get_json_object(goods_dict, '$.search_score') as  search_score
    , dt
from temp_log_goods_exposure_20201125
lateral view outer explode (split(click_goods, '\\|-\\|'))t as goods_dict
-- lateral view outer explode (split(click_banner, '\\|-\\|'))t as banner_dict
where goods_dict is not null
and goods_dict <> ''
;
