-- CREATE EXTERNAL TABLE yishou_data.dcl_event_goods_detail_stall_exposure_d (
-- 	`user_id` STRING COMMENT '用户id',
-- 	`special_id` STRING COMMENT '专场id',
-- 	`os` STRING COMMENT '操作系统',
-- 	`pid` STRING COMMENT '来源',
-- 	`ptime` STRING COMMENT '页面停留时间',
-- 	`source` STRING COMMENT '专场列表来源',
-- 	`report_time` STRING COMMENT '上报时间',
-- 	`event_id` STRING COMMENT '事件id',
-- 	`search_event_id` STRING COMMENT '搜索事件id',
-- 	`keyword` STRING COMMENT '关键词',
-- 	`app_version` STRING COMMENT 'APP版本',
-- 	`is_pro` STRING COMMENT '是否为Pro版{1:童装货源APP,0:一手APP}',
-- 	`cat_id` STRING COMMENT '处于分类详情页的品类id',
-- 	`cat_name` STRING COMMENT '处于分类详情页的品类名',
-- 	`stall_id` STRING COMMENT '处于档口主页的档口id',
-- 	`special_index` STRING COMMENT '处于专场详情时，专场从列表第几坑进',
-- 	`pgm_code` STRING COMMENT '处于买手主页的品类id',
-- 	`activity_id` STRING COMMENT '处于首页分类页tab的活动id',
-- 	`activity_name` STRING COMMENT '处于首页分类页tab的活动名',
--     `stall_tab` STRING COMMENT '商详页的档口tab',
--     `goods_id` STRING COMMENT '商详页的档口数组下的商品id',
--  `secondsource` STRING COMMENT '二级来源'
-- ) 
-- COMMENT '商品列表的商详页档口数组曝光' 
-- PARTITIONED BY (`dt` STRING COMMENT '自然日期分区') 
-- STORED AS orc LOCATION 'obs://yishou-bigdata/yishou_data.db/dcl_event_goods_detail_stall_exposure_d' 
-- ;

insert overwrite table yishou_data.dcl_event_goods_detail_stall_exposure_d partition (dt)
select 
    user_id
    , special_id
    , os 
    , pid 
    , coalesce(ptime,0)
    , source
    , cast(report_time as string) as report_time
    , event_id
    , search_event_id
    , keyword
    , app_version
    , is_pro
    , cat_id
    , cat_name
    , stall_id
    , special_index
    , pgm_code
    , activity_id
    , activity_name
    , get_json_object(goods_detail_stall_arr_dict, '$.stall_tab') as stall_tab
    , regexp_replace(goods_id, '"', '') as goods_id
    , secondsource
    , dt 
from temp.exposure_pretable
lateral view outer explode (split(goods_detail_stall_arr, '\\|-\\|'))t as goods_detail_stall_arr_dict
LATERAL VIEW explode(split(regexp_replace(get_json_object(goods_detail_stall_arr, '$.goods_id_lists'), '\\[|\\]', ''), ',')) exploded_table AS goods_id
where goods_detail_stall_arr is not null 
and goods_detail_stall_arr <> ''
DISTRIBUTE BY dt
; 