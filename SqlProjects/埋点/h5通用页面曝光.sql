--odps sql
--********************************************************************--
--author:liangzhaoguang
--create time:2019-03-12 15:51:37
--desc：解析H5曝光数据
--********************************************************************--

-- CREATE EXTERNAL TABLE `yishou_data`.`dcl_h5_element_exposure_d`(
-- `activity` STRING COMMENT '活动',
-- `user_id` STRING COMMENT '用户id',
-- `element` STRING COMMENT '元素',
-- `from_id` STRING,
-- `time` STRING COMMENT '时间',
-- `goods_id` STRING COMMENT '商品id',
-- `source` STRING COMMENT 'h5链接',
-- `act_from` STRING COMMENT '活动来源（20200801开始刷）',
-- `supply_id` STRING COMMENT '供应商id',
-- `brand_name` STRING COMMENT '品牌名称',
-- `activity_date` STRING COMMENT '活动日期',
-- `template_id` STRING COMMENT '活动模板id',
-- `event_id` STRING COMMENT '事件id',
-- `from_banner_id` STRING COMMENT '运营位id'
-- ) COMMENT 'H5页面元素曝光表'
-- PARTITIONED BY (`dt` STRING)
-- ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
-- WITH SERDEPROPERTIES ( 'serialization.format' = '1' )
-- STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
-- OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
-- LOCATION 'obs://yishou-bigdata/yishou_data.db/dcl_h5_element_exposure_d'
-- TBLPROPERTIES (
-- 'hive.serialization.extend.nesting.levels' = 'true',
-- 'transient_lastDdlTime' = '1641974565',
-- 'parquet.compression' = 'SNAPPY'
-- )

-- ALTER TABLE yishou_data.dcl_h5_element_exposure_d add columns (
--     promotion_id  STRING COMMENT '推广id'
-- );


insert overwrite table yishou_data.dcl_h5_element_exposure_d partition(dt)
select
    activity
    ,json_parse(data, '$.uid') AS user_id
    ,replace(replace(replace(replace(element,'"','') ,'[',''),']',''),']','') as element
    ,SUBSTR(json_parse(data, '$.from') ,1,10) AS from_id
    ,from_unixtime(cast(json_parse(data,'$.created') / 1000 as int)) as time
    ,goods as goods_id
    ,json_parse(data, '$.source') as source
    ,json_parse(data, '$.from') as act_from
    ,json_parse(data, '$.supply_id') as supply_id
    ,json_parse(data, '$.brand_name') as brand_name
    ,json_parse(data, '$.activity_date') as activity_date
    ,json_parse(data, '$.template_id') as template_id  --template_id
    ,split(split(get_json_object(data, '$.source'), '[&\|?]event_id=') [1],'&')[0] as event_id       -- event_id
    ,split(split(get_json_object(data, '$.source'), '[&\|?]from_banner_id=') [1],'&')[0] as from_banner_id -- from_banner_id
    ,json_parse(data, '$.promotion_id') as promotion_id
    ,dt
from yishou_data.ods_h5_event_action_d
lateral view outer explode (split(json_parse(data, '$.elements'),',')) t as element
lateral view outer explode (split(json_parse(data, '$.goods'),',')) g as goods
where dt = if(
    to_char(to_date1(${bdp.system.cyctime} ,'yyyymmddhhmiss'),'hh')=0
	,to_char(dateadd(to_date1(${bdp.system.cyctime} ,'yyyymmddhhmiss'),-1,'dd'),'yyyymmdd')
 	,to_char(to_date1(${bdp.system.cyctime} ,'yyyymmddhhmiss'),'yyyymmdd')
)
and to_char(from_unixtime(cast(json_parse(data,'$.created') / 1000 as int)),'yyyymmdd') = dt
and event = 'h5exposure'
;



