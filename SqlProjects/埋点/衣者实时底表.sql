-- 创建日期格式化函数，将10位时间戳转换成8位年月日（yyyyMMdd）
CREATE FUNCTION date_formatted AS 'com.fumi.flink.sql.udf.DateFormattedUDF';
-- ============================================================================
-- 3个主题在obs中的具体路径
-- ods_yizhe_yz_click_dt
-- ods_yizhe_yz_exposure_dt
-- ods_yizhe_yz_route_dt
-- ============================================================================
-- ods_yizhe_yz_click_dt 主题数据 （衣者埋点中的点击日志数据）
-- 创建kafka的source源
CREATE SOURCE STREAM bigdata_yizhe_yz_click_source (
	`data` STRING,
	`__receive_time__` string,
	`topic` STRING
) WITH (
	type = "kafka",
	kafka_bootstrap_servers = "192.168.9.220:9092,192.168.9.232:9092,192.168.9.98:9092",
	kafka_group_id = "behavior_yizhe_log_to_obs",
	kafka_topic = "bigdata_yizhe_yz_click",
    start_time = "2025-01-06 12:00:00",
	encode = "json",
	json_config = "data=data;__receive_time__=__receive_time__;topic=topic;"
);

-- 创建obs的sink源
CREATE SINK STREAM bigdata_yizhe_yz_click_sink (
	`data` STRING,
	`__receive_time__` STRING,
	`topic` STRING,
	`dt` STRING
) PARTITIONED BY(dt) WITH (
	type = "filesystem",
	file.path = "obs://yishou-bigdata/yishou_data.db/ods_yizhe_yz_click_dt",
	encode = "parquet",
	ak = "EULIXLKJQLUS62GPNIAX",
	sk = "vmoTDJVUOXVtwt8pwh5LQ6LnE2bJPDtxid7biimK"
);

-- 将source源数据写入到sink源中
INSERT INTO 
	bigdata_yizhe_yz_click_sink
SELECT
	`data`,
	`__receive_time__`,
	`topic`,
	date_formatted(cast(CAST(`__receive_time__` AS BIGINT) / 1000 as string),'yyyyMMdd') as dt
FROM
	bigdata_yizhe_yz_click_source
;
	
-- ods_yizhe_yz_exposure_dt 主题数据 （衣者埋点中的点击日志数据）
-- 创建kafka的source源
CREATE SOURCE STREAM bigdata_yizhe_yz_exposure_source (
    `data` STRING,
	`__receive_time__` string,
	`topic` STRING
) WITH (
	type = "kafka",
	kafka_bootstrap_servers = "192.168.9.220:9092,192.168.9.232:9092,192.168.9.98:9092",
	kafka_group_id = "behavior_yizhe_log_to_obs",
	kafka_topic = "bigdata_yizhe_yz_exposure",
    start_time = "2025-01-06 12:00:00",
	encode = "json",
	json_config = "data=data;__receive_time__=__receive_time__;topic=topic;"
);

-- 创建obs的sink源
CREATE SINK STREAM bigdata_yizhe_yz_exposure_sink (
	`data` STRING,
	`__receive_time__` STRING,
	`topic` STRING,
	`dt` STRING
) PARTITIONED BY(dt) WITH (
	type = "filesystem",
	file.path = "obs://yishou-bigdata/yishou_data.db/ods_yizhe_yz_exposure_dt",
	encode = "parquet",
	ak = "EULIXLKJQLUS62GPNIAX",
	sk = "vmoTDJVUOXVtwt8pwh5LQ6LnE2bJPDtxid7biimK"
);

-- 将source源数据写入到sink源中
INSERT INTO 
	bigdata_yizhe_yz_exposure_sink
SELECT
   	`data`,
	`__receive_time__`,
	`topic`,
	date_formatted(cast(CAST(`__receive_time__` AS BIGINT) / 1000 as string),'yyyyMMdd') as dt
FROM
	bigdata_yizhe_yz_exposure_source
;
	
-- -- ods_yizhe_yz_route_dt 主题数据 （衣者埋点中的点击日志数据）
-- -- 创建kafka的source源
-- CREATE SOURCE STREAM bigdata_yizhe_yz_route_source (
--     __user_agent__ STRING,
-- 	data STRING,
-- 	__receive_time__ STRING,
-- 	__isLtsSplicedContent__ STRING,
-- 	__client_ip__ STRING,
-- 	`topic` STRING,
-- 	__client_time__ STRING
-- ) WITH (
-- 	type = "kafka",
-- 	kafka_bootstrap_servers = "192.168.9.220:9092,192.168.9.232:9092,192.168.9.98:9092",
-- 	kafka_group_id = "behavior_yizhe_log_to_obs",
-- 	kafka_topic = "bigdata_yizhe_yz_route",
--     start_time = "2025-01-02 00:00:00",
-- 	encode = "json",
-- 	json_config = "__user_agent__=__labels__.user_agent;data=data;__receive_time__=__receive_time__;__isLtsSplicedContent__=__isLtsSplicedContent__;__client_ip__=__labels__.client_ip;topic=topic;__client_time__=__client_time__;"
-- );

-- -- 创建obs的sink源
-- CREATE SINK STREAM bigdata_yizhe_yz_route_sink (
-- 	__user_agent__ STRING,
-- 	data STRING,
-- 	__receive_time__ bigint,
-- 	__isLtsSplicedContent__ STRING,
-- 	__client_ip__ STRING,
-- 	`topic` STRING,
-- 	__client_time__ bigint,
-- 	`dt` STRING
-- ) PARTITIONED BY(dt) WITH (
-- 	type = "filesystem",
-- 	file.path = "obs://yishou-bigdata/yishou_data.db/ods_yizhe_yz_route_dt",
-- 	encode = "parquet",
-- 	ak = "EULIXLKJQLUS62GPNIAX",
-- 	sk = "vmoTDJVUOXVtwt8pwh5LQ6LnE2bJPDtxid7biimK"
-- );

-- -- 将source源数据写入到sink源中
-- INSERT INTO 
-- 	bigdata_yizhe_yz_route_sink
-- SELECT
--     __user_agent__ ,
-- 	data ,
-- 	cast(__receive_time__ as BIGINT) as __receive_time__ ,
-- 	__isLtsSplicedContent__ ,
-- 	__client_ip__ ,
-- 	`topic` ,
-- 	cast(__client_time__ as BIGINT) as __client_time__ ,
-- 	date_formatted(__receive_time__, 'yyyyMMdd') as dt
-- FROM
-- 	bigdata_yizhe_yz_route_source;