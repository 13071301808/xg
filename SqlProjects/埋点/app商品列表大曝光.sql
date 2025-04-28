-- DLI sql
-- ******************************************************************** --
-- author: hewenbao
-- CREATE time: 2024/04/22 11:23:56 GMT+08:00
-- ******************************************************************** --

-- CREATE EXTERNAL TABLE yishou_data.dwd_event_goods_exposure_incr_dt(
--     `user_id`	string	COMMENT '用户id'
--     , `goods_id`	string	COMMENT '商品id'
--     , `special_id`	string	COMMENT '专场id'
--     , `os`	string	COMMENT '操作系统'
--     , `goods_no`	string	COMMENT '货号'
--     , `pid`	string	COMMENT '来源'
--     , `ptime`	string	COMMENT '页面停留时间'
--     , `special_time`	string	COMMENT '专场时间'
--     , `source`	string	COMMENT '专场列表来源'
--     , `report_time`	string	COMMENT '上报时间'
--     , `event_id`	string	COMMENT '事件id'
--     , `search_event_id`	string	COMMENT '搜索事件id'
--     , `goods_count`	string	COMMENT '曝光商品数'
--     , `click_goods_id_count`	string	COMMENT '点击商品数'
--     , `keyword`	string	COMMENT '关键词'
--     , `app_version`	string	COMMENT 'APP版本'
--     , `log_type`	string	COMMENT '日志类型（数据来源的日志库类型：new/old）'
--     , `is_rec`	string	COMMENT '是否搜索结果推荐商品'
--     , `abtest`	string	COMMENT 'abtest的标识（后台传，如果没有则默认”“）'
--     , `index`	string	COMMENT '列表下标'
--     , `exposure_index`	string	COMMENT '曝光商品下标'
--     , `strategy_id`	string	COMMENT '策略id'
--     , `is_operat`	string	COMMENT '0'
--     , `is_default`	string	COMMENT '主被动切换(1主动，0被动)'
--     , `special_index`	string	COMMENT '处于专场详情时，专场从列表第几坑进'
--     , `page_name`	string	COMMENT '页面名'
--     , `recall_num`	string	COMMENT '召回数量'
--     , `is_pro`	string	COMMENT '是否为Pro版{1:童装货源APP,0:一手APP}'
--     , `goods_seat_id`	string	COMMENT '坑位策略id'
--     , `tab_name`	string	COMMENT 'tab名称'
--     , `cat_id`	string	COMMENT '处于分类详情页的品类id'
--     , `cat_name`	string	COMMENT '处于分类详情页的品类名'
--     , `stall_id`	string	COMMENT '处于档口主页的档口id'
--     , `pgm_code`	string	COMMENT '处于买手主页的品类id'
--     , `activity_id`	string	COMMENT '处于首页分类页tab的活动id'
--     , `activity_name`	string	COMMENT '处于首页分类页tab的活动名'
--     , `search_score`	string	COMMENT '搜索相关性得分'

-- )
-- STORED AS ORC
-- PARTITIONED BY (dt STRING COMMENT "日期分区")
-- COMMENT "商品曝光数据"
-- LOCATION "obs://yishou-bigdata/yishou_data.db/dwd_event_goods_exposure_incr_dt";


-- DROP TABLE yishou_data.dcl_event_goods_exposure_d_new;
-- CREATE TABLE
-- 	yishou_data.dcl_event_goods_exposure_d_new
-- LIKE
--     yishou_data.dcl_event_goods_exposure_d;


-- alter table yishou_data.dwd_event_goods_exposure_incr_dt
-- add columns (
--   room_id string comment '直播间id'
-- );


WITH

old_pre_table AS (
-- 旧的曝光解析（app_log_store）
    SELECT
        uid AS user_id,
        goods,
        special_id,
        os,
        pid,
        ptime,
        source,
        reportTime AS report_time,
        event_id,
        search_event_id,
        goods_count,
        click_goods_id_count,
        keyword,
        app_version,
        'old' AS type,
        search_rec_goods_id_count,
        search_rec_click_goods_id_count,
        dt
    FROM
        yishou_data.sls_log_app_action
    WHERE
        dt BETWEEN ${bdp.system.bizdate} AND TO_CHAR(DATEADD(TO_DATE1(${bdp.system.bizdate},'yyyymmdd'),1,'dd'), 'yyyymmdd')
        AND COALESCE(topic, __topic__) = 'goodsExposure'
    UNION ALL
    -- 点击日志库的商品曝光（new_app_log_store，全量）
    SELECT
        get_json_object(action,'$.properties.uid') AS user_id
        ,get_json_object(action,'$.properties.goods') AS goods
        ,get_json_object(action,'$.properties.special_id') AS special_id
        ,get_json_object(action,'$.properties.os') AS os
        ,get_json_object(action,'$.properties.pid') AS pid
        ,get_json_object(action,'$.properties.ptime') AS ptime
        ,get_json_object(action,'$.properties.source') AS source
        ,get_json_object(action,'$.properties.reportTime') AS report_time
        ,get_json_object(action,'$.properties.event_id') AS event_id
        ,get_json_object(action,'$.properties.search_event_id') AS search_event_id
        ,get_json_object(action,'$.properties.goods_count') AS goods_count
        ,get_json_object(action,'$.properties.click_goods_id_count') AS click_goods_id_count
        ,coalesce(get_json_object(action,'$.properties.key_word'), get_json_object(action,'$.properties.keyword')) AS key_word
        ,get_json_object(action,'$.properties.app_version') AS app_version
        ,'new' AS type
        ,get_json_object(action,'$.properties.search_rec_goods_id_count') AS search_rec_goods_id_count
        ,get_json_object(action,'$.properties.search_rec_click_goods_id_count') AS search_rec_click_goods_id_count
        ,dt
    FROM
        yishou_data.ods_app_event_log_d
    WHERE
        dt BETWEEN ${bdp.system.bizdate} AND to_char(dateadd(to_date1(${bdp.system.bizdate},'yyyymmdd'),1,'dd') ,'yyyymmdd')
        AND event = 'goodsexposure'
        AND length(action) < 8388608
),

old_removal_table AS (
-- 去重
    SELECT
        CASE
            WHEN coalesce(event_id, '') = '' AND coalesce(search_event_id, '') = '' THEN NULL
            ELSE
                CASE
                    WHEN coalesce(search_event_id, '') != '' THEN search_event_id
                    ELSE event_id
                END
        END AS comment_event_id,
        user_id,
        goods,
        special_id,
        os,
        pid,
        ptime,
        source,
        report_time,
        event_id,
        search_event_id,
        goods_count,
        click_goods_id_count,
        keyword,
        app_version,
        type,
        search_rec_goods_id_count,
        search_rec_click_goods_id_count,
        dt,
        CASE
            WHEN coalesce(event_id, '') = '' AND coalesce(search_event_id, '') = '' THEN 1
            ELSE
                row_number() over(
                    PARTITION BY
                        CASE WHEN coalesce(search_event_id, '') != '' THEN concat('search_event_id', search_event_id) ELSE concat('event_id', event_id) END
                        , dt
                    ORDER BY
                        CASE WHEN coalesce(goods_count , '') != '' AND is_number(goods_count) THEN cast(goods_count AS BIGINT) ELSE 0 END DESC
                        , report_time DESC)
        END AS goods_count_rank
    FROM
        old_pre_table
    HAVING
        goods_count_rank = 1
),

old_result_table AS (
-- 炸裂, 得到旧版本结果表
    SELECT
        user_id
        , goods_list AS goods_id
        , special_id
        , os
        , pid
        , ptime
        , source
        , report_time
        , event_id
        , search_event_id
        , goods_count
        , click_goods_id_count
        , keyword
        , app_version
        , type
        , dt
    FROM old_removal_table
    -- 每条goods_id拆成一行
    lateral VIEW outer explode (split(goods,','))t AS goods_list
    WHERE goods_list IS NOT NULL AND goods_list <> ''
),

new_exposure_detail AS (
    SELECT
        DISTINCT user_id
        , coalesce(get_json_object(goods_dict, '$.goods_id'), get_json_object(goods_dict, '$.good_id')) AS goods_id
        , get_json_object(goods_dict, '$.is_rec') AS is_rec
        , special_id
        , os
        , get_json_object(goods_dict, '$.goods_no') AS goods_no
        , pid
        , coalesce(ptime,0) AS ptime
        , source
        , report_time
        , event_id
        , search_event_id
        , keyword
        , app_version
        , type AS log_type
        , CASE
            WHEN search_event_id IS NOT NULL AND search_event_id <> ''
            THEN concat('search_event_id', search_event_id)
            ELSE concat('event_id', event_id)
        END AS comment_event_id
        -- , get_json_object(goods_dict, '$.is_rec') AS is_rec
        , get_json_object(goods_dict, '$.abtest') AS abtest
        , get_json_object(goods_dict, '$.index') AS index
        , get_json_object(goods_dict, '$.strategy_id') AS strategy_id
        , get_json_object(goods_dict, '$.is_operat') AS is_operat
        , get_json_object(goods_dict, '$.is_default') AS is_default
        , get_json_object(goods_dict, '$.goods_seat_id') as goods_seat_id
        , get_json_object(goods_dict, '$.tab_name') as tab_name
        , get_json_object(goods_dict, '$.search_score') as  search_score
        , special_index
        , page_name
        , recall_num
        , is_pro
        , cat_id
        , cat_name
        , stall_id
        , pgm_code
        , activity_id
        , activity_name
        , secondsource
        , get_json_object(goods_dict, '$.goods_similarity') as  goods_similarity
        , room_id
        , dt
    FROM
        temp.exposure_pretable
    lateral VIEW
        outer explode (split(goods, '\\|-\\|'))t AS goods_dict
    WHERE
        goods_dict IS NOT NULL
        AND goods_dict <> ''
),

new_click_detail AS (
    SELECT
        DISTINCT user_id
        , coalesce(get_json_object(goods_dict, '$.goods_id'), get_json_object(goods_dict, '$.good_id')) AS goods_id
        , get_json_object(goods_dict, '$.is_rec') AS is_rec
        , special_id
        , os
        , get_json_object(goods_dict, '$.goods_no') AS goods_no
        , pid
        , ptime
        , source
        , report_time
        , event_id
        , search_event_id
        , keyword
        , app_version
        , type AS log_type
        , CASE
            WHEN search_event_id IS NOT NULL AND search_event_id <> ''
            THEN concat('search_event_id', search_event_id)
            ELSE concat('event_id', event_id)
        END AS comment_event_id
        , dt
    FROM
        temp.exposure_pretable
    lateral VIEW
        outer explode (split(click_goods, '\\|-\\|'))t AS goods_dict
    WHERE
        goods_dict IS NOT NULL
    AND
        goods_dict <> ''
),

new_count_table AS (
    SELECT
        coalesce(ged.comment_event_id, gec.comment_event_id) AS comment_event_id
        , coalesce(ged.goods_count, '0') AS goods_count
        , coalesce(gec.click_goods_id_count, '0') AS click_goods_id_count
        , coalesce(ged.dt, gec.dt) AS dt
    FROM (
        -- 曝光事件统计
        SELECT
            comment_event_id
            , dt
            , count(DISTINCT goods_id) AS goods_count
        FROM
            new_exposure_detail
        GROUP BY
            comment_event_id
            , dt
    ) ged
    FULL JOIN (
        -- 曝光点击事件统计
        SELECT
            comment_event_id
            , dt
            , count(DISTINCT goods_id) AS click_goods_id_count
        FROM
            new_click_detail
        GROUP BY
            comment_event_id
            , dt
    ) gec
    ON
        ged.dt = gec.dt
        AND ged.comment_event_id = gec.comment_event_id
),

result_table AS (
-- 6.0.0版本之前商品曝光
    SELECT
    /*+ BROADCAST(e) */
        a.user_id
        , a.goods_id
        , a.special_id
        , a.os
        , b.goods_no
        , a.pid
        , a.ptime
        , to_char(from_unixtime(e.special_start_time) ,'yyyymmdd') AS special_time
        , a.source
        , a.report_time
        , a.event_id
        , a.search_event_id
        , a.goods_count
        , a.click_goods_id_count
        , a.keyword
        , a.app_version
        , a.type AS log_type
        , NULL AS is_rec
        , NULL AS abtest
        , NULL AS index
        , NULL AS exposure_index
        , NULL AS strategy_id
        , NULL AS is_operat
        , NULL AS is_default
        , NULL AS special_index
        , NULL AS page_name
        , NULL AS recall_num
        , NULL AS is_pro
        , NULL AS goods_seat_id
        , NULL AS tab_name
        , NULL AS cat_id
        , NULL AS cat_name
        , NULL AS stall_id
        , NULL AS pgm_code
        , NULL AS activity_id
        , NULL AS activity_name
        , NULL AS search_score
        , null as secondsource
        , null as goods_similarity
        , null as room_id
        , a.dt
    FROM old_result_table a
    LEFT JOIN yishou_data.all_fmys_goods_h b ON cast(a.goods_id AS INT) = b.goods_id
    LEFT JOIN yishou_data.all_fmys_special_h e ON b.special_id = e.special_id AND a.goods_id IS NOT NULL AND length(trim(a.goods_id))  > 0
    UNION ALL
    -- 6.0.0版本以后商品曝光
    SELECT
    /*+ BROADCAST(fs) */
        ged.user_id
        , ged.goods_id
        , ged.special_id
        , ged.os
        , fg.goods_no
        , ged.pid
        , ged.ptime
        , to_char(from_unixtime(fs.special_start_time) ,'yyyymmdd') AS special_time
        , CASE WHEN ged.source = '' OR NOT is_number(ged.source) THEN NULL ELSE source END AS source
        , cast(ged.report_time AS STRING) AS report_time
        , ged.event_id
        , ged.search_event_id
        , geb.goods_count
        , geb.click_goods_id_count
        , ged.keyword
        , ged.app_version
        , ged.log_type
        , ged.is_rec
        , ged.abtest
        , ged.index
        , ged.index AS exposure_index
        , ged.strategy_id
        , ged.is_operat
        , ged.is_default
        , ged.special_index
        , ged.page_name
        , ged.recall_num
        , ged.is_pro
        , ged.goods_seat_id
        , ged.tab_name
        , ged.cat_id
        , ged.cat_name
        , ged.stall_id
        , ged.pgm_code
        , ged.activity_id
        , ged.activity_name
        , ged.search_score
        , ged.secondsource
        , ged.goods_similarity
        , ged.room_id
        , ged.dt
    FROM new_exposure_detail ged
    LEFT JOIN new_count_table geb ON ged.dt = geb.dt AND ged.comment_event_id = geb.comment_event_id
    LEFT JOIN yishou_data.all_fmys_goods_h fg ON cast(ged.goods_id AS INT) = fg.goods_id
    LEFT JOIN yishou_data.all_fmys_special_h fs ON fg.special_id = fs.special_id
    WHERE ged.goods_id IS NOT NULL AND length(trim(ged.goods_id)) > 0
)
INSERT OVERWRITE TABLE yishou_data.dwd_event_goods_exposure_incr_dt PARTITION (dt)
SELECT
    user_id,
    goods_id,
    special_id,
    os,
    goods_no,
    pid,
    ptime,
    special_time,
    source,
    report_time,
    event_id,
    search_event_id,
    goods_count,
    click_goods_id_count,
    keyword,
    app_version,
    log_type,
    is_rec,
    abtest,
    INDEX,
    exposure_index,
    strategy_id,
    is_operat,
    is_default,
    special_index,
    page_name,
    recall_num,
    is_pro,
    goods_seat_id,
    tab_name,
    cat_id,
    cat_name,
    stall_id,
    pgm_code,
    activity_id,
    activity_name,
    search_score,
    secondsource,
    goods_similarity,
    room_id,
    dt
FROM
    result_table
DISTRIBUTE BY floor(rand()*200)
-- SELECT * FROM dcl_event_goods_exposure_d
-- SELECT
-- 	COUNT(1),
-- 	COUNT(DISTINCT user_id),
-- 	COUNT(DISTINCT user_id, goods_id)
-- FROM
-- 	dcl_event_goods_exposure_d
-- DISTRIBUTE BY
-- 	FLOOR(RAND() * 400)

;

-- SELECT count(1), count(DISTINCT user_id), count(DISTINCT user_id, goods_id) FROM yishou_data.dcl_event_goods_exposure_d_new WHERE dt = ${bdp.system.bizdate}