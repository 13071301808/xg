
-- ====================================== 配置表开始 ======================================
-- 从后端的两个配置表中获取数据，并对这些数据进行计算梳理
--      1、以goods_no为主键，后面加上策略id和阶段序号（需要剔除这个策略id中的黑名单goods_no）【产品prd上，同一个goods_no如果命中多条策略，只需要计算其中一条策略】
--      2、进行时间合并
--              【a:在24小时清除策略中，需要对日期和时间进行处理；b:根据当前运算时间进行判断，只获取当前需要统计的阶段】
--      3、在时间合并中，除了这个阶段的开始时间和结束时间的 时间格式，还需要
--              【a:将时间格式转换成时间戳 b:当前阶段从几点开始的 c:当前阶段一共几个小时 d:当前运算时间处在第几个小时上】
--      4、只需要当前生效的策略，当前时间的阶段

-- 建表语句
-- ads_artificial_flow_control_config
-- DROP TABLE if exists yishou_daily.ads_artificial_flow_control_config;
-- create external table if not exists yishou_daily.ads_artificial_flow_control_config (
--     goods_no bigint comment '商品货号【123412】'
--     , strategy_id bigint comment '策略id【1】'
--     , strategy_name string comment '策略名【策略1】'
--     , strategy_start_date string comment '策略开始时间【2025-03-27 00:10:00】'
--     , strategy_end_date string comment '策略结束时间【2025-03-28 00:10:00】'
--     , strategy_priority bigint comment '该策略优先级【1】'
--     , strategy_layer_id bigint comment '实验层【2】'
--     , strategy_user_tails string comment '实验用户尾号，多个用英文逗号分隔【1,2,3】'
--     , strategy_addflow_type bigint comment '加流数量的类型{1:保证实验组的商品曝光UV，2:实验组商品曝光UV高于对照组} 【1】'
--     , strategy_addflow_time_type bigint comment '加流数量的累计时间{1:每24小时累计计算，2:在策略生效时间内累计计算} 【1】'

--     , stage_number bigint comment '阶段序号【1】'
--     , stage_smooth_start_date string comment '阶段开始时间【2025-03-28 00:10:00】'
--     , stage_smooth_start_time bigint comment '阶段开始时间戳【12345678900】'
--     , stage_smooth_end_date string comment '阶段结束时间【2025-03-29 00:10:00】'
--     , stage_smooth_end_time bigint comment '阶段结束时间戳，需要注意，这个需要根据阶段结束时间来换算，比如业务要求是8点结束，那这个时间戳就是8点59分59秒的时间戳【12345678911】'
--     , stage_target_exposure_uv bigint comment '目标曝光UV（保证实验组的商品曝光UV到达值）【1000】'
--     , stage_target_exposure_uv_diff bigint comment '目标曝光UV差值（实验组商品曝光UV高于对照组值）【1000】'
--     , stage_next_stage_exposure_uv bigint comment '进入下一阶段的曝光UV条件（>=该配置值）'
--     , stage_next_stage_uv_value double comment '进入下一阶段的UV价值条件（>=该配置值）'
--     , stage_next_stage_gmv double comment '进入下一阶段的GMV条件（>=该配置值）'
--     , stage_next_stage_add_cart_count bigint comment '进入下一阶段的加购数量条件（>=该配置值）'
--     , stage_next_stage_add_cart_rate double comment '进入下一阶段的点击加购率条件（>=该配置值）'
--     , stage_next_stage_exposure_rate double comment '进入下一阶段的曝光点击率条件（>=该配置值）'
--     , stage_terminate_uv_value double comment '终止加流的UV价值条件（小于该配置值）'

--     , stage_start_hour bigint comment '这个阶段的开始小时数，比如开始时间是7点，那就是7 【7】'
--     , stage_total_hour bigint comment '这个阶段的总小时数，比如开始时间是8点，结束时间是12点，那就是5 【5】'
--     , stage_current_hour bigint comment '当前运行时间在这个阶段的小时数，比如开始时间是8点，那当前时间如果是8点，那就是0，当前时间是9点，那就是1 【2】'
--     , current_date string comment '当前时间，格式为 yyyyMMddHHmmss'
-- ) comment '人工流量调控配置表'
-- STORED AS ORC LOCATION 'obs://yishou-bigdata/yishou_daily.db/ads_artificial_flow_control_config'
-- ;

-- 跑数脚本
with

strategy_table as (
    -- 选品id里的商品，和上传的商品进行合并，并打散去重
    -- 只取优先级最高的
    select
        id
        , strategy_name
        , select_goods_rule_id
        , upload_file_name
        , file_path
        -- , goods_no_list
        , start_time
        , end_time
        , priority
        , layer_id
        , user_tails
        , addflow_type
        , addflow_time_type
        , remark
        , status
        , created_admin_id
        , updated_admin_id
        , created_at
        , updated_at
        , is_deleted
        , goods_no
        , row_number() over(partition by goods_no order by priority desc, id desc) as priority_row_number_id
    from (
        -- 对商品进行打散，并只需要启用和未删除的策略
        select
            id
            , strategy_name
            , select_goods_rule_id
            , upload_file_name
            , file_path
            , goods_no_list
            , start_time
            , end_time
            , priority
            , layer_id
            , user_tails
            , addflow_type
            , addflow_time_type
            , remark
            , status
            , created_admin_id
            , updated_admin_id
            , created_at
            , updated_at
            , is_deleted
            , explode(split(goods_no_list, ',')) as goods_no
        from(
            -- 选品id 和 对应的商品列表
            select
                rule_table.id
                , rule_table.strategy_name
                , rule_table.rule_id as select_goods_rule_id
                , rule_table.upload_file_name
                , rule_table.file_path
                , rule_goods_table.goods_no as goods_no_list
                , rule_table.start_time
                , rule_table.end_time
                , rule_table.priority
                , rule_table.layer_id
                , rule_table.user_tails
                , rule_table.addflow_type
                , rule_table.addflow_time_type
                , rule_table.remark
                , rule_table.status
                , rule_table.created_admin_id
                , rule_table.updated_admin_id
                , rule_table.created_at
                , rule_table.updated_at
                , rule_table.is_deleted
            from (
                select
                    id
                    , strategy_name
                    , select_goods_rule_id
                    , upload_file_name
                    , file_path
                    , goods_no
                    , start_time
                    , end_time
                    , priority
                    , layer_id
                    , user_tails
                    , addflow_type
                    , addflow_time_type
                    , remark
                    , status
                    , created_admin_id
                    , updated_admin_id
                    , created_at
                    , updated_at
                    , is_deleted
                    , explode(split(select_goods_rule_id, ',')) as rule_id
                from yishou_data.ods_fmys_flow_package_strategy_view
                where
                    unix_timestamp('${bdp.system.cyctime}','yyyyMMddHHmmss') between
                        unix_timestamp(substr(start_time, 1, 10),'yyyy-MM-dd') + 7 * 3600
                        and
                        unix_timestamp(substr(end_time, 1, 10),'yyyy-MM-dd') + 7 * 3600
            ) as rule_table

            left join yishou_data.ods_fmys_business_strategy_goods_view as rule_goods_table
            on rule_table.rule_id = rule_goods_table.rule_header_id

            where length(rule_goods_table.goods_no) > 0

            -- 上传的商品列表
            union all
            select
                id
                , strategy_name
                , select_goods_rule_id
                , upload_file_name
                , file_path
                , goods_no as goods_no_list
                , start_time
                , end_time
                , priority
                , layer_id
                , user_tails
                , addflow_type
                , addflow_time_type
                , remark
                , status
                , created_admin_id
                , updated_admin_id
                , created_at
                , updated_at
                , is_deleted
            from yishou_data.ods_fmys_flow_package_strategy_view
            where 
                length(goods_no) > 0
                and unix_timestamp('${bdp.system.cyctime}','yyyyMMddHHmmss') between
                    unix_timestamp(substr(start_time, 1, 10),'yyyy-MM-dd') + 7 * 3600
                    and
                    unix_timestamp(substr(end_time, 1, 10),'yyyy-MM-dd') + 7 * 3600
        )
        where 
            status = 1 
            and is_deleted = 0
    )
    having priority_row_number_id = 1
)

, stage_table as (
    -- 去除删除的阶段
    -- 当加流数量的累计时间（addflow_time_type）等于1时（每24小时累计计算） 需要对阶段的开始时间和结束时间进行匹配计算
    -- 阶段结束时间，需要进行转换
    select 
        id
        , strategy_id
        , stage_number
        , case
            when length(cast(smooth_start_time as string)) = 19
                then smooth_start_time
            when length(cast(smooth_start_time as string)) = 8 and cast(substr(smooth_start_time, 1, 2) as int) >= 7
                then concat(date_format(from_unixtime(unix_timestamp('${bdp.system.cyctime}', 'yyyyMMddHHmmss') - 7 * 3600), 'yyyy-MM-dd'), ' ', smooth_start_time)
            when length(cast(smooth_start_time as string)) = 8 and cast(substr(smooth_start_time, 1, 2) as int) < 7
                then concat(date_format(from_unixtime(unix_timestamp('${bdp.system.cyctime}', 'yyyyMMddHHmmss') - 7 * 3600 + 24 * 3600), 'yyyy-MM-dd'), ' ', smooth_start_time)
        end as smooth_start_time
        , case
            when length(cast(smooth_end_time as string)) = 19
                then concat(date_format(from_unixtime(unix_timestamp(smooth_end_time, 'yyyy-MM-dd HH:mm:ss') - 3600), 'yyyy-MM-dd HH:'), '59:59')
            when length(cast(smooth_end_time as string)) = 8 and cast(substr(smooth_end_time, 1, 2) as int) >= 8
                then concat(date_format(from_unixtime(unix_timestamp('${bdp.system.cyctime}', 'yyyyMMddHHmmss') - 7 * 3600), 'yyyy-MM-dd'), ' ', from_unixtime(unix_timestamp(cast(smooth_end_time as string), 'HH:mm:ss') - 3600, 'HH:'), '59:59')
            when length(cast(smooth_end_time as string)) = 8 and cast(substr(smooth_end_time, 1, 2) as int) < 8
                then concat(date_format(from_unixtime(unix_timestamp('${bdp.system.cyctime}', 'yyyyMMddHHmmss') - 7 * 3600 + 24 * 3600), 'yyyy-MM-dd'), ' ', from_unixtime(unix_timestamp(cast(smooth_end_time as string), 'HH:mm:ss') - 3600, 'HH:'), '59:59')
        end as smooth_end_time
        , target_exposure_uv
        , target_exposure_uv_diff
        , next_stage_uv_value
        , next_stage_gmv
        , next_stage_add_cart_count
        , next_stage_add_cart_rate
        , next_stage_exposure_rate
        , terminate_uv_value
        , created_admin_id
        , updated_admin_id
        , created_at
        , updated_at
        , is_deleted
        , smooth_start_time as old_smooth_start_time
        , smooth_end_time as old_smooth_end_time
    from yishou_data.ods_fmys_flow_package_strategy_rule_view
    where is_deleted = 0
    having
        unix_timestamp('${bdp.system.cyctime}','yyyyMMddHHmmss') between
            unix_timestamp(smooth_start_time)
            and
            unix_timestamp(smooth_end_time)
)

, black_list_table as (
    -- 这里面存在的策略id和商品都要剔除，不需要进行计算了
    select
        goods_no
        , strategy_id
        , is_black
        , to_black_date
    from yishou_daily.ads_artificial_flow_control_black_list_partition_table
    where partition_field = (
        select
            max(partition_field)
        from yishou_daily.ads_artificial_flow_control_black_list_partition_table
        where
            partition_field between
                concat(TO_CHAR(DATEADD(to_date1(${bdp.system.cyctime},'yyyymmddhhmiss'), -2, "dd"),"yyyymmdd"), '000000')
                and
                concat(TO_CHAR(DATEADD(to_date1(${bdp.system.cyctime},'yyyymmddhhmiss'), 1, "dd"),"yyyymmdd"), '000000')
    )
)


-- 黑名单中的那些策略和商品 都不需要
-- 当前的运行时间（${bdp.system.cyctime}）要在这个阶段中的才需要
-- 阶段的 开始小时数、总小时数、当前时间在这个阶段的哪个小时 需要进行计算
insert OVERWRITE table yishou_daily.ads_artificial_flow_control_config
select
    strategy_table.goods_no
    , strategy_table.id as strategy_id
    , strategy_table.strategy_name
    , strategy_table.start_time as strategy_start_date
    , strategy_table.end_time as strategy_end_date
    , strategy_table.priority as strategy_priority
    , strategy_table.layer_id as strategy_layer_id
    , strategy_table.user_tails as strategy_user_tails
    , strategy_table.addflow_type as strategy_addflow_type
    , strategy_table.addflow_time_type as strategy_addflow_time_type
    , stage_table.stage_number as stage_number
    , stage_table.smooth_start_time as stage_smooth_start_date
    , unix_timestamp(stage_table.smooth_start_time) as stage_smooth_start_time
    , stage_table.smooth_end_time as stage_smooth_end_date
    , unix_timestamp(stage_table.smooth_end_time) as stage_smooth_end_time
    , stage_table.target_exposure_uv as stage_target_exposure_uv
    , stage_table.target_exposure_uv_diff as stage_target_exposure_uv_diff
    , if(strategy_table.addflow_type = 1, stage_table.target_exposure_uv, stage_table.target_exposure_uv_diff) as stage_next_stage_exposure_uv
    , stage_table.next_stage_uv_value as stage_next_stage_uv_value
    , stage_table.next_stage_gmv as stage_next_stage_gmv
    , stage_table.next_stage_add_cart_count as stage_next_stage_add_cart_count
    , stage_table.next_stage_add_cart_rate as stage_next_stage_add_cart_rate
    , stage_table.next_stage_exposure_rate as stage_next_stage_exposure_rate
    , stage_table.terminate_uv_value as stage_terminate_uv_value
    , hour(from_unixtime(unix_timestamp(stage_table.smooth_start_time))) as stage_start_hour
    , round((unix_timestamp(stage_table.smooth_end_time) - unix_timestamp(stage_table.smooth_start_time)) / 3600) as stage_total_hour
    , ceil((unix_timestamp('${bdp.system.cyctime}', 'yyyyMMddHHmmss') - unix_timestamp(stage_table.smooth_start_time)) / 3600 - 1) as stage_current_hour
    , '${bdp.system.cyctime}' as current_date
from strategy_table

left join stage_table
on strategy_table.id = stage_table.strategy_id

left join black_list_table 
on 
    strategy_table.goods_no = black_list_table.goods_no 
    and strategy_table.id = black_list_table.strategy_id

where 
    black_list_table.goods_no is null 
    and black_list_table.strategy_id is null
    and stage_table.id is not null
;



-- 分区备份表（供业务复盘）
-- DROP TABLE if exists yishou_daily.ads_artificial_flow_control_config_partition_table;
-- create external table if not exists yishou_daily.ads_artificial_flow_control_config_partition_table (
--     goods_no bigint comment '商品货号【123412】'
--     , strategy_id bigint comment '策略id【1】'
--     , strategy_name string comment '策略名【策略1】'
--     , strategy_start_date string comment '策略开始时间【2025-03-27 00:10:00】'
--     , strategy_end_date string comment '策略结束时间【2025-03-28 00:10:00】'
--     , strategy_priority bigint comment '该策略优先级【1】'
--     , strategy_layer_id bigint comment '实验层【2】'
--     , strategy_user_tails string comment '实验用户尾号，多个用英文逗号分隔【1,2,3】'
--     , strategy_addflow_type bigint comment '加流数量的类型{1:保证实验组的商品曝光UV，2:实验组商品曝光UV高于对照组} 【1】'
--     , strategy_addflow_time_type bigint comment '加流数量的累计时间{1:每24小时累计计算，2:在策略生效时间内累计计算} 【1】'

--     , stage_number bigint comment '阶段序号【1】'
--     , stage_smooth_start_date string comment '阶段开始时间【2025-03-28 00:10:00】'
--     , stage_smooth_start_time bigint comment '阶段开始时间戳【12345678900】'
--     , stage_smooth_end_date string comment '阶段结束时间【2025-03-29 00:10:00】'
--     , stage_smooth_end_time bigint comment '阶段结束时间戳，需要注意，这个需要根据阶段结束时间来换算，比如业务要求是8点结束，那这个时间戳就是8点59分59秒的时间戳【12345678911】'
--     , stage_target_exposure_uv bigint comment '目标曝光UV（保证实验组的商品曝光UV到达值）【1000】'
--     , stage_target_exposure_uv_diff bigint comment '目标曝光UV差值（实验组商品曝光UV高于对照组值）【1000】'
--     , stage_next_stage_exposure_uv bigint comment '进入下一阶段的曝光UV条件（>=该配置值）'
--     , stage_next_stage_uv_value double comment '进入下一阶段的UV价值条件（>=该配置值）'
--     , stage_next_stage_gmv double comment '进入下一阶段的GMV条件（>=该配置值）'
--     , stage_next_stage_add_cart_count bigint comment '进入下一阶段的加购数量条件（>=该配置值）'
--     , stage_next_stage_add_cart_rate double comment '进入下一阶段的加购率条件（>=该配置值）'
--     , stage_next_stage_exposure_rate double comment '进入下一阶段的曝光点击率条件（>=该配置值）'
--     , stage_terminate_uv_value double comment '终止加流的UV价值条件（小于该配置值）'

--     , stage_start_hour bigint comment '这个阶段的开始小时数，比如开始时间是7点，那就是7 【7】'
--     , stage_total_hour bigint comment '这个阶段的总小时数，比如开始时间是8点，结束时间是12点，那就是5 【5】'
--     , stage_current_hour bigint comment '当前运行时间在这个阶段的小时数，比如开始时间是8点，那当前时间如果是8点，那就是0，当前时间是9点，那就是1 【2】'
--     , current_date string comment '当前时间，格式为 yyyyMMddHHmmss'
-- ) comment '人工流量调控配置表(分区备份表，供业务复盘)'
-- PARTITIONED BY (partition_field STRING COMMENT '分区字段（格式：yyyyMMddHHmmss）') 
-- STORED AS ORC LOCATION 'obs://yishou-bigdata/yishou_daily.db/ads_artificial_flow_control_config_partition_table'
-- ;

-- 数据备份
insert overwrite table yishou_daily.ads_artificial_flow_control_config_partition_table partition(partition_field)
select
    *
    , current_date as partition_field
from yishou_daily.ads_artificial_flow_control_config
distribute by partition_field
;



-- ====================================== 配置表结束 ======================================
-- =======================================================================================
-- ====================================== 统计表开始 ======================================

-- ads_artificial_flow_control_result_partition_table
-- DROP TABLE if exists yishou_daily.ads_artificial_flow_control_result_partition_table;
-- create external table if not exists yishou_daily.ads_artificial_flow_control_result_partition_table (
--     `goods_no` STRING comment '商品货号',
--     `strategy_id` BIGINT comment '策略id',
--     `experiment_exposure_uv` BIGINT comment '实验组在该策略该阶段的曝光UV',
--     `contrast_exposure_uv` BIGINT comment '对照组在该策略该阶段的曝光UV',
--     `experiment_add_cart_uv` BIGINT comment '实验组在该策略该阶段的加购UV',
--     `contrast_add_cart_uv` BIGINT comment '对照组在该策略该阶段的加购UV',
--     `experiment_add_cart_num` DOUBLE comment '实验组在该策略该阶段的加购数量',
--     `contrast_add_cart_num` DOUBLE comment '对照组在该策略该阶段的加购数量',
--     `experiment_gmv` DOUBLE comment '实验组在该策略该阶段的gmv',
--     `contrast_gmv` DOUBLE comment '对照组在该策略该阶段的gmv',
--     `experiment_click_uv` bigint comment '实验组在该策略该阶段的点击UV',
--     `contrast_click_uv` bigint comment '对照组在该策略该阶段的点击UV',
--     `strategy_name` STRING COMMENT '策略名【策略1】',
--     `strategy_start_date` STRING COMMENT '策略开始时间【2025-03-27 00:10:00】',
--     `strategy_end_date` STRING COMMENT '策略结束时间【2025-03-28 00:10:00】',
--     `strategy_priority` BIGINT COMMENT '该策略优先级【1】',
--     `strategy_layer_id` BIGINT COMMENT '实验层【2】',
--     `strategy_user_tails` STRING COMMENT '实验用户尾号，多个用英文逗号分隔【1,2,3】',
--     `strategy_addflow_type` BIGINT COMMENT '加流数量的类型{1:保证实验组的商品曝光UV，2:实验组商品曝光UV高于对照组} 【1】',
--     `strategy_addflow_time_type` BIGINT COMMENT '加流数量的累计时间{1:每24小时累计计算，2:在策略生效时间内累计计算} 【1】',
--     `stage_number` BIGINT COMMENT '阶段序号【1】',
--     `stage_smooth_start_date` STRING COMMENT '阶段开始时间【2025-03-28 00:10:00】',
--     `stage_smooth_start_time` BIGINT COMMENT '阶段开始时间戳【12345678900】',
--     `stage_smooth_end_date` STRING COMMENT '阶段结束时间【2025-03-29 00:10:00】',
--     `stage_smooth_end_time` BIGINT COMMENT '阶段结束时间戳，需要注意，这个需要根据阶段结束时间来换算，比如业务要求是8点结束，那这个时间戳就是8点59分59秒的时间戳【12345678911】',
--     `stage_target_exposure_uv` BIGINT COMMENT '目标曝光UV（保证实验组的商品曝光UV到达值）【1000】',
--     `stage_target_exposure_uv_diff` BIGINT COMMENT '目标曝光UV差值（实验组商品曝光UV高于对照组值）【1000】',
--     `stage_next_stage_exposure_uv` BIGINT COMMENT '进入下一阶段的曝光UV条件（>=该配置值）',
--     `stage_next_stage_uv_value` DOUBLE COMMENT '进入下一阶段的UV价值条件（>=该配置值）',
--     `stage_next_stage_gmv` DOUBLE COMMENT '进入下一阶段的GMV条件（>=该配置值）',
--     `stage_next_stage_add_cart_count` BIGINT COMMENT '进入下一阶段的加购数量条件（>=该配置值）',
--     `stage_next_stage_add_cart_rate` double COMMENT '进入下一阶段的点击加购率条件（>=该配置值）',
--     `stage_next_stage_exposure_rate` double comment '进入下一阶段的曝光点击率条件（>=该配置值）',
--     `stage_terminate_uv_value` DOUBLE COMMENT '终止加流的UV价值条件（小于该配置值）',
--     `stage_start_hour` BIGINT COMMENT '这个阶段的开始小时数，比如开始时间是7点，那就是7 【7】',
--     `stage_total_hour` BIGINT COMMENT '这个阶段的总小时数，比如开始时间是8点，结束时间是12点，那就是5 【5】',
--     `stage_current_hour` BIGINT COMMENT '当前运行时间在这个阶段的小时数，比如开始时间是8点，那当前时间如果是8点，那就是0，当前时间是9点，那就是1 【2】',
--     `current_date` STRING COMMENT '当前时间，格式为 yyyyMMddHHmmss',
--     `start_date` BIGINT COMMENT '开始时间',
--     `total_date` BIGINT COMMENT '开始时间',
--     `smooth_coefficient` MAP < BIGINT,DOUBLE > COMMENT '平滑系数（所有数据）',
--     `exposure_uv_coefficient` DOUBLE comment '平滑系数（当前阶段需要的系数）',
--     `target_exposure_uv_value` DOUBLE comment '目标值（实验组的兜底流量）',
--     `target_exposure_uv_diff_value` DOUBLE comment '差值（实验组单尾号需要大于对照组的值）',
--     `is_black` INT comment '0：不拉黑；其他：拉黑',
--     `is_addition` INT comment '0：不加流；其他：加流'
-- ) comment '人工流量统计结果表'
-- PARTITIONED BY (partition_field STRING COMMENT '分区字段（格式：yyyyMMddHHmmss）') 
-- STORED AS ORC LOCATION 'obs://yishou-bigdata/yishou_daily.db/ads_artificial_flow_control_result_partition_table'
-- ;


with

date_interval_table as (
    -- 从配置表中获取需要统计数据的最大值和最小值（因为是专场日，所以对最大值和最小值 扩充1天）
    select
        min(stage_smooth_start_time) as min_time
        , from_unixtime(min(stage_smooth_start_time - 24 * 3600), 'yyyyMMdd') as min_special_dt
        , max(stage_smooth_end_time) as max_time
        , from_unixtime(max(stage_smooth_end_time + 24 * 3600), 'yyyyMMdd') as max_special_dt
    from yishou_daily.ads_artificial_flow_control_config
)

, exposure_original_table as (
    -- 根据最小专场日和最大专场日，获取对应的曝光数据
    select
        user_id
        , goods_no
        , receive_time
        , dt
    from yishou_data.dwd_goods_exposure_view
    where
        dt between
            (select min_special_dt from date_interval_table)
            and
            (select max_special_dt from date_interval_table)
)

, exposure_table as (
    -- 统计这个商品，在对应的时间中（根据配置表中配置的这个阶段的开始时间和结束时间）和对应的用户组中（配置有实验层和对应尾号）的数据
    select
        main_table.goods_no
        , config_table.strategy_id
        , count(
            distinct
            if(
                receive_time between config_table.stage_smooth_start_time and config_table.stage_smooth_end_time
                    and array_contains(split(config_table.strategy_user_tails, ','), cast(user_tail_table.user_md5_tail as string))
                , main_table.user_id
                , null
            ),
            if(
                receive_time between config_table.stage_smooth_start_time and config_table.stage_smooth_end_time
                    and array_contains(split(config_table.strategy_user_tails, ','), cast(user_tail_table.user_md5_tail as string))
                , main_table.dt
                , null
            )
        ) as experiment_exposure_uv
        , count(
            distinct
            if(
                receive_time between config_table.stage_smooth_start_time and config_table.stage_smooth_end_time
                    and not array_contains(split(config_table.strategy_user_tails, ','), cast(user_tail_table.user_md5_tail as string))
                , main_table.user_id
                , null
            ),
            if(
                receive_time between config_table.stage_smooth_start_time and config_table.stage_smooth_end_time
                    and not array_contains(split(config_table.strategy_user_tails, ','), cast(user_tail_table.user_md5_tail as string))
                , main_table.dt
                , null
            )
        ) as contrast_exposure_uv
    from exposure_original_table as main_table

    inner join yishou_daily.ads_artificial_flow_control_config as config_table
    on main_table.goods_no = config_table.goods_no

    inner join (
        select 
            user_id
            , head_number
            , user_md5_tail
        from yishou_data.dim_user_md5_tail_info
    ) as user_tail_table
    on
        main_table.user_id = user_tail_table.user_id
        and config_table.strategy_layer_id = user_tail_table.head_number

    group by
        main_table.goods_no
        , config_table.strategy_id
)

, click_original_table as (
    -- 根据最小专场日和最大专场日，获取对应的点击数据
    select
        user_id
        , goods_no
        , receive_time
        , dt
    from yishou_data.dwd_goods_click_view
    where
        dt between
            (select min_special_dt from date_interval_table)
            and
            (select max_special_dt from date_interval_table)
)

, click_table as (
    -- 统计这个商品，在对应的时间中（根据配置表中配置的这个阶段的开始时间和结束时间）和对应的用户组中（配置有实验层和对应尾号）的数据
    select
        main_table.goods_no
        , config_table.strategy_id
        , count(
            distinct
            if(
                receive_time between config_table.stage_smooth_start_time and config_table.stage_smooth_end_time
                    and array_contains(split(config_table.strategy_user_tails, ','), cast(user_tail_table.user_md5_tail as string))
                , main_table.user_id
                , null
            ),
            if(
                receive_time between config_table.stage_smooth_start_time and config_table.stage_smooth_end_time
                    and array_contains(split(config_table.strategy_user_tails, ','), cast(user_tail_table.user_md5_tail as string))
                , main_table.dt
                , null
            )
        ) as experiment_click_uv
        , count(
            distinct
            if(
                receive_time between config_table.stage_smooth_start_time and config_table.stage_smooth_end_time
                    and not array_contains(split(config_table.strategy_user_tails, ','), cast(user_tail_table.user_md5_tail as string))
                , main_table.user_id
                , null
            ),
            if(
                receive_time between config_table.stage_smooth_start_time and config_table.stage_smooth_end_time
                    and not array_contains(split(config_table.strategy_user_tails, ','), cast(user_tail_table.user_md5_tail as string))
                , main_table.dt
                , null
            )
        ) as contrast_click_uv
    from click_original_table as main_table

    inner join yishou_daily.ads_artificial_flow_control_config as config_table
    on main_table.goods_no = config_table.goods_no

    inner join (
        select 
            user_id
            , head_number
            , user_md5_tail
        from yishou_data.dim_user_md5_tail_info
    ) as user_tail_table
    on
        main_table.user_id = user_tail_table.user_id
        and config_table.strategy_layer_id = user_tail_table.head_number

    group by
        main_table.goods_no
        , config_table.strategy_id
)

, add_cart_original_table as (
    -- 根据最小专场日和最大专场日，获取对应的加购数据
    select
        main_table.user_id
        , goods_table.goods_no
        , main_table.receive_time
        , main_table.goods_number as add_cart_num
        , main_table.dt
    from yishou_data.dwd_app_goods_add_cart_view as main_table

    left join yishou_data.ods_fmys_goods_view as goods_table
    on main_table.goods_id = goods_table.goods_id

    where
        main_table.dt between
            (select min_special_dt from date_interval_table)
            and
            (select max_special_dt from date_interval_table)
)

, add_cart_table as (
    -- 统计这个商品，在对应的时间中（根据配置表中配置的这个阶段的开始时间和结束时间）和对应的用户组中（配置有实验层和对应尾号）的数据
    select
        main_table.goods_no
        , config_table.strategy_id
        , count(
            distinct
            if(
                receive_time between config_table.stage_smooth_start_time and config_table.stage_smooth_end_time
                    and array_contains(split(config_table.strategy_user_tails, ','), cast(user_tail_table.user_md5_tail as string))
                , main_table.user_id
                , null
            ),
            if(
                receive_time between config_table.stage_smooth_start_time and config_table.stage_smooth_end_time
                    and array_contains(split(config_table.strategy_user_tails, ','), cast(user_tail_table.user_md5_tail as string))
                , main_table.dt
                , null
            )
        ) as experiment_add_cart_uv
        , count(
            distinct
            if(
                receive_time between config_table.stage_smooth_start_time and config_table.stage_smooth_end_time
                    and not array_contains(split(config_table.strategy_user_tails, ','), cast(user_tail_table.user_md5_tail as string))
                , main_table.user_id
                , null
            ),
            if(
                receive_time between config_table.stage_smooth_start_time and config_table.stage_smooth_end_time
                    and not array_contains(split(config_table.strategy_user_tails, ','), cast(user_tail_table.user_md5_tail as string))
                , main_table.dt
                , null
            )
        ) as contrast_add_cart_uv
        , sum(
            if(
                receive_time between config_table.stage_smooth_start_time and config_table.stage_smooth_end_time
                    and array_contains(split(config_table.strategy_user_tails, ','), cast(user_tail_table.user_md5_tail as string))
                , main_table.add_cart_num
                , 0
            )
        ) as experiment_add_cart_num
        , sum(
            if(
                receive_time between config_table.stage_smooth_start_time and config_table.stage_smooth_end_time
                    and not array_contains(split(config_table.strategy_user_tails, ','), cast(user_tail_table.user_md5_tail as string))
                , main_table.add_cart_num
                , 0
            )
        ) as contrast_add_cart_num
    from add_cart_original_table as main_table

    inner join yishou_daily.ads_artificial_flow_control_config as config_table
    on main_table.goods_no = config_table.goods_no

    inner join (
        select 
            user_id
            , head_number
            , user_md5_tail
        from yishou_data.dim_user_md5_tail_info
    ) as user_tail_table
    on
        main_table.user_id = user_tail_table.user_id
        and config_table.strategy_layer_id = user_tail_table.head_number

    group by
        main_table.goods_no
        , config_table.strategy_id
)

, order_original_table as (
    -- 根据最小专场日和最大专场日，获取对应的订单数据
    select
        user_id
        , goods_no
        , create_time as receive_time
        , buy_num * shop_price as gmv
        , dt
    from yishou_data.dwd_order_infos_view
    where
        dt between
            (select min_special_dt from date_interval_table)
            and
            (select max_special_dt from date_interval_table)
        and is_real_pay = 1
)

, order_table as (
    -- 统计这个商品，在对应的时间中（根据配置表中配置的这个阶段的开始时间和结束时间）和对应的用户组中（配置有实验层和对应尾号）的数据
    select
        main_table.goods_no
        , config_table.strategy_id
        , sum(
            if(
                receive_time between config_table.stage_smooth_start_time and config_table.stage_smooth_end_time
                    and array_contains(split(config_table.strategy_user_tails, ','), cast(user_tail_table.user_md5_tail as string))
                , main_table.gmv
                , 0
            )
        ) as experiment_gmv
        , sum(
            if(
                receive_time between config_table.stage_smooth_start_time and config_table.stage_smooth_end_time
                    and not array_contains(split(config_table.strategy_user_tails, ','), cast(user_tail_table.user_md5_tail as string))
                , main_table.gmv
                , 0
            )
        ) as contrast_gmv
    from order_original_table as main_table

    inner join yishou_daily.ads_artificial_flow_control_config as config_table
    on main_table.goods_no = config_table.goods_no

    inner join (
        select 
            user_id
            , head_number
            , user_md5_tail
        from yishou_data.dim_user_md5_tail_info
    ) as user_tail_table
    on
        main_table.user_id = user_tail_table.user_id
        and config_table.strategy_layer_id = user_tail_table.head_number

    group by
        main_table.goods_no
        , config_table.strategy_id
)

, flow_table as (
    -- 对曝光、加购和下单数据进行合并
    select
        coalesce(exposure_table.goods_no, add_cart_table.goods_no, order_table.goods_no, click_table.goods_no) as goods_no
        , coalesce(exposure_table.strategy_id, add_cart_table.strategy_id, order_table.strategy_id, click_table.strategy_id) as strategy_id
        , coalesce(exposure_table.experiment_exposure_uv, 0) as experiment_exposure_uv
        , coalesce(exposure_table.contrast_exposure_uv, 0) as contrast_exposure_uv
        , coalesce(add_cart_table.experiment_add_cart_uv, 0) as experiment_add_cart_uv
        , coalesce(add_cart_table.contrast_add_cart_uv, 0) as contrast_add_cart_uv
        , coalesce(add_cart_table.experiment_add_cart_num, 0) as experiment_add_cart_num
        , coalesce(add_cart_table.contrast_add_cart_num, 0) as contrast_add_cart_num
        , coalesce(order_table.experiment_gmv, 0) as experiment_gmv
        , coalesce(order_table.contrast_gmv, 0) as contrast_gmv
        , coalesce(click_table.experiment_click_uv, 0) as experiment_click_uv
        , coalesce(click_table.contrast_click_uv, 0) as contrast_click_uv
    from exposure_table

    full join add_cart_table
    on 
        exposure_table.goods_no = add_cart_table.goods_no
        and exposure_table.strategy_id = add_cart_table.strategy_id
    
    full join order_table
    on 
        coalesce(exposure_table.goods_no, add_cart_table.goods_no) = order_table.goods_no
        and coalesce(exposure_table.strategy_id, add_cart_table.strategy_id) = order_table.strategy_id
    
    full join click_table
    on 
        coalesce(exposure_table.goods_no, add_cart_table.goods_no, order_table.goods_no) = click_table.goods_no
        and coalesce(exposure_table.strategy_id, add_cart_table.strategy_id, order_table.strategy_id) = click_table.strategy_id
)

insert overwrite table yishou_daily.ads_artificial_flow_control_result_partition_table partition(partition_field)
select
    flow_table.goods_no
    , flow_table.strategy_id
    
    -- 流量数据
    , flow_table.experiment_exposure_uv
    , flow_table.contrast_exposure_uv
    , flow_table.experiment_add_cart_uv
    , flow_table.contrast_add_cart_uv
    , flow_table.experiment_add_cart_num
    , flow_table.contrast_add_cart_num
    , flow_table.experiment_gmv
    , flow_table.contrast_gmv
    , flow_table.experiment_click_uv
    , flow_table.contrast_click_uv

    -- 配置数据
    , config_table.strategy_name
    , config_table.strategy_start_date
    , config_table.strategy_end_date
    , config_table.strategy_priority
    , config_table.strategy_layer_id
    , config_table.strategy_user_tails
    , config_table.strategy_addflow_type
    , config_table.strategy_addflow_time_type
    , config_table.stage_number
    , config_table.stage_smooth_start_date
    , config_table.stage_smooth_start_time
    , config_table.stage_smooth_end_date
    , config_table.stage_smooth_end_time
    , config_table.stage_target_exposure_uv
    , config_table.stage_target_exposure_uv_diff
    , config_table.stage_next_stage_exposure_uv
    , config_table.stage_next_stage_uv_value
    , config_table.stage_next_stage_gmv
    , config_table.stage_next_stage_add_cart_count
    , config_table.stage_next_stage_add_cart_rate
    , config_table.stage_next_stage_exposure_rate
    , config_table.stage_terminate_uv_value
    , config_table.stage_start_hour
    , config_table.stage_total_hour
    , config_table.stage_current_hour
    , config_table.current_date

    -- 根据该阶段的开始时间和统计的总小时数计算出的平滑系数
    , smooth_coefficient_table.start_date
    , smooth_coefficient_table.total_date
    , smooth_coefficient_table.smooth_coefficient

    -- 根据平滑系数和配置，算出曝光uv的临界值
    , smooth_coefficient_table.smooth_coefficient[config_table.stage_current_hour] as exposure_uv_coefficient
    , if(config_table.strategy_addflow_type = 1, config_table.stage_target_exposure_uv * smooth_coefficient_table.smooth_coefficient[config_table.stage_current_hour], 0) as target_exposure_uv_value
    , if(config_table.strategy_addflow_type = 2, config_table.stage_target_exposure_uv_diff * smooth_coefficient_table.smooth_coefficient[config_table.stage_current_hour], 0) as target_exposure_uv_diff_value

    -- 对该商品的该策略id进行拉黑
    --      1. 不管什么时候统计，如果该商品在这个阶段的uv价值小于配置的终止加流的uv价值，就拉黑
    --      2. 在这个阶段的最后一次统计中，只要有一个没有满足进入下一个阶段的条件（5个条件，其中曝光UV是配置的百分之80，其他都需要满足），就拉黑
    , case
        when flow_table.experiment_gmv / flow_table.experiment_exposure_uv < config_table.stage_terminate_uv_value then 1
        when
            config_table.stage_current_hour + 1 = config_table.stage_total_hour
            and substr(config_table.current_date, 11, 2) = '55'
            and (
                -- stage_next_stage_exposure_uv 字段在最开始的配置中就使用了 target_exposure_uv 或者 target_exposure_uv_diff来替换
                -- 所以，如果strategy_addflow_type等于1，就直接判断实验组的曝光uv的百分之80就行，如果不等于1，需要实验组和对照组的单尾号来判断
                if(
                    config_table.strategy_addflow_type = 1,
                    flow_table.experiment_exposure_uv,
                    (
                        flow_table.experiment_exposure_uv / size(split(config_table.strategy_user_tails, ',')) 
                            - flow_table.contrast_exposure_uv / (10 - size(split(config_table.strategy_user_tails, ',')))
                    )
                ) < config_table.stage_next_stage_exposure_uv * 0
                or flow_table.experiment_gmv / flow_table.experiment_exposure_uv < config_table.stage_next_stage_uv_value
                or flow_table.experiment_gmv < config_table.stage_next_stage_gmv
                or flow_table.experiment_add_cart_num < config_table.stage_next_stage_add_cart_count
                or flow_table.experiment_add_cart_uv / flow_table.experiment_click_uv < config_table.stage_next_stage_add_cart_rate
                or flow_table.experiment_click_uv / flow_table.experiment_exposure_uv < config_table.stage_next_stage_exposure_rate
            )
        then 2
        else 0
    end as is_black

    -- 对该商品是否进行加流
    --      1. 如果使用类型一（保证实验组的商品曝光UV），需要兜底流量（即根据平滑系数算出来的曝光uv的临界值）要大于该阶段这个商品的曝光UV值，就加流
    --      2. 如果使用类型二（实验组商品曝光UV高于对照组），需要兜底流量（即根据平滑系数算出来的diff曝光uv的临界值）要大于 （实验组单尾号的曝光UV值 - 对照组单尾号的曝光UV值），就加流
    , case
        when 
            config_table.strategy_addflow_type = 1
            and config_table.stage_target_exposure_uv * smooth_coefficient_table.smooth_coefficient[config_table.stage_current_hour] > flow_table.experiment_exposure_uv
        then 1
        when 
            strategy_addflow_type = 2 
            and
                config_table.stage_target_exposure_uv_diff * smooth_coefficient_table.smooth_coefficient[config_table.stage_current_hour] > (
                    flow_table.experiment_exposure_uv / size(split(config_table.strategy_user_tails, ',')) 
                        - flow_table.contrast_exposure_uv / (10 - size(split(config_table.strategy_user_tails, ',')))
                )
        then 2
        else 0
    end as is_addition

    , current_date as partition_field
from flow_table

inner join yishou_daily.ads_artificial_flow_control_config as config_table
on 
    flow_table.goods_no = config_table.goods_no
    and flow_table.strategy_id = config_table.strategy_id

inner join yishou_daily.ads_artificial_flow_control_smooth_coefficient as smooth_coefficient_table
on
    config_table.stage_start_hour = smooth_coefficient_table.start_date
    and config_table.stage_total_hour = smooth_coefficient_table.total_date

distribute by partition_field
;

-- ====================================== 统计表结束 ======================================
-- =======================================================================================
-- ====================================== 结果输出开始 ======================================

-- 需要加流的商品（给后端和算法，这个商品统计出来是需要加流的，并且不在黑名单中的）
-- ads_artificial_flow_control_goods_partition_table
-- DROP TABLE if exists yishou_daily.ads_artificial_flow_control_goods_partition_table;
-- create external table if not exists yishou_daily.ads_artificial_flow_control_goods_partition_table (
--     `goods_no` bigint comment '商品货号【123412】',
--     `is_black` INT comment '0：不拉黑；其他：拉黑',
--     `is_addition` INT comment '0：不加流；其他：加流',
--     `strategy_id` bigint comment '策略id'
-- ) comment '人工流量调控商品表(分区表)'
-- PARTITIONED BY (partition_field STRING COMMENT '分区字段（格式：yyyyMMddHHmmss）') 
-- STORED AS ORC LOCATION 'obs://yishou-bigdata/yishou_daily.db/ads_artificial_flow_control_goods_partition_table'
-- ;

insert overwrite table yishou_daily.ads_artificial_flow_control_goods_partition_table partition(partition_field)
select
    goods_no
    , is_black
    , is_addition
    , strategy_id
    , partition_field
from yishou_daily.ads_artificial_flow_control_result_partition_table
where 
    partition_field = '${bdp.system.cyctime}'
    and is_black = 0
    and is_addition != 0
distribute by partition_field
;



-- 黑名单商品（每个分区都要有，然后以策略id和货号为主键，这个分区保留了历史截止到现在为止的所有黑名单商品）
-- ads_artificial_flow_control_black_list_partition_table
-- DROP TABLE if exists yishou_daily.ads_artificial_flow_control_black_list_partition_table;
-- create external table if not exists yishou_daily.ads_artificial_flow_control_black_list_partition_table (
--     `goods_no` bigint comment '商品货号【123412】',
--     `strategy_id` bigint comment '策略id【1】',
--     `is_black` INT comment '0：不拉黑；其他：拉黑',
--     `to_black_date` string comment '被拉黑的时间'
-- ) comment '人工流量调控黑名单表(分区表)'
-- PARTITIONED BY (partition_field STRING COMMENT '分区字段（格式：yyyyMMddHHmmss）') 
-- STORED AS ORC LOCATION 'obs://yishou-bigdata/yishou_daily.db/ads_artificial_flow_control_black_list_partition_table'
-- ;


insert overwrite table yishou_daily.ads_artificial_flow_control_black_list_partition_table partition(partition_field)

-- 黑名单表中最后那次的数据（查近3天）
select
    goods_no
    , strategy_id
    , is_black
    , to_black_date
    , '${bdp.system.cyctime}' as partition_field
from yishou_daily.ads_artificial_flow_control_black_list_partition_table
where partition_field = (
    select
        max(partition_field)
    from yishou_daily.ads_artificial_flow_control_black_list_partition_table
    where
        partition_field between
            concat(TO_CHAR(DATEADD(to_date1(${bdp.system.cyctime},'yyyymmddhhmiss'), -2, "dd"),"yyyymmdd"), '000000')
            and
            concat(TO_CHAR(DATEADD(to_date1(${bdp.system.cyctime},'yyyymmddhhmiss'), 1, "dd"),"yyyymmdd"), '000000')
)

-- 当前运行阶段，被拉黑名单的数据
union all
select
    goods_no
    , strategy_id
    , is_black
    , partition_field as to_black_date
    , '${bdp.system.cyctime}' as partition_field
from yishou_daily.ads_artificial_flow_control_result_partition_table
where 
    partition_field = '${bdp.system.cyctime}'
    and is_black != 0

distribute by partition_field

;


-- ====================================== 结果输出结束 ======================================
