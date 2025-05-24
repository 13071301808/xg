-- 刷数
INSERT OVERWRITE TABLE yishou_data.dcl_event_big_goods_exposure_d PARTITION (dt)
select user_id, goods_id, is_rec, special_id, os, goods_no, pid, ptime, source, report_time, event_id, search_event_id, keyword, app_version, log_type, exposure_index, strategy_id, is_default, is_operat, recall_num, is_pro, goods_seat_id, tab_name, cat_id, cat_name, stall_id, special_index, pgm_code, activity_id, activity_name, search_score, secondsource, goods_similarity, dt
from yishou_data.dcl_event_big_goods_exposure_d_backup
where dt = '20250501' and pid != 32
union all
select user_id, goods_id, is_rec, special_id, os, goods_no, pid, ptime, source, report_time, event_id, search_event_id, keyword, app_version, log_type, exposure_index, strategy_id, is_default, is_operat, recall_num, is_pro, goods_seat_id, tab_name, cat_id, cat_name, stall_id, special_index, pgm_code, activity_id, activity_name, search_score, secondsource, goods_similarity, dt
from (
    select
        *,
        row_number() over(partition by user_id,goods_id order by event_id) nu
    from yishou_data.dcl_event_big_goods_exposure_d_backup
    where dt = '20250501' and pid = 32
    having nu = 1
)
where (MOD(ABS(HASH(CONCAT(user_id,goods_id))), 200000000) / 200000000.0) < 0.094
DISTRIBUTE BY floor(rand()*200)
;

-- 验数
select
    count(distinct case when pid = 32 then concat(user_id,goods_id) end) as new
from yishou_data.dcl_event_big_goods_exposure_d where dt = '20250501';

-- select count(distinct case when pid = 32  then concat(user_id,goods_id) end) from yishou_data.dcl_event_big_goods_exposure_d_backup where dt = '20241020';

-- -- 有问题就回滚
-- INSERT OVERWRITE TABLE yishou_data.dcl_event_big_goods_exposure_d PARTITION (dt)
-- select * from yishou_data.dcl_event_big_goods_exposure_d_backup where dt = '20241021'
-- DISTRIBUTE BY floor(rand()*200)
-- ;

-- select
--     dt,
--     count(case when pid <> 32  then concat(goods_id,user_id) end) 总曝光,
--     count(distinct case when pid = 32  then concat(user_id,goods_id) end) 曝光uv
-- from yishou_data.dcl_event_big_goods_exposure_d
-- where dt = '20241021'
-- group by 1







