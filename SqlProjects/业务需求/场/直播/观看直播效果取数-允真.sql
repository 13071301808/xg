--show create table yishou_daily.fmdes_user_live_info_d;
select * from yishou_data.dim_live_room_info where room_id = '97888';

-- 当发现有异常时(例如他们要的直播间数据为空)，用这段sql
select 
    room_id
    ,user_id
    ,user_type
    ,total_time
from yishou_daily.fmdes_user_live_info_d_temp
where dt = '20240816' and room_id = '97888';

-- 日常使用
select
    room_id
    ,user_id
    ,user_type
    ,total_time
from yishou_daily.fmdes_user_live_info_d
where dt > 0 and room_id in ('98147')
order by dt desc
; 