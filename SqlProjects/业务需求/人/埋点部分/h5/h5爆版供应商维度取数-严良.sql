-- 爆版pc
select 
    supply_id
    ,count(supply_id) supply_pv
from yishou_data.dwd_baoban_log_h5_event_dt
-- where event in ('pcgoodsmanager','pcreleasegoods')
where event in ('pcgoodsmanager') 
and dt between '20240701' and '20240819'
group by supply_id
having count(supply_id) >= 10
;

-- 爆版app的
select 
    DISTINCT supply_id as `供应商id` 
from yishou_data.dwd_baoban_log_app_click_event_dt 
where event in ('goodsmanager','pcreleasegoods') and dt = '20240616'
;
