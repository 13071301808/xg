-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/09/06 11:20:07 GMT+08:00
-- ******************************************************************** --
create or replace view yishou_data.dwd_storage_pur_plan_info_view 
as
select 
    fwp.planid
    , fwp.supply_id
    , fwp.pgid
    , fwp.is_share
    , fwp.status as plan_status
    , fwp.ptype as p_type
    , fwp.remarks
    , from_unixtime(fwp.check_time) as check_time
    , fwp.check_user_id
    , fwp.check_remarks
    , fwp.plan_type
    , fwp.create_admin
    , from_unixtime(fwp.expect_time) as expect_time
    , from_unixtime(fwp.done_time) as done_time
    , fwp.special_id
    , from_unixtime(fwp.create_time) as plan_create_time
    , fwpi.planinfoid
    , fwpi.goods_no 
    , fwpi.plan_price
    , fwpi.shop_price
    , fwpi.plan_num
    , fwpi.cg_num
    , fwpi.cancel_num
    , fwpi.si_id
    , fwpi.si_val
    , fwpi.co_id
    , fwpi.co_val
    , fwpi.status as plan_info_status
    , from_unixtime(fwpi.create_time) as plan_info_create_time
    , case when fwp.status = 0 and fwpi.status in (1, 2, 3) then 1 else 0 end as is_valid
from yishou_data.all_fmys_wcg_plan_h fwp 
join yishou_data.all_fmys_wcg_plan_info_h fwpi 
on fwp.planid = fwpi.planid;
select * from yishou_data.dwd_storage_pur_plan_info_view limit 10;