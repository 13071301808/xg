-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/04/08 11:14:35 GMT+08:00
-- ******************************************************************** --
drop table if exists yishou_daily.temp_realtime_order_allot_24h_department;
create table yishou_daily.temp_realtime_order_allot_24h_department
as 
with last_log as (
    select 
        供应商id_合并
        ,需求日期
        ,sum(开单h24小时履约金额) 前一日开单h24小时履约金额
      	,sum(前置仓发货) 前一日前置仓发货
      	,sum(订货单计划代采-前置仓入库取消断货h360小时以外履约金额) 前一日订货单计划代采
    from (
        SELECT
            a.supply_id,
            a.supply_id as 供应商id_合并,
            to_char(datetrunc(from_unixtime(demanded_date),'dd'),'yyyy-mm') as 需求月份,
            to_char(datetrunc(from_unixtime(demanded_date),'dd'),'yyyy-mm-dd') as 需求日期,
            a.goods_no 货号,
            b.goods_kh 款号,
            sum(
                if(
                    data_type = 1 and datediff1(from_unixtime(performance_time),datetrunc(from_unixtime(demanded_date),'dd'),'mi') <= 1440 and performance_type <> 0,
                    if(data_type=1,plan_price,price),
                    0
                )
            ) 开单h24小时履约金额,
            sum(if(data_type=2,price,0)) 前置仓发货,
            sum(if(data_type=1,plan_price,0)) 订货单计划代采,
            sum(if( cancel_type = 9  and performance_type = 0,if(data_type=1,plan_price,price),0)) as 前置仓入库取消断货h360小时以外履约金额
        from yishou_data.ods_fmys_supply_performance_statement_detail_view a 
        left join yishou_data.dim_goods_no_info_full_h b on a.goods_no = b.goods_no
        where 1=1
        and to_char(datetrunc(from_unixtime(a.demanded_date),'dd'),'yyyy-mm-dd') = dateadd(current_date(),-1,'dd')
        and a.is_delete=2
        and a.is_count = 1
        group by 1,2,3,4,5,6  
    )
    group by 1,2
)
, if_open_order as (
    select  
        supply_id 供应商id
      	,current_date() dt
        ,if(实数单件数>0,'已开单','未开单') 是否已开单
        ,实数单件数
    from (
        select 
            gn.supply_id
            ,sum(coalesce(num,0)) 实数单件数
        from yishou_data.ods_fmys_wcg_order_info_view a
        left join yishou_data.ods_fmys_wcg_order_view b on a.woid = b.woid 
        left join yishou_data.dim_goods_no_info_full_h gn on a.goods_no = gn.goods_no
        where to_date(dateadd(from_unixtime(a.create_time),-6,'hh')) = current_date()
        and b.cg_type = 0
        group by 1
    )
)
SELECT
    a.supply_id
    ,if(gn.origin = 1 ,'广州','杭州') as 发货地
    ,si.supply_name as 供应商名称_合并
    ,coalesce(t.是否已开单,'未开单') 是否已开单
    ,to_date(from_unixtime(a.demanded_date)) as 需求日期
	,si.pg_member_name as 采购员
    ,si.follower_name as 商运
    ,pii.pg_name as 买手组
    ,pii.pgm_name as 买手
    ,if(sp.is_platform = 1,"是","否") 是否平台化
    ,si.department 供给部门_合并
    ,mi.big_market 
    ,if(
        substr(c.需求日期,1,10)<'2024-11-15',
        case 
            when (mi.big_market = '平湖市场' and c.需求日期 BETWEEN '2024-10-01' and '2024-10-12') then '异地商家' 
            else if(sdd1.supply_type = 2,'异地商家','本地商家') 
        end,
        if(sdd.supply_type = 2,'异地商家','本地商家')
    ) as 是否异地商家
    ,前一日开单h24小时履约金额
    ,前一日前置仓发货
    ,前一日订货单计划代采
    ,sum(if(a.data_type=1,a.plan_price,0)) as 订货单计划代采
    ,sum(if(a.data_type=1 and a.performance_type=0 and a.status in (3,4),a.plan_price,0)) as 配送在途
    ,sum(if(a.data_type=2,a.price,0)) as 前置仓发货
    ,sum(
        if(
            a.data_type = 1 
            and (UNIX_TIMESTAMP(DATE_FORMAT(FROM_UNIXTIME(a.demanded_date),'yyyy-MM-dd 00:00:00')) - UNIX_TIMESTAMP(FROM_UNIXTIME(a.performance_time))) / 60 <= 1440 
            and a.performance_type <> 0,
            if(a.data_type=1,a.plan_price,a.price),
            0
        )
    ) as 开单h24小时履约金额
    ,sum(if(data_type=1,plan_price,price)) as 需求小计
from yishou_data.ods_fmys_supply_performance_statement_detail_view a 
left join last_log c on c.需求日期 = to_date(dateadd(from_unixtime(a.demanded_date),-1,'dd')) and c.供应商id_合并 = a.supply_id
left join yishou_data.dim_goods_no_info_full_h gn on a.goods_no = gn.goods_no
left join yishou_data.dim_supply_info si on a.supply_id = si.supply_id 
left join yishou_data.dim_market_info mi on si.market_id = mi.market_id
left join yishou_data.dim_cat_info ci on ci.cat_id = gn.cat_id
left join yishou_data.dim_picker_info pii on gn.pgm_code = pii.pgm_code 
left join yishou_data.ods_fmys_supply_ext_dt sdd on sdd.supply_id = if(gn.vendor_supply_id<>0,gn.vendor_supply_id,a.supply_id) and to_date(sdd.dt) = to_date(from_unixtime(a.demanded_date))
-- 15号以前取sdd1表，15号以后取sdd表
left join (
    SELECT supply_id,supply_type,is_offsite from yishou_data.ods_fmys_supply_ext_dt where dt = '20241115'
) sdd1 on sdd1.supply_id = if(gn.vendor_supply_id<>0,gn.vendor_supply_id,a.supply_id) 	
left join yishou_data.ods_fmys_supply_permissions_view sp on sp.supply_id = si.supply_id
left join if_open_order t on a.supply_id = t.供应商id
where 1=1 
and a.is_delete=2 
and a.is_count = 1
and to_date(from_unixtime(a.demanded_date)) = current_date()
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
;
