with 
date_data AS(
SELECT 
 date_str
,date_time
,date_54_week
,case when '${table_name}' = 'lunar_calendar' THEN lunar_date_1_y_ago 
when '${table_name}' = 'gregorian_calendar' and  ${year_gap} = 1 THEN date_str_1_y_ago 
WHEN '${table_name}' = 'gregorian_calendar' and  ${year_gap} = 2 THEN date_str_2_y_ago
WHEN '${table_name}' = 'gregorian_calendar' and  ${year_gap} = 3 THEN date_str_3_y_ago END date_str_ago
FROM finebi.lunar_date_wxz WHERE date_time  between date '${bgn_time}' and date '${end_time}'
GROUP BY 1,2,3,4
)
,all_date AS (
SELECT date_str date_s FROM date_data 
UNION ALL 
SELECT date_str_ago date_s  FROM date_data 
)
-- 上月商家分层
,supply_level_t as (
SELECT 
supply_id
,实际销售额
,ranks
,(case 
    when ranks between 1 and 100 then '1_[1-100]'
    when ranks between 101 and 300 then '2_[101-300]'
    when ranks between 301 and 500 then '3_[301-500]'
    when ranks between 501 and 1000 then '4_[501-1000]'
    when ranks > 1000 then '5_[1000+]'
    end) as 上月商家实际销售额分层
FROM (
SELECT 
supply_id
,ifnull(sum(real_buy_amount),0) 实际销售额
,ROW_NUMBER() over(ORDER BY ifnull(sum(real_buy_amount),0) desc) ranks
FROM finebi.finebi_dwm_goods_sp_up_info_dt
where substring(special_date,1,7) = substring(now() - interval '1 month',1,7)
GROUP BY 1))
,final_data AS(
select t1.date_str::date
        ,case when '发货地' in ('${履约_维度}') then t2.origin_name  end 发货地
        ,case when '大市场' in ('${履约_维度}') then t2.big_market  end 大市场
        ,case when '供应商ID' in ('${履约_维度}') then t2.supply_id  end 供应商ID
        ,case when '供应商名称' in ('${履约_维度}') then si.supply_name  end 供应商名称
        ,CASE WHEN '上月商家实际销售额分层' IN ('${履约_维度}') then t3.上月商家实际销售额分层 end 上月商家实际销售额分层
        -- ,case when '供应商上月销售层级' in ('${履约_维度}') then coalesce(t1.销售分层,'10w以下')  end 供应商上月销售层级
        -- ,case when '供应商当月销售层级' in ('${履约_维度}') then coalesce(t4.销售分层,'10w以下')  end 供应商当月销售层级
        ,case when '是否平台化' in ('${履约_维度}') then if(si.is_platform=1,'是','否')  end 是否平台化
        ,case when '三级品类' in ('${履约_维度}') then t2.third_cat_name  end 三级品类
        -- ,case when '是否异常剔除款' in ('${履约_维度}') then 
        -- (case when jk.goods_no is not null then '是' 
        -- when sh.goods_no is not null and substr(special_date,1,10) between '2024-02-04' and '2024-02-16' then '是' 
        -- else '否' end) end 是否异常剔除款
        ,case when '部门' in ('${履约_维度}') then si.department end 部门
        ,case when '子部门' in ('${履约_维度}') then si.sub_department end 子部门   
        ,cast(round(SUM(if('${指标展示}'='件数',create_num,create_amount)),0) as bigint) as 应开单
        ,cast(round(SUM(if('${指标展示}'='件数',allot_num_24h,allot_amount_24h)),0) as bigint) as 配货24h
        ,cast(round(SUM(if('${指标展示}'='件数',allot_num_48h,allot_amount_48h)),0) as bigint) as 配货48h
        ,cast(round(SUM(if('${指标展示}'='件数',allot_num_72h,allot_amount_72h)),0) as bigint) as 配货72h
        ,cast(round(SUM(if('${指标展示}'='件数',allot_num_120h,allot_amount_120h)),0) as bigint) as 配货120h
        ,cast(round(SUM(if('${指标展示}'='件数',allot_num_240h,allot_amount_240h)),0) as bigint) as 配货240h
        ,cast(round(SUM(if('${指标展示}'='件数',allot_num_168h,allot_amount_168h)),0) as bigint) as 配货168h
        ,cast(round(SUM(if('${指标展示}'='件数',allot_num_192h,allot_amount_192h)),0) as bigint) as 配货192h 
        ,0 应开单_同期
        ,0 配货24h_同期
        ,0 配货48h_同期
        ,0 配货72h_同期
        ,0 配货120h_同期
        ,0 配货240h_同期
        ,0 配货168h_同期
        ,0 配货192h_同期
FROM date_data t1
LEFT JOIN finebi.finebi_supply_chain_sp_purchase_allot_dt t2 on t1.date_str = t2.special_date -- 今年的值
LEFT JOIN supply_level_t t3 on t2.supply_id = t3.supply_id
left join finebi.dim_supply_info si on t2.supply_id = si.supply_id
GROUP BY 1,2,3,4,5,6,7,8,9,10

UNION ALL
select t1.date_str::date
        ,case when '发货地' in ('${履约_维度}') then t2.origin_name  end 发货地
        ,case when '大市场' in ('${履约_维度}') then t2.big_market  end 大市场
        ,case when '供应商ID' in ('${履约_维度}') then t2.supply_id  end 供应商ID
        ,case when '供应商名称' in ('${履约_维度}') then si.supply_name  end 供应商名称
        ,CASE WHEN '上月商家实际销售额分层' IN ('${履约_维度}') then t3.上月商家实际销售额分层 end 上月商家实际销售额分层
        -- ,case when '供应商上月销售层级' in ('${履约_维度}') then coalesce(t1.销售分层,'10w以下')  end 供应商上月销售层级
        -- ,case when '供应商当月销售层级' in ('${履约_维度}') then coalesce(t4.销售分层,'10w以下')  end 供应商当月销售层级
        ,case when '是否平台化' in ('${履约_维度}') then if(si.is_platform=1,'是','否')  end 是否平台化
        ,case when '三级品类' in ('${履约_维度}') then t2.third_cat_name  end 三级品类
        -- ,case when '是否异常剔除款' in ('${履约_维度}') then 
        -- (case when jk.goods_no is not null then '是' 
        -- when sh.goods_no is not null and substr(special_date,1,10) between '2024-02-04' and '2024-02-16' then '是' 
        -- else '否' end) end 是否异常剔除款
        ,case when '部门' in ('${履约_维度}') then si.department end 部门  
        ,case when '子部门' in ('${履约_维度}') then si.sub_department end 子部门  
        ,0 应开单
        ,0 配货24h
        ,0 配货48h
        ,0 配货72h
        ,0 配货120h
        ,0 配货240h
        ,0 配货168h
        ,0 配货192h
        ,cast(round(SUM(if('${指标展示}'='件数',create_num,create_amount)),0) as bigint) as 应开单_同期
        ,cast(round(SUM(if('${指标展示}'='件数',allot_num_24h,allot_amount_24h)),0) as bigint) as 配货24h_同期
        ,cast(round(SUM(if('${指标展示}'='件数',allot_num_48h,allot_amount_48h)),0) as bigint) as 配货48h_同期
        ,cast(round(SUM(if('${指标展示}'='件数',allot_num_72h,allot_amount_72h)),0) as bigint) as 配货72h_同期
        ,cast(round(SUM(if('${指标展示}'='件数',allot_num_120h,allot_amount_120h)),0) as bigint) as 配货120h_同期
        ,cast(round(SUM(if('${指标展示}'='件数',allot_num_240h,allot_amount_240h)),0) as bigint) as 配货240h_同期
        ,cast(round(SUM(if('${指标展示}'='件数',allot_num_168h,allot_amount_168h)),0) as bigint) as 配货168h_同期
        ,cast(round(SUM(if('${指标展示}'='件数',allot_num_192h,allot_amount_192h)),0) as bigint) as 配货192h_同期
FROM date_data t1
LEFT JOIN finebi.finebi_supply_chain_sp_purchase_allot_dt t2 on t1.date_str_ago::date = t2.special_date 
LEFT JOIN supply_level_t t3 on t2.supply_id = t3.supply_id
left join finebi.dim_supply_info si on t2.supply_id = si.supply_id
GROUP BY 1,2,3,4,5,6,7,8,9,10
)


select *
from final_data a

