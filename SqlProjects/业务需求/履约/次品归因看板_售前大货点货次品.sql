select 
    case when  '${汇总周期}' = '按天' then substr(stock_in_date,1,10)
            when  '${汇总周期}' = '按周' then substr('${结束日期}'::date - (timestampdiff(week,stock_in_date::date,'${结束日期}'::date)*7+6||' day')::interval,1,10) || '-' || substr('${结束日期}'::date - (timestampdiff(week,stock_in_date::date,'${结束日期}'::date)*7||' day')::interval,1,10)
            when  '${汇总周期}' = '按月' then mon_date
            when '${汇总周期}' = '汇总' then '汇总'
        end  入库日期
    ,case when '入库仓库地' in ('${售前大货点货次品维度}') then origin_name end  入库仓库地
    ,case when '大市场' in ('${售前大货点货次品维度}') then big_market end 大市场
    ,case when '入库来源' in ('${售前大货点货次品维度}') then (case  stock_source when 1 then '采购欠货'
    when 2 then '退换货欠货'
    when 3 then '售后欠货'
    when 4 then '售后拆包'
    when 5 then '调拨'
    when 6 then '现货调拨'
    when 7 then '核销'
    when 8 then '其他入库'
    when 9 then '生产盘盈'
    when 10 then '售后盘盈'
    when 11 then '旧前置仓'
    when 12 then '售后拆包错发核销'
    when 13 then '退换货其它入库'
    when 14 then '新前置仓'
    when 15 then '达人售后'
    when 16 then '售后拆包(退供)'
    when 17 then '售后欠货(退供)'
    when 18 then '买版计划'
    when 19 then '前置仓异常入库'
    else to_char(stock_source) end ) end 入库来源
    ,case when '打标方式' in ('${售前大货点货次品维度}') then (case da_biao_method when 1 then '爆版打标' 
    when 2 then 'app点货打标' 
    when 3 then '无数据打标' 
    when 4 then 'pc点货打标' 
    when 5 then '前置质检打标'
    end)  end 打标方式
    ,case when '供应商' in ('${售前大货点货次品维度}') then supply_id end 供应商id
    ,case when '供应商' in ('${售前大货点货次品维度}') then supply_name end 供应商名字
    ,case when '采购组名称' in ('${售前大货点货次品维度}') then pg_name end 采购组名称
    ,case when '采购员名称' in ('${售前大货点货次品维度}') then pg_member_name end 采购员名称
    ,case when '次品员名称' in ('${售前大货点货次品维度}') then dgm_name end 次品员名称
    ,case when '是否质检' in ('${售前大货点货次品维度}') then (case when is_pc_check = 1 then '是' else '否' end) end 是否质检
    ,case when '是否改标商品' in ('${售前大货点货次品维度}') then (case when is_gb = 1 then '是' else '否' end) end 是否改标商品
    ,sum(rk_num) 入库件数
    ,sum(rk_blp_num) 入库不良品件数
    ,sum(rk_blp_cp_num) 入库质量次品件数
    ,sum(rk_blp_clf_num) 入库质量错漏发件数
    ,sum(rk_blp_clf_spj_num) 入库质量少配货件数
    ,sum(rk_blp_clf_ck_num) 入库质量错款件数
    ,sum(rk_blp_clf_cs_num) 入库质量错色件数
    ,sum(rk_blp_clf_cm_num) 入库质量错码件数
    ,sum(rk_blp_clf_sc_num) 入库质量色差件数
    ,sum(rk_blp_clf_tzbq_num) 入库质量套装不全件数
    ,sum(rk_blp_clf_qt_num) 入库质量其他件数
    ,sum(rk_thcpq_num) 入库退货区次品区件数
    ,case when sum(rk_num) > 0 then sum(rk_blp_num)/sum(rk_num) else 0 end 入库不良品率
    ,case when sum(rk_num) > 0 then sum(rk_blp_cp_num)/sum(rk_num) else 0 end 入库质量次品率
    ,case when sum(rk_num) > 0 then sum(rk_blp_clf_num)/sum(rk_num) else 0 end 入库质量错漏发率
from cross_origin.finebi_supply_chain_cp_reason_sqdhdhcp
where dt between  replace('${开始日期}','-','') and replace('${结束日期}','-','')
group by 1,2,3,4,5,6,7,8,9,10,11,12
