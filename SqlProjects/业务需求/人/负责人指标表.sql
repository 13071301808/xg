-- DLI sql 
-- ******************************************************************** --
-- author: lixion
-- create time: 2024/04/19 16:04:58 GMT+08:00
-- ******************************************************************** --
-- CREATE EXTERNAL TABLE `yishou_daily`.`responsible_person_indicators` (
--     `部门` STRING,
--     `负责人` STRING,
--     `季度or月份` STRING,
--     `指标名称` STRING,
--     `S级目标值` STRING, 
--     `A级目标值` STRING,
--     `B级目标值` STRING,
--     `当前实际值` STRING, 
--     `指标达成等级` STRING,
--     `S级达成率` STRING, 
--     `A级达成率` STRING, 
--     `B级达成率` STRING, 
--     `指标定义` STRING,
--     `权重` FLOAT
-- )
-- comment '负责人KPI指标'
-- STORED AS ORC LOCATION 'obs://yishou-bigdata/yishou_daily.db/responsible_person_indicators'
-- ;

-- 清空表数据
TRUNCATE TABLE yishou_daily.responsible_person_indicators;

-- 超级买卖飞船	歌莉	现货总体实发GMV
with tg as (
    select 
       stat_month,
       max(if(target_type='现货总体GMV',level_b_taget,null)) spot_gmv_target_b,
       max(if(target_type='现货总体GMV',level_a_taget,null)) spot_gmv_target_a,
       max(if(target_type='现货总体GMV',level_s_taget,null)) spot_gmv_target_s 
    from yishou_daily.dim_spot_sale_2024_target_value
    group by stat_month
)
, t1 as (
    select 
        a.stat_quarter as 季度
        ,tg.spot_gmv_target_s as S级目标值
        ,tg.spot_gmv_target_a as A级目标值
        ,tg.spot_gmv_target_b as B级目标值
        ,round(sum(a.sale_amount),4) as `现货实发GMV`
    from yishou_daily.dws_super_airship_spot_sale_detail_full a
    left join tg on a.stat_quarter = tg.stat_month
    where a.stat_month >= concat(substr('${bizdate}',1,4),'01')
    group by 1,2,3,4
)
, t2 as (
    select 
        a.stat_month as 月份
        ,tg.spot_gmv_target_s as S级目标值
        ,tg.spot_gmv_target_a as A级目标值
        ,tg.spot_gmv_target_b as B级目标值
        ,round(sum(a.sale_amount),4) as `现货实发GMV`
    from yishou_daily.dws_super_airship_spot_sale_detail_full a
    left join tg on a.stat_month = tg.stat_month
    where a.stat_month >= concat(substr('${bizdate}',1,4),'01')
    group by 1,2,3,4
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
    '超级买卖飞船' as 部门
    ,'歌莉' as 负责人
    ,季度 as 季度or月份
    ,'现货总体实发GMV' as 指标名称
    ,S级目标值
    ,A级目标值
    ,B级目标值
    ,现货实发GMV as 当前实际值
    , CASE
        when 现货实发GMV >= S级目标值 then 'S'
        when 现货实发GMV >= A级目标值 then 'A'
        WHEN 现货实发GMV >= B级目标值 then 'B'
        ELSE 'C'
    end as 指标达成等级
    , concat(to_char(round((现货实发GMV / S级目标值) * 100, 2)), '%') as S级达成率
    , concat(to_char(round((现货实发GMV / A级目标值) * 100, 2)), '%') as A级达成率
    , concat(to_char(round((现货实发GMV / B级目标值) * 100, 2)), '%') as B级达成率
    ,'统计口径：专场结束后，前置仓+备货+未来确定性库存的总实发GMV+公司库存实发gmv' as 指标定义
    ,0.40 as 权重
from t1
union ALL 
SELECT 
    '超级买卖飞船' as 部门
    ,'歌莉' as 负责人
    ,月份 as 季度or月份
    ,'现货总体实发GMV' as 指标名称
    ,S级目标值
    ,A级目标值
    ,B级目标值
    ,现货实发GMV as 当前实际值
    , CASE
        when 现货实发GMV >= S级目标值 then 'S'
        when 现货实发GMV >= A级目标值 then 'A'
        WHEN 现货实发GMV >= B级目标值 then 'B'
        ELSE 'C'
    end as 指标达成等级
    , concat(to_char(round((现货实发GMV / S级目标值) * 100, 2)), '%') as S级达成率
    , concat(to_char(round((现货实发GMV / A级目标值) * 100, 2)), '%') as A级达成率
    , concat(to_char(round((现货实发GMV / B级目标值) * 100, 2)), '%') as B级达成率
    ,'统计口径：专场结束后，前置仓+备货+未来确定性库存的总实发GMV+公司库存实发gmv' as 指标定义
    ,0.40 as 权重
from t2
;

-- 超级买卖飞船	歌莉	前置仓周转率
with tg as(
    select
        stat_month,
        max(if(target_type='前置仓周转率',level_s_taget,null)) turnover_target_s,
        max(if(target_type='前置仓周转率',level_a_taget,null)) turnover_target_a,
        max(if(target_type='前置仓周转率',level_b_taget,null)) turnover_target_b
    from yishou_daily.dim_spot_sale_2024_target_value
    group by stat_month
)
, t1 as(
    -- 季度
    select
        concat(to_char(a.stat_date,'yyyy'),'Q',quarter(a.stat_date)) as 季度
        ,tg.turnover_target_s as S级目标值
        ,tg.turnover_target_a as A级目标值
        ,tg.turnover_target_b as B级目标值
        ,round(coalesce(sum(a.day30_outbound_up_stock_num),0),4) / round(coalesce(sum(a.in_stock_up_num_without_tg),0),4) as 周转比
    from yishou_daily.dws_super_airship_stock_inventory_and_turnover_detail_full a
    left join tg on concat(to_char(a.stat_date,'yyyy'),'Q',quarter(a.stat_date)) = tg.stat_month
    where to_char(stat_date,'yyyymm') between concat(substr('${bizdate}',1,4),'01') and substr('${bizdate}',1,6)
    group by 1,2,3,4
)
, t2 as (
    -- 月份
    select
        to_char(last_day(stat_date),'yyyymm') as 月份
        ,tg.turnover_target_s as S级目标值
        ,tg.turnover_target_a as A级目标值
        ,tg.turnover_target_b as B级目标值
        ,round(coalesce(sum(a.day30_outbound_up_stock_num),0),4) / round(coalesce(sum(a.in_stock_up_num_without_tg),0),4) as 周转比
    from yishou_daily.dws_super_airship_stock_inventory_and_turnover_detail_full a
    left join tg on to_char(last_day(stat_date),'yyyymm') = tg.stat_month
    where to_char(stat_date,'yyyymm') between concat(substr('${bizdate}',1,4),'01') and substr('${bizdate}',1,6)
    group by 1,2,3,4
)
INSERT INTO  yishou_daily.responsible_person_indicators
select  
    '超级买卖飞船' as 部门
    ,'歌莉' as 负责人
    ,季度 as 季度or月份
    ,'前置仓周转率' as  指标名称
    ,concat(S级目标值 * 100,'%') as S级目标值	
    ,concat(A级目标值 * 100,'%') as A级目标值
    ,concat(B级目标值 * 100,'%') as B级目标值
    ,concat(round(周转比 * 100,2),'%') as 当前实际值
    ,CASE 
        when 周转比 >= S级目标值 then 'S'
        when 周转比 >= A级目标值 then 'A'
        WHEN 周转比 >= B级目标值 then 'B'
        ELSE 'C'
    end as 指标达成等级
    ,concat(to_char(round((周转比/S级目标值) * 100,2)),'%') as S级达成率
    ,concat(to_char(round((周转比/A级目标值) * 100,2)),'%') as A级达成率
    ,concat(to_char(round((周转比/B级目标值) * 100,2)),'%') as B级达成率
    ,'统计口径：（一手推荐前置仓+一手推荐多开）的商品，商品入仓之日算起30天周转，入仓件数/出库件数' as  指标定义
    ,0.15 as 权重
from t1
UNION ALL 
SELECT 
    '超级买卖飞船' as 部门
    ,'歌莉' as 负责人
    ,月份 as 季度or月份
    ,'前置仓周转率' as 指标名称
    ,concat(S级目标值 * 100,'%') as S级目标值	
    ,concat(A级目标值 * 100,'%') as A级目标值
    ,concat(B级目标值 * 100,'%') as B级目标值
    ,concat(round(周转比 * 100,2),'%') as 当前实际值
    ,CASE 
        when 周转比 >= S级目标值 then 'S'
        when 周转比 >= A级目标值 then 'A'
        WHEN 周转比 >= B级目标值 then 'B'
        ELSE 'C'
    end as 指标达成等级
    ,concat(to_char(round((周转比/S级目标值) * 100,2)),'%') as S级达成率
    ,concat(to_char(round((周转比/A级目标值) * 100,2)),'%') as A级达成率
    ,concat(to_char(round((周转比/B级目标值) * 100,2)),'%') as B级达成率
    ,'统计口径：（一手推荐前置仓+一手推荐多开）的商品，商品入仓之日算起30天周转，入仓件数/出库件数' as  指标定义
    ,0.15 as 权重
from t2
;

-- 超级买卖飞船	歌莉 折扣率目标
with tg as(
    select
        stat_month,
        max(if(target_type='折扣率目标',level_s_taget,null)) discount_target_s,
        max(if(target_type='折扣率目标',level_a_taget,null)) discount_target_a,
        max(if(target_type='折扣率目标',level_b_taget,null)) discount_target_b
    from yishou_daily.dim_spot_sale_2024_target_value
    group by stat_month
)
, t1 as (
    select 
        coalesce(t1.月份,t2.月份) 月份
        ,coalesce(t1.季度,t2.季度) 季度
        ,coalesce(t1.GMV,0)+coalesce(t2.GMV,0) as GMV
        ,coalesce(t1.拿货成本,0)+coalesce(t2.拿货成本,0) as 拿货成本
        ,tg.discount_target_s as S级目标值	
        ,tg.discount_target_a as A级目标值
        ,tg.discount_target_b as B级目标值
    from (
        select
            to_char(stat_date,'yyyymm') as 月份,
            concat(to_char(stat_date,'yyyy'),'Q',quarter(stat_date)) as 季度,
            coalesce(sum(buy_amount),0) as GMV,
            coalesce(sum(buy_market_amount),0) as 拿货成本
        from yishou_daily.dws_super_airship_spot_special_sale_statistics
        where to_char(stat_date,'yyyymm') between concat(substr('${bizdate}',1,4),'01') and substr('${bizdate}',1,6)
        group by 1,2
    ) t1
    full join (
        select
            substr(dt,1,6) as 月份,
            concat(substr(dt,1,4),'Q',quarter(to_date1(dt,'yyyymmdd'))) as 季度,
            sum(coalesce(成交gmv,0)) as GMV,
            sum(coalesce(拿货价,0)) as 拿货成本
        from yishou_daily.ads_presale_spot_discount_advice_dt dt 
        left join yishou_data.dim_supply_info sup
        on dt.supply_id = sup.supply_id 
        where dt.dt between concat(substr('${bizdate}',1,4),'0101') and to_char(last_day(to_date1(dt.dt,'yyyymmdd')),'yyyymmdd')
        group by 1,2
    ) t2
    on t1.月份 = t2.月份 and t1.季度 = t2.季度
    left join tg on coalesce(t1.月份,t1.季度) = tg.stat_month
)
, t2 as (
    select 
        月份
        ,sum(coalesce(GMV,0))/sum(coalesce(拿货成本,0)) as 折扣率
        ,max(S级目标值) as S级目标值	
        ,max(A级目标值) as A级目标值
        ,max(B级目标值) as B级目标值
    from t1 
    group by 月份
)
, t3 as (
    select 
        季度
        ,sum(coalesce(GMV,0))/sum(coalesce(拿货成本,0)) as 折扣率
        ,max(S级目标值) as S级目标值	
        ,max(A级目标值) as A级目标值
        ,max(B级目标值) as B级目标值
    from (
        select *,row_number() over (partition by 季度 order by 月份 desc) as rn from t1
    ) tt1
    where rn = 1
    group by 季度
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
    '超级买卖飞船' as 部门
    ,'歌莉' as 负责人
    ,月份 as 季度or月份
    ,'折扣率目标' as  指标名称
    ,concat(round(S级目标值 * 100,4),'%') as S级目标值	
    ,concat(round(A级目标值 * 100,4),'%') as A级目标值
    ,concat(round(B级目标值 * 100,4),'%') as  B级目标值
    ,concat(round(折扣率 * 100,4),'%') as 当前实际值
    , CASE 
        when 折扣率 >= S级目标值 then 'S'
        when 折扣率 >= A级目标值 then 'A'
        WHEN 折扣率 >= B级目标值 then 'B'
        ELSE 'C'
    end as 指标达成等级
    ,concat(to_char(round((折扣率/S级目标值) * 100,2)),'%') as S级达成率
    ,concat(to_char(round((折扣率/A级目标值) * 100,2)),'%') as A级达成率
    ,concat(to_char(round((折扣率/B级目标值) * 100,2)),'%') as B级达成率
    ,'统计口径: 现货形式上架_拿货折扣率折扣率=（现货形式上架实际销售额+预售现货打折实际销售额）÷（现货形式上架销售商品总拿货额+预售现货打折销售商品总拿货额）---5月调整口径' as  指标定义
    ,0.2 as 权重
from t2
UNION all
SELECT  
    '超级买卖飞船' as 部门
    ,'歌莉' as 负责人
    ,季度 as 季度or月份
    ,'折扣率目标' as  指标名称
    ,concat(round(S级目标值 * 100,4),'%') as S级目标值	
    ,concat(round(A级目标值 * 100,4),'%') as A级目标值
    ,concat(round(B级目标值 * 100,4),'%') as  B级目标值
    ,concat(round(折扣率 * 100,4),'%') as 当前实际值
    , CASE when 折扣率 >= S级目标值 then 'S'
        when 折扣率 >= A级目标值 then 'A'
        WHEN 折扣率 >= B级目标值 then 'B'
        ELSE 'C'
    end as 指标达成等级
    ,concat(to_char(round((折扣率/S级目标值) * 100,2)),'%') as S级达成率
    ,concat(to_char(round((折扣率/A级目标值) * 100,2)),'%') as A级达成率
    ,concat(to_char(round((折扣率/B级目标值) * 100,2)),'%') as B级达成率
    ,'统计口径: 现货形式上架_拿货折扣率折扣率=（现货形式上架实际销售额+预售现货打折实际销售额）÷（现货形式上架销售商品总拿货额+预售现货打折销售商品总拿货额）---5月调整口径' as  指标定义
    ,0.2 as 权重
from t3
;

-- 超级买卖飞船	歌莉 公司库存在库货值
with tg as(
    select 
        stat_month,
        max(if(target_type='期末在库货值',level_s_taget,null)) turnover_target_s,
        max(if(target_type='期末在库货值',level_a_taget,null)) turnover_target_a,
        max(if(target_type='期末在库货值',level_b_taget,null)) turnover_target_b
    from yishou_daily.dim_spot_sale_2024_target_value
    group by stat_month
)
, t1 as(
    -- 季度
    select 
        a.季度
        ,tg.turnover_target_s as S级目标值
        ,tg.turnover_target_a as A级目标值
        ,tg.turnover_target_b as B级目标值
        ,round(max(if(q_rank=1,a.stock_purchase_amount,null)),4) as stock_purchase_amount
    from (
        select 
            stat_date
            ,concat(to_char(stat_date,'yyyy'),'Q',quarter(stat_date)) as 季度
            ,sum(stock_purchase_amount) as stock_purchase_amount
            ,row_number() over(partition by concat(to_char(stat_date,'yyyy'),'Q',quarter(stat_date)) order by stat_date desc) q_rank
        from yishou_daily.dws_super_airship_stock_inventory_and_turnover_detail_full
        where one_source !='寄售'
        group by 1,2
    ) a
    left join tg on a.季度 = tg.stat_month
    where to_char(a.stat_date,'yyyymm') between concat(substr('${bizdate}',1,4),'01') and substr('${bizdate}',1,6)
    group by 1,2,3,4
)
,t2 as(
    -- 月份
    select
        a.月份
        ,tg.turnover_target_s as S级目标值
        ,tg.turnover_target_a as A级目标值
        ,tg.turnover_target_b as B级目标值
        ,round(max(if(m_rank=1,a.stock_purchase_amount,null)),4) as stock_purchase_amount
    from (
        select 
            stat_date
            ,to_char(last_day(stat_date),'yyyymm') as 月份
            ,sum(stock_purchase_amount) as stock_purchase_amount
            ,row_number() over(partition by to_char(last_day(stat_date),'yyyymm') order by stat_date desc) m_rank
        from yishou_daily.dws_super_airship_stock_inventory_and_turnover_detail_full
        where one_source !='寄售'
        group by 1,2
    ) a
    left join tg on a.月份 = tg.stat_month
    where to_char(stat_date,'yyyymm') between concat(substr('${bizdate}',1,4),'01') and substr('${bizdate}',1,6)
    group by 1,2,3,4
)
INSERT INTO  yishou_daily.responsible_person_indicators
select
    '超级买卖飞船' as 部门
    ,'歌莉' as 负责人
    ,季度 as 季度or月份
    ,'公司库存在库货值' as  指标名称
    ,S级目标值 as S级目标值	
    ,A级目标值 as A级目标值
    ,B级目标值 as  B级目标值
    ,t1.stock_purchase_amount as 当前实际值
    ,CASE 
        when t1.stock_purchase_amount <= S级目标值 then 'S'
        when t1.stock_purchase_amount <= A级目标值 then 'A'
        WHEN t1.stock_purchase_amount <= B级目标值 then 'B'
        ELSE 'C'
    end as 指标达成等级
    ,concat(to_char(round((S级目标值/t1.stock_purchase_amount) * 100,2)),'%') as S级达成率
    ,concat(to_char(round((A级目标值/t1.stock_purchase_amount) * 100,2)),'%') as A级达成率
    ,concat(to_char(round((B级目标值/t1.stock_purchase_amount) * 100,2)),'%') as B级达成率
    ,'统计口径:季度指标，到每个季度最后一天，公司库存仓库在库库存货值低于1200万且过季库存（口径：Q1的过季库存为冬款的库存）占比低于20%； 红线值：未达成的情况下自动降1档，如其他指标达成了S，红线指标未达成，则自动降低为A' as  指标定义
    ,0.0 as 权重
from t1
union all 
select
    '超级买卖飞船' as 部门
    ,'歌莉' as 负责人
    ,月份 as 季度or月份
    ,'公司库存在库货值' as  指标名称
    ,S级目标值 as S级目标值	
    ,A级目标值 as A级目标值
    ,B级目标值 as  B级目标值
    ,t2.stock_purchase_amount as 当前实际值
    ,CASE 
        when t2.stock_purchase_amount <= S级目标值 then 'S'
        when t2.stock_purchase_amount <= A级目标值 then 'A'
        WHEN t2.stock_purchase_amount <= B级目标值 then 'B'
        ELSE 'C'
    end as 指标达成等级
    ,concat(to_char(round((S级目标值/t2.stock_purchase_amount) * 100,2)),'%') as S级达成率
    ,concat(to_char(round((A级目标值/t2.stock_purchase_amount) * 100,2)),'%') as A级达成率
    ,concat(to_char(round((B级目标值/t2.stock_purchase_amount) * 100,2)),'%') as B级达成率
    ,'统计口径:季度指标，到每个季度最后一天，公司库存仓库在库库存货值低于1200万且过季库存（口径：Q1的过季库存为冬款的库存）占比低于20%； 红线值：未达成的情况下自动降1档，如其他指标达成了S，红线指标未达成，则自动降低为A' as  指标定义
    ,0.0 as 权重
from t2
;

-- 平台运营 青田 大盘实际GMV
-- 月份
WITH t1 (
    select 
        to_char(special_date,'yyyy-mm') as stat_month
        ,round(sum(real_buy_amount),4) as GMV
        ,QUARTER(to_char(special_date,'yyyy-mm')) as QUA
    from yishou_data.dwm_goods_sp_up_info_dt
    where dt >= concat(substr('${bizdate}',1,4),'0101')
    GROUP BY to_char(special_date,'yyyy-mm')
)
-- 季度
,t2 as  (
    SELECT   
        concat(to_char(max(stat_month),'yyyy'),'Q',QUA) as stat_month
        ,sum(GMV) as GMV
        ,QUA
        ,case when QUA = 1 then 781584582 
         when QUA = 2 then 959832931 
         when QUA = 3 then 871137054 
         when QUA = 4 then 1421482117 
        end  as goods_value_target_s
        ,case when QUA = 1 then 719057816 
         when QUA = 2 then 888734196 
         when QUA = 3 then 806608383 
         when QUA = 4 then 1316187147 
        end  as goods_value_target_a
        ,case when QUA = 1 then 702280485.58 
         when QUA = 2 then 817635459 
         when QUA = 3 then 742079712 
         when QUA = 4 then 1210892175 
        end  as goods_value_target_b
    FROM t1 
    GROUP BY QUA 
)
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,GMV
        ,case when  to_char(stat_month,'mm') = '01' then 294712995
        when  to_char(stat_month,'mm') = '02' then 214185243
        when  to_char(stat_month,'mm') = '03' then 335213112 
        when  to_char(stat_month,'mm') = '04' then 348762122
        when  to_char(stat_month,'mm') = '05' then 329895871
        when  to_char(stat_month,'mm') = '06' then 281174938
        when  to_char(stat_month,'mm') = '07' then 194498030
        when  to_char(stat_month,'mm') = '08' then 301573423
        when  to_char(stat_month,'mm') = '09' then 375065601
        when  to_char(stat_month,'mm') = '10' then 457716347
        when  to_char(stat_month,'mm') = '11' then 501956322
        when  to_char(stat_month,'mm') = '12' then 461809448
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 272882402
        when  to_char(stat_month,'mm') = '02' then 198319669
        when  to_char(stat_month,'mm') = '03' then 310382511 
        when  to_char(stat_month,'mm') = '04' then 322927891
        when  to_char(stat_month,'mm') = '05' then 305459140
        when  to_char(stat_month,'mm') = '06' then 260347165
        when  to_char(stat_month,'mm') = '07' then 180090768
        when  to_char(stat_month,'mm') = '08' then 279234651
        when  to_char(stat_month,'mm') = '09' then 347282964
        when  to_char(stat_month,'mm') = '10' then 423811433
        when  to_char(stat_month,'mm') = '11' then 464774373
        when  to_char(stat_month,'mm') = '12' then 427601341
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 251051810
        when  to_char(stat_month,'mm') = '02' then 182454096
        when  to_char(stat_month,'mm') = '03' then 285551910 
        when  to_char(stat_month,'mm') = '04' then 297093659
        when  to_char(stat_month,'mm') = '05' then 281022408
        when  to_char(stat_month,'mm') = '06' then 239519392
        when  to_char(stat_month,'mm') = '07' then 165683507
        when  to_char(stat_month,'mm') = '08' then 256895879
        when  to_char(stat_month,'mm') = '09' then 319500326
        when  to_char(stat_month,'mm') = '10' then 389906518
        when  to_char(stat_month,'mm') = '11' then 427592423
        when  to_char(stat_month,'mm') = '12' then 393393234
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '平台运营' as 部门
	 ,'青田' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'大盘实际GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,GMV as 当前实际值
     , CASE when t3.GMV >= goods_value_target_s then 'S'
            when t3.GMV >= goods_value_target_a then 'A'
            when t3.GMV >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((GMV/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((GMV/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((GMV/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'指标释义：大盘实际GMV，不含专场结束前取消' as  指标定义
	 ,case when SUBSTR(stat_month,5,2) BETWEEN '01' and '04' THEN  0.20
	    ELSE  0.10 end as 权重
from t3
UNION ALL 
SELECT 
     '平台运营' as 部门
	 ,'青田' as 负责人
	 ,stat_month  as 季度or月份
	 ,'大盘实际GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,GMV as 当前实际值
     , CASE when t2.GMV >= goods_value_target_s then 'S'
            when t2.GMV >= goods_value_target_a then 'A'
            when t2.GMV >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((GMV/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((GMV/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((GMV/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'指标释义：大盘实际GMV，不含专场结束前取消' as  指标定义
	 ,case when QUA = 1 then 0.20 
	  when QUA = 2 then 0.20 /3 + 0.10/3 +0.10/3
	  else 0.10 end as 权重	
from t2;
    
--平台运营 青田 月爆款GMV
with t1 as (
    SELECT 
    	to_char(ym,'yyyy-mm') as stat_month,
        round(sum(cbk_real_gmv),4) as GMV,
        QUARTER(to_char(ym,'yyyy-mm')) as QUA
    from cross_origin.finebi_kpi_cbk_lzj_ym
    where ym >= concat(substr('${bizdate}',1,4),'-','01')
    GROUP BY ym
)
-- 季度
,t2 as  (
    SELECT   
        concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
        ,sum(GMV) as GMV
        ,QUA
        ,case when QUA = 1 then 439089663 
         when QUA = 2 then 463559589 
         when QUA = 3 then 444069433 
         when QUA = 4 then 874384669 
        end  as goods_value_target_s
        ,case when QUA = 1 then 406351548 
         when QUA = 2 then 430176797 
         when QUA = 3 then 412958915 
         when QUA = 4 then 811729296 
        end  as goods_value_target_a
        ,case when QUA = 1 then 367454778 
         when QUA = 2 then 390514073 
         when QUA = 3 then 375995922 
         when QUA = 4 then 737287270 
        end  as goods_value_target_b
    FROM t1 
    GROUP BY to_char(stat_month,'yyyy'),QUA 
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,GMV
        ,case when  to_char(stat_month,'mm') = '01' then 125126804
        when  to_char(stat_month,'mm') = '02' then 137280848
        when  to_char(stat_month,'mm') = '03' then 176682010 
        when  to_char(stat_month,'mm') = '04' then 178180259
        when  to_char(stat_month,'mm') = '05' then 158166920
        when  to_char(stat_month,'mm') = '06' then 127212410
        when  to_char(stat_month,'mm') = '07' then 83198036
        when  to_char(stat_month,'mm') = '08' then 158487759
        when  to_char(stat_month,'mm') = '09' then 202383638
        when  to_char(stat_month,'mm') = '10' then 262254088
        when  to_char(stat_month,'mm') = '11' then 318220867
        when  to_char(stat_month,'mm') = '12' then 293909713
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 119423823
        when  to_char(stat_month,'mm') = '02' then 96348429
        when  to_char(stat_month,'mm') = '03' then 163853194 
        when  to_char(stat_month,'mm') = '04' then 165335001
        when  to_char(stat_month,'mm') = '05' then 146780705
        when  to_char(stat_month,'mm') = '06' then 118061090
        when  to_char(stat_month,'mm') = '07' then 77221505
        when  to_char(stat_month,'mm') = '08' then 147363878
        when  to_char(stat_month,'mm') = '09' then 188373530
        when  to_char(stat_month,'mm') = '10' then 244124271
        when  to_char(stat_month,'mm') = '11' then 295442478
        when  to_char(stat_month,'mm') = '12' then 272162547
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 112648003
        when  to_char(stat_month,'mm') = '02' then 106195738
        when  to_char(stat_month,'mm') = '03' then 148611036 
        when  to_char(stat_month,'mm') = '04' then 150073308
        when  to_char(stat_month,'mm') = '05' then 133252530
        when  to_char(stat_month,'mm') = '06' then 107188234
        when  to_char(stat_month,'mm') = '07' then 70120677
        when  to_char(stat_month,'mm') = '08' then 134147387
        when  to_char(stat_month,'mm') = '09' then 171727857
        when  to_char(stat_month,'mm') = '10' then 222583894
        when  to_char(stat_month,'mm') = '11' then 268379044
        when  to_char(stat_month,'mm') = '12' then 246324331
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '平台运营' as 部门
	 ,'青田' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'月爆款GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,GMV as 当前实际值
     , CASE when t3.GMV >= goods_value_target_s then 'S'
            when t3.GMV >= goods_value_target_a then 'A'
            when t3.GMV >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((GMV/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((GMV/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((GMV/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'月爆款定义：	2~7月，月实际gmv>5000	8~1月，月实际gmv>6000	且实际uv价值>大盘的实际uv价值均值*80%的商品	(23年业绩占比52%左右)	（包含未上架专场商品曝光、多渠道曝光不去重）' as  指标定义
	 ,case 
	    when SUBSTR(stat_month,5,2) BETWEEN '01' and '04' THEN  0.20
	    ELSE  0.15  
	  end as 权重
from t3
UNION ALL 
SELECT 
     '平台运营' as 部门
	 ,'青田' as 负责人
	 ,stat_month  as 季度or月份
	 ,'月爆款GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,GMV as 当前实际值
     , CASE when t2.GMV >= goods_value_target_s then 'S'
            when t2.GMV >= goods_value_target_a then 'A'
            when t2.GMV >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((GMV/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((GMV/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((GMV/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'月爆款定义：	2~7月，月实际gmv>5000	8~1月，月实际gmv>6000	且实际uv价值>大盘的实际uv价值均值*80%的商品	(23年业绩占比52%左右)	（包含未上架专场商品曝光、多渠道曝光不去重）' as  指标定义
	 ,case 
	    when QUA = 1 then 0.20
	    when QUA = 2 then 0.20 / 3 + 0.15/3  + 0.15/3
	    else 0.15 
	 end as 权重
from t2;

-- 日不动销流量占比(4个负责人的指标融合)
with t1 as (
	SELECT 
		to_char(special_date,'yyyy-mm') as stat_month
		,(sum(bdx_goods_uv)/sum(uv)) * 100  as 不动销款曝光uv占比
		,QUARTER(to_char(special_date,'yyyy-mm')) as QUA
	from cross_origin.finebi_kpi_bdx_lzj_dt
	where dt >= concat(substr('${bizdate}',1,4),'-','01')
	GROUP BY to_char(special_date,'yyyy-mm')
)
,t2 as  (
    SELECT   
        concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
        ,avg(不动销款曝光uv占比) as 不动销款曝光uv占比
        ,case when QUA = 1 then 37.766716378392 
         when QUA = 2 then 36.2658142524604 
         when QUA = 3 then 37.3719669976217 
         when QUA = 4 then 39.0895708796624 
        end  as goods_value_target_s
        ,case when QUA = 1 then 39.8800497117253 
         when QUA = 2 then 38.2591475857937 
         when QUA = 3 then 39.3406336642884 
         when QUA = 4 then 41.0375708796624 
        end  as goods_value_target_a
        ,case when QUA = 1 then 43.0500497117253 
         when QUA = 2 then 41.2491475857937 
         when QUA = 3 then 42.2936336642884 
         when QUA = 4 then 43.9595708796624 
        end  as goods_value_target_b
    FROM t1 
    GROUP BY to_char(stat_month,'yyyy'),QUA 
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,不动销款曝光uv占比
        ,case when  to_char(stat_month,'mm') = '01' then 43.2201720176232
        when  to_char(stat_month,'mm') = '02' then 34.485920402563
        when  to_char(stat_month,'mm') = '03' then 36.2572474919621 
        when  to_char(stat_month,'mm') = '04' then 35.3158766156317
        when  to_char(stat_month,'mm') = '05' then 37.3429858744541
        when  to_char(stat_month,'mm') = '06' then 36.6835324211207
        when  to_char(stat_month,'mm') = '07' then 37.7339300514432
        when  to_char(stat_month,'mm') = '08' then 37.3529952498495
        when  to_char(stat_month,'mm') = '09' then 37.4101265001729
        when  to_char(stat_month,'mm') = '10' then 37.4777110657426
        when  to_char(stat_month,'mm') = '11' then 39.2011155551848
        when  to_char(stat_month,'mm') = '12' then 40.5898860180598
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 45.4801720176232
        when  to_char(stat_month,'mm') = '02' then 36.565920402563
        when  to_char(stat_month,'mm') = '03' then 38.2972474919621 
        when  to_char(stat_month,'mm') = '04' then 37.3258766156317
        when  to_char(stat_month,'mm') = '05' then 39.3489858744541
        when  to_char(stat_month,'mm') = '06' then 38.6775324211207
        when  to_char(stat_month,'mm') = '07' then 39.7139300514432
        when  to_char(stat_month,'mm') = '08' then 39.3289952498495
        when  to_char(stat_month,'mm') = '09' then 39.3801265001729
        when  to_char(stat_month,'mm') = '10' then 39.4317110657426
        when  to_char(stat_month,'mm') = '11' then 41.1511155551848
        when  to_char(stat_month,'mm') = '12' then 42.5298860180598
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 48.8701720176231
        when  to_char(stat_month,'mm') = '02' then 39.68592040256
        when  to_char(stat_month,'mm') = '03' then 41.3572474919621 
        when  to_char(stat_month,'mm') = '04' then 40.3408766156317
        when  to_char(stat_month,'mm') = '05' then 42.3579858744541
        when  to_char(stat_month,'mm') = '06' then 41.6685324211207
        when  to_char(stat_month,'mm') = '07' then 42.6839300514432
        when  to_char(stat_month,'mm') = '08' then 42.2929952498495
        when  to_char(stat_month,'mm') = '09' then 42.3351265001729
        when  to_char(stat_month,'mm') = '10' then 42.3627110657426
        when  to_char(stat_month,'mm') = '11' then 44.0761155551848
        when  to_char(stat_month,'mm') = '12' then 45.4398860180598
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
-- 平台运营	青田 日不动销流量占比（全盘商品全场景情况）
SELECT 
     '平台运营' as 部门
	 ,'青田' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'日不动销流量占比（全盘商品全场景情况）' as  指标名称
	 ,concat(round(goods_value_target_s,2),'%') as S级目标值	
     ,concat(round(goods_value_target_a,2),'%') as A级目标值
     ,concat(round(goods_value_target_b,2),'%') as B级目标值
     ,concat(round(t3.不动销款曝光uv占比,2),'%') as 当前实际值
     , CASE 
        when round(t3.不动销款曝光uv占比,2) <= goods_value_target_s then 'S'
        when round(t3.不动销款曝光uv占比,2) <= goods_value_target_a then 'A'
        when round(t3.不动销款曝光uv占比,2) <= goods_value_target_b then 'B'
        ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/不动销款曝光uv占比) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/不动销款曝光uv占比) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/不动销款曝光uv占比) * 100,2)),'%') as B级达成率
	 ,'月爆款定义：2~7月，月实际gmv>5000	8~1月，月实际gmv>6000	且实际uv价值>大盘的实际uv价值均值*80%的商品(23年业绩占比52%左右)	（包含未上架专场商品曝光、多渠道曝光不去重）' as  指标定义
	 ,0.10 as 权重
from t3
UNION ALL 
SELECT 
     '平台运营' as 部门
	 ,'青田' as 负责人
	 ,stat_month  as 季度or月份
	 ,'日不动销流量占比（全盘商品全场景情况）' as  指标名称
	 ,concat(round(goods_value_target_s,2),'%') as S级目标值	
     ,concat(round(goods_value_target_a,2),'%') as A级目标值
     ,concat(round(goods_value_target_b,2),'%') as B级目标值
     ,concat(round(t2.不动销款曝光uv占比,2),'%') as 当前实际值
     , CASE 
        when round(t2.不动销款曝光uv占比,2) <= goods_value_target_s then 'S'
        when round(t2.不动销款曝光uv占比,2) <= goods_value_target_a then 'A'
        when round(t2.不动销款曝光uv占比,2) <= goods_value_target_b then 'B'
        ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/不动销款曝光uv占比) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/不动销款曝光uv占比) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/不动销款曝光uv占比) * 100,2)),'%') as B级达成率
	 ,'月爆款定义：	2~7月，月实际gmv>5000	8~1月，月实际gmv>6000	且实际uv价值>大盘的实际uv价值均值*80%的商品	(23年业绩占比52%左右)	（包含未上架专场商品曝光、多渠道曝光不去重）' as  指标定义
	 ,0.10 as 权重
from t2
union all
-- 产研中心 产品（按各50%权重考核产品部达成） 秀则 日不动销流量占比（全盘商品全场景情况）
SELECT 
     '产研中心 产品（按各50%权重考核产品部达成）' as 部门
	 ,'秀则' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'日不动销流量占比（全盘商品全场景情况）' as  指标名称
	 ,concat(round(goods_value_target_s,2),'%') as S级目标值	
     ,concat(round(goods_value_target_a,2),'%') as A级目标值
     ,concat(round(goods_value_target_b,2),'%') as B级目标值
     ,concat(round(t3.不动销款曝光uv占比,2),'%') as 当前实际值
     , CASE 
        when round(t3.不动销款曝光uv占比,2) <= goods_value_target_s then 'S'
        when round(t3.不动销款曝光uv占比,2) <= goods_value_target_a then 'A'
        when round(t3.不动销款曝光uv占比,2) <= goods_value_target_b then 'B'
        ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/不动销款曝光uv占比) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/不动销款曝光uv占比) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/不动销款曝光uv占比) * 100,2)),'%') as B级达成率
	 ,'统计口径：日均全盘不动销商品曝光uv/日均全盘商品曝光uv 全盘指的是不区分是否当日上架、不区分任何场景' as  指标定义
	 ,0.10 as 权重
from t3
UNION ALL 
SELECT 
     '产研中心 产品（按各50%权重考核产品部达成）' as 部门
	 ,'秀则' as 负责人
	 ,stat_month  as 季度or月份
	 ,'日不动销流量占比（全盘商品全场景情况）' as  指标名称
	 ,concat(round(goods_value_target_s,2),'%') as S级目标值	
     ,concat(round(goods_value_target_a,2),'%') as A级目标值
     ,concat(round(goods_value_target_b,2),'%') as B级目标值
     ,concat(round(t2.不动销款曝光uv占比,2),'%') as 当前实际值
     , CASE 
        when round(t2.不动销款曝光uv占比,2) <= goods_value_target_s then 'S'
        when round(t2.不动销款曝光uv占比,2) <= goods_value_target_a then 'A'
        when round(t2.不动销款曝光uv占比,2) <= goods_value_target_b then 'B'
        ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/不动销款曝光uv占比) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/不动销款曝光uv占比) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/不动销款曝光uv占比) * 100,2)),'%') as B级达成率
	 ,'统计口径：日均全盘不动销商品曝光uv/日均全盘商品曝光uv 全盘指的是不区分是否当日上架、不区分任何场景' as  指标定义
	 ,0.10 as 权重
from t2
-- 场：平台中心 商品&活动运营 责任人: 暂无  日不动销流量占比（全盘商品全场景情况）
union ALL 
SELECT 
     '场：平台中心 商品&活动运营' as 部门
	 ,'暂无' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'日不动销流量占比（全盘商品全场景情况）' as  指标名称
	 ,concat(round(goods_value_target_s,2),'%') as S级目标值	
     ,concat(round(goods_value_target_a,2),'%') as A级目标值
     ,concat(round(goods_value_target_b,2),'%') as B级目标值
     ,concat(round(t3.不动销款曝光uv占比,2),'%') as 当前实际值
     , CASE 
        when round(t3.不动销款曝光uv占比,2) <= goods_value_target_s then 'S'
        when round(t3.不动销款曝光uv占比,2) <= goods_value_target_a then 'A'
        when round(t3.不动销款曝光uv占比,2) <= goods_value_target_b then 'B'
        ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/不动销款曝光uv占比) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/不动销款曝光uv占比) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/不动销款曝光uv占比) * 100,2)),'%') as B级达成率
	 ,'统计口径：日均全盘不动销商品曝光uv/日均全盘商品曝光uv 全盘指的是不区分是否当日上架、不区分任何场景' as  指标定义
	 ,0.15 as 权重
from t3
UNION ALL 
SELECT 
     '场：平台中心 商品&活动运营' as 部门
	 ,'暂无' as 负责人
	 ,stat_month  as 季度or月份
	 ,'日不动销流量占比（全盘商品全场景情况）' as  指标名称
	 ,concat(round(goods_value_target_s,2),'%') as S级目标值	
     ,concat(round(goods_value_target_a,2),'%') as A级目标值
     ,concat(round(goods_value_target_b,2),'%') as B级目标值
     ,concat(round(t2.不动销款曝光uv占比,2),'%') as 当前实际值
     , CASE 
        when round(t2.不动销款曝光uv占比,2) <= goods_value_target_s then 'S'
        when round(t2.不动销款曝光uv占比,2) <= goods_value_target_a then 'A'
        when round(t2.不动销款曝光uv占比,2) <= goods_value_target_b then 'B'
        ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/不动销款曝光uv占比) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/不动销款曝光uv占比) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/不动销款曝光uv占比) * 100,2)),'%') as B级达成率
	 ,'统计口径：日均全盘不动销商品曝光uv/日均全盘商品曝光uv 全盘指的是不区分是否当日上架、不区分任何场景' as  指标定义
	 ,0.15 as 权重
from t2
union ALL 
--场：平台中心 责任人: 苏苏  日不动销流量占比（全盘商品全场景情况）
SELECT 
     '场：平台中心' as 部门
	 ,'苏苏' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'日不动销流量占比（全盘商品全场景情况）' as  指标名称
	 ,concat(round(goods_value_target_s,2),'%') as S级目标值	
     ,concat(round(goods_value_target_a,2),'%') as A级目标值
     ,concat(round(goods_value_target_b,2),'%') as B级目标值
     ,concat(round(t3.不动销款曝光uv占比,2),'%') as 当前实际值
     , CASE 
        when round(t3.不动销款曝光uv占比,2) <= goods_value_target_s then 'S'
        when round(t3.不动销款曝光uv占比,2) <= goods_value_target_a then 'A'
        when round(t3.不动销款曝光uv占比,2) <= goods_value_target_b then 'B'
        ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/不动销款曝光uv占比) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/不动销款曝光uv占比) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/不动销款曝光uv占比) * 100,2)),'%') as B级达成率
	 ,'统计口径：日均全盘不动销商品曝光uv/日均全盘商品曝光uv 全盘指的是不区分是否当日上架、不区分任何场景' as  指标定义
	 ,case 
	    when SUBSTR(stat_month,5,2) BETWEEN '01' and '04' THEN  0.15 
	    ELSE  0.10 
	  end as 权重
from t3
UNION ALL 
SELECT 
     '场：平台中心' as 部门
	 ,'苏苏' as 负责人
	 ,t2.stat_month  as 季度or月份
	 ,'日不动销流量占比（全盘商品全场景情况）' as  指标名称
	 ,concat(round(goods_value_target_s,2),'%') as S级目标值	
     ,concat(round(goods_value_target_a,2),'%') as A级目标值
     ,concat(round(goods_value_target_b,2),'%') as B级目标值
     ,concat(round(t2.不动销款曝光uv占比,2),'%') as 当前实际值
     , CASE 
        when round(t2.不动销款曝光uv占比,2) <= goods_value_target_s then 'S'
        when round(t2.不动销款曝光uv占比,2) <= goods_value_target_a then 'A'
        when round(t2.不动销款曝光uv占比,2) <= goods_value_target_b then 'B'
        ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/不动销款曝光uv占比) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/不动销款曝光uv占比) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/不动销款曝光uv占比) * 100,2)),'%') as B级达成率
	 ,'统计口径：日均全盘不动销商品曝光uv/日均全盘商品曝光uv 全盘指的是不区分是否当日上架、不区分任何场景' as  指标定义
	 ,case 
	    when substr(t2.stat_month,6,1) = 1 then 0.15 
	    when substr(t2.stat_month,6,1) = 2 then 0.15/3 + 0.10/3 +0.10/3
	    else 0.10 
	  end as 权重		
from t2
;

-- 履约-24配（10位负责人的指标融合）
WITH t1 as (
    SELECT 
        to_char(special_date,'yyyy-mm') as stat_month
        ,round(sum(allot_amount_24h)/sum(create_amount),2) as h24配率
        ,QUARTER(to_char(special_date,'yyyy-mm')) as QUA 
    from yishou_daily.finebi_supply_chain_sp_purchase_allot_dt a 
    left join yishou_data.dim_supply_info si 
    on a.supply_id = si.supply_id 
    WHERE dt >= concat(substr('${bizdate}',1,4),'-','01')
    GROUP by to_char(special_date,'yyyy-mm')
)
,t2 as  (
    SELECT   
        concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
        ,avg(h24配率) as 当前实际值
        ,QUA
        ,case when QUA = 1 then 0.6
         when QUA = 2 then 0.7
         when QUA = 3 then 0.75
         when QUA = 4 then 0.8
        end  as goods_value_target_s
        ,case when QUA = 1 then 0.55
         when QUA = 2 then 0.65
         when QUA = 3 then 0.7
         when QUA = 4 then 0.75
        end  as goods_value_target_a
        ,case when QUA = 1 then 0.5
         when QUA = 2 then 0.62
         when QUA = 3 then 0.65
         when QUA = 4 then 0.68
        end  as goods_value_target_b
    FROM t1 
    GROUP by to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,h24配率 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 0.6
        when  to_char(stat_month,'mm') = '02' then 0.6
        when  to_char(stat_month,'mm') = '03' then 0.6
        when  to_char(stat_month,'mm') = '04' then 0.7
        when  to_char(stat_month,'mm') = '05' then 0.7
        when  to_char(stat_month,'mm') = '06' then 0.7
        when  to_char(stat_month,'mm') = '07' then 0.75
        when  to_char(stat_month,'mm') = '08' then 0.75
        when  to_char(stat_month,'mm') = '09' then 0.75
        when  to_char(stat_month,'mm') = '10' then 0.8
        when  to_char(stat_month,'mm') = '11' then 0.8
        when  to_char(stat_month,'mm') = '12' then 0.8
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 0.55
        when  to_char(stat_month,'mm') = '02' then 0.55
        when  to_char(stat_month,'mm') = '03' then 0.55
        when  to_char(stat_month,'mm') = '04' then 0.65 
        when  to_char(stat_month,'mm') = '05' then 0.65 
        when  to_char(stat_month,'mm') = '06' then 0.65 
        when  to_char(stat_month,'mm') = '07' then 0.7
        when  to_char(stat_month,'mm') = '08' then 0.7
        when  to_char(stat_month,'mm') = '09' then 0.7
        when  to_char(stat_month,'mm') = '10' then 0.75
        when  to_char(stat_month,'mm') = '11' then 0.75
        when  to_char(stat_month,'mm') = '12' then 0.75
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 0.5
        when  to_char(stat_month,'mm') = '02' then 0.5
        when  to_char(stat_month,'mm') = '03' then 0.5
        when  to_char(stat_month,'mm') = '04' then 0.62
        when  to_char(stat_month,'mm') = '05' then 0.62
        when  to_char(stat_month,'mm') = '06' then 0.62
        when  to_char(stat_month,'mm') = '07' then 0.65
        when  to_char(stat_month,'mm') = '08' then 0.65
        when  to_char(stat_month,'mm') = '09' then 0.65
        when  to_char(stat_month,'mm') = '10' then 0.68
        when  to_char(stat_month,'mm') = '11' then 0.68
        when  to_char(stat_month,'mm') = '12' then 0.68
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
-- 平台运营	青田 履约-24配 : 统计周期（专场日期），24小时配货金额/应开单金额（与浩兵一致）
SELECT 
     '场：平台中心 平台运营' as 部门 
	 ,'青田' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'履约-24配' as  指标名称
	 ,concat(goods_value_target_s,'%') as S级目标值	
     ,concat(goods_value_target_a,'%') as A级目标值
     ,concat(goods_value_target_b,'%') as  B级目标值
     ,concat(当前实际值,'%') as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'统计周期（专场日期），24小时配货金额/应开单金额（与浩兵一致）' as  指标定义
	 ,case when SUBSTR(stat_month,5,2) BETWEEN '01' and '04' THEN  0.15 
	    ELSE  0.30 end as 权重
from t3
UNION ALL 
SELECT 
     '场：平台中心 平台运营' as 部门
	 ,'青田' as 负责人
	 ,stat_month  as 季度or月份
	 ,'履约-24配' as  指标名称
	 ,concat(goods_value_target_s,'%') as S级目标值	
     ,concat(goods_value_target_a,'%') as A级目标值
     ,concat(goods_value_target_b,'%') as  B级目标值
     ,concat(当前实际值,'%') as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'统计周期（专场日期），24小时配货金额/应开单金额（与浩兵一致）' as  指标定义
	 ,case when QUA = 1 then 0.15 
	  when QUA = 2 then 0.15/3 + 0.30/3 + 0.30/3
	  else 0.30 end as 权重
from t2
-- 货：供给 履约 浩兵 履约-24配
UNION ALL 
SELECT 
     '货：供给 履约' as 部门 
	 ,'浩兵' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'履约-24配' as  指标名称
	 ,concat(goods_value_target_s,'%') as S级目标值	
     ,concat(goods_value_target_a,'%') as A级目标值
     ,concat(goods_value_target_b,'%') as  B级目标值
     ,concat(当前实际值,'%') as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'统计周期（专场日期），24小时配货金额/应开单金额（与浩兵一致）' as  指标定义
	 ,0.4 as 权重
from t3
UNION ALL 
SELECT 
     '货：供给 履约' as 部门
	 ,'浩兵' as 负责人
	 ,stat_month  as 季度or月份
	 ,'履约-24配' as  指标名称
	 ,concat(goods_value_target_s,'%') as S级目标值	
     ,concat(goods_value_target_a,'%') as A级目标值
     ,concat(goods_value_target_b,'%') as  B级目标值
     ,concat(当前实际值,'%') as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'统计周期（专场日期），24小时配货金额/应开单金额（与浩兵一致）' as  指标定义
	 ,0.4 as 权重
from t2
-- 产研中心 产品（按各50%权重考核产品部达成） 至尊宝 履约-24配
UNION ALL 
SELECT 
     '产研中心 产品（按各50%权重考核产品部达成）' as 部门 
	 ,'至尊宝' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'履约-24配' as  指标名称
	 ,concat(goods_value_target_s,'%') as S级目标值	
     ,concat(goods_value_target_a,'%') as A级目标值
     ,concat(goods_value_target_b,'%') as  B级目标值
     ,concat(当前实际值,'%') as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'统计周期（专场日期），24小时配货金额/应开单金额（与浩兵一致）' as  指标定义
	 ,0.3 as 权重
from t3
UNION ALL 
SELECT 
     '产研中心 产品（按各50%权重考核产品部达成）' as 部门
	 ,'至尊宝' as 负责人
	 ,stat_month  as 季度or月份
	 ,'履约-24配' as  指标名称
	 ,concat(goods_value_target_s,'%') as S级目标值	
     ,concat(goods_value_target_a,'%') as A级目标值
     ,concat(goods_value_target_b,'%') as  B级目标值
     ,concat(当前实际值,'%') as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'统计周期（专场日期），24小时配货金额/应开单金额（与浩兵一致）' as  指标定义
	 ,0.3 as 权重
from t2
union ALL 
-- 货：供给 --> 十三行  --> 面包 履约-24配 
SELECT 
     '货：供给 十三行' as 部门 
	 ,'面包' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'履约-24配' as  指标名称
	 ,concat(goods_value_target_s,'%') as S级目标值	
     ,concat(goods_value_target_a,'%') as A级目标值
     ,concat(goods_value_target_b,'%') as  B级目标值
     ,concat(当前实际值,'%') as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'统计周期（专场日期），24小时配货金额/应开单金额 备注：GMV销售分布会影响全年结果；按照主流市场预设目标，目标值可能会随着各市场拓版图时权重同步调整，目标值发生动态变化' as  指标定义
	 ,case when SUBSTR(stat_month,5,2) BETWEEN '01' and '04' THEN  0.15 
	    ELSE  0.45 end as 权重
from t3
UNION ALL 
SELECT 
     '货：供给 十三行' as 部门
	 ,'面包' as 负责人
	 ,stat_month  as 季度or月份
	 ,'履约-24配' as  指标名称
	 ,concat(goods_value_target_s,'%') as S级目标值	
     ,concat(goods_value_target_a,'%') as A级目标值
     ,concat(goods_value_target_b,'%') as  B级目标值
     ,concat(当前实际值,'%') as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'统计周期（专场日期），24小时配货金额/应开单金额 备注：GMV销售分布会影响全年结果；按照主流市场预设目标，目标值可能会随着各市场拓版图时权重同步调整，目标值发生动态变化' as  指标定义
	 ,case when QUA = 1 then 0.15 
	  when QUA = 2 then 0.15/3 + 0.45/3 + 0.45/3
	  else 0.45 end as 权重
from t2
union ALL 
--  货：供给  -->  沙河 --> 天真    履约-24配 
SELECT 
     '货：供给 沙河' as 部门 
	 ,'天真' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'履约-24配' as  指标名称
	 ,concat(goods_value_target_s,'%') as S级目标值	
     ,concat(goods_value_target_a,'%') as A级目标值
     ,concat(goods_value_target_b,'%') as  B级目标值
     ,concat(当前实际值,'%') as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'统计周期（专场日期），24小时配货金额/应开单金额 备注：GMV销售分布会影响全年结果；按照主流市场预设目标，目标值可能会随着各市场拓版图时权重同步调整，目标值发生动态变化' as  指标定义
	 ,case when SUBSTR(stat_month,5,2) BETWEEN '01' and '04' THEN  0.15 
	    ELSE  0.45 end as 权重
from t3
UNION ALL 
SELECT 
     '货：供给 沙河' as 部门
	 ,'天真' as 负责人
	 ,stat_month  as 季度or月份
	 ,'履约-24配' as  指标名称
	 ,concat(goods_value_target_s,'%') as S级目标值	
     ,concat(goods_value_target_a,'%') as A级目标值
     ,concat(goods_value_target_b,'%') as  B级目标值
     ,concat(当前实际值,'%') as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'统计周期（专场日期），24小时配货金额/应开单金额 备注：GMV销售分布会影响全年结果；按照主流市场预设目标，目标值可能会随着各市场拓版图时权重同步调整，目标值发生动态变化' as  指标定义
	 ,case when QUA = 1 then 0.15 
	  when QUA = 2 then 0.15/3 + 0.45/3 + 0.45/3
	  else 0.45 end as 权重
from t2
union ALL 
-- 货：供给 杭濮	凯尔        履约-24配
SELECT 
     '货：供给 杭州市场&平湖市场' as 部门 
	 ,'凯尔' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'履约-24配' as  指标名称
	 ,concat(goods_value_target_s,'%') as S级目标值	
     ,concat(goods_value_target_a,'%') as A级目标值
     ,concat(goods_value_target_b,'%') as  B级目标值
     ,concat(当前实际值,'%') as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'统计周期（专场日期），24小时配货金额/应开单金额 备注：GMV销售分布会影响全年结果；按照主流市场预设目标，目标值可能会随着各市场拓版图时权重同步调整，目标值发生动态变化' as  指标定义
	 ,case when SUBSTR(stat_month,5,2) BETWEEN '01' and '04' THEN  0.15 
	    ELSE  0.45 end as 权重
from t3
UNION ALL 
SELECT 
     '货：供给 杭州市场&平湖市场' as 部门
	 ,'凯尔' as 负责人
	 ,stat_month  as 季度or月份
	 ,'履约-24配' as  指标名称
	 ,concat(goods_value_target_s,'%') as S级目标值	
     ,concat(goods_value_target_a,'%') as A级目标值
     ,concat(goods_value_target_b,'%') as  B级目标值
     ,concat(当前实际值,'%') as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'统计周期（专场日期），24小时配货金额/应开单金额 备注：GMV销售分布会影响全年结果；按照主流市场预设目标，目标值可能会随着各市场拓版图时权重同步调整，目标值发生动态变化' as  指标定义
	 ,case when QUA = 1 then 0.15 
	  when QUA = 2 then 0.15/3 + 0.45/3 + 0.45/3
	  else 0.45 end as 权重
from t2
union all
-- 货：供给 南油&新品类	妮娜	履约-24配
SELECT 
     '货：供给 南油&新品类' as 部门 
	 ,'妮娜' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'履约-24配' as  指标名称
	 ,concat(goods_value_target_s,'%') as S级目标值	
     ,concat(goods_value_target_a,'%') as A级目标值
     ,concat(goods_value_target_b,'%') as  B级目标值
     ,concat(当前实际值,'%') as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'统计周期（专场日期），24小时配货金额/应开单金额 备注：GMV销售分布会影响全年结果；按照主流市场预设目标，目标值可能会随着各市场拓版图时权重同步调整，目标值发生动态变化' as  指标定义
	 ,case when SUBSTR(stat_month,5,2) BETWEEN '01' and '04' THEN  0.15 
	    ELSE  0.45 end as 权重
from t3
UNION ALL 
SELECT 
     '货：供给 南油&新品类' as 部门
	 ,'妮娜' as 负责人
	 ,stat_month  as 季度or月份
	 ,'履约-24配' as  指标名称
	 ,concat(goods_value_target_s,'%') as S级目标值	
     ,concat(goods_value_target_a,'%') as A级目标值
     ,concat(goods_value_target_b,'%') as  B级目标值
     ,concat(当前实际值,'%') as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'统计周期（专场日期），24小时配货金额/应开单金额 备注：GMV销售分布会影响全年结果；按照主流市场预设目标，目标值可能会随着各市场拓版图时权重同步调整，目标值发生动态变化' as  指标定义
	 ,case when QUA = 1 then 0.15 
	  when QUA = 2 then 0.15/3 + 0.45/3 + 0.45/3
	  else 0.45 end as 权重
from t2
union ALL 
-- 货：供给 濮院市场	花花	履约-24配 -- 部门筛选:'6新意法&濮院'
SELECT 
     '货：供给 濮院市场' as 部门 
	 ,'花花' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'履约-24配' as  指标名称
	 ,concat(goods_value_target_s,'%') as S级目标值	
     ,concat(goods_value_target_a,'%') as A级目标值
     ,concat(goods_value_target_b,'%') as  B级目标值
     ,concat(当前实际值,'%') as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'统计周期（专场日期），24小时配货金额/应开单金额 备注：GMV销售分布会影响全年结果；按照主流市场预设目标，目标值可能会随着各市场拓版图时权重同步调整，目标值发生动态变化' as  指标定义
	 ,case when SUBSTR(stat_month,5,2) BETWEEN '01' and '04' THEN  0.15 
	    ELSE  0.45 end as 权重
from t3
UNION ALL 
SELECT 
     '货：供给 濮院市场' as 部门
	 ,'花花' as 负责人
	 ,stat_month  as 季度or月份
	 ,'履约-24配' as  指标名称
	 ,concat(goods_value_target_s,'%') as S级目标值	
     ,concat(goods_value_target_a,'%') as A级目标值
     ,concat(goods_value_target_b,'%') as  B级目标值
     ,concat(当前实际值,'%') as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'统计周期（专场日期），24小时配货金额/应开单金额 备注：GMV销售分布会影响全年结果；按照主流市场预设目标，目标值可能会随着各市场拓版图时权重同步调整，目标值发生动态变化' as  指标定义
	 ,case when QUA = 1 then 0.15 
	  when QUA = 2 then 0.15/3 + 0.45/3 + 0.45/3
	  else 0.45 end as 权重
from t2
union all
-- 场：平台中心	苏苏 履约-24配 : 统计周期（专场日期），24小时配货金额/应开单金额（与浩兵一致）
SELECT 
     '场：平台中心' as 部门 
	 ,'苏苏' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'履约-24配' as  指标名称
	 ,concat(goods_value_target_s,'%') as S级目标值	
     ,concat(goods_value_target_a,'%') as A级目标值
     ,concat(goods_value_target_b,'%') as  B级目标值
     ,concat(当前实际值,'%') as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'统计周期（专场日期），24小时配货金额/应开单金额（与浩兵一致）' as  指标定义
	 ,case when SUBSTR(stat_month,5,2) BETWEEN '01' and '04' THEN  0.00 
	    ELSE  0.30 end as 权重
from t3
UNION ALL 
SELECT 
     '场：平台中心' as 部门
	 ,'苏苏' as 负责人
	 ,stat_month  as 季度or月份
	 ,'履约-24配' as  指标名称
	 ,concat(goods_value_target_s,'%') as S级目标值	
     ,concat(goods_value_target_a,'%') as A级目标值
     ,concat(goods_value_target_b,'%') as  B级目标值
     ,concat(当前实际值,'%') as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'统计周期（专场日期），24小时配货金额/应开单金额（与浩兵一致）' as  指标定义
	 ,case when QUA = 1 then 0.00
	  when QUA = 2 then 0.00/3 + 0.30/3 + 0.30/3
	  else 0.30 end as 权重
from t2
union all
-- ceo办公室 伯牙 履约-24配
SELECT 
     'CEO办公室' as 部门 
	 ,'伯牙' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'履约-24配' as  指标名称
	 ,concat(goods_value_target_s,'%') as S级目标值	
     ,concat(goods_value_target_a,'%') as A级目标值
     ,concat(goods_value_target_b,'%') as  B级目标值
     ,concat(当前实际值,'%') as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'统计周期（专场日期），24小时配货金额/应开单金额（与浩兵一致）' as  指标定义
	 ,case 
	    when SUBSTR(stat_month,5,2) BETWEEN '01' and '04' THEN  0.00 
	    ELSE  0.40 end as 权重
from t3
UNION ALL 
SELECT 
     'CEO办公室' as 部门
	 ,'伯牙' as 负责人
	 ,stat_month  as 季度or月份
	 ,'履约-24配' as  指标名称
	 ,concat(goods_value_target_s,'%') as S级目标值	
     ,concat(goods_value_target_a,'%') as A级目标值
     ,concat(goods_value_target_b,'%') as  B级目标值
     ,concat(当前实际值,'%') as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'统计周期（专场日期），24小时配货金额/应开单金额（与浩兵一致）' as  指标定义
	 ,case 
	    when QUA = 1 then 0.00
	    when QUA = 2 then 0.00/3 + 0.30/3 + 0.30/3
	    else 0.40 
	  end as 权重
from t2
;

-- 平台运营	青田 商家增值利润率 : 指标释义：商家增值利润率=商家增值利润量/大盘GMV，其中商家增值利润量=平台化商家收入(扣除返佣)+买手议价收入+男装付费
with gmv as (
    select 年月
        , sum(p_大盘gmv) 支付gmv
    from yishou_daily.finebi_cwf_ka_gmv_monthly
    group by 年月)
        , earning as (
    select 年月
        , sum(佣金)+sum(退换货服务费)-sum(佣金返还) 平台化收益
    from yishou_daily.finebi_cwf_supplier_earnings_monthly
    group by 年月)
        , bargain as (
    select 年月
        , sum(议价金额) 议价金额
    from yishou_daily.finebi_cwf_maishouyijia_monthly
    group by 年月)
        , adjust as (
    select 年月
        , sum(调整金额) 调整金额
    from yishou_daily.finebi_cwf_supply_earning_adjust
    group by 年月)
        , t1 as (
    select to_date1(a.年月,'yyyymm') as stat_month
        , a.支付gmv
        , b.平台化收益
        , c.议价金额
        , d.调整金额
        ,((b.平台化收益 + c.议价金额  + coalesce(d.调整金额,0.0) )  /a.支付gmv) * 100 as  商家增值利润率
        ,QUARTER(to_date1(a.年月,'yyyymm')) as QUA
    from gmv a
    left join earning b on a.年月=b.年月
    left join bargain c on a.年月=c.年月
    left join adjust d on a.年月=d.年月
    where a.年月>=202201
    )
-- 季度
,t2 as  (
    SELECT   concat(to_char(max(stat_month),'yyyy'),'Q',QUA) as stat_month
            ,avg(商家增值利润率) as 商家增值利润率
            ,case when QUA = 1 then 3.3099
             when QUA = 2 then 3.7388
             when QUA = 3 then 3.7033
             when QUA = 4 then 3.5338
            end  as goods_value_target_s
            ,case when QUA = 1 then 3.2553
             when QUA = 2 then 3.6758
             when QUA = 3 then 3.6418
             when QUA = 4 then 3.474
            end  as goods_value_target_a
            ,case when QUA = 1 then 3.1871
             when QUA = 2 then 3.5977
             when QUA = 3 then 3.5658
             when QUA = 4 then 3.4015
            end  as goods_value_target_b
            FROM t1 
            GROUP BY QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,商家增值利润率
        ,case when  to_char(stat_month,'mm') = '01' then 2.5748
        when  to_char(stat_month,'mm') = '02' then 2.8563
        when  to_char(stat_month,'mm') = '03' then 4.2256
        when  to_char(stat_month,'mm') = '04' then 3.6911
        when  to_char(stat_month,'mm') = '05' then 3.7442
        when  to_char(stat_month,'mm') = '06' then 3.7915
        when  to_char(stat_month,'mm') = '07' then 3.2654
        when  to_char(stat_month,'mm') = '08' then 3.7465
        when  to_char(stat_month,'mm') = '09' then 3.7465
        when  to_char(stat_month,'mm') = '10' then 3.8405
        when  to_char(stat_month,'mm') = '11' then 3.5276
        when  to_char(stat_month,'mm') = '12' then 3.2365
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 2.5328
        when  to_char(stat_month,'mm') = '02' then 2.8100
        when  to_char(stat_month,'mm') = '03' then 4.1551
        when  to_char(stat_month,'mm') = '04' then 3.6293
        when  to_char(stat_month,'mm') = '05' then 3.6812
        when  to_char(stat_month,'mm') = '06' then 3.7271
        when  to_char(stat_month,'mm') = '07' then 3.2116
        when  to_char(stat_month,'mm') = '08' then 3.6843
        when  to_char(stat_month,'mm') = '09' then 3.6843
        when  to_char(stat_month,'mm') = '10' then 3.7758
        when  to_char(stat_month,'mm') = '11' then 3.4677
        when  to_char(stat_month,'mm') = '12' then 3.1819
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 2.4807
        when  to_char(stat_month,'mm') = '02' then 2.7518
        when  to_char(stat_month,'mm') = '03' then 4.067
        when  to_char(stat_month,'mm') = '04' then 3.5524
        when  to_char(stat_month,'mm') = '05' then 3.6031
        when  to_char(stat_month,'mm') = '06' then 3.6477
        when  to_char(stat_month,'mm') = '07' then 3.1453
        when  to_char(stat_month,'mm') = '08' then 3.6071
        when  to_char(stat_month,'mm') = '09' then 3.6071
        when  to_char(stat_month,'mm') = '10' then 3.6969
        when  to_char(stat_month,'mm') = '11' then 3.395
        when  to_char(stat_month,'mm') = '12' then 3.1156
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '平台运营' as 部门
	 ,'青田' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'商家增值利润率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,商家增值利润率 as 当前实际值
     , CASE when t3.商家增值利润率 >= goods_value_target_s then 'S'
            when t3.商家增值利润率 >= goods_value_target_a then 'A'
            when t3.商家增值利润率 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((商家增值利润率/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((商家增值利润率/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((商家增值利润率/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'指标释义：商家增值利润率=商家增值利润量/大盘GMV，其中商家增值利润量=平台化商家收入(扣除返佣)+买手议价收入+男装付费' as  指标定义
	 ,0.20 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '平台运营' as 部门
	 ,'青田' as 负责人
	 ,stat_month  as 季度or月份
	 ,'商家增值利润率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,商家增值利润率 as 当前实际值
     , CASE when t2.商家增值利润率 >= goods_value_target_s then 'S'
            when t2.商家增值利润率 >= goods_value_target_a then 'A'
            when t2.商家增值利润率 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((商家增值利润率/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((商家增值利润率/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((商家增值利润率/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'指标释义：商家增值利润率=商家增值利润量/大盘GMV，其中商家增值利润量=平台化商家收入(扣除返佣)+买手议价收入+男装付费' as  指标定义
	 ,0.20 as 权重
	 from t2
	 
;

-- 产研中心 产品（按各50%权重考核产品部达成） 秀则 月爆款GMV ( 同 青田 -> 月爆款GMV )
-- 相同指标
with t1 as (
SELECT 
	to_char(ym,'yyyy-mm') as stat_month,
    round(sum(cbk_real_gmv),4) as GMV,
    QUARTER(to_char(ym,'yyyy-mm')) as QUA
from cross_origin.finebi_kpi_cbk_lzj_ym
where ym >= '2023-01-01'
GROUP BY ym
)
-- 季度
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(GMV) as GMV
            ,case when QUA = 1 then 439089663.28734 
             when QUA = 2 then 463559589 
             when QUA = 3 then 444069433 
             when QUA = 4 then 874384669 
            end  as goods_value_target_s
            ,case when QUA = 1 then 406351548.38685 
             when QUA = 2 then 430176797 
             when QUA = 3 then 412958915 
             when QUA = 4 then 811729296 
            end  as goods_value_target_a
            ,case when QUA = 1 then 367454778.20805 
             when QUA = 2 then 390514073 
             when QUA = 3 then 375995922 
             when QUA = 4 then 737287270 
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA 
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,GMV
        ,case when  to_char(stat_month,'mm') = '01' then 125126804.73144
        when  to_char(stat_month,'mm') = '02' then 137280848.2368
        when  to_char(stat_month,'mm') = '03' then 176682010.3191 
        when  to_char(stat_month,'mm') = '04' then 178180259
        when  to_char(stat_month,'mm') = '05' then 158166920
        when  to_char(stat_month,'mm') = '06' then 127212410
        when  to_char(stat_month,'mm') = '07' then 83198036
        when  to_char(stat_month,'mm') = '08' then 158487759
        when  to_char(stat_month,'mm') = '09' then 202383638
        when  to_char(stat_month,'mm') = '10' then 262254088
        when  to_char(stat_month,'mm') = '11' then 318220867
        when  to_char(stat_month,'mm') = '12' then 293909713
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 119423823.10785
        when  to_char(stat_month,'mm') = '02' then 123074530.95
        when  to_char(stat_month,'mm') = '03' then 163853194.329 
        when  to_char(stat_month,'mm') = '04' then 165335001
        when  to_char(stat_month,'mm') = '05' then 146780705
        when  to_char(stat_month,'mm') = '06' then 118061090
        when  to_char(stat_month,'mm') = '07' then 77221505
        when  to_char(stat_month,'mm') = '08' then 147363878
        when  to_char(stat_month,'mm') = '09' then 188373530
        when  to_char(stat_month,'mm') = '10' then 244124271
        when  to_char(stat_month,'mm') = '11' then 295442478
        when  to_char(stat_month,'mm') = '12' then 272162547
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 112648003.35705
        when  to_char(stat_month,'mm') = '02' then 106195738.134
        when  to_char(stat_month,'mm') = '03' then 148611036.717 
        when  to_char(stat_month,'mm') = '04' then 150073308
        when  to_char(stat_month,'mm') = '05' then 133252530
        when  to_char(stat_month,'mm') = '06' then 107188234
        when  to_char(stat_month,'mm') = '07' then 70120677
        when  to_char(stat_month,'mm') = '08' then 134147387
        when  to_char(stat_month,'mm') = '09' then 171727857
        when  to_char(stat_month,'mm') = '10' then 222583894
        when  to_char(stat_month,'mm') = '11' then 268379044
        when  to_char(stat_month,'mm') = '12' then 246324331
        end as goods_value_target_b
    from t1
)
 INSERT INTO  yishou_daily.responsible_person_indicators
    SELECT 
     '产研中心 产品（按各50%权重考核产品部达成）' as 部门
	 ,'秀则' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'月爆款GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,GMV as 当前实际值
     , CASE when t3.GMV >= goods_value_target_s then 'S'
            when t3.GMV >= goods_value_target_a then 'A'
            when t3.GMV >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((GMV/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((GMV/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((GMV/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'月爆款定义：	2~7月，月实际gmv>5000	8~1月，月实际gmv>6000	且实际uv价值>大盘的实际uv价值均值*80%的商品	(23年业绩占比52%左右)	（包含未上架专场商品曝光、多渠道曝光不去重）' as  指标定义
	 ,0.2 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '产研中心 产品（按各50%权重考核产品部达成）' as 部门
	 ,'秀则' as 负责人
	 ,stat_month  as 季度or月份
	 ,'月爆款GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,GMV as 当前实际值
     , CASE when t2.GMV >= goods_value_target_s then 'S'
            when t2.GMV >= goods_value_target_a then 'A'
            when t2.GMV >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((GMV/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((GMV/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((GMV/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'月爆款定义：	2~7月，月实际gmv>5000	8~1月，月实际gmv>6000	且实际uv价值>大盘的实际uv价值均值*80%的商品	(23年业绩占比52%左右)	（包含未上架专场商品曝光、多渠道曝光不去重）' as  指标定义
	 ,0.2 as 权重
	 from t2;

-- 产研中心 产品（按各50%权重考核产品部达成） 秀则 5000+用户GMV
with t1 as (
    select
        to_char(stat_date,'yyyymm') as 月份
        ,concat(to_char(stat_date,'yyyy'),'Q',quarter(to_date1(stat_date))) as 季度
        ,case 
            when quarter(to_date1(stat_date)) = 1 then 527874664
            when quarter(to_date1(stat_date)) = 2 then 628259802
            when quarter(to_date1(stat_date)) = 3 then 602026268
            when quarter(to_date1(stat_date)) = 4 then 1011851330
        end  as goods_value_target_s
        ,case 
            when quarter(to_date1(stat_date)) = 1 then 482959816
            when quarter(to_date1(stat_date)) = 2 then 574803565
            when quarter(to_date1(stat_date)) = 3 then 550802142
            when quarter(to_date1(stat_date)) = 4 then 925756747
        end  as goods_value_target_a
        ,case 
            when quarter(to_date1(stat_date)) = 1 then 441781909
            when quarter(to_date1(stat_date)) = 2 then 525794917
            when quarter(to_date1(stat_date)) = 3 then 503839893
            when quarter(to_date1(stat_date)) = 4 then 846825286
        end  as goods_value_target_b
        ,round(sum(a.over_5K_user_buy_amount),4) AS 当月拿货额超5K用户GMV
    from  yishou_daily.finebi_user_operator_KPI_monitor a
    where mt between concat(substr('${bizdate}',1,4),'01') and substr('${bizdate}',1,6)
    GROUP BY 1,2,3,4,5
)
,t2 as  (
    -- 季度
    SELECT   
        季度
        ,sum(当月拿货额超5K用户GMV) as 当前实际值
        ,max(goods_value_target_s) as S级目标值
        ,max(goods_value_target_a) as A级目标值
        ,max(goods_value_target_b) as B级目标值
    from (
        select *,row_number() over (partition by 季度 order by 月份 desc) as rn from t1
    ) tt1
    where rn = 1
    group by 季度
)
-- 月份
,t3 as (
    select
        月份
        ,当月拿货额超5K用户GMV as 当前实际值
        ,case 
            when  substr(月份,5,2) = '01' then 166200724
            when  substr(月份,5,2) = '02' then 146810639
            when  substr(月份,5,2) = '03' then 214863301
            when  substr(月份,5,2) = '04' then 235747864
            when  substr(月份,5,2) = '05' then 216418186
            when  substr(月份,5,2) = '06' then 176093752
            when  substr(月份,5,2) = '07' then 110057991
            when  substr(月份,5,2) = '08' then 220204396
            when  substr(月份,5,2) = '09' then 271763881
            when  substr(月份,5,2) = '10' then 330403849
            when  substr(月份,5,2) = '11' then 364057181
            when  substr(月份,5,2) = '12' then 317390300
        end as S级目标值
        ,case 
            when  substr(月份,5,2) = '01' then 152059336
            when  substr(月份,5,2) = '02' then 134319080
            when  substr(月份,5,2) = '03' then 196581400
            when  substr(月份,5,2) = '04' then 215688974
            when  substr(月份,5,2) = '05' then 198003986
            when  substr(月份,5,2) = '06' then 161110605
            when  substr(月份,5,2) = '07' then 100693575
            when  substr(月份,5,2) = '08' then 201468041
            when  substr(月份,5,2) = '09' then 248640526
            when  substr(月份,5,2) = '10' then 302291042
            when  substr(月份,5,2) = '11' then 333080939
            when  substr(月份,5,2) = '12' then 290384766
        end as A级目标值,
        case 
            when  substr(月份,5,2) = '01' then 139094520
            when  substr(月份,5,2) = '02' then 122866826
            when  substr(月份,5,2) = '03' then 179820563
            when  substr(月份,5,2) = '04' then 197298996
            when  substr(月份,5,2) = '05' then 181121857
            when  substr(月份,5,2) = '06' then 147374064
            when  substr(月份,5,2) = '07' then 92108284
            when  substr(月份,5,2) = '08' then 184290562
            when  substr(月份,5,2) = '09' then 227441047
            when  substr(月份,5,2) = '10' then 276517237
            when  substr(月份,5,2) = '11' then 304681940
            when  substr(月份,5,2) = '12' then 265626109
        end as B级目标值
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '产研中心 产品（按各50%权重考核产品部达成）' as 部门
	 ,'秀则' as 负责人
	 ,月份 as 季度or月份
	 ,'5000+用户GMV' as  指标名称
	 ,t3.S级目标值 as S级目标值	
     ,t3.A级目标值 as A级目标值
     ,t3.B级目标值 as  B级目标值
     ,t3.当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= S级目标值 then 'S'
            when t3.当前实际值 >= A级目标值 then 'A'
            when t3.当前实际值 >= B级目标值 then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((t3.当前实际值/S级目标值) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((t3.当前实际值/A级目标值) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((t3.当前实际值/B级目标值) * 100,2)),'%') as B级达成率
	 ,'当月实际支付5000+的用户GMV' as  指标定义
	 ,0.30 as 权重
from t3
UNION ALL 
SELECT 
     '产研中心 产品（按各50%权重考核产品部达成）' as 部门
	 ,'秀则' as 负责人
	 ,季度  as 季度or月份
	 ,'5000+用户GMV' as  指标名称
	 ,t2.S级目标值 as S级目标值	
     ,t2.S级目标值 as A级目标值
     ,t2.S级目标值 as  B级目标值
     ,t2.当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= S级目标值 then 'S'
            when t2.当前实际值 >= A级目标值 then 'A'
            when t2.当前实际值 >= B级目标值 then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/S级目标值) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/A级目标值) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/B级目标值) * 100,2)),'%') as B级达成率
	 ,'当月实际支付5000+的用户GMV' as  指标定义
	 ,0.30 as 权重
from t2
;

-- 老表部（数据）  大盘实际GMV (同 平台运营 青田 大盘实际GMV)
WITH t1 (
    select to_char(special_date, 'yyyy-mm')          as stat_month,
       sum(real_buy_amount)                        GMV,
       QUARTER(to_char(special_date, 'yyyy-mm')) as QUA
    from yishou_data.dwm_goods_sp_up_info_dt
    where dt >= '20230101'
    GROUP BY to_char(special_date, 'yyyy-mm')
)
-- 季度
,t2 as  (
    SELECT   concat(to_char(max(stat_month),'yyyy'),'Q',QUA) as stat_month
            ,sum(GMV) as GMV
            ,case when QUA = 1 then 844111350 
             when QUA = 2 then 959832931 
             when QUA = 3 then 871137054 
             when QUA = 4 then 1421482117 
            end  as goods_value_target_s
            ,case when QUA = 1 then 781584582 
             when QUA = 2 then 888734196 
             when QUA = 3 then 806608383 
             when QUA = 4 then 1316187147 
            end  as goods_value_target_a
            ,case when QUA = 1 then 719057816 
             when QUA = 2 then 817635459 
             when QUA = 3 then 742079712 
             when QUA = 4 then 1210892175 
            end  as goods_value_target_b
            FROM t1 
            GROUP BY QUA 
)
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,GMV
        ,case when  to_char(stat_month,'mm') = '01' then 294712995
        when  to_char(stat_month,'mm') = '02' then 214185243
        when  to_char(stat_month,'mm') = '03' then 335213112 
        when  to_char(stat_month,'mm') = '04' then 348762122
        when  to_char(stat_month,'mm') = '05' then 329895871
        when  to_char(stat_month,'mm') = '06' then 281174938
        when  to_char(stat_month,'mm') = '07' then 194498030
        when  to_char(stat_month,'mm') = '08' then 301573423
        when  to_char(stat_month,'mm') = '09' then 375065601
        when  to_char(stat_month,'mm') = '10' then 457716347
        when  to_char(stat_month,'mm') = '11' then 501956322
        when  to_char(stat_month,'mm') = '12' then 461809448
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 272882402
        when  to_char(stat_month,'mm') = '02' then 198319669
        when  to_char(stat_month,'mm') = '03' then 310382511 
        when  to_char(stat_month,'mm') = '04' then 322927891
        when  to_char(stat_month,'mm') = '05' then 305459140
        when  to_char(stat_month,'mm') = '06' then 260347165
        when  to_char(stat_month,'mm') = '07' then 180090768
        when  to_char(stat_month,'mm') = '08' then 279234651
        when  to_char(stat_month,'mm') = '09' then 347282964
        when  to_char(stat_month,'mm') = '10' then 423811433
        when  to_char(stat_month,'mm') = '11' then 464774373
        when  to_char(stat_month,'mm') = '12' then 427601341
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 251051810
        when  to_char(stat_month,'mm') = '02' then 182454096
        when  to_char(stat_month,'mm') = '03' then 285551910 
        when  to_char(stat_month,'mm') = '04' then 297093659
        when  to_char(stat_month,'mm') = '05' then 281022408
        when  to_char(stat_month,'mm') = '06' then 239519392
        when  to_char(stat_month,'mm') = '07' then 165683507
        when  to_char(stat_month,'mm') = '08' then 256895879
        when  to_char(stat_month,'mm') = '09' then 319500326
        when  to_char(stat_month,'mm') = '10' then 389906518
        when  to_char(stat_month,'mm') = '11' then 427592423
        when  to_char(stat_month,'mm') = '12' then 393393234
        end as goods_value_target_b
    from t1
)
 INSERT INTO  yishou_daily.responsible_person_indicators
    SELECT 
     '老表部（数据）' as 部门
	 ,'暂无' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'大盘实际GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,GMV as 当前实际值
     , CASE when t3.GMV >= goods_value_target_s then 'S'
            when t3.GMV >= goods_value_target_a then 'A'
            when t3.GMV >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((GMV/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((GMV/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((GMV/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'指标释义：大盘实际GMV，不含专场结束前取消' as  指标定义
	 ,0.40 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '老表部（数据）' as 部门
	 ,'暂无' as 负责人
	 ,stat_month  as 季度or月份
	 ,'大盘实际GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,GMV as 当前实际值
     , CASE when t2.GMV >= goods_value_target_s then 'S'
            when t2.GMV >= goods_value_target_a then 'A'
            when t2.GMV >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((GMV/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((GMV/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((GMV/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'指标释义：大盘实际GMV，不含专场结束前取消' as  指标定义
	 ,0.40 as 权重
	 from t2;


-- 老表部（数据） 数据平台故障次数与等级
-- 只能线下统计


-- 老表部（数据） 协同方满意度
-- 只能线下统计

-- 旺财部（财务）	皮特 净利润 
-->不做上去了，内部月会使用就行

	

-- 旺财部（财务）	皮特 大盘实际GMV  (同 平台运营 青田 大盘实际GMV)
WITH t1 (
    select to_char(special_date, 'yyyy-mm')          as stat_month,
       sum(real_buy_amount)                        GMV,
       QUARTER(to_char(special_date, 'yyyy-mm')) as QUA
    from yishou_data.dwm_goods_sp_up_info_dt
    where dt >= '20230101'
    GROUP BY to_char(special_date, 'yyyy-mm')
)
-- 季度
,t2 as  (
    SELECT   concat(to_char(max(stat_month),'yyyy'),'Q',QUA) as stat_month
            ,sum(GMV) as GMV
            ,case when QUA = 1 then 844111350 
             when QUA = 2 then 959832931 
             when QUA = 3 then 871137054 
             when QUA = 4 then 1421482117 
            end  as goods_value_target_s
            ,case when QUA = 1 then 781584582 
             when QUA = 2 then 888734196 
             when QUA = 3 then 806608383 
             when QUA = 4 then 1316187147 
            end  as goods_value_target_a
            ,case when QUA = 1 then 719057816 
             when QUA = 2 then 817635459 
             when QUA = 3 then 742079712 
             when QUA = 4 then 1210892175 
            end  as goods_value_target_b
            FROM t1 
            GROUP BY QUA 
)
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,GMV
        ,case when  to_char(stat_month,'mm') = '01' then 294712995
        when  to_char(stat_month,'mm') = '02' then 214185243
        when  to_char(stat_month,'mm') = '03' then 335213112 
        when  to_char(stat_month,'mm') = '04' then 348762122
        when  to_char(stat_month,'mm') = '05' then 329895871
        when  to_char(stat_month,'mm') = '06' then 281174938
        when  to_char(stat_month,'mm') = '07' then 194498030
        when  to_char(stat_month,'mm') = '08' then 301573423
        when  to_char(stat_month,'mm') = '09' then 375065601
        when  to_char(stat_month,'mm') = '10' then 457716347
        when  to_char(stat_month,'mm') = '11' then 501956322
        when  to_char(stat_month,'mm') = '12' then 461809448
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 272882402
        when  to_char(stat_month,'mm') = '02' then 198319669
        when  to_char(stat_month,'mm') = '03' then 310382511 
        when  to_char(stat_month,'mm') = '04' then 322927891
        when  to_char(stat_month,'mm') = '05' then 305459140
        when  to_char(stat_month,'mm') = '06' then 260347165
        when  to_char(stat_month,'mm') = '07' then 180090768
        when  to_char(stat_month,'mm') = '08' then 279234651
        when  to_char(stat_month,'mm') = '09' then 347282964
        when  to_char(stat_month,'mm') = '10' then 423811433
        when  to_char(stat_month,'mm') = '11' then 464774373
        when  to_char(stat_month,'mm') = '12' then 427601341
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 251051810
        when  to_char(stat_month,'mm') = '02' then 182454096
        when  to_char(stat_month,'mm') = '03' then 285551910 
        when  to_char(stat_month,'mm') = '04' then 297093659
        when  to_char(stat_month,'mm') = '05' then 281022408
        when  to_char(stat_month,'mm') = '06' then 239519392
        when  to_char(stat_month,'mm') = '07' then 165683507
        when  to_char(stat_month,'mm') = '08' then 256895879
        when  to_char(stat_month,'mm') = '09' then 319500326
        when  to_char(stat_month,'mm') = '10' then 389906518
        when  to_char(stat_month,'mm') = '11' then 427592423
        when  to_char(stat_month,'mm') = '12' then 393393234
        end as goods_value_target_b
    from t1
)
 INSERT INTO  yishou_daily.responsible_person_indicators
    SELECT 
     '旺财部（财务）' as 部门
	 ,'皮特' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'大盘实际GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,GMV as 当前实际值
     , CASE when t3.GMV >= goods_value_target_s then 'S'
            when t3.GMV >= goods_value_target_a then 'A'
            when t3.GMV >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((GMV/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((GMV/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((GMV/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'指标释义：大盘实际GMV，不含专场结束前取消' as  指标定义
	 ,0.40 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '旺财部（财务）' as 部门
	 ,'皮特' as 负责人
	 ,stat_month  as 季度or月份
	 ,'大盘实际GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,GMV as 当前实际值
     , CASE when t2.GMV >= goods_value_target_s then 'S'
            when t2.GMV >= goods_value_target_a then 'A'
            when t2.GMV >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((GMV/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((GMV/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((GMV/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'指标释义：大盘实际GMV，不含专场结束前取消' as  指标定义
	 ,0.40 as 权重
	 from t2;





-- 雁门关（仓储）	华生 仓储费用率
-- 只能线下统计


-- 雁门关（仓储）	华生 到仓-发货时效
with t1 as (
    select  to_char(date_time,'yyyy-mm') as stat_month,
            if(sum(a.sign_num)=0,0,sum(a.dc_sign_time_15)/sum(a.sign_num)) as 到仓签收时效,
            QUARTER(to_char(date_time,'yyyy-mm')) as QUA
    from yishou_daily.finebi_indicator_disassembly_kanban_cckpi2023_1 a where dt BETWEEN  '20230101' and to_char(current_date(),'yyyymmdd')
    GROUP BY to_char(date_time,'yyyy-mm')
)
,t2 as  (
    SELECT   concat(to_char(max(stat_month),'yyyy'),'Q',QUA) as stat_month
            ,avg(到仓签收时效) as 当前实际值
            ,case when QUA = 1 then 66.66
             when QUA = 2 then 54.65
             when QUA = 3 then 57.02
             when QUA = 4 then 59.09
            end  as goods_value_target_s
            ,case when QUA = 1 then 68.95
             when QUA = 2 then 56.56
             when QUA = 3 then 59.03
             when QUA = 4 then 61.18
            end  as goods_value_target_a
            ,case when QUA = 1 then 71.24
             when QUA = 2 then 58.47
             when QUA = 3 then 61.04
             when QUA = 4 then 63.26
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,到仓签收时效 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 65.13
        when  to_char(stat_month,'mm') = '02' then 80.2
        when  to_char(stat_month,'mm') = '03' then 56.86
        when  to_char(stat_month,'mm') = '04' then 54.48
        when  to_char(stat_month,'mm') = '05' then 55.11
        when  to_char(stat_month,'mm') = '06' then 54.33
        when  to_char(stat_month,'mm') = '07' then 54.20
        when  to_char(stat_month,'mm') = '08' then 57.95
        when  to_char(stat_month,'mm') = '09' then 58.26
        when  to_char(stat_month,'mm') = '10' then 58.72
        when  to_char(stat_month,'mm') = '11' then 59.00
        when  to_char(stat_month,'mm') = '12' then 59.59
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 67.36
        when  to_char(stat_month,'mm') = '02' then 82.95
        when  to_char(stat_month,'mm') = '03' then 58.82
        when  to_char(stat_month,'mm') = '04' then 56.37
        when  to_char(stat_month,'mm') = '05' then 57.03
        when  to_char(stat_month,'mm') = '06' then 56.24
        when  to_char(stat_month,'mm') = '07' then 56.11
        when  to_char(stat_month,'mm') = '08' then 60.00
        when  to_char(stat_month,'mm') = '09' then 60.32
        when  to_char(stat_month,'mm') = '10' then 60.79
        when  to_char(stat_month,'mm') = '11' then 61.08
        when  to_char(stat_month,'mm') = '12' then 61.69
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 69.58
        when  to_char(stat_month,'mm') = '02' then 85.7
        when  to_char(stat_month,'mm') = '03' then 60.78
        when  to_char(stat_month,'mm') = '04' then 58.26
        when  to_char(stat_month,'mm') = '05' then 58.96
        when  to_char(stat_month,'mm') = '06' then 58.15
        when  to_char(stat_month,'mm') = '07' then 58.03
        when  to_char(stat_month,'mm') = '08' then 62.05
        when  to_char(stat_month,'mm') = '09' then 62.37
        when  to_char(stat_month,'mm') = '10' then 62.86
        when  to_char(stat_month,'mm') = '11' then 63.17
        when  to_char(stat_month,'mm') = '12' then 63.79
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '雁门关（仓储）' as 部门
	 ,'华生' as 负责人
	 ,stat_month as 季度or月份
	 ,'到仓-发货时效' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 < goods_value_target_s then 'S'
            when t3.当前实际值 < goods_value_target_a then 'A'
            when t3.当前实际值 < goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/当前实际值) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/当前实际值) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/当前实际值) * 100,2)),'%') as B级达成率
	 ,'15点到仓至用户签收时间/总签收件数' as  指标定义
	 ,0.30 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '雁门关（仓储）' as 部门
	 ,'华生' as 负责人
	 ,stat_month  as 季度or月份
	 ,'到仓-发货时效' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 < goods_value_target_s then 'S'
            when t2.当前实际值 < goods_value_target_a then 'A'
            when t2.当前实际值 < goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/当前实际值) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/当前实际值) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/当前实际值) * 100,2)),'%') as B级达成率
	 ,'15点到仓至用户签收时间/总签收件数' as  指标定义
	 ,0.30 as 权重
	 from t2
;


-- 雁门关（仓储）	华生 次品亏损率
-- 只能线下统计


-- 雁门关（仓储）华生 7天申请次品率
WITH tmp1 as (
SELECT 
    *,case when sign_time is null then dateadd(work_add_time,-7,'hh') else sign_time end sign_time1
from yishou_daily.finebi_delivery_ck_work_rk_dbid_date_dt
where dt >= 20240101
)
,tmp2 as (

SELECT to_date1(dt,'yyyymmdd') as stat_date,count(distinct case when sign_time is not null and is_sign_7_work_cp  = '是' then ck_dbid end)/count(distinct case when sign_time is not null then ck_dbid end) as 次品率
    ,count(distinct case when sign_time is not null and is_sign_7_work_cp  = '是' then ck_dbid end) as sign_7_apply_cp_num
    ,count(distinct case when sign_time is not null then ck_dbid end) as sign_num
FROM tmp1
GROUP BY dt
)
,t1 as (
SELECT  to_char(stat_date,'yyyy-mm') as stat_month
        ,sum(sign_7_apply_cp_num) as sign_7_apply_cp_num
        ,sum(sign_num) as sign_num
        ,round(sum(sign_7_apply_cp_num),4)/round(sum(sign_num),4) as 7天申请次品率
        ,quarter(to_char(stat_date,'yyyy-mm')) as QUA
from tmp2
GROUP BY  to_char(stat_date,'yyyy-mm')
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA)  as stat_month
            ,(sum(sign_7_apply_cp_num))/(sum(sign_num)) * 100 as 当前实际值
            ,case when QUA = 1 then 2.91575089803883
             when QUA = 2 then 2.98179769700782
             when QUA = 3 then 3.10824620998459
             when QUA = 4 then 3.10291395247525
            end  as goods_value_target_s
            ,case when QUA = 1 then 3.27658135941399
             when QUA = 2 then 3.35080159218363
             when QUA = 3 then 3.49289838132429
             when QUA = 4 then 3.4869062454493
            end  as goods_value_target_a
            ,case when QUA = 1 then 3.70525791007203
             when QUA = 2 then 3.78918840786571
             when QUA = 3 then 3.94987578113863
             when QUA = 4 then 3.94309969154597
            end  as goods_value_target_b
            FROM t1 
            GROUP by to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,7天申请次品率 * 100 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 2.87226720410263
        when  to_char(stat_month,'mm') = '02' then 2.43709904961804
        when  to_char(stat_month,'mm') = '03' then 3.09024474260576
        when  to_char(stat_month,'mm') = '04' then 2.85967644375588
        when  to_char(stat_month,'mm') = '05' then 3.00604403278686
        when  to_char(stat_month,'mm') = '06' then 3.07922782730235
        when  to_char(stat_month,'mm') = '07' then 3.36645454771262
        when  to_char(stat_month,'mm') = '08' then 3.07371936965065
        when  to_char(stat_month,'mm') = '09' then 2.92735178061967
        when  to_char(stat_month,'mm') = '10' then 2.79571506260985
        when  to_char(stat_month,'mm') = '11' then 3.12473425917102
        when  to_char(stat_month,'mm') = '12' then 3.45514545374777
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 3.2277164645817
        when  to_char(stat_month,'mm') = '02' then 2.73869531255055
        when  to_char(stat_month,'mm') = '03' then 3.47266919353763
        when  to_char(stat_month,'mm') = '04' then 3.21356757049038
        when  to_char(stat_month,'mm') = '05' then 3.37804846430194
        when  to_char(stat_month,'mm') = '06' then 3.46028891120772
        when  to_char(stat_month,'mm') = '07' then 3.78306055766589
        when  to_char(stat_month,'mm') = '08' then 3.45409877004277
        when  to_char(stat_month,'mm') = '09' then 3.28961787623121
        when  to_char(stat_month,'mm') = '10' then 3.14169083049644
        when  to_char(stat_month,'mm') = '11' then 3.5114268621536
        when  to_char(stat_month,'mm') = '12' then 3.88272715458258
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 3.65
        when  to_char(stat_month,'mm') = '02' then 3.097
        when  to_char(stat_month,'mm') = '03' then 3.927
        when  to_char(stat_month,'mm') = '04' then 3.634
        when  to_char(stat_month,'mm') = '05' then 3.913
        when  to_char(stat_month,'mm') = '06' then 4.278
        when  to_char(stat_month,'mm') = '07' then 4.278
        when  to_char(stat_month,'mm') = '08' then 3.906
        when  to_char(stat_month,'mm') = '09' then 3.72
        when  to_char(stat_month,'mm') = '10' then 3.55271959515133
        when  to_char(stat_month,'mm') = '11' then 3.97082835109609
        when  to_char(stat_month,'mm') = '12' then 4.39070602072325
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '雁门关（仓储）' as 部门
	 ,'华生' as 负责人
	 ,stat_month as 季度or月份
	 ,'7天申请次品率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE 
        when (goods_value_target_s = goods_value_target_a and goods_value_target_a = goods_value_target_b) then 
            case    WHEN  t3.当前实际值 >= goods_value_target_b then 'C' 
                    ELSE 'B' 
                    end 
            when t3.当前实际值 < goods_value_target_s then 'S'
            when t3.当前实际值 < goods_value_target_a then 'A'
            when t3.当前实际值 < goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/当前实际值) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/当前实际值) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/当前实际值) * 100,2)),'%') as B级达成率
	 ,'7天申请次品率=7天前签收总件数产生的次品工单件数/7天前的签收总件数 次品工单类型：质量问题+实物不符）' as  指标定义
	 ,0.10 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '雁门关（仓储）' as 部门
	 ,'华生' as 负责人
	 ,stat_month  as 季度or月份
	 ,'7天申请次品率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 < goods_value_target_s then 'S'
            when t2.当前实际值 < goods_value_target_a then 'A'
            when t2.当前实际值 < goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/当前实际值) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/当前实际值) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/当前实际值) * 100,2)),'%') as B级达成率
	 ,'7天申请次品率=7天前签收总件数产生的次品工单件数/7天前的签收总件数 次品工单类型：质量问题+实物不符）' as  指标定义
	 ,0.10 as 权重
	 from t2
;


-- CEO办公室 次品项目	维尼 7天申请次品率 -- sign_7_apply_cp_num/sign_num
WITH tmp1 as (
SELECT 
    *,case when sign_time is null then dateadd(work_add_time,-7,'hh') else sign_time end sign_time1
from yishou_daily.finebi_delivery_ck_work_rk_dbid_date_dt
where dt >= 20240101
)
,tmp2 as (

SELECT to_date1(dt,'yyyymmdd') as stat_date,count(distinct case when sign_time is not null and is_sign_7_work_cp  = '是' then ck_dbid end)/count(distinct case when sign_time is not null then ck_dbid end) as 次品率
    ,count(distinct case when sign_time is not null and is_sign_7_work_cp  = '是' then ck_dbid end) as sign_7_apply_cp_num
    ,count(distinct case when sign_time is not null then ck_dbid end) as sign_num

FROM tmp1
GROUP BY dt
)
,t1 as (
SELECT  to_char(stat_date,'yyyy-mm') as stat_month
        ,sum(sign_7_apply_cp_num) as sign_7_apply_cp_num
        ,sum(sign_num) as sign_num
        ,round(sum(sign_7_apply_cp_num),4)/round(sum(sign_num),4) as 7天申请次品率
        ,quarter(to_char(stat_date,'yyyy-mm')) as QUA
from tmp2
GROUP BY  to_char(stat_date,'yyyy-mm')
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA)  as stat_month
            ,(sum(sign_7_apply_cp_num))/(sum(sign_num)) * 100 as 当前实际值
            ,case when QUA = 1 then 2.91575089803883
             when QUA = 2 then 2.98179769700782
             when QUA = 3 then 3.10824620998459
             when QUA = 4 then 3.10291395247525
            end  as goods_value_target_s
            ,case when QUA = 1 then 3.27658135941399
             when QUA = 2 then 3.35080159218363
             when QUA = 3 then 3.49289838132429
             when QUA = 4 then 3.4869062454493
            end  as goods_value_target_a
            ,case when QUA = 1 then 3.70525791007203
             when QUA = 2 then 3.78918840786571
             when QUA = 3 then 3.94987578113863
             when QUA = 4 then 3.94309969154597
            end  as goods_value_target_b
            FROM t1 
            GROUP by to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,7天申请次品率 * 100 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 2.87226720410263
        when  to_char(stat_month,'mm') = '02' then 2.43709904961804
        when  to_char(stat_month,'mm') = '03' then 3.09024474260576
        when  to_char(stat_month,'mm') = '04' then 2.85967644375588
        when  to_char(stat_month,'mm') = '05' then 3.00604403278686
        when  to_char(stat_month,'mm') = '06' then 3.07922782730235
        when  to_char(stat_month,'mm') = '07' then 3.36645454771262
        when  to_char(stat_month,'mm') = '08' then 3.07371936965065
        when  to_char(stat_month,'mm') = '09' then 2.92735178061967
        when  to_char(stat_month,'mm') = '10' then 2.79571506260985
        when  to_char(stat_month,'mm') = '11' then 3.12473425917102
        when  to_char(stat_month,'mm') = '12' then 3.45514545374777
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 3.2277164645817
        when  to_char(stat_month,'mm') = '02' then 2.73869531255055
        when  to_char(stat_month,'mm') = '03' then 3.47266919353763
        when  to_char(stat_month,'mm') = '04' then 3.21356757049038
        when  to_char(stat_month,'mm') = '05' then 3.37804846430194
        when  to_char(stat_month,'mm') = '06' then 3.46028891120772
        when  to_char(stat_month,'mm') = '07' then 3.78306055766589
        when  to_char(stat_month,'mm') = '08' then 3.45409877004277
        when  to_char(stat_month,'mm') = '09' then 3.28961787623121
        when  to_char(stat_month,'mm') = '10' then 3.14169083049644
        when  to_char(stat_month,'mm') = '11' then 3.5114268621536
        when  to_char(stat_month,'mm') = '12' then 3.88272715458258
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 3.65
        when  to_char(stat_month,'mm') = '02' then 3.097
        when  to_char(stat_month,'mm') = '03' then 3.927
        when  to_char(stat_month,'mm') = '04' then 3.634
        when  to_char(stat_month,'mm') = '05' then 3.913
        when  to_char(stat_month,'mm') = '06' then 4.278
        when  to_char(stat_month,'mm') = '07' then 4.278
        when  to_char(stat_month,'mm') = '08' then 3.906
        when  to_char(stat_month,'mm') = '09' then 3.72
        when  to_char(stat_month,'mm') = '10' then 3.55271959515133
        when  to_char(stat_month,'mm') = '11' then 3.97082835109609
        when  to_char(stat_month,'mm') = '12' then 4.39070602072325
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     'CEO办公室 次品项目' as 部门
	 ,'维尼' as 负责人
	 ,stat_month as 季度or月份
	 ,'7天申请次品率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE 
        when (goods_value_target_s = goods_value_target_a and goods_value_target_a = goods_value_target_b) then 
            case    WHEN  t3.当前实际值 >= goods_value_target_b then 'C' 
                    ELSE 'B' 
                    end 
            when t3.当前实际值 < goods_value_target_s then 'S'
            when t3.当前实际值 < goods_value_target_a then 'A'
            when t3.当前实际值 < goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/当前实际值) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/当前实际值) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/当前实际值) * 100,2)),'%') as B级达成率
	 ,'7天申请次品率=7天前签收总件数产生的次品工单件数/7天前的签收总件数 次品工单类型：质量问题+实物不符）' as  指标定义
	 ,0.50 as 权重
	 from t3
    UNION ALL 
    SELECT 
     'CEO办公室 次品项目' as 部门
	 ,'维尼' as 负责人
	 ,stat_month  as 季度or月份
	 ,'7天申请次品率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 < goods_value_target_s then 'S'
            when t2.当前实际值 < goods_value_target_a then 'A'
            when t2.当前实际值 < goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/当前实际值) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/当前实际值) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/当前实际值) * 100,2)),'%') as B级达成率
	 ,'7天申请次品率=7天前签收总件数产生的次品工单件数/7天前的签收总件数 次品工单类型：质量问题+实物不符）' as  指标定义
	 ,0.50 as 权重
	 from t2
;


-- CEO办公室 次品项目	维尼 次品亏损率
-- 只能线下统计

-- 人：用户运营 子木 :  5000+用户GMV (同 产研中心 产品（按各50%权重考核产品部达成） 秀则 )
WITH t1 as (
select
   to_char(stat_date,'yyyy-mm')  as    stat_month
  ,round(sum(a.over_5K_user_buy_amount),4) as 当月拿货额超5K用户GMV
  ,QUARTER(to_char(stat_date,'yyyy-mm')) as QUA
from  yishou_daily.finebi_user_operator_KPI_monitor a
where mt >= '2023-01-01'
GROUP BY to_char(stat_date,'yyyy-mm')
)
,t2 as  (
    SELECT   concat(to_char(max(stat_month),'yyyy'),'Q',QUA) as stat_month
            ,sum(当月拿货额超5K用户GMV) as 当前实际值
            ,case when QUA = 1 then 527874664
             when QUA = 2 then 628259802
             when QUA = 3 then 602026268
             when QUA = 4 then 1011851330
            end  as goods_value_target_s
            ,case when QUA = 1 then 482959816
             when QUA = 2 then 574803565
             when QUA = 3 then 550802142
             when QUA = 4 then 925756747
            end  as goods_value_target_a
            ,case when QUA = 1 then 441781909
             when QUA = 2 then 525794917
             when QUA = 3 then 503839893
             when QUA = 4 then 846825286
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,当月拿货额超5K用户GMV as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 166200724
        when  to_char(stat_month,'mm') = '02' then 146810639
        when  to_char(stat_month,'mm') = '03' then 214863301
        when  to_char(stat_month,'mm') = '04' then 235747864
        when  to_char(stat_month,'mm') = '05' then 216418186
        when  to_char(stat_month,'mm') = '06' then 176093752
        when  to_char(stat_month,'mm') = '07' then 110057991
        when  to_char(stat_month,'mm') = '08' then 220204396
        when  to_char(stat_month,'mm') = '09' then 271763881
        when  to_char(stat_month,'mm') = '10' then 330403849
        when  to_char(stat_month,'mm') = '11' then 364057181
        when  to_char(stat_month,'mm') = '12' then 317390300
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 152059336
        when  to_char(stat_month,'mm') = '02' then 134319080
        when  to_char(stat_month,'mm') = '03' then 196581400
        when  to_char(stat_month,'mm') = '04' then 215688974
        when  to_char(stat_month,'mm') = '05' then 198003986
        when  to_char(stat_month,'mm') = '06' then 161110605
        when  to_char(stat_month,'mm') = '07' then 100693575
        when  to_char(stat_month,'mm') = '08' then 201468041
        when  to_char(stat_month,'mm') = '09' then 248640526
        when  to_char(stat_month,'mm') = '10' then 302291042
        when  to_char(stat_month,'mm') = '11' then 333080939
        when  to_char(stat_month,'mm') = '12' then 290384766
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 139094520
        when  to_char(stat_month,'mm') = '02' then 122866826
        when  to_char(stat_month,'mm') = '03' then 179820563
        when  to_char(stat_month,'mm') = '04' then 197298996
        when  to_char(stat_month,'mm') = '05' then 181121857
        when  to_char(stat_month,'mm') = '06' then 147374064
        when  to_char(stat_month,'mm') = '07' then 92108284
        when  to_char(stat_month,'mm') = '08' then 184290562
        when  to_char(stat_month,'mm') = '09' then 227441047
        when  to_char(stat_month,'mm') = '10' then 276517237
        when  to_char(stat_month,'mm') = '11' then 304681940
        when  to_char(stat_month,'mm') = '12' then 265626109
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '人：用户运营' as 部门
	 ,'子木' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'5000+用户GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'当月实际支付5000+的用户GMV' as  指标定义
	 ,0.30 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '人：用户运营' as 部门
	 ,'子木' as 负责人
	 ,stat_month  as 季度or月份
	 ,'5000+用户GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'当月实际支付5000+的用户GMV' as  指标定义
	 ,0.30 as 权重
	 from t2
;


-- 人：用户运营 子木 :  B端用户的次月复购率  -> 近6个月有下单且当月也下单的B端用户/近6个月有下单的B端用户
with t1 as (
    SELECT  to_char(月份,'yyyy-mm') as stat_month
            -- ,avg(coalesce(result,0.0)) as result 
            -- ,sum(近6个月有支付用户数) as 近6个月有支付用户数
            -- ,max(近6个月有支付用户数) 
            ,(sum(近6个月有支付复购用户数) / sum(近6个月有支付用户数)) * 100  as B端用户次月复购率
            ,QUARTER(to_char(月份,'yyyy-mm')) as QUA
    from ( 
    select
      a.mon_time                月份
    --   ,a.user_level_in6m         近6个月实际拿货额峰值层级
    --   ,a.user_level_last_month   上个月实际拿货额层级
    --   ,a.province_zone           片区
    --   ,a.province_name           省份
    --   ,case when a.is_private_user =1 then '私域' else '非私域' end       私域用户
      ,case when a.is_b_port=1 then 'B端' else '其他' end                 B端用户
      ,max(a.pay_user_num_in6m)   近6个月有支付用户数
      ,sum(a.repeat_pay_user_num) 近6个月有支付复购用户数
    from  yishou_daily.finebi_user_operator_KPI_monitor a
    where mt >= '202301' -- or mt = '2024-04'
    group by a.mon_time,a.user_level_in6m,a.user_level_last_month,a.province_zone,a.province_name,case when a.is_private_user =1 then '私域' else '非私域' end,case when a.is_b_port=1 then 'B端' else '其他' end 
    )
    WHERE B端用户 = 'B端'
    GROUP BY to_char(月份,'yyyy-mm')
)
,t2 as  (
    SELECT   concat(to_char(max(stat_month),'yyyy'),'Q',QUA) as stat_month
            ,avg(B端用户次月复购率) as 当前实际值
            ,case when QUA = 1 then 36.6441
             when QUA = 2 then 41.2317
             when QUA = 3 then 37.1446
             when QUA = 4 then 46.9056
            end  as goods_value_target_s
            ,case when QUA = 1 then 34.6441
             when QUA = 2 then 39.2317
             when QUA = 3 then 35.144
             when QUA = 4 then 44.9056
            end  as goods_value_target_a
            ,case when QUA = 1 then 32.6441
             when QUA = 2 then 37.2317
             when QUA = 3 then 33.1446
             when QUA = 4 then 42.9056
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,B端用户次月复购率 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 31.1824
        when  to_char(stat_month,'mm') = '02' then 36.0832
        when  to_char(stat_month,'mm') = '03' then 42.698
        when  to_char(stat_month,'mm') = '04' then 41.7618
        when  to_char(stat_month,'mm') = '05' then 41.6633
        when  to_char(stat_month,'mm') = '06' then 40.2719
        when  to_char(stat_month,'mm') = '07' then 37.2777
        when  to_char(stat_month,'mm') = '08' then 34.4571
        when  to_char(stat_month,'mm') = '09' then 39.7034
        when  to_char(stat_month,'mm') = '10' then 44.8194
        when  to_char(stat_month,'mm') = '11' then 47.6929
        when  to_char(stat_month,'mm') = '12' then 48.0905
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 29.1824
        when  to_char(stat_month,'mm') = '02' then 34.0832
        when  to_char(stat_month,'mm') = '03' then 40.698
        when  to_char(stat_month,'mm') = '04' then 39.7618
        when  to_char(stat_month,'mm') = '05' then 39.6633
        when  to_char(stat_month,'mm') = '06' then 38.2719
        when  to_char(stat_month,'mm') = '07' then 35.2777
        when  to_char(stat_month,'mm') = '08' then 32.4571
        when  to_char(stat_month,'mm') = '09' then 37.7034
        when  to_char(stat_month,'mm') = '10' then 42.8194
        when  to_char(stat_month,'mm') = '11' then 45.6929
        when  to_char(stat_month,'mm') = '12' then 46.0905
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 27.1824
        when  to_char(stat_month,'mm') = '02' then 32.0832
        when  to_char(stat_month,'mm') = '03' then 38.698
        when  to_char(stat_month,'mm') = '04' then 37.7618
        when  to_char(stat_month,'mm') = '05' then 37.6633
        when  to_char(stat_month,'mm') = '06' then 36.2719
        when  to_char(stat_month,'mm') = '07' then 33.2777
        when  to_char(stat_month,'mm') = '08' then 30.4571
        when  to_char(stat_month,'mm') = '09' then 35.7034
        when  to_char(stat_month,'mm') = '10' then 40.8194
        when  to_char(stat_month,'mm') = '11' then 43.6929
        when  to_char(stat_month,'mm') = '12' then 44.0905
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '人：用户运营' as 部门
	 ,'子木' as 负责人
	 ,stat_month as 季度or月份
	 ,'B端用户的次月复购率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'近6个月有下单且当月也下单的B端用户/近6个月有下单的B端用户' as  指标定义
	 ,0.20 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '人：用户运营' as 部门
	 ,'子木' as 负责人
	 ,stat_month  as 季度or月份
	 ,'B端用户的次月复购率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'近6个月有下单且当月也下单的B端用户/近6个月有下单的B端用户' as  指标定义
	 ,0.20 as 权重
	 from t2
;




-- 人：用户运营 子木 :  B端用户月累计DAU
with t1 as (
select to_char(a.stat_date, 'yyyy-mm')             stat_month
--   ,case when a.is_b_port=1 then 'B端' else '其他' end                 B端用户
     , sum(a.login_user_num)                       登录DAU
     , QUARTER(to_char(a.stat_date, 'yyyy-mm')) as QUA
from yishou_daily.finebi_user_operator_KPI_monitor a
WHERE a.is_b_port = 1
  and a.mt >= '20230101'
GROUP by to_char(a.stat_date, 'yyyy-mm')
)
,t2 as  (
    SELECT   concat(to_char(max(stat_month),'yyyy'),'Q',QUA) as stat_month
            ,sum(登录DAU) as 当前实际值
            ,case when QUA = 1 then 17675574
             when QUA = 2 then 20634472
             when QUA = 3 then 19211338
             when QUA = 4 then 24070519
            end  as goods_value_target_s
            ,case when QUA = 1 then 16720327
             when QUA = 2 then 18993178
             when QUA = 3 then 17679457
             when QUA = 4 then 21942704
            end  as goods_value_target_a
            ,case when QUA = 1 then 15482885
             when QUA = 2 then 17507066
             when QUA = 3 then 16244308
             when QUA = 4 then 19978271
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,登录DAU as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 6310443
        when  to_char(stat_month,'mm') = '02' then 4149711
        when  to_char(stat_month,'mm') = '03' then 7215420
        when  to_char(stat_month,'mm') = '04' then 7035514
        when  to_char(stat_month,'mm') = '05' then 7089413
        when  to_char(stat_month,'mm') = '06' then 6509544
        when  to_char(stat_month,'mm') = '07' then 6060126
        when  to_char(stat_month,'mm') = '08' then 6285444
        when  to_char(stat_month,'mm') = '09' then 6865767
        when  to_char(stat_month,'mm') = '10' then 7996341
        when  to_char(stat_month,'mm') = '11' then 8119809
        when  to_char(stat_month,'mm') = '12' then 7954369
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 5981152
        when  to_char(stat_month,'mm') = '02' then 3925819
        when  to_char(stat_month,'mm') = '03' then 6813356
        when  to_char(stat_month,'mm') = '04' then 6478195
        when  to_char(stat_month,'mm') = '05' then 6524315
        when  to_char(stat_month,'mm') = '06' then 5990667
        when  to_char(stat_month,'mm') = '07' then 5577073
        when  to_char(stat_month,'mm') = '08' then 5785879
        when  to_char(stat_month,'mm') = '09' then 6316506
        when  to_char(stat_month,'mm') = '10' then 7356634
        when  to_char(stat_month,'mm') = '11' then 7453481
        when  to_char(stat_month,'mm') = '12' then 7132589
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 5545418
        when  to_char(stat_month,'mm') = '02' then 3634842
        when  to_char(stat_month,'mm') = '03' then 6302626
        when  to_char(stat_month,'mm') = '04' then 5982232
        when  to_char(stat_month,'mm') = '05' then 6010590
        when  to_char(stat_month,'mm') = '06' then 5514244
        when  to_char(stat_month,'mm') = '07' then 5133542
        when  to_char(stat_month,'mm') = '08' then 5313562
        when  to_char(stat_month,'mm') = '09' then 5797204
        when  to_char(stat_month,'mm') = '10' then 6746004
        when  to_char(stat_month,'mm') = '11' then 6825137
        when  to_char(stat_month,'mm') = '12' then 6407130
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '人：用户运营' as 部门
	 ,'子木' as 负责人
	 ,stat_month as 季度or月份
	 ,'B端用户月累计DAU' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"现在的B端口径用户DUA	(a.B端满足以下任一条件：	1.首单30天内服装总件数大于等于10件；	2.首单30天内的拿货额大于等于700	3.首单30天内同个货号件数大于等于4	4.首单30天内收货人大于等于3	5.有资料验证信息（开启或自动认证）且身份类型不为'个人自用'.'未填写'	6.渠道为地推	7.身份类型为非个人自用，且电销记录客户类型为非个人自用	b.DAU：每日去重用户数在整个月的累计值)" as  指标定义
	 ,0.30 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '人：用户运营' as 部门
	 ,'子木' as 负责人
	 ,stat_month  as 季度or月份
	 ,'B端用户月累计DAU' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"现在的B端口径用户DUA	(a.B端满足以下任一条件：	1.首单30天内服装总件数大于等于10件；	2.首单30天内的拿货额大于等于700	3.首单30天内同个货号件数大于等于4	4.首单30天内收货人大于等于3	5.有资料验证信息（开启或自动认证）且身份类型不为'个人自用'.'未填写'	6.渠道为地推	7.身份类型为非个人自用，且电销记录客户类型为非个人自用	b.DAU：每日去重用户数在整个月的累计值)" as  指标定义
	 ,0.30 as 权重
	 from t2
;


-- 人：用户运营 子木 :  营销活动费率
with t1 as (
	SELECT  to_char(日期,'yyyy-mm') as stat_month
			,sum(实际销售额) as 实际销售额
			,sum(平台营销费用) as 平台营销费用
			,(sum(平台营销费用)/sum(实际销售额)) * 100 as 营销费率
			,QUARTER(to_char(日期,'yyyy-mm')) as QUA
	from (
		SELECT  日期
				,sum(CASE when 上月实际拿货额层级 ='0_未支付' then 0 ELSE 实际销售额 end) 实际销售额
				,sum(平台营销费用) as 平台营销费用
				,(sum(平台营销费用)/sum(CASE when 上月实际拿货额层级 ='0_未支付' then 0 ELSE 实际销售额 end)) * 100 as 营销费率
		FROM (
			select
			t.stat_date              日期
			,t.user_level_in6m        近6个月实际拿货额峰值层级
			,t.user_level_last_month  上月实际拿货额层级
			,sum(t.实际销售额)         实际销售额
			,sum(t.平台营销费用)       平台营销费用
			,sum(t.红包)      红包
			,sum(t.满减)      满减
			,sum(t.秒杀)      秒杀
			,sum(t.新人专享)  新人专享
			,sum(t.运费券)    运费券
			,sum(t.包邮)      包邮
			from(
			select
				a.stat_date
				,a.user_level_in6m
				,a.user_level_last_month
				,sum(a.real_buy_amount)  实际销售额
				,0.00 平台营销费用
				,0.00 红包    
				,0.00 满减
				,0.00 秒杀
				,0.00 新人专享
				,0.00 运费券
				,0.00 包邮
			from  yishou_daily.finebi_user_operator_KPI_monitor a
			where a.mt >= '20230101'
			group by 1,2,3
			union all
			select
				f.special_date
				,f.user_level_in6m
				,f.user_level_last_month
				,0.00 实际销售额
				,sum(f.fee_platform)       平台营销费用
				,sum(case when f.fee_type='红包' then f.fee_platform else 0 end) 红包
				,sum(case when f.fee_type='满减' then f.fee_platform else 0 end) 满减
				,sum(case when f.fee_type='秒杀' then f.fee_platform else 0 end) 秒杀
				,sum(case when f.fee_type='新人专享' then f.fee_platform else 0 end) 新人专享
				,sum(case when f.fee_type='运费券' then f.fee_platform else 0 end) 运费券
				,sum(case when f.fee_type='包邮' then f.fee_platform else 0 end) 包邮
			from yishou_daily.finebi_user_operation_activity_fee_mt f
			where f.fee_platform<>0 and f.is_new_user_fee=0
			and  mt >= '20230101'
			group by 1,2,3
			)t
			group by 1,2,3
		) a
		GROUP BY 日期
	) a 
	GROUP by to_char(日期,'yyyy-mm')
)
,t2 as  (
    SELECT   concat(to_char(max(stat_month),'yyyy'),'Q',QUA) as stat_month
            ,(avg(营销费率) ) as 当前实际值
            ,case when QUA = 1 then 1.76
             when QUA = 2 then 1.597
             when QUA = 3 then 1.78
             when QUA = 4 then 1.758
            end  as goods_value_target_s
            ,case when QUA = 1 then 1.76
             when QUA = 2 then 1.611
             when QUA = 3 then 1.79
             when QUA = 4 then 1.77
            end  as goods_value_target_a
            ,case when QUA = 1 then 1.763
             when QUA = 2 then 1.617
             when QUA = 3 then 1.80
             when QUA = 4 then 1.774
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,营销费率 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 1.84
        when  to_char(stat_month,'mm') = '02' then 1.84
        when  to_char(stat_month,'mm') = '03' then 1.64
        when  to_char(stat_month,'mm') = '04' then 1.647
        when  to_char(stat_month,'mm') = '05' then 1.579
        when  to_char(stat_month,'mm') = '06' then 1.557
        when  to_char(stat_month,'mm') = '07' then 1.739
        when  to_char(stat_month,'mm') = '08' then 1.816
        when  to_char(stat_month,'mm') = '09' then 1.775
        when  to_char(stat_month,'mm') = '10' then 1.812
        when  to_char(stat_month,'mm') = '11' then 1.787
        when  to_char(stat_month,'mm') = '12' then 1.669
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 1.84
        when  to_char(stat_month,'mm') = '02' then 1.84
        when  to_char(stat_month,'mm') = '03' then 1.64
        when  to_char(stat_month,'mm') = '04' then 1.671
        when  to_char(stat_month,'mm') = '05' then 1.587
        when  to_char(stat_month,'mm') = '06' then 1.566
        when  to_char(stat_month,'mm') = '07' then 1.742
        when  to_char(stat_month,'mm') = '08' then 1.827
        when  to_char(stat_month,'mm') = '09' then 1.792
        when  to_char(stat_month,'mm') = '10' then 1.821
        when  to_char(stat_month,'mm') = '11' then 1.796
        when  to_char(stat_month,'mm') = '12' then 1.682
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 1.84
        when  to_char(stat_month,'mm') = '02' then 1.84
        when  to_char(stat_month,'mm') = '03' then 1.651
        when  to_char(stat_month,'mm') = '04' then 1.673
        when  to_char(stat_month,'mm') = '05' then 1.595
        when  to_char(stat_month,'mm') = '06' then 1.573
        when  to_char(stat_month,'mm') = '07' then 1.753
        when  to_char(stat_month,'mm') = '08' then 1.839
        when  to_char(stat_month,'mm') = '09' then 1.802
        when  to_char(stat_month,'mm') = '10' then 1.826
        when  to_char(stat_month,'mm') = '11' then 1.801
        when  to_char(stat_month,'mm') = '12' then 1.683
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '人：用户运营' as 部门
	 ,'子木' as 负责人
	 ,stat_month as 季度or月份
	 ,'营销活动费率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE 
            when t3.当前实际值 < goods_value_target_s and goods_value_target_s < goods_value_target_a then 'S'
            when t3.当前实际值 < goods_value_target_a and goods_value_target_a < goods_value_target_b then 'A'
            when t3.当前实际值 < goods_value_target_b  then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/当前实际值) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/当前实际值) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/当前实际值) * 100,2)),'%') as B级达成率
	 ,'（预售场营销活动支出+运费抵免支出+包邮支出-运费月卡收入）/实际GMV（数据口径均为实际GMV口径）' as  指标定义
	 ,0.10 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '人：用户运营' as 部门
	 ,'子木' as 负责人
	 ,stat_month  as 季度or月份
	 ,'营销活动费率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     ,  CASE 
            when t2.当前实际值 < goods_value_target_s and goods_value_target_s < goods_value_target_a then 'S'
            when t2.当前实际值 < goods_value_target_a and goods_value_target_a < goods_value_target_b then 'A'
            when t2.当前实际值 < goods_value_target_b  then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/当前实际值) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/当前实际值) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/当前实际值) * 100,2)),'%') as B级达成率
	 ,'（预售场营销活动支出+运费抵免支出+包邮支出-运费月卡收入）/实际GMV（数据口径均为实际GMV口径）' as  指标定义
	 ,0.10 as 权重
	 from t2
;


-- 人：用户运营 子木 :  良品退货率
WITH t1 as (
	select
	to_char(stat_date,'yyyy-mm')    as stat_month
	,sum(coalesce(shipped_gmv,0))             实发GMV
	,sum(coalesce(return_amount,0))           良品退货GMV
	,(sum(coalesce(return_amount,0)) /sum(coalesce(shipped_gmv,0))) * 100   as 良品退货率
	,QUARTER(to_char(stat_date,'yyyy-mm') ) as QUA
	from  yishou_daily.finebi_user_operator_KPI_monitor a
	where mt >= '20230101'
	GROUP BY to_char(stat_date,'yyyy-mm')
)
,t2 as  (
    SELECT   concat(to_char(max(stat_month),'yyyy'),'Q',QUA) as stat_month
            ,(avg(良品退货率) ) as 当前实际值
            ,case when QUA = 1 then 6.73937766133519
             when QUA = 2 then 8.17853188604866
             when QUA = 3 then 6.72219640623428
             when QUA = 4 then 5.08990005666929
            end  as goods_value_target_s
            ,case when QUA = 1 then 7.28844388822683
             when QUA = 2 then 8.84127565341588
             when QUA = 3 then 7.26454786269018
             when QUA = 4 then 5.42479966187376
            end  as goods_value_target_a
            ,case when QUA = 1 then 7.62041191997048
             when QUA = 2 then 9.16935447105626
             when QUA = 3 then 7.50570078983621
             when QUA = 4 then 5.72917906161094
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,良品退货率 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 8.92022314939724
        when  to_char(stat_month,'mm') = '02' then 8.92022314939724
        when  to_char(stat_month,'mm') = '03' then 7.25825034218853
        when  to_char(stat_month,'mm') = '04' then 8.80203201371337
        when  to_char(stat_month,'mm') = '05' then 8.26581912062798
        when  to_char(stat_month,'mm') = '06' then 7.30274641436346
        when  to_char(stat_month,'mm') = '07' then 5.6694839488609
        when  to_char(stat_month,'mm') = '08' then 7.70584893000623
        when  to_char(stat_month,'mm') = '09' then 6.47719137281098
        when  to_char(stat_month,'mm') = '10' then 5.95138989982891
        when  to_char(stat_month,'mm') = '11' then 5.19642124684799
        when  to_char(stat_month,'mm') = '12' then 4.12026426747685
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 8.93809934809343
        when  to_char(stat_month,'mm') = '02' then 8.92916124874534
        when  to_char(stat_month,'mm') = '03' then 7.84855037608848
        when  to_char(stat_month,'mm') = '04' then 9.51626599957198
        when  to_char(stat_month,'mm') = '05' then 8.93474831892737
        when  to_char(stat_month,'mm') = '06' then 7.89436582720265
        when  to_char(stat_month,'mm') = '07' then 6.13033480379999
        when  to_char(stat_month,'mm') = '08' then 8.32807567170514
        when  to_char(stat_month,'mm') = '09' then 6.99758251946772
        when  to_char(stat_month,'mm') = '10' then 6.31226273669547
        when  to_char(stat_month,'mm') = '11' then 5.50120937650559
        when  to_char(stat_month,'mm') = '12' then 4.46215002858729
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 8.93809934809343
        when  to_char(stat_month,'mm') = '02' then 8.93809934809343
        when  to_char(stat_month,'mm') = '03' then 8.18875425162413
        when  to_char(stat_month,'mm') = '04' then 9.89440635193518
        when  to_char(stat_month,'mm') = '05' then 9.25273380751837
        when  to_char(stat_month,'mm') = '06' then 8.1721918748194
        when  to_char(stat_month,'mm') = '07' then 6.34574068845206
        when  to_char(stat_month,'mm') = '08' then 8.62000865772669
        when  to_char(stat_month,'mm') = '09' then 7.21125714112217
        when  to_char(stat_month,'mm') = '10' then 6.59421063314979
        when  to_char(stat_month,'mm') = '11' then 5.81848743315652
        when  to_char(stat_month,'mm') = '12' then 4.77474213027119
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '人：用户运营' as 部门
	 ,'子木' as 负责人
	 ,stat_month as 季度or月份
	 ,'良品退货率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 < goods_value_target_s then 'S'
            when t3.当前实际值 < goods_value_target_a then 'A'
            when t3.当前实际值 < goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/当前实际值) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/当前实际值) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/当前实际值) * 100,2)),'%') as B级达成率
	 ,'（政策性退货GMV+测试活动退货GMV）/实发GMV	政策性退货：无理由，无忧退，营销发放的无理由额度，超归发放的无理由额度等	测试活动退货：特指工单问题二级类型为活动100％退的退货	备注：	剔除商家出钱的退货GMV' as  指标定义
	 ,0.10 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '人：用户运营' as 部门
	 ,'子木' as 负责人
	 ,stat_month  as 季度or月份
	 ,'良品退货率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 < goods_value_target_s then 'S'
            when t2.当前实际值 < goods_value_target_a then 'A'
            when t2.当前实际值 < goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/当前实际值) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/当前实际值) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/当前实际值) * 100,2)),'%') as B级达成率
	 ,'（政策性退货GMV+测试活动退货GMV）/实发GMV	政策性退货：无理由，无忧退，营销发放的无理由额度，超归发放的无理由额度等	测试活动退货：特指工单问题二级类型为活动100％退的退货	备注：	剔除商家出钱的退货GMV' as  指标定义
	 ,0.10 as 权重
	 from t2
;
	
-- 人：户部（用户服务）--> 波波 -->  5000+用户GMV ( 同 (产研中心 产品（按各50%权重考核产品部达成） 秀则 ) )
with t1 as (
select
   to_char(stat_date,'yyyy-mm')  as    stat_month
  ,round(sum(a.over_5K_user_buy_amount),4) as 当月拿货额超5K用户GMV
  ,QUARTER(to_char(stat_date,'yyyy-mm')) as QUA
from  yishou_daily.finebi_user_operator_KPI_monitor a
where mt >= '2023-01-01'
GROUP BY to_char(stat_date,'yyyy-mm')
)
,t2 as  (
    SELECT   concat(to_char(max(stat_month),'yyyy'),'Q',QUA) as stat_month
            ,sum(当月拿货额超5K用户GMV) as 当前实际值
            ,case when QUA = 1 then 527874664
             when QUA = 2 then 628259802
             when QUA = 3 then 602026268
             when QUA = 4 then 1011851330
            end  as goods_value_target_s
            ,case when QUA = 1 then 482959816
             when QUA = 2 then 574803565
             when QUA = 3 then 550802142
             when QUA = 4 then 925756747
            end  as goods_value_target_a
            ,case when QUA = 1 then 441781909
             when QUA = 2 then 525794917
             when QUA = 3 then 503839893
             when QUA = 4 then 846825286
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,当月拿货额超5K用户GMV as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 166200724
        when  to_char(stat_month,'mm') = '02' then 146810639
        when  to_char(stat_month,'mm') = '03' then 214863301
        when  to_char(stat_month,'mm') = '04' then 235747864
        when  to_char(stat_month,'mm') = '05' then 216418186
        when  to_char(stat_month,'mm') = '06' then 176093752
        when  to_char(stat_month,'mm') = '07' then 110057991
        when  to_char(stat_month,'mm') = '08' then 220204396
        when  to_char(stat_month,'mm') = '09' then 271763881
        when  to_char(stat_month,'mm') = '10' then 330403849
        when  to_char(stat_month,'mm') = '11' then 364057181
        when  to_char(stat_month,'mm') = '12' then 317390300
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 152059336
        when  to_char(stat_month,'mm') = '02' then 134319080
        when  to_char(stat_month,'mm') = '03' then 196581400
        when  to_char(stat_month,'mm') = '04' then 215688974
        when  to_char(stat_month,'mm') = '05' then 198003986
        when  to_char(stat_month,'mm') = '06' then 161110605
        when  to_char(stat_month,'mm') = '07' then 100693575
        when  to_char(stat_month,'mm') = '08' then 201468041
        when  to_char(stat_month,'mm') = '09' then 248640526
        when  to_char(stat_month,'mm') = '10' then 302291042
        when  to_char(stat_month,'mm') = '11' then 333080939
        when  to_char(stat_month,'mm') = '12' then 290384766
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 139094520
        when  to_char(stat_month,'mm') = '02' then 122866826
        when  to_char(stat_month,'mm') = '03' then 179820563
        when  to_char(stat_month,'mm') = '04' then 197298996
        when  to_char(stat_month,'mm') = '05' then 181121857
        when  to_char(stat_month,'mm') = '06' then 147374064
        when  to_char(stat_month,'mm') = '07' then 92108284
        when  to_char(stat_month,'mm') = '08' then 184290562
        when  to_char(stat_month,'mm') = '09' then 227441047
        when  to_char(stat_month,'mm') = '10' then 276517237
        when  to_char(stat_month,'mm') = '11' then 304681940
        when  to_char(stat_month,'mm') = '12' then 265626109
        end as goods_value_target_b
    from t1
)
 INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '人：户部（用户服务）' as 部门
	 ,'波波' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'5000+用户GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'当月实际支付5000+的用户GMV' as  指标定义
	 ,0.40 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '人：户部（用户服务）' as 部门
	 ,'波波' as 负责人
	 ,stat_month  as 季度or月份
	 ,'5000+用户GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'当月实际支付5000+的用户GMV' as  指标定义
	 ,0.40 as 权重
	 from t2
;

-- 人：户部（用户服务）--> 波波 -->  潜力大客数
WITH t1 as (
    SELECT  to_char(to_date1(dt,'yyyymmdd'),'yyyy-mm') as stat_month
            ,sum(潜力大客数)  as 潜力大客数
            ,QUARTER(to_char(to_date1(dt,'yyyymmdd'),'yyyy-mm')) as QUA
    from (
        select
            to_char(x1.real_first_time,'yyyymmdd') dt
            ,count(distinct case when x1.real_first_30d_buy_amount >= 1000 then x1.user_id end) as 潜力大客数
        from yishou_data.dim_ys_user_order_data x1
        WHERE to_char(x1.real_first_time,'yyyymmdd') >= '20230101'
        group by 1
    )
    GROUP BY to_char(to_date1(dt,'yyyymmdd'),'yyyy-mm')
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(潜力大客数) as 当前实际值
            ,case when QUA = 1 then 8449
             when QUA = 2 then 9979
             when QUA = 3 then 9215
             when QUA = 4 then 16713
            end  as goods_value_target_s
            ,case when QUA = 1 then 7754
             when QUA = 2 then 9159
             when QUA = 3 then 8457
             when QUA = 4 then 15340
            end  as goods_value_target_a
            ,case when QUA = 1 then 7060
             when QUA = 2 then 8339
             when QUA = 3 then 7700
             when QUA = 4 then 13966
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,潜力大客数 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 2393
        when  to_char(stat_month,'mm') = '02' then 2034
        when  to_char(stat_month,'mm') = '03' then 4021
        when  to_char(stat_month,'mm') = '04' then 3830
        when  to_char(stat_month,'mm') = '05' then 3397
        when  to_char(stat_month,'mm') = '06' then 2752
        when  to_char(stat_month,'mm') = '07' then 2154
        when  to_char(stat_month,'mm') = '08' then 3231
        when  to_char(stat_month,'mm') = '09' then 3830
        when  to_char(stat_month,'mm') = '10' then 5385
        when  to_char(stat_month,'mm') = '11' then 5984
        when  to_char(stat_month,'mm') = '12' then 5345
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 2197
        when  to_char(stat_month,'mm') = '02' then 1867
        when  to_char(stat_month,'mm') = '03' then 3690
        when  to_char(stat_month,'mm') = '04' then 3515
        when  to_char(stat_month,'mm') = '05' then 3118
        when  to_char(stat_month,'mm') = '06' then 2526
        when  to_char(stat_month,'mm') = '07' then 1977
        when  to_char(stat_month,'mm') = '08' then 2966
        when  to_char(stat_month,'mm') = '09' then 3515
        when  to_char(stat_month,'mm') = '10' then 4943
        when  to_char(stat_month,'mm') = '11' then 5492
        when  to_char(stat_month,'mm') = '12' then 4905
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 2000
        when  to_char(stat_month,'mm') = '02' then 1700
        when  to_char(stat_month,'mm') = '03' then 3360
        when  to_char(stat_month,'mm') = '04' then 3200
        when  to_char(stat_month,'mm') = '05' then 2839
        when  to_char(stat_month,'mm') = '06' then 2300
        when  to_char(stat_month,'mm') = '07' then 1800
        when  to_char(stat_month,'mm') = '08' then 2700
        when  to_char(stat_month,'mm') = '09' then 3200
        when  to_char(stat_month,'mm') = '10' then 4500
        when  to_char(stat_month,'mm') = '11' then 5000
        when  to_char(stat_month,'mm') = '12' then 4466
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '人：户部（用户服务）' as 部门
	 ,'波波' as 负责人
	 ,stat_month as 季度or月份
	 ,'潜力大客数' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"用户首单后30天内实际GMV大于等于1000+的用户数（含首单当天）-全尾号" as  指标定义
	 ,0.40 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '人：户部（用户服务）' as 部门
	 ,'波波' as 负责人
	 ,stat_month  as 季度or月份
	 ,'潜力大客数' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"用户首单后30天内实际GMV大于等于1000+的用户数（含首单当天）-全尾号" as  指标定义
	 ,0.40 as 权重
	 from t2
;



-- 人：户部（用户服务）--> 波波 -->  服务差评率
with t1 as (
	SELECT 
		to_char(日期,'yyyy-mm') as stat_month
		,sum(不满意标签个数)/sum(评价标签个数) as 服务差评率
		,QUARTER(to_char(日期,'yyyy-mm')) as QUA
	FROM (
		select
		date_54_week 周,
			to_date1(dt, 'yyyymmdd')                                                                             日期
			, sum(tags_cnt)                                                                                     评价标签个数
			, sum(case when tags in ('找客服很快', '问题已解决', '客服态度好', '解决问题快') then tags_cnt end) 满意标签个数
			, sum(case when tags in ('找客服很慢', '问题没解决', '客服态度差', '解决问题慢') then tags_cnt end) 不满意标签个数
		from yishou_daily.finebi_qiyu_user_satisfaction_analyse a
				left join yishou_daily.finebi_calendar_friday_to_thursday b on a.dt = b.date_str
		where dt between '20230720' and '20231231'
		and dt >= '20230101'
		GROUP BY 1, 2
		union all
		select
		date_54_week 周,
			to_date1(dt, 'yyyymmdd')                         日期
			, sum(session_cnt)                              评价会话数
			, sum(session_cnt) - sum(un_satify_session_cnt) 不含差评标签会话数
			, sum(un_satify_session_cnt)                    含差评标签会话数
		from cross_origin.finebi_qiyu_user_satisfaction_analyse_v2 a
				left join yishou_daily.finebi_calendar_friday_to_thursday b on a.dt = b.date_str
		where dt >= '20230101'
		GROUP BY 1, 2
	)
	GROUP BY to_char(日期,'yyyy-mm')    
)
,t2 as  (
    SELECT   concat(to_char(max(stat_month),'yyyy'),'Q',QUA) as stat_month
            ,(avg(服务差评率) * 100) as 当前实际值
            ,case when QUA = 1 then 4
             when QUA = 2 then 4
             when QUA = 3 then 4
             when QUA = 4 then 4
            end  as goods_value_target_s
            ,case when QUA = 1 then 5
             when QUA = 2 then 5
             when QUA = 3 then 5
             when QUA = 4 then 5
            end  as goods_value_target_a
            ,case when QUA = 1 then 5.95
             when QUA = 2 then 5.95
             when QUA = 3 then 5.95
             when QUA = 4 then 5.95
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,服务差评率 * 100 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 4
        when  to_char(stat_month,'mm') = '02' then 4
        when  to_char(stat_month,'mm') = '03' then 4
        when  to_char(stat_month,'mm') = '04' then 4
        when  to_char(stat_month,'mm') = '05' then 4
        when  to_char(stat_month,'mm') = '06' then 4
        when  to_char(stat_month,'mm') = '07' then 4
        when  to_char(stat_month,'mm') = '08' then 4
        when  to_char(stat_month,'mm') = '09' then 4
        when  to_char(stat_month,'mm') = '10' then 4
        when  to_char(stat_month,'mm') = '11' then 4
        when  to_char(stat_month,'mm') = '12' then 4
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 5
        when  to_char(stat_month,'mm') = '02' then 5
        when  to_char(stat_month,'mm') = '03' then 5
        when  to_char(stat_month,'mm') = '04' then 5
        when  to_char(stat_month,'mm') = '05' then 5
        when  to_char(stat_month,'mm') = '06' then 5
        when  to_char(stat_month,'mm') = '07' then 5
        when  to_char(stat_month,'mm') = '08' then 5
        when  to_char(stat_month,'mm') = '09' then 5
        when  to_char(stat_month,'mm') = '10' then 5
        when  to_char(stat_month,'mm') = '11' then 5
        when  to_char(stat_month,'mm') = '12' then 5
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 5.95
        when  to_char(stat_month,'mm') = '02' then 5.95
        when  to_char(stat_month,'mm') = '03' then 5.95
        when  to_char(stat_month,'mm') = '04' then 5.95
        when  to_char(stat_month,'mm') = '05' then 5.95
        when  to_char(stat_month,'mm') = '06' then 5.95
        when  to_char(stat_month,'mm') = '07' then 5.95
        when  to_char(stat_month,'mm') = '08' then 5.95
        when  to_char(stat_month,'mm') = '09' then 5.95
        when  to_char(stat_month,'mm') = '10' then 5.95
        when  to_char(stat_month,'mm') = '11' then 5.95
        when  to_char(stat_month,'mm') = '12' then 5.95
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '人：户部（用户服务）' as 部门
	 ,'波波' as 负责人
	 ,stat_month as 季度or月份
	 ,'服务差评率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 < goods_value_target_s then 'S'
            when t3.当前实际值 < goods_value_target_a then 'A'
            when t3.当前实际值 < goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/当前实际值) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/当前实际值) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/当前实际值) * 100,2)),'%') as B级达成率
	 ,'用户评价差评标签数/总评价标签数' as  指标定义
	 ,0.20 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '人：户部（用户服务）' as 部门
	 ,'波波' as 负责人
	 ,stat_month  as 季度or月份
	 ,'服务差评率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 < goods_value_target_s then 'S'
            when t2.当前实际值 < goods_value_target_a then 'A'
            when t2.当前实际值 < goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/当前实际值) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/当前实际值) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/当前实际值) * 100,2)),'%') as B级达成率
	 ,'用户评价差评标签数/总评价标签数' as  指标定义
	 ,0.20 as 权重
	 from t2
;


---------------------------------------------------------------------


-- 人：户部（用户服务）--> 用户护卫队（客服）--> 嘎嘎 -->  服务差评率
with t1 as (
	SELECT 
		to_char(日期,'yyyy-mm') as stat_month
		,sum(不满意标签个数)/sum(评价标签个数) as 服务差评率
		,QUARTER(to_char(日期,'yyyy-mm')) as QUA
	FROM (
		select
		date_54_week 周,
			to_date1(dt, 'yyyymmdd')                                                                             日期
			, sum(tags_cnt)                                                                                     评价标签个数
			, sum(case when tags in ('找客服很快', '问题已解决', '客服态度好', '解决问题快') then tags_cnt end) 满意标签个数
			, sum(case when tags in ('找客服很慢', '问题没解决', '客服态度差', '解决问题慢') then tags_cnt end) 不满意标签个数
		from yishou_daily.finebi_qiyu_user_satisfaction_analyse a
				left join yishou_daily.finebi_calendar_friday_to_thursday b on a.dt = b.date_str
		where dt between '20230720' and '20231231'
		and dt >= '20230101'
		GROUP BY 1, 2
		union all
		select
		date_54_week 周,
			to_date1(dt, 'yyyymmdd')                         日期
			, sum(session_cnt)                              评价会话数
			, sum(session_cnt) - sum(un_satify_session_cnt) 不含差评标签会话数
			, sum(un_satify_session_cnt)                    含差评标签会话数
		from cross_origin.finebi_qiyu_user_satisfaction_analyse_v2 a
				left join yishou_daily.finebi_calendar_friday_to_thursday b on a.dt = b.date_str
		where dt >= '20230101'
		GROUP BY 1, 2
	)
	GROUP BY to_char(日期,'yyyy-mm')    
)
,t2 as  (
    SELECT   concat(to_char(max(stat_month),'yyyy'),'Q',QUA) as stat_month
            ,(avg(服务差评率) * 100) as 当前实际值
            ,case when QUA = 1 then 4
             when QUA = 2 then 4
             when QUA = 3 then 4
             when QUA = 4 then 4
            end  as goods_value_target_s
            ,case when QUA = 1 then 5
             when QUA = 2 then 5
             when QUA = 3 then 5
             when QUA = 4 then 5
            end  as goods_value_target_a
            ,case when QUA = 1 then 5.95
             when QUA = 2 then 5.95
             when QUA = 3 then 5.95
             when QUA = 4 then 5.95
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,服务差评率 * 100 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 4
        when  to_char(stat_month,'mm') = '02' then 4
        when  to_char(stat_month,'mm') = '03' then 4
        when  to_char(stat_month,'mm') = '04' then 4
        when  to_char(stat_month,'mm') = '05' then 4
        when  to_char(stat_month,'mm') = '06' then 4
        when  to_char(stat_month,'mm') = '07' then 4
        when  to_char(stat_month,'mm') = '08' then 4
        when  to_char(stat_month,'mm') = '09' then 4
        when  to_char(stat_month,'mm') = '10' then 4
        when  to_char(stat_month,'mm') = '11' then 4
        when  to_char(stat_month,'mm') = '12' then 4
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 5
        when  to_char(stat_month,'mm') = '02' then 5
        when  to_char(stat_month,'mm') = '03' then 5
        when  to_char(stat_month,'mm') = '04' then 5
        when  to_char(stat_month,'mm') = '05' then 5
        when  to_char(stat_month,'mm') = '06' then 5
        when  to_char(stat_month,'mm') = '07' then 5
        when  to_char(stat_month,'mm') = '08' then 5
        when  to_char(stat_month,'mm') = '09' then 5
        when  to_char(stat_month,'mm') = '10' then 5
        when  to_char(stat_month,'mm') = '11' then 5
        when  to_char(stat_month,'mm') = '12' then 5
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 5.95
        when  to_char(stat_month,'mm') = '02' then 5.95
        when  to_char(stat_month,'mm') = '03' then 5.95
        when  to_char(stat_month,'mm') = '04' then 5.95
        when  to_char(stat_month,'mm') = '05' then 5.95
        when  to_char(stat_month,'mm') = '06' then 5.95
        when  to_char(stat_month,'mm') = '07' then 5.95
        when  to_char(stat_month,'mm') = '08' then 5.95
        when  to_char(stat_month,'mm') = '09' then 5.95
        when  to_char(stat_month,'mm') = '10' then 5.95
        when  to_char(stat_month,'mm') = '11' then 5.95
        when  to_char(stat_month,'mm') = '12' then 5.95
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '人：户部（用户服务）用户护卫队（客服）' as 部门
	 ,'嘎嘎' as 负责人
	 ,stat_month as 季度or月份
	 ,'服务差评率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 < goods_value_target_s then 'S'
            when t3.当前实际值 < goods_value_target_a then 'A'
            when t3.当前实际值 < goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/当前实际值) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/当前实际值) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/当前实际值) * 100,2)),'%') as B级达成率
	 ,'用户评价差评标签数/总评价标签数' as  指标定义
	 ,0.30 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '人：户部（用户服务）用户护卫队（客服）' as 部门
	 ,'嘎嘎' as 负责人
	 ,stat_month  as 季度or月份
	 ,'服务差评率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 < goods_value_target_s then 'S'
            when t2.当前实际值 < goods_value_target_a then 'A'
            when t2.当前实际值 < goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/当前实际值) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/当前实际值) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/当前实际值) * 100,2)),'%') as B级达成率
	 ,'用户评价差评标签数/总评价标签数' as  指标定义
	 ,0.30 as 权重
	 from t2
;



-- 人：户部（用户服务）--> 用户护卫队（客服）--> 嘎嘎 -->  全量入线占比
-- 暂无取数路径,跳过
-- 只能线下统计

-- 人：户部（用户服务）--> 用户护卫队（客服）--> 嘎嘎 -->  普通工单退货率
with t1 as (
	select
	to_char(to_date1(t1.date_str,'yyyymmdd'),'yyyy-mm') as stat_month
	,sum(发货gmv) 发货gmv
	,sum(退货gmv) 退货gmv
	,(sum(退货gmv) / sum(发货gmv)) * 100  as 普通工单退货率
	,QUARTER(to_char(to_date1(t1.date_str,'yyyymmdd'),'yyyy-mm')) as QUA
	from yishou_daily.finebi_calendar_friday_to_thursday t1
	left join
	(
	select 发货时间
		,sum(发货gmv) 发货gmv
	from yishou_daily.finebi_ship_daily_detail
	group by 发货时间
	)t2
	on t1.date_str=t2.发货时间
	left join
	(
	select 发货时间
		,sum(退货gmv) 退货gmv
	from yishou_daily.finebi_aftersales_defective_daily_detail
	group by 发货时间
	)t4
	on t1.date_str=t4.发货时间
	where  to_char(to_date1(t1.date_str,'yyyymmdd'),'yyyymm') BETWEEN  '202301' and '${bizmonth}'
	group by 1
	order by 1
)
,t2 as  (
    SELECT   concat(to_char(max(stat_month),'yyyy'),'Q',QUA) as stat_month
            ,(avg(普通工单退货率) ) as 当前实际值
            ,case when QUA = 1 then 3.46426040409956
             when QUA = 2 then 3.57114301323867
             when QUA = 3 then 4.13969180394394
             when QUA = 4 then 4.31637765382773
            end  as goods_value_target_s
            ,case when QUA = 1 then 3.92616179131284
             when QUA = 2 then 4.04729541500383
             when QUA = 3 then 4.69165071113647
             when QUA = 4 then 4.8918946743381
            end  as goods_value_target_a
            ,case when QUA = 1 then 4.29568290108346
             when QUA = 2 then 4.42821733641595
             when QUA = 3 then 5.13321783689049
             when QUA = 4 then 5.35230829074639
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,普通工单退货率 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 4.10977114564316
        when  to_char(stat_month,'mm') = '02' then 3.12458630310415
        when  to_char(stat_month,'mm') = '03' then 3.53348911867795
        when  to_char(stat_month,'mm') = '04' then 3.50023394575311
        when  to_char(stat_month,'mm') = '05' then 3.65199667969872
        when  to_char(stat_month,'mm') = '06' then 3.56044950225811
        when  to_char(stat_month,'mm') = '07' then 3.76533655566682
        when  to_char(stat_month,'mm') = '08' then 3.22404205344228
        when  to_char(stat_month,'mm') = '09' then 5.00764995140099
        when  to_char(stat_month,'mm') = '10' then 3.83608610726551
        when  to_char(stat_month,'mm') = '11' then 4.71017937142943
        when  to_char(stat_month,'mm') = '12' then 4.34720424842642
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 4.65774063172891
        when  to_char(stat_month,'mm') = '02' then 3.5411978101847
        when  to_char(stat_month,'mm') = '03' then 4.00462100116834
        when  to_char(stat_month,'mm') = '04' then 3.96693180518685
        when  to_char(stat_month,'mm') = '05' then 4.13892957032522
        when  to_char(stat_month,'mm') = '06' then 4.03517610255919
        when  to_char(stat_month,'mm') = '07' then 4.26738142975572
        when  to_char(stat_month,'mm') = '08' then 3.65391432723458
        when  to_char(stat_month,'mm') = '09' then 5.67533661158779
        when  to_char(stat_month,'mm') = '10' then 4.34756425490091
        when  to_char(stat_month,'mm') = '11' then 5.33820328762001
        when  to_char(stat_month,'mm') = '12' then 4.92683148154994
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 5.09611622059752
        when  to_char(stat_month,'mm') = '02' then 3.87448701584915
        when  to_char(stat_month,'mm') = '03' then 4.38152650716065
        when  to_char(stat_month,'mm') = '04' then 4.34029009273385
        when  to_char(stat_month,'mm') = '05' then 4.52847588282641
        when  to_char(stat_month,'mm') = '06' then 4.41495738280006
        when  to_char(stat_month,'mm') = '07' then 4.66901732902685
        when  to_char(stat_month,'mm') = '08' then 3.99781214626842
        when  to_char(stat_month,'mm') = '09' then 6.20948593973723
        when  to_char(stat_month,'mm') = '10' then 4.75674677300923
        when  to_char(stat_month,'mm') = '11' then 5.84062242057249
        when  to_char(stat_month,'mm') = '12' then 5.39053326804876
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '人：户部（用户服务）用户护卫队（客服）' as 部门
	 ,'嘎嘎' as 负责人
	 ,stat_month as 季度or月份
	 ,'普通工单退货率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 < goods_value_target_s then 'S'
            when t3.当前实际值 < goods_value_target_a then 'A'
            when t3.当前实际值 < goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/当前实际值) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/当前实际值) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/当前实际值) * 100,2)),'%') as B级达成率
	 ,'普通工单退货gmv/实发gmv	除无理由、无忧退、退货卡等政策性退货和退货测试造成的退货，其余归类为次品退货，即普通工单退货，与实发GMV的比率' as  指标定义
	 ,0.15 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '人：户部（用户服务）用户护卫队（客服）' as 部门
	 ,'嘎嘎' as 负责人
	 ,stat_month  as 季度or月份
	 ,'普通工单退货率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 < goods_value_target_s then 'S'
            when t2.当前实际值 < goods_value_target_a then 'A'
            when t2.当前实际值 < goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/当前实际值) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/当前实际值) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/当前实际值) * 100,2)),'%') as B级达成率
	 ,'普通工单退货gmv/实发gmv	除无理由、无忧退、退货卡等政策性退货和退货测试造成的退货，其余归类为次品退货，即普通工单退货，与实发GMV的比率' as  指标定义
	 ,0.15 as 权重
	 from t2
;

-- 人：户部（用户服务）--> 用户护卫队（客服）--> 嘎嘎 -->  人工费用率
-- 只能线下统计



-- 人：户部（用户服务）--> 用户护卫队（客服）--> 嘎嘎 -->  售后成本率
WITH t1 as (
	select
	to_char(to_date1(t1.date_str,'yyyymmdd'),'yyyy-mm') as stat_month
	,sum(发货gmv) 发货gmv
	,sum(错漏发补偿金额	+ 质量补偿金额 + 其他补偿金额 + 附加补偿金额) as 总补偿金额
	,(sum(错漏发补偿金额	+ 质量补偿金额 + 其他补偿金额 + 附加补偿金额) / sum(发货gmv) ) * 100    as  售后成本率
	,QUARTER(to_char(to_date1(t1.date_str,'yyyymmdd'),'yyyy-mm')) as QUA
	from yishou_daily.finebi_calendar_friday_to_thursday t1
	left join
	(
	select 发货时间
		,sum(发货gmv) 发货gmv
	from yishou_daily.finebi_ship_daily_detail
	group by 发货时间
	)t2
	on t1.date_str=t2.发货时间
	left join
    (
      select 业务发生时间
        -- ,sum(无理由服务费) 无理由服务费
        -- ,sum(退货商品拿货额) 退货商品拿货额
        ,sum(错漏发补偿金额) 错漏发补偿金额
        ,sum(质量补偿金额)  质量补偿金额
        ,sum(其他补偿金额)  其他补偿金额
        ,sum(附加补偿金额)  附加补偿金额
      from yishou_daily.finebi_aftersales_defective_daily_detail
      group by 业务发生时间
     )t3
    on t1.date_str=t3.业务发生时间
	where  to_char(to_date1(t1.date_str,'yyyymmdd'),'yyyymm') BETWEEN  '202301' and '${bizmonth}'
	group by 1
	order by 1
)
,t2 as  (
    SELECT   concat(to_char(max(stat_month),'yyyy'),'Q',QUA) as stat_month
            ,(avg(售后成本率) ) as 当前实际值
            ,case when QUA = 1 then 0.39124940476585
             when QUA = 2 then 0.35852435796316
             when QUA = 3 then 0.28799741271535
             when QUA = 4 then 4.31637765382773
            end  as goods_value_target_s
            ,case when QUA = 1 then 0.40124940476585
             when QUA = 2 then 0.36852435796316
             when QUA = 3 then 0.29799741271535
             when QUA = 4 then 4.8918946743381
            end  as goods_value_target_a
            ,case when QUA = 1 then 0.41124940476585
             when QUA = 2 then 0.37852435796316
             when QUA = 3 then 0.30799741271535
             when QUA = 4 then 5.35230829074639
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,售后成本率 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 0.67324964960319
        when  to_char(stat_month,'mm') = '02' then 0.28044343042509
        when  to_char(stat_month,'mm') = '03' then 0.38878942735022
        when  to_char(stat_month,'mm') = '04' then 0.34524291994374
        when  to_char(stat_month,'mm') = '05' then 0.35789582890245
        when  to_char(stat_month,'mm') = '06' then 0.37505352303754
        when  to_char(stat_month,'mm') = '07' then 0.51008288823198
        when  to_char(stat_month,'mm') = '08' then 0.22336807460202
        when  to_char(stat_month,'mm') = '09' then 0.22118915334911
        when  to_char(stat_month,'mm') = '10' then 0.24572878744634
        when  to_char(stat_month,'mm') = '11' then 0.25124653780634
        when  to_char(stat_month,'mm') = '12' then 0.30443364839057
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 0.68324964960319
        when  to_char(stat_month,'mm') = '02' then 0.29044343042509
        when  to_char(stat_month,'mm') = '03' then 0.39878942735022
        when  to_char(stat_month,'mm') = '04' then 0.35524291994374
        when  to_char(stat_month,'mm') = '05' then 0.36789582890245
        when  to_char(stat_month,'mm') = '06' then 0.38505352303754
        when  to_char(stat_month,'mm') = '07' then 0.52008288823198
        when  to_char(stat_month,'mm') = '08' then 0.23336807460202
        when  to_char(stat_month,'mm') = '09' then 0.23118915334911
        when  to_char(stat_month,'mm') = '10' then 0.25572878744634
        when  to_char(stat_month,'mm') = '11' then 0.26124653780634
        when  to_char(stat_month,'mm') = '12' then 0.31443364839057
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 0.69324964960319
        when  to_char(stat_month,'mm') = '02' then 0.30044343042509
        when  to_char(stat_month,'mm') = '03' then 0.40878942735022
        when  to_char(stat_month,'mm') = '04' then 0.36524291994374
        when  to_char(stat_month,'mm') = '05' then 0.37789582890245
        when  to_char(stat_month,'mm') = '06' then 0.39505352303754
        when  to_char(stat_month,'mm') = '07' then 0.53008288823198
        when  to_char(stat_month,'mm') = '08' then 0.24336807460202
        when  to_char(stat_month,'mm') = '09' then 0.24118915334911
        when  to_char(stat_month,'mm') = '10' then 0.26572878744634
        when  to_char(stat_month,'mm') = '11' then 0.27124653780634
        when  to_char(stat_month,'mm') = '12' then 0.32443364839057
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '人：户部（用户服务）用户护卫队（客服）' as 部门
	 ,'嘎嘎' as 负责人
	 ,stat_month as 季度or月份
	 ,'售后成本率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 < goods_value_target_s then 'S'
            when t3.当前实际值 < goods_value_target_a then 'A'
            when t3.当前实际值 < goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/当前实际值) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/当前实际值) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/当前实际值) * 100,2)),'%') as B级达成率
	 ,'错漏发等物流问题（/实发GMV）	质量问题（/实发GMV）	其他问题（/实发GMV）	附加补偿（/实发GMV）' as  指标定义
	 ,0.10 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '人：户部（用户服务）用户护卫队（客服）' as 部门
	 ,'嘎嘎' as 负责人
	 ,stat_month  as 季度or月份
	 ,'售后成本率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 < goods_value_target_s then 'S'
            when t2.当前实际值 < goods_value_target_a then 'A'
            when t2.当前实际值 < goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/当前实际值) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/当前实际值) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/当前实际值) * 100,2)),'%') as B级达成率
	 ,'错漏发等物流问题（/实发GMV）	质量问题（/实发GMV）	其他问题（/实发GMV）	附加补偿（/实发GMV）' as  指标定义
	 ,0.10 as 权重
	 from t2
;

-- 人：户部（用户服务） 招生办（新客） --> 潜力大客数
WITH t1 as (
    SELECT  to_char(to_date1(dt,'yyyymmdd'),'yyyy-mm') as stat_month
            ,sum(潜力大客数)  as 潜力大客数
            ,QUARTER(to_char(to_date1(dt,'yyyymmdd'),'yyyy-mm')) as QUA
    from (
        select
            to_char(x1.real_first_time,'yyyymmdd') dt
            ,count(distinct case when x1.real_first_30d_buy_amount >= 1000 then x1.user_id end) as 潜力大客数
        from yishou_data.dim_ys_user_order_data x1
        WHERE to_char(x1.real_first_time,'yyyymmdd') >= '20230101'
        group by 1
    )
    GROUP BY to_char(to_date1(dt,'yyyymmdd'),'yyyy-mm')
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(潜力大客数) as 当前实际值
            ,case when QUA = 1 then 8449
             when QUA = 2 then 9979
             when QUA = 3 then 9215
             when QUA = 4 then 16713
            end  as goods_value_target_s
            ,case when QUA = 1 then 7754
             when QUA = 2 then 9159
             when QUA = 3 then 8457
             when QUA = 4 then 15340
            end  as goods_value_target_a
            ,case when QUA = 1 then 7060
             when QUA = 2 then 8339
             when QUA = 3 then 7700
             when QUA = 4 then 13966
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,潜力大客数 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 2393
        when  to_char(stat_month,'mm') = '02' then 2034
        when  to_char(stat_month,'mm') = '03' then 4021
        when  to_char(stat_month,'mm') = '04' then 3830
        when  to_char(stat_month,'mm') = '05' then 3397
        when  to_char(stat_month,'mm') = '06' then 2752
        when  to_char(stat_month,'mm') = '07' then 2154
        when  to_char(stat_month,'mm') = '08' then 3231
        when  to_char(stat_month,'mm') = '09' then 3830
        when  to_char(stat_month,'mm') = '10' then 5385
        when  to_char(stat_month,'mm') = '11' then 5984
        when  to_char(stat_month,'mm') = '12' then 5345
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 2197
        when  to_char(stat_month,'mm') = '02' then 1867
        when  to_char(stat_month,'mm') = '03' then 3690
        when  to_char(stat_month,'mm') = '04' then 3515
        when  to_char(stat_month,'mm') = '05' then 3118
        when  to_char(stat_month,'mm') = '06' then 2526
        when  to_char(stat_month,'mm') = '07' then 1977
        when  to_char(stat_month,'mm') = '08' then 2966
        when  to_char(stat_month,'mm') = '09' then 3515
        when  to_char(stat_month,'mm') = '10' then 4943
        when  to_char(stat_month,'mm') = '11' then 5492
        when  to_char(stat_month,'mm') = '12' then 4905
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 2000
        when  to_char(stat_month,'mm') = '02' then 1700
        when  to_char(stat_month,'mm') = '03' then 3360
        when  to_char(stat_month,'mm') = '04' then 3200
        when  to_char(stat_month,'mm') = '05' then 2839
        when  to_char(stat_month,'mm') = '06' then 2300
        when  to_char(stat_month,'mm') = '07' then 1800
        when  to_char(stat_month,'mm') = '08' then 2700
        when  to_char(stat_month,'mm') = '09' then 3200
        when  to_char(stat_month,'mm') = '10' then 4500
        when  to_char(stat_month,'mm') = '11' then 5000
        when  to_char(stat_month,'mm') = '12' then 4466
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '人：户部（用户服务）招生办（新客）' as 部门
	 ,'守诚' as 负责人
	 ,stat_month as 季度or月份
	 ,'潜力大客数' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"用户首单后30天内实际GMV大于等于1000+的用户数（含首单当天）-全尾号" as  指标定义
	 ,0.40 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '人：户部（用户服务）招生办（新客）' as 部门
	 ,'守诚' as 负责人
	 ,stat_month  as 季度or月份
	 ,'潜力大客数' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"用户首单后30天内实际GMV大于等于1000+的用户数（含首单当天）-全尾号" as  指标定义
	 ,0.40 as 权重
	 from t2
;



-- 人：户部（用户服务） 招生办（新客） --> 	守诚 : 有效新客数
with t1 (
	SELECT  to_char(to_date1(cur_date,'yyyymmdd'),'yyyy-mm') as stat_month
			, sum(eff_new_user) as 有效新客数
			,QUARTER((to_char(to_date1(cur_date,'yyyymmdd'),'yyyy-mm'))) as QUA
	from cross_origin.newuser_daily_report_data_yk_v6
	where to_char(to_date1(cur_date,'yyyymmdd'),'yyyy-mm') >= '2023-01'
	and tail = '全部尾号'
	GROUP BY to_char(to_date1(cur_date,'yyyymmdd'),'yyyy-mm')
)
,t2 as  (
    SELECT   concat(to_char(max(stat_month),'yyyy'),'Q',QUA) as stat_month
            ,sum(有效新客数) as 当前实际值
            ,case when QUA = 1 then 27159
             when QUA = 2 then 32080
             when QUA = 3 then 29622
             when QUA = 4 then 53738
            end  as goods_value_target_s
            ,case when QUA = 1 then 24927
             when QUA = 2 then 29443
             when QUA = 3 then 27187
             when QUA = 4 then 49321
            end  as goods_value_target_a
            ,case when QUA = 1 then 22695
             when QUA = 2 then 26806
             when QUA = 3 then 24752
             when QUA = 4 then 44905
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,有效新客数 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 7694
        when  to_char(stat_month,'mm') = '02' then 6540
        when  to_char(stat_month,'mm') = '03' then 12926
        when  to_char(stat_month,'mm') = '04' then 12310
        when  to_char(stat_month,'mm') = '05' then 10922
        when  to_char(stat_month,'mm') = '06' then 8848
        when  to_char(stat_month,'mm') = '07' then 6925
        when  to_char(stat_month,'mm') = '08' then 10387
        when  to_char(stat_month,'mm') = '09' then 12310
        when  to_char(stat_month,'mm') = '10' then 17323
        when  to_char(stat_month,'mm') = '11' then 19235
        when  to_char(stat_month,'mm') = '12' then 17180
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 7062
        when  to_char(stat_month,'mm') = '02' then 6002
        when  to_char(stat_month,'mm') = '03' then 11863
        when  to_char(stat_month,'mm') = '04' then 11298
        when  to_char(stat_month,'mm') = '05' then 10024
        when  to_char(stat_month,'mm') = '06' then 8121
        when  to_char(stat_month,'mm') = '07' then 6355
        when  to_char(stat_month,'mm') = '08' then 9533
        when  to_char(stat_month,'mm') = '09' then 11298
        when  to_char(stat_month,'mm') = '10' then 15899
        when  to_char(stat_month,'mm') = '11' then 17654
        when  to_char(stat_month,'mm') = '12' then 15768
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 6429
        when  to_char(stat_month,'mm') = '02' then 5465
        when  to_char(stat_month,'mm') = '03' then 10801
        when  to_char(stat_month,'mm') = '04' then 10287
        when  to_char(stat_month,'mm') = '05' then 9126
        when  to_char(stat_month,'mm') = '06' then 7394
        when  to_char(stat_month,'mm') = '07' then 5786
        when  to_char(stat_month,'mm') = '08' then 8679
        when  to_char(stat_month,'mm') = '09' then 10287
        when  to_char(stat_month,'mm') = '10' then 14476
        when  to_char(stat_month,'mm') = '11' then 16073
        when  to_char(stat_month,'mm') = '12' then 14356
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '人：户部（用户服务）招生办（新客）' as 部门
	 ,'守诚' as 负责人
	 ,stat_month as 季度or月份
	 ,'有效新客数' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"首单后14天内实际GMV大于等于300的用户数（含首单当天）--全尾号" as  指标定义
	 ,0.40 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '人：户部（用户服务）招生办（新客）' as 部门
	 ,'守诚' as 负责人
	 ,stat_month  as 季度or月份
	 ,'有效新客数' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"首单后14天内实际GMV大于等于300的用户数（含首单当天）--全尾号" as  指标定义
	 ,0.40 as 权重
	 from t2
;	

-- 人：户部（用户服务） 招生办（新客） --> 	守诚 : 全年新客GMV
with t1 as (
    select 
        to_char(to_date1(dt,'yyyymmdd'),'yyyy-mm') as stat_month
        ,round(sum(cur_year_newuser_gmv),4) as `全年新客GMV`
        ,quarter(to_date1(dt,'yyyymmdd')) as QUA 
    from cross_origin.channel_section_data_yk x1 
    left join yishou_data.dim_calendar x2 on x1.dt = x2.date_str 
    where dt between concat(substr('${bizdate}',1,4),'0101') and to_char(last_day(to_date1(dt,'yyyymmdd')),'yyyymmdd')
    and tail in ('0', '2', '3', '5', '6', '7') 
    group by to_char(to_date1(dt,'yyyymmdd'),'yyyy-mm'),quarter(to_date1(dt,'yyyymmdd'))
)
,t2 as  (
    select   
        concat(to_char(max(stat_month),'yyyy'),'Q',QUA) as stat_month
        ,max(`全年新客GMV`) as 当前实际值
        ,case 
            when QUA = 1 then 56962229
            when QUA = 2 then 178564488
            when QUA = 3 then 341990713
            when QUA = 4 then 549260519
        end as goods_value_target_s
        ,case 
            when QUA = 1 then 52283319
            when QUA = 2 then 163890930
            when QUA = 3 then 313888941
            when QUA = 4 then 609772271
        end as goods_value_target_a
        ,case 
            when QUA = 1 then 45539895
            when QUA = 2 then 145693226
            when QUA = 3 then 280610507
            when QUA = 4 then 660974523
        end as goods_value_target_b
    from t1 
    GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,全年新客GMV as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 11962350
        when  to_char(stat_month,'mm') = '02' then 28221490
        when  to_char(stat_month,'mm') = '03' then 56962229
        when  to_char(stat_month,'mm') = '04' then 94108266
        when  to_char(stat_month,'mm') = '05' then 136322293
        when  to_char(stat_month,'mm') = '06' then 178564488
        when  to_char(stat_month,'mm') = '07' then 212660016
        when  to_char(stat_month,'mm') = '08' then 269459432
        when  to_char(stat_month,'mm') = '09' then 341990713
        when  to_char(stat_month,'mm') = '10' then 436453682
        when  to_char(stat_month,'mm') = '11' then 551014564
        when  to_char(stat_month,'mm') = '12' then 660974523
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 10980719
        when  to_char(stat_month,'mm') = '02' then 25905156
        when  to_char(stat_month,'mm') = '03' then 52283319
        when  to_char(stat_month,'mm') = '04' then 86375584
        when  to_char(stat_month,'mm') = '05' then 125120436
        when  to_char(stat_month,'mm') = '06' then 195184800
        when  to_char(stat_month,'mm') = '07' then 247317576
        when  to_char(stat_month,'mm') = '08' then 247317576
        when  to_char(stat_month,'mm') = '09' then 313888941
        when  to_char(stat_month,'mm') = '10' then 400590015
        when  to_char(stat_month,'mm') = '11' then 505737328
        when  to_char(stat_month,'mm') = '12' then 609772271
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 8540399
        when  to_char(stat_month,'mm') = '02' then 21278398
        when  to_char(stat_month,'mm') = '03' then 45539895
        when  to_char(stat_month,'mm') = '04' then 76445901
        when  to_char(stat_month,'mm') = '05' then 111217082
        when  to_char(stat_month,'mm') = '06' then 145693226
        when  to_char(stat_month,'mm') = '07' then 173910072
        when  to_char(stat_month,'mm') = '08' then 220895120
        when  to_char(stat_month,'mm') = '09' then 280610507
        when  to_char(stat_month,'mm') = '10' then 359632759
        when  to_char(stat_month,'mm') = '11' then 455849534
        when  to_char(stat_month,'mm') = '12' then 549260519
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '人：户部（用户服务）招生办（新客）' as 部门
	 ,'守诚' as 负责人
	 ,stat_month as 季度or月份
	 ,'全年新客GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"24年实际首单用户的全年GMV加总（含首单当天）" as  指标定义
	 ,0.20 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '人：户部（用户服务）招生办（新客）' as 部门
	 ,'守诚' as 负责人
	 ,stat_month  as 季度or月份
	 ,'全年新客GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"24年实际首单用户的全年GMV加总（含首单当天）" as  指标定义
	 ,0.20 as 权重
	 from t2
;

-- 人：户部（用户服务） 万元俱乐部（私域）波波 5000+用户次月保级率
WITH t1 as (
    SELECT  
        to_char(to_date1(a.mon,'yyyymm'),'yyyy-mm') as stat_month
        ,round(b.5K加次月保级率,4) as 5K加次月保级率
        ,QUARTER(to_date1(a.mon,'yyyymm')) as QUA
    from (
        SELECT  mon
                ,max(dt) as dt
        from cross_origin.last_5k_still_5k_users_goal_dt
        where if_private = '全量'
        and dt >= '20230101'
        GROUP BY mon
    ) a LEFT JOIN (
        select 
            mon 月份
            ,if_private 是否私域
            ,last_5k_users 上月5k用户数
            ,still_5k_new_users_day 日新增达成5k
            ,still_5k_mon 本月达成保级人数
            ,no_still_5k_mon 本月未达成保级人数
            ,still_5k_rate 5K加次月保级率
            ,goal_gap 缺口_A级目标
            -- ,case when goal_gap is null then 0 else goal_gap end 缺口_A级目标
            ,to_date1(dt,'yyyymmdd') 日期
            ,dt
        from cross_origin.last_5k_still_5k_users_goal_dt x1
        where dt >= '20230101'
        and if_private = '全量'
    ) b on a.dt = b.dt  
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,max(5K加次月保级率) * 100 as 当前实际值
            ,case when QUA = 1 then 40.4795
             when QUA = 2 then 46.0298
             when QUA = 3 then 43.3586
             when QUA = 4 then 58.3148
            end  as goods_value_target_s
            ,case when QUA = 1 then 37.4601
             when QUA = 2 then 43.9045
             when QUA = 3 then 42.5214
             when QUA = 4 then 56.7081
            end  as goods_value_target_a
            ,case when QUA = 1 then 34.7326
             when QUA = 2 then 41.7426
             when QUA = 3 then 41.6118
             when QUA = 4 then 54.9984
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,5K加次月保级率 * 100  as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 36.1358
        when  to_char(stat_month,'mm') = '02' then 37.8058
        when  to_char(stat_month,'mm') = '03' then 47.2501
        when  to_char(stat_month,'mm') = '04' then 48.7919
        when  to_char(stat_month,'mm') = '05' then 47.2529
        when  to_char(stat_month,'mm') = '06' then 41.8872
        when  to_char(stat_month,'mm') = '07' then 30.0281
        when  to_char(stat_month,'mm') = '08' then 46.7961
        when  to_char(stat_month,'mm') = '09' then 52.4777
        when  to_char(stat_month,'mm') = '10' then 57.6347
        when  to_char(stat_month,'mm') = '11' then 58.2157
        when  to_char(stat_month,'mm') = '12' then 59.1256
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 33.0725
        when  to_char(stat_month,'mm') = '02' then 34.8905
        when  to_char(stat_month,'mm') = '03' then 44.2862
        when  to_char(stat_month,'mm') = '04' then 46.1052
        when  to_char(stat_month,'mm') = '05' then 45.0652
        when  to_char(stat_month,'mm') = '06' then 40.3517
        when  to_char(stat_month,'mm') = '07' then 29.2151
        when  to_char(stat_month,'mm') = '08' then 46.0351
        when  to_char(stat_month,'mm') = '09' then 51.7002
        when  to_char(stat_month,'mm') = '10' then 56.7903
        when  to_char(stat_month,'mm') = '11' then 57.443
        when  to_char(stat_month,'mm') = '12' then 55.8702
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 30.2133
        when  to_char(stat_month,'mm') = '02' then 32.5144
        when  to_char(stat_month,'mm') = '03' then 41.4085
        when  to_char(stat_month,'mm') = '04' then 43.4413
        when  to_char(stat_month,'mm') = '05' then 42.8403
        when  to_char(stat_month,'mm') = '06' then 38.7404
        when  to_char(stat_month,'mm') = '07' then 28.3307
        when  to_char(stat_month,'mm') = '08' then 45.2199
        when  to_char(stat_month,'mm') = '09' then 50.8549
        when  to_char(stat_month,'mm') = '10' then 55.8738
        when  to_char(stat_month,'mm') = '11' then 56.5991
        when  to_char(stat_month,'mm') = '12' then 52.599
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '人：户部（用户服务）' as 部门  
	 ,'波波' as 负责人
	 ,stat_month as 季度or月份
	 ,'5000+用户次月保级率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"近6个月5k+且当月5k+的用户（去重）/近6个月实际支付5k+的用户（去重） （年同比=24年保级率/23年保级率-1）" as  指标定义
	 ,0.40 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '人：户部（用户服务）' as 部门
	 ,'波波' as 负责人
	 ,stat_month  as 季度or月份
	 ,'5000+用户次月保级率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"近6个月5k+且当月5k+的用户（去重）/近6个月实际支付5k+的用户（去重） （年同比=24年保级率/23年保级率-1）" as  指标定义
	 ,0.40 as 权重
	 from t2
;



-- 人：户部（用户服务 ） 万元俱乐部（私域） --> 波波 -私域用户满意度
-- 暂无取数路径,跳过


--  货：供给 --> 十三行  --> 面包 --> 实际GMV (供给部门筛选: 1十三行) 
with t1 as (
    select to_char(special_date, 'yyyy-mm')          as stat_month,
       sum(real_buy_amount)                        实际GMV,
       QUARTER(to_char(special_date, 'yyyy-mm')) as QUA
    from yishou_data.dwm_goods_sp_up_info_dt
    where dt >= '20230101'
    and department = '1十三行'
    GROUP BY to_char(special_date, 'yyyy-mm')
)
,t2 as  (
    SELECT   concat(to_char(max(stat_month),'yyyy'),'Q',QUA) as stat_month
            ,sum(实际GMV) as 当前实际值
            ,QUA
            ,case when QUA = 1 then 336950540.2
             when QUA = 2 then 414235259.07
             when QUA = 3 then 344159547.35
             when QUA = 4 then 490734816.15
            end  as goods_value_target_s
            ,case when QUA = 1 then 311917166.86
             when QUA = 2 then 383551165.8
             when QUA = 3 then 318666247.54
             when QUA = 4 then 454384089.02
            end  as goods_value_target_a
            ,case when QUA = 1 then 286883793.52
             when QUA = 2 then 352867072.54
             when QUA = 3 then 304250644.92
             when QUA = 4 then 418033361.9
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,实际GMV as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 87142653.18
        when  to_char(stat_month,'mm') = '02' then 95439908.52
        when  to_char(stat_month,'mm') = '03' then 154367978.5
        when  to_char(stat_month,'mm') = '04' then 152763716.45
        when  to_char(stat_month,'mm') = '05' then 144558011.83
        when  to_char(stat_month,'mm') = '06' then 116913530.79
        when  to_char(stat_month,'mm') = '07' then 70021432.79
        when  to_char(stat_month,'mm') = '08' then 124589202.59
        when  to_char(stat_month,'mm') = '09' then 149548911.97
        when  to_char(stat_month,'mm') = '10' then 171855834.34
        when  to_char(stat_month,'mm') = '11' then 169160648.21
        when  to_char(stat_month,'mm') = '12' then 149718333.6
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 80613567.76
        when  to_char(stat_month,'mm') = '02' then 88370285.67
        when  to_char(stat_month,'mm') = '03' then 142933313.43
        when  to_char(stat_month,'mm') = '04' then 141447885.6
        when  to_char(stat_month,'mm') = '05' then 133850010.95
        when  to_char(stat_month,'mm') = '06' then 108253269.25
        when  to_char(stat_month,'mm') = '07' then 64834659.99
        when  to_char(stat_month,'mm') = '08' then 115360372.77
        when  to_char(stat_month,'mm') = '09' then 138471214.78
        when  to_char(stat_month,'mm') = '10' then 159125772.53
        when  to_char(stat_month,'mm') = '11' then 156630229.82
        when  to_char(stat_month,'mm') = '12' then 138628086.67
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 74084482.34
        when  to_char(stat_month,'mm') = '02' then 81300662.82
        when  to_char(stat_month,'mm') = '03' then 131498648.36
        when  to_char(stat_month,'mm') = '04' then 130132054.75
        when  to_char(stat_month,'mm') = '05' then 123142010.08
        when  to_char(stat_month,'mm') = '06' then 99593007.71
        when  to_char(stat_month,'mm') = '07' then 59647887.19
        when  to_char(stat_month,'mm') = '08' then 106131542.95
        when  to_char(stat_month,'mm') = '09' then 127393517.6
        when  to_char(stat_month,'mm') = '10' then 146395710.73
        when  to_char(stat_month,'mm') = '11' then 144099811.44
        when  to_char(stat_month,'mm') = '12' then 127537839.73
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '货：供给 十三行' as 部门
	 ,'面包' as 负责人
	 ,stat_month as 季度or月份
	 ,'实际GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"自然月对应市场团队的总实际GMV" as  指标定义
	 ,case when SUBSTR(stat_month,5,2) BETWEEN '01' and '04' THEN  0.50
	    ELSE  0.30 end as 权重
	 from t3
    UNION ALL 
    SELECT 
     '货：供给 十三行' as 部门
	 ,'面包' as 负责人
	 ,stat_month  as 季度or月份
	 ,'实际GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"自然月对应市场团队的总实际GMV" as  指标定义
	 ,case when QUA = 1 then 0.50
	  when QUA = 2 then 0.50/3 + 0.30/3 +0.30/3
	  else 0.30 end as 权重	
	 from t2
;



-- 货：供给 --> 十三行  --> 面包 --> 潜力商家
WITH t1 as (
	SELECT  to_date1(月份,'yyyymm') as stat_month 
			-- , max(供应商ID)
			-- , max(潜力类型)
			-- , min(潜力类型)
			, sum(潜力商家得分) as 潜力商家得分
			-- ,count(1)
			-- ,count(DISTINCT  供应商ID)
			,QUARTER(to_date1(月份,'yyyymm') ) as QUA
	from (SELECT mt                                                                           月份
			, supply_id                                                                    供应商ID
			, supply_name                                                                  供应商名称
			, department                                                                   责任部门
			, primary_market_name                                                          一级市场
			, big_market                                                                   大市场
			, big_market_ql                                                                对比大市场
			, poten_score                                                                  潜力商家得分
			, case
					when real_buy_amount <= 500000 AND poten_score > 0 then '达成双潜力'
					when poten_score > 0 and real_buy_amount > 500000 then '达成50W潜力' end 潜力类型
			, uv_value                                                                     当月UV价值
			, big_market_ql_uv_value                                                       当月对照大市场UV价值
			, big_market_uv_value                                                          当月大市场UV价值
			, department_uv_value                                                          当月部门UV价值
			, buy_amount                                                                   当月gmv
			, real_buy_amount                                                              当月实际gmv
			, poten_score_this_mon                                                         当月是否符合要求
			, poten_score_11_mons                                                          过去11个月是否符合要求
		FROM yishou_daily.finebi_dwd_poten_supply_summary_mt t1
		where mt >= 202301) a
	where 责任部门 = '1十三行'
	GROUP BY 月份
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(潜力商家得分) as 当前实际值
            ,QUA
            ,case when QUA = 1 then 34
             when QUA = 2 then 32
             when QUA = 3 then 23
             when QUA = 4 then 51
            end  as goods_value_target_s
            ,case when QUA = 1 then 31
             when QUA = 2 then 30
             when QUA = 3 then 21
             when QUA = 4 then 49
            end  as goods_value_target_a
            ,case when QUA = 1 then 29
             when QUA = 2 then 28
             when QUA = 3 then 19
             when QUA = 4 then 47
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,潜力商家得分 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 8
        when  to_char(stat_month,'mm') = '02' then 16
        when  to_char(stat_month,'mm') = '03' then 10
        when  to_char(stat_month,'mm') = '04' then 11
        when  to_char(stat_month,'mm') = '05' then 9
        when  to_char(stat_month,'mm') = '06' then 12
        when  to_char(stat_month,'mm') = '07' then 5
        when  to_char(stat_month,'mm') = '08' then 9
        when  to_char(stat_month,'mm') = '09' then 9
        when  to_char(stat_month,'mm') = '10' then 17
        when  to_char(stat_month,'mm') = '11' then 23
        when  to_char(stat_month,'mm') = '12' then 11
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 7
        when  to_char(stat_month,'mm') = '02' then 15
        when  to_char(stat_month,'mm') = '03' then 9
        when  to_char(stat_month,'mm') = '04' then 11
        when  to_char(stat_month,'mm') = '05' then 8
        when  to_char(stat_month,'mm') = '06' then 11
        when  to_char(stat_month,'mm') = '07' then 5
        when  to_char(stat_month,'mm') = '08' then 8
        when  to_char(stat_month,'mm') = '09' then 8
        when  to_char(stat_month,'mm') = '10' then 16
        when  to_char(stat_month,'mm') = '11' then 22
        when  to_char(stat_month,'mm') = '12' then 11
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 6
        when  to_char(stat_month,'mm') = '02' then 14
        when  to_char(stat_month,'mm') = '03' then 9
        when  to_char(stat_month,'mm') = '04' then 10
        when  to_char(stat_month,'mm') = '05' then 7
        when  to_char(stat_month,'mm') = '06' then 11
        when  to_char(stat_month,'mm') = '07' then 4
        when  to_char(stat_month,'mm') = '08' then 7
        when  to_char(stat_month,'mm') = '09' then 8
        when  to_char(stat_month,'mm') = '10' then 15
        when  to_char(stat_month,'mm') = '11' then 21
        when  to_char(stat_month,'mm') = '12' then 11
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '货：供给 十三行' as 部门
	 ,'面包' as 负责人
	 ,stat_month as 季度or月份
	 ,'潜力商家' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s and goods_value_target_s > goods_value_target_a then 'S'
            when t3.当前实际值 >= goods_value_target_a and goods_value_target_a > goods_value_target_b then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"累计12个月实际GMV首次达到20万+、50万+的商家总数量；若直接从20万以下直接做到50万的，算2家；实际UV价值大于所在市场的实际UV价值平均值" as  指标定义
	 ,case when SUBSTR(stat_month,5,2) BETWEEN '01' and '04' THEN  0.20
	    ELSE  0.10 end as 权重
	 from t3
    UNION ALL 
    SELECT 
     '货：供给 十三行' as 部门
	 ,'面包' as 负责人
	 ,stat_month  as 季度or月份
	 ,'潜力商家' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s and goods_value_target_s > goods_value_target_a then 'S'
            when t2.当前实际值 >= goods_value_target_a and goods_value_target_a > goods_value_target_b then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"累计12个月实际GMV首次达到20万+、50万+的商家总数量；若直接从20万以下直接做到50万的，算2家；实际UV价值大于所在市场的实际UV价值平均值" as  指标定义
	 ,case when QUA = 1 then 0.20
	  when QUA = 2 then 0.20/3 + 0.10/3 +0.10/3
	  else 0.10 end as 权重
	 from t2
;

-- 货：供给 --> 十三行  --> 面包 盈利额 ( 责任归属 筛选: 1十三行)
with earning as (
    select 年月
    ,责任归属
    ,大市场
    ,sum(佣金)+sum(退换货服务费)-sum(佣金返还) 平台化收益
    from yishou_daily.finebi_cwf_supplier_earnings_monthly 
    WHERE 责任归属 = '1十三行'
    group by 年月,责任归属,大市场)
,bargain as (
    select 年月
    ,责任归属
    ,大市场
    ,sum(议价金额) 议价金额
    from yishou_daily.finebi_cwf_maishouyijia_monthly 
    WHERE 责任归属 = '1十三行'
    group by 年月,责任归属,大市场
)
, t1 as (
	select to_char(to_date1(b.年月,'yyyymm'),'yyyy-mm') as stat_month
	-- ,b.大市场
	,b.责任归属
	,sum(b.平台化收益) + sum(c.议价金额) as 盈利额_面包
	,QUARTER(to_date1(b.年月,'yyyymm')) as QUA
	from     earning b 
	left join bargain c on b.年月=c.年月 and b.责任归属=c.责任归属 and b.大市场=c.大市场
	where b.年月>=202301
	GROUP BY b.年月,b.责任归属
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(盈利额_面包) as 当前实际值
            ,QUA
            ,case when QUA = 1 then 12363137
             when QUA = 2 then 17176110
             when QUA = 3 then 14027497
             when QUA = 4 then 20284964
            end  as goods_value_target_s
            ,case when QUA = 1 then 11817598
             when QUA = 2 then 16295568
             when QUA = 3 then 13264239
             when QUA = 4 then 18961015
            end  as goods_value_target_a
            ,case when QUA = 1 then 11314500
             when QUA = 2 then 15628900
             when QUA = 3 then 12731400
             when QUA = 4 then 18185300
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,盈利额_面包 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 2466122
        when  to_char(stat_month,'mm') = '02' then 2873327
        when  to_char(stat_month,'mm') = '03' then 7023688
        when  to_char(stat_month,'mm') = '04' then 6193616
        when  to_char(stat_month,'mm') = '05' then 5938209
        when  to_char(stat_month,'mm') = '06' then 5044285
        when  to_char(stat_month,'mm') = '07' then 2725745
        when  to_char(stat_month,'mm') = '08' then 5044285
        when  to_char(stat_month,'mm') = '09' then 6257467
        when  to_char(stat_month,'mm') = '10' then 7215243
        when  to_char(stat_month,'mm') = '11' then 7323067
        when  to_char(stat_month,'mm') = '12' then 5746654
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 2427959
        when  to_char(stat_month,'mm') = '02' then 2726024
        when  to_char(stat_month,'mm') = '03' then 6663615
        when  to_char(stat_month,'mm') = '04' then 5876097
        when  to_char(stat_month,'mm') = '05' then 5633784
        when  to_char(stat_month,'mm') = '06' then 4785687
        when  to_char(stat_month,'mm') = '07' then 2541876
        when  to_char(stat_month,'mm') = '08' then 4785687
        when  to_char(stat_month,'mm') = '09' then 5936676
        when  to_char(stat_month,'mm') = '10' then 6845350
        when  to_char(stat_month,'mm') = '11' then 6903515
        when  to_char(stat_month,'mm') = '12' then 5452049
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 2309000
        when  to_char(stat_month,'mm') = '02' then 2614500
        when  to_char(stat_month,'mm') = '03' then 6391000
        when  to_char(stat_month,'mm') = '04' then 5635700
        when  to_char(stat_month,'mm') = '05' then 5403300
        when  to_char(stat_month,'mm') = '06' then 4589900
        when  to_char(stat_month,'mm') = '07' then 2447700
        when  to_char(stat_month,'mm') = '08' then 4589900
        when  to_char(stat_month,'mm') = '09' then 5693800
        when  to_char(stat_month,'mm') = '10' then 6565300
        when  to_char(stat_month,'mm') = '11' then 6630900
        when  to_char(stat_month,'mm') = '12' then 5229000
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '货：供给 十三行' as 部门
	 ,'面包' as 负责人
	 ,stat_month as 季度or月份
	 ,'盈利额_面包' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"自然月平台化收入与买手议价金额" as  指标定义
	 ,0.15 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '货：供给 十三行' as 部门
	 ,'面包' as 负责人
	 ,stat_month  as 季度or月份
	 ,'盈利额_面包' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"自然月平台化收入与买手议价金额" as  指标定义
	 ,0.15 as 权重
	 from t2
;




    

--  货：供给  -->  沙河 --> 天真    实际GMV -- 供给部门 筛选: 2沙河南
with t1 as ( 
    select to_char(special_date, 'yyyy-mm')          as stat_month,
       sum(real_buy_amount)                        实际GMV,
       QUARTER(to_char(special_date, 'yyyy-mm')) as QUA
    from yishou_data.dwm_goods_sp_up_info_dt
    where dt >= '20230101'
    and department = '2沙河南'
    GROUP BY to_char(special_date, 'yyyy-mm')
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(实际GMV) as 当前实际值
            ,QUA
            ,case when QUA = 1 then 271315909.93
             when QUA = 2 then 359555313.36
             when QUA = 3 then 350473252.64
             when QUA = 4 then 497911892.14
            end  as goods_value_target_s
            ,case when QUA = 1 then 251218435.12
             when QUA = 2 then 332921586.44
             when QUA = 3 then 324512270.96
             when QUA = 4 then 461029529.77
            end  as goods_value_target_a
            ,case when QUA = 1 then 231120960.31
             when QUA = 2 then 306287859.52
             when QUA = 3 then 298551289.28
             when QUA = 4 then 424147167.38
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,实际GMV as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 79041065.76
        when  to_char(stat_month,'mm') = '02' then 74625565.18
        when  to_char(stat_month,'mm') = '03' then 117649278.99
        when  to_char(stat_month,'mm') = '04' then 129729705.01
        when  to_char(stat_month,'mm') = '05' then 122089748.7
        when  to_char(stat_month,'mm') = '06' then 107735859.65
        when  to_char(stat_month,'mm') = '07' then 83894914.92
        when  to_char(stat_month,'mm') = '08' then 121447698.59
        when  to_char(stat_month,'mm') = '09' then 145130639.13
        when  to_char(stat_month,'mm') = '10' then 168622475.85
        when  to_char(stat_month,'mm') = '11' then 172375837.14
        when  to_char(stat_month,'mm') = '12' then 156913579.15
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 73186172
        when  to_char(stat_month,'mm') = '02' then 69097745.54
        when  to_char(stat_month,'mm') = '03' then 108934517.58
        when  to_char(stat_month,'mm') = '04' then 120120097.23
        when  to_char(stat_month,'mm') = '05' then 113046063.61
        when  to_char(stat_month,'mm') = '06' then 99755425.6
        when  to_char(stat_month,'mm') = '07' then 77680476.78
        when  to_char(stat_month,'mm') = '08' then 112451572.76
        when  to_char(stat_month,'mm') = '09' then 134380221.42
        when  to_char(stat_month,'mm') = '10' then 156131922.09
        when  to_char(stat_month,'mm') = '11' then 159607256.61
        when  to_char(stat_month,'mm') = '12' then 145290351.07
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 67331278.24
        when  to_char(stat_month,'mm') = '02' then 63569925.9
        when  to_char(stat_month,'mm') = '03' then 100219756.17
        when  to_char(stat_month,'mm') = '04' then 110510489.45
        when  to_char(stat_month,'mm') = '05' then 104002378.52
        when  to_char(stat_month,'mm') = '06' then 91774991.55
        when  to_char(stat_month,'mm') = '07' then 71466038.63
        when  to_char(stat_month,'mm') = '08' then 103455446.94
        when  to_char(stat_month,'mm') = '09' then 123629803.71
        when  to_char(stat_month,'mm') = '10' then 143641368.32
        when  to_char(stat_month,'mm') = '11' then 146838676.08
        when  to_char(stat_month,'mm') = '12' then 133667122.98
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '货：供给 沙河' as 部门
	 ,'天真' as 负责人
	 ,stat_month as 季度or月份
	 ,'实际GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"自然月对应市场团队的总实际GMV" as  指标定义
	 ,case when SUBSTR(stat_month,5,2) BETWEEN '01' and '04' THEN  0.50
	    ELSE  0.30 end as 权重
	 from t3
    UNION ALL 
    SELECT 
     '货：供给 沙河' as 部门
	 ,'天真' as 负责人
	 ,stat_month  as 季度or月份
	 ,'实际GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"自然月对应市场团队的总实际GMV" as  指标定义
	 ,case when QUA = 1 then 0.50
	  when QUA = 2 then 0.50/3 + 0.30/3 +0.30/3
	  else 0.30 end as 权重	
	 from t2
;


--  货：供给  -->  沙河 --> 天真    潜力商家 (同 面包 : 潜力商家) 筛选 '2沙河南'
WITH t1 as (
	SELECT  to_date1(月份,'yyyymm') as stat_month 
			-- , max(供应商ID)
			-- , max(潜力类型)
			-- , min(潜力类型)
			, sum(潜力商家得分) as 潜力商家得分
			-- ,count(1)
			-- ,count(DISTINCT  供应商ID)
			,QUARTER(to_date1(月份,'yyyymm') ) as QUA
	from (SELECT mt                                                                           月份
			, supply_id                                                                    供应商ID
			, supply_name                                                                  供应商名称
			, department                                                                   责任部门
			, primary_market_name                                                          一级市场
			, big_market                                                                   大市场
			, big_market_ql                                                                对比大市场
			, poten_score                                                                  潜力商家得分
			, case
					when real_buy_amount <= 500000 AND poten_score > 0 then '达成双潜力'
					when poten_score > 0 and real_buy_amount > 500000 then '达成50W潜力' end 潜力类型
			, uv_value                                                                     当月UV价值
			, big_market_ql_uv_value                                                       当月对照大市场UV价值
			, big_market_uv_value                                                          当月大市场UV价值
			, department_uv_value                                                          当月部门UV价值
			, buy_amount                                                                   当月gmv
			, real_buy_amount                                                              当月实际gmv
			, poten_score_this_mon                                                         当月是否符合要求
			, poten_score_11_mons                                                          过去11个月是否符合要求
		FROM yishou_daily.finebi_dwd_poten_supply_summary_mt t1
		where mt >= 202301) a
	where 责任部门 = '2沙河南'
	GROUP BY 月份
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(潜力商家得分) as 当前实际值
            ,QUA
            ,case when QUA = 1 then 15
             when QUA = 2 then 26
             when QUA = 3 then 27
             when QUA = 4 then 42
            end  as goods_value_target_s
            ,case when QUA = 1 then 12
             when QUA = 2 then 24
             when QUA = 3 then 25
             when QUA = 4 then 40
            end  as goods_value_target_a
            ,case when QUA = 1 then 10
             when QUA = 2 then 22
             when QUA = 3 then 23
             when QUA = 4 then 38
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,潜力商家得分 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 4
        when  to_char(stat_month,'mm') = '02' then 4
        when  to_char(stat_month,'mm') = '03' then 7
        when  to_char(stat_month,'mm') = '04' then 9
        when  to_char(stat_month,'mm') = '05' then 9
        when  to_char(stat_month,'mm') = '06' then 8
        when  to_char(stat_month,'mm') = '07' then 6
        when  to_char(stat_month,'mm') = '08' then 11
        when  to_char(stat_month,'mm') = '09' then 10
        when  to_char(stat_month,'mm') = '10' then 11
        when  to_char(stat_month,'mm') = '11' then 17
        when  to_char(stat_month,'mm') = '12' then 14
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 3
        when  to_char(stat_month,'mm') = '02' then 3
        when  to_char(stat_month,'mm') = '03' then 6
        when  to_char(stat_month,'mm') = '04' then 9
        when  to_char(stat_month,'mm') = '05' then 8
        when  to_char(stat_month,'mm') = '06' then 11
        when  to_char(stat_month,'mm') = '07' then 6
        when  to_char(stat_month,'mm') = '08' then 8
        when  to_char(stat_month,'mm') = '09' then 9
        when  to_char(stat_month,'mm') = '10' then 11
        when  to_char(stat_month,'mm') = '11' then 16
        when  to_char(stat_month,'mm') = '12' then 13
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 2
        when  to_char(stat_month,'mm') = '02' then 2
        when  to_char(stat_month,'mm') = '03' then 6
        when  to_char(stat_month,'mm') = '04' then 8
        when  to_char(stat_month,'mm') = '05' then 7
        when  to_char(stat_month,'mm') = '06' then 7
        when  to_char(stat_month,'mm') = '07' then 5
        when  to_char(stat_month,'mm') = '08' then 9
        when  to_char(stat_month,'mm') = '09' then 9
        when  to_char(stat_month,'mm') = '10' then 10
        when  to_char(stat_month,'mm') = '11' then 15
        when  to_char(stat_month,'mm') = '12' then 13
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '货：供给 沙河' as 部门
	 ,'天真' as 负责人
	 ,stat_month as 季度or月份
	 ,'潜力商家' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s and goods_value_target_s > goods_value_target_a then 'S'
            when t3.当前实际值 >= goods_value_target_a and goods_value_target_a > goods_value_target_b then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"累计12个月实际GMV首次达到20万+、50万+的商家总数量；若直接从20万以下直接做到50万的，算2家；实际UV价值大于所在市场的实际UV价值平均值" as  指标定义
	 ,case when SUBSTR(stat_month,5,2) BETWEEN '01' and '04' THEN  0.20
	    ELSE  0.10 end as 权重
	 from t3
    UNION ALL 
    SELECT 
     '货：供给 沙河' as 部门
	 ,'天真' as 负责人
	 ,stat_month  as 季度or月份
	 ,'潜力商家' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s and goods_value_target_s > goods_value_target_a then 'S'
            when t2.当前实际值 >= goods_value_target_a and goods_value_target_a > goods_value_target_b then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"累计12个月实际GMV首次达到20万+、50万+的商家总数量；若直接从20万以下直接做到50万的，算2家；实际UV价值大于所在市场的实际UV价值平均值" as  指标定义
	 ,case when QUA = 1 then 0.20 
	  when QUA = 2 then 0.20 / 3 + 0.10 / 3 + 0.10 / 3
	  else 0.10 end as 权重	
	 from t2
;

--  货：供给  -->  沙河 --> 天真    盈利额 (责任归属 筛选: 2沙河南)
with earning as (
    select 年月
    ,责任归属
    ,大市场
    ,sum(佣金)+sum(退换货服务费)-sum(佣金返还) 平台化收益
    from yishou_daily.finebi_cwf_supplier_earnings_monthly 
    WHERE 责任归属 = '2沙河南'
    group by 年月,责任归属,大市场)
,bargain as (
    select 年月
    ,责任归属
    ,大市场
    ,sum(议价金额) 议价金额
    from yishou_daily.finebi_cwf_maishouyijia_monthly 
    WHERE 责任归属 = '2沙河南'
    group by 年月,责任归属,大市场
)
, t1 as (
	select to_char(to_date1(b.年月,'yyyymm'),'yyyy-mm') as stat_month
	-- ,b.大市场
	,b.责任归属
	,sum(b.平台化收益) + sum(c.议价金额) as 盈利额_天真
	,QUARTER(to_date1(b.年月,'yyyymm')) as QUA
	from     earning b 
	left join bargain c on b.年月=c.年月 and b.责任归属=c.责任归属 and b.大市场=c.大市场
	where b.年月>=202301
	GROUP BY b.年月,b.责任归属
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(盈利额_天真) as 当前实际值
            ,case when QUA = 1 then 10866879
             when QUA = 2 then 13979787
             when QUA = 3 then 13357206
             when QUA = 4 then 18394457
            end  as goods_value_target_s
            ,case when QUA = 1 then 10309784
             when QUA = 2 then 13263107
             when QUA = 3 then 12672443
             when QUA = 4 then 17451457
            end  as goods_value_target_a
            ,case when QUA = 1 then 9888000
             when QUA = 2 then 12720500
             when QUA = 3 then 12154000
             when QUA = 4 then 16737500
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,盈利额_天真 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 2886515
        when  to_char(stat_month,'mm') = '02' then 2433728
        when  to_char(stat_month,'mm') = '03' then 5546636
        when  to_char(stat_month,'mm') = '04' then 5037251
        when  to_char(stat_month,'mm') = '05' then 4810858
        when  to_char(stat_month,'mm') = '06' then 4131678
        when  to_char(stat_month,'mm') = '07' then 2943113
        when  to_char(stat_month,'mm') = '08' then 4641063
        when  to_char(stat_month,'mm') = '09' then 5773030
        when  to_char(stat_month,'mm') = '10' then 6565406
        when  to_char(stat_month,'mm') = '11' then 6395611
        when  to_char(stat_month,'mm') = '12' then 5433440
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 2738536
        when  to_char(stat_month,'mm') = '02' then 2308962
        when  to_char(stat_month,'mm') = '03' then 5262286
        when  to_char(stat_month,'mm') = '04' then 4779014
        when  to_char(stat_month,'mm') = '05' then 4564227
        when  to_char(stat_month,'mm') = '06' then 3919866
        when  to_char(stat_month,'mm') = '07' then 2792233
        when  to_char(stat_month,'mm') = '08' then 4403137
        when  to_char(stat_month,'mm') = '09' then 5477073
        when  to_char(stat_month,'mm') = '10' then 6228828
        when  to_char(stat_month,'mm') = '11' then 6067737
        when  to_char(stat_month,'mm') = '12' then 5154892
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 2626500
        when  to_char(stat_month,'mm') = '02' then 2214500
        when  to_char(stat_month,'mm') = '03' then 5047000
        when  to_char(stat_month,'mm') = '04' then 4583500
        when  to_char(stat_month,'mm') = '05' then 4377500
        when  to_char(stat_month,'mm') = '06' then 3759500
        when  to_char(stat_month,'mm') = '07' then 2678000
        when  to_char(stat_month,'mm') = '08' then 4223000
        when  to_char(stat_month,'mm') = '09' then 5253000
        when  to_char(stat_month,'mm') = '10' then 5974000
        when  to_char(stat_month,'mm') = '11' then 5819500
        when  to_char(stat_month,'mm') = '12' then 4944000
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '货：供给 沙河' as 部门
	 ,'天真' as 负责人
	 ,stat_month as 季度or月份
	 ,'盈利额_天真' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"自然月平台化收入与买手议价金额" as  指标定义
	 ,0.15 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '货：供给 沙河' as 部门
	 ,'天真' as 负责人
	 ,stat_month  as 季度or月份
	 ,'盈利额_天真' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"自然月平台化收入与买手议价金额" as  指标定义
	 ,0.15 as 权重
	 from t2
;


-- 货：杭州市场&平湖市场	凯尔	    实际GMV -- 部门筛选 '5杭州&平湖'
with t1 as (
    select to_char(special_date, 'yyyy-mm')          as stat_month,
      sum(real_buy_amount)                        实际GMV,
      QUARTER(to_char(special_date, 'yyyy-mm')) as QUA
    from yishou_data.dwm_goods_sp_up_info_dt
    where dt >= '20230101'
    and  department = '5杭州&平湖'
    GROUP BY to_char(special_date, 'yyyy-mm')
)
,t2 as  (
    SELECT   concat(to_char(max(stat_month),'yyyy'),'Q',QUA) as stat_month
            ,sum(实际GMV) as 当前实际值
            ,QUA
            ,case when QUA = 1 then 191369818.78
             when QUA = 2 then 106753453.258474
             when QUA = 3 then 89927304.4901605
             when QUA = 4 then 207847077.515381
            end  as goods_value_target_s
            ,case when QUA = 1 then 177194276.64
             when QUA = 2 then 98448195.6643579
             when QUA = 3 then 82904813.3322671
             when QUA = 4 then 191586176.960153
            end  as goods_value_target_a
            ,case when QUA = 1 then 163018734.52
             when QUA = 2 then 90142938.0702412
             when QUA = 3 then 75882322.1743738
             when QUA = 4 then 175325276.404924
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,实际GMV as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 106956037.97
        when  to_char(stat_month,'mm') = '02' then 35943316.36
        when  to_char(stat_month,'mm') = '03' then 48470464.45
        when  to_char(stat_month,'mm') = '04' then 53919881.27
        when  to_char(stat_month,'mm') = '05' then 36316091.9044439
        when  to_char(stat_month,'mm') = '06' then 31674781.8168685
        when  to_char(stat_month,'mm') = '07' then 21646465.9606462
        when  to_char(stat_month,'mm') = '08' then 29859523.4632115
        when  to_char(stat_month,'mm') = '09' then 38421315.0663028
        when  to_char(stat_month,'mm') = '10' then 51485033.4074073
        when  to_char(stat_month,'mm') = '11' then 79433806.3312503
        when  to_char(stat_month,'mm') = '12' then 76928237.7767236
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 99033368.49
        when  to_char(stat_month,'mm') = '02' then 33280848.48
        when  to_char(stat_month,'mm') = '03' then 44880059.67
        when  to_char(stat_month,'mm') = '04' then 49925815.99
        when  to_char(stat_month,'mm') = '05' then 33486385.2171739
        when  to_char(stat_month,'mm') = '06' then 29235224.5116117
        when  to_char(stat_month,'mm') = '07' then 19985627.1308058
        when  to_char(stat_month,'mm') = '08' then 27484806.6098032
        when  to_char(stat_month,'mm') = '09' then 35434379.5916581
        when  to_char(stat_month,'mm') = '10' then 47441131.9331103
        when  to_char(stat_month,'mm') = '11' then 73242430.7924687
        when  to_char(stat_month,'mm') = '12' then 70902614.2345737
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 91110699.01
        when  to_char(stat_month,'mm') = '02' then 30618380.61
        when  to_char(stat_month,'mm') = '03' then 41289654.9
        when  to_char(stat_month,'mm') = '04' then 45931750.71
        when  to_char(stat_month,'mm') = '05' then 30656678.529904
        when  to_char(stat_month,'mm') = '06' then 26795667.2063547
        when  to_char(stat_month,'mm') = '07' then 18324788.3009654
        when  to_char(stat_month,'mm') = '08' then 25110089.756395
        when  to_char(stat_month,'mm') = '09' then 32447444.1170134
        when  to_char(stat_month,'mm') = '10' then 43397230.4588134
        when  to_char(stat_month,'mm') = '11' then 67051055.2536871
        when  to_char(stat_month,'mm') = '12' then 64876990.6924237
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '货：供给 杭州市场&平湖市场' as 部门
	 ,'凯尔' as 负责人
	 ,stat_month as 季度or月份
	 ,'实际GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"自然月对应市场团队的总实际GMV" as  指标定义
	 ,case when SUBSTR(stat_month,5,2) BETWEEN '01' and '04' THEN  0.50
	    ELSE  0.30 end as 权重
	 from t3
    UNION ALL 
    SELECT 
     '货：供给 杭州市场&平湖市场' as 部门
	 ,'凯尔' as 负责人
	 ,stat_month  as 季度or月份
	 ,'实际GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"自然月对应市场团队的总实际GMV" as  指标定义
	 ,case when QUA = 1 then 0.50
	  when QUA = 2 then 0.50/3 + 0.30/3 +0.30/3
	  else 0.30 end as 权重	
	 from t2
;



-- 货：供给 杭濮	凯尔        潜力商家    -- 责任部门 筛选 '5杭州&平湖'
WITH t1 as (
	SELECT  to_date1(月份,'yyyymm') as stat_month 
			-- , max(供应商ID)
			-- , max(潜力类型)
			-- , min(潜力类型)
			, sum(潜力商家得分) as 潜力商家得分
			-- ,count(1)
			-- ,count(DISTINCT  供应商ID)
			,QUARTER(to_date1(月份,'yyyymm') ) as QUA
	from (SELECT mt                                                                           月份
			, supply_id                                                                    供应商ID
			, supply_name                                                                  供应商名称
			, department                                                                   责任部门
			, primary_market_name                                                          一级市场
			, big_market                                                                   大市场
			, big_market_ql                                                                对比大市场
			, poten_score                                                                  潜力商家得分
			, case
					when real_buy_amount <= 500000 AND poten_score > 0 then '达成双潜力'
					when poten_score > 0 and real_buy_amount > 500000 then '达成50W潜力' end 潜力类型
			, uv_value                                                                     当月UV价值
			, big_market_ql_uv_value                                                       当月对照大市场UV价值
			, big_market_uv_value                                                          当月大市场UV价值
			, department_uv_value                                                          当月部门UV价值
			, buy_amount                                                                   当月gmv
			, real_buy_amount                                                              当月实际gmv
			, poten_score_this_mon                                                         当月是否符合要求
			, poten_score_11_mons                                                          过去11个月是否符合要求
		FROM yishou_daily.finebi_dwd_poten_supply_summary_mt t1
		where mt >= 202301) a
	where 责任部门 = '5杭州&平湖'
	GROUP BY 月份
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(潜力商家得分) as 当前实际值
            ,QUA
            ,case when QUA = 1 then 24
             when QUA = 2 then 9
             when QUA = 3 then 12
             when QUA = 4 then 33
            end  as goods_value_target_s
            ,case when QUA = 1 then 21
             when QUA = 2 then 7
             when QUA = 3 then 11
             when QUA = 4 then 31
            end  as goods_value_target_a
            ,case when QUA = 1 then 19
             when QUA = 2 then 6
             when QUA = 3 then 10
             when QUA = 4 then 30
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,潜力商家得分 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 9
        when  to_char(stat_month,'mm') = '02' then 7
        when  to_char(stat_month,'mm') = '03' then 8
        when  to_char(stat_month,'mm') = '04' then 6
        when  to_char(stat_month,'mm') = '05' then 4
        when  to_char(stat_month,'mm') = '06' then 2
        when  to_char(stat_month,'mm') = '07' then 2
        when  to_char(stat_month,'mm') = '08' then 4
        when  to_char(stat_month,'mm') = '09' then 6
        when  to_char(stat_month,'mm') = '10' then 8
        when  to_char(stat_month,'mm') = '11' then 11
        when  to_char(stat_month,'mm') = '12' then 14
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 8
        when  to_char(stat_month,'mm') = '02' then 6
        when  to_char(stat_month,'mm') = '03' then 7
        when  to_char(stat_month,'mm') = '04' then 6
        when  to_char(stat_month,'mm') = '05' then 3
        when  to_char(stat_month,'mm') = '06' then 1
        when  to_char(stat_month,'mm') = '07' then 2
        when  to_char(stat_month,'mm') = '08' then 4
        when  to_char(stat_month,'mm') = '09' then 5
        when  to_char(stat_month,'mm') = '10' then 8
        when  to_char(stat_month,'mm') = '11' then 10
        when  to_char(stat_month,'mm') = '12' then 13
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 7
        when  to_char(stat_month,'mm') = '02' then 5
        when  to_char(stat_month,'mm') = '03' then 7
        when  to_char(stat_month,'mm') = '04' then 5
        when  to_char(stat_month,'mm') = '05' then 3
        when  to_char(stat_month,'mm') = '06' then 1
        when  to_char(stat_month,'mm') = '07' then 1
        when  to_char(stat_month,'mm') = '08' then 4
        when  to_char(stat_month,'mm') = '09' then 5
        when  to_char(stat_month,'mm') = '10' then 7
        when  to_char(stat_month,'mm') = '11' then 10
        when  to_char(stat_month,'mm') = '12' then 13
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '货：供给 杭州市场&平湖市场' as 部门
	 ,'凯尔' as 负责人
	 ,stat_month as 季度or月份
	 ,'潜力商家' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s and goods_value_target_s > goods_value_target_a then 'S'
            when t3.当前实际值 >= goods_value_target_a and goods_value_target_a > goods_value_target_b then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"累计12个月实际GMV首次达到20万+、50万+的商家总数量；若直接从20万以下直接做到50万的，算2家；实际UV价值大于所在市场的实际UV价值平均值" as  指标定义
	 ,case when SUBSTR(stat_month,5,2) BETWEEN '01' and '04' THEN  0.2 
	    ELSE  0.1 end as 权重
	 from t3
    UNION ALL 
    SELECT 
     '货：供给 杭州市场&平湖市场' as 部门
	 ,'凯尔' as 负责人
	 ,stat_month  as 季度or月份
	 ,'潜力商家' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s and goods_value_target_s > goods_value_target_a then 'S'
            when t2.当前实际值 >= goods_value_target_a and goods_value_target_a > goods_value_target_b then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"累计12个月实际GMV首次达到20万+、50万+的商家总数量；若直接从20万以下直接做到50万的，算2家；实际UV价值大于所在市场的实际UV价值平均值" as  指标定义
	 ,case when QUA = 1 then 0.2
	  when QUA = 2 then 0.2/3 + 0.1/3 + 0.1/3 -- 从左到右分别代表: 4月 5月 6月
	  else 0.1 end as 权重
	 from t2
;

-- 货：供给 杭濮	凯尔        盈利额  -- 责任归属   筛选: 4深圳南油
with earning as (
    select 年月
        ,责任归属
        ,大市场
        ,sum(佣金)+sum(退换货服务费)-sum(佣金返还) 平台化收益
    from yishou_daily.finebi_cwf_supplier_earnings_monthly 
    WHERE 责任归属 = '5杭州&平湖'
    group by 年月,责任归属,大市场)
,bargain as (
    select 年月
        ,责任归属
        ,大市场
        ,sum(议价金额) 议价金额
    from yishou_daily.finebi_cwf_maishouyijia_monthly 
    WHERE 责任归属 = '5杭州&平湖'
    group by 年月,责任归属,大市场
)
, t1 as (
	select to_char(to_date1(b.年月,'yyyymm'),'yyyy-mm') as stat_month
	-- ,b.大市场
	,b.责任归属
	,sum(b.平台化收益) + sum(c.议价金额) as 盈利额_凯尔
	,QUARTER(to_date1(b.年月,'yyyymm')) as QUA
	from     earning b 
	left join bargain c on b.年月=c.年月 and b.责任归属=c.责任归属 and b.大市场=c.大市场
	where b.年月>=202301
	GROUP BY b.年月,b.责任归属
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(盈利额_凯尔) as 当前实际值
            ,case when QUA = 1 then 2689740
             when QUA = 2 then 1937488.58987875
             when QUA = 3 then 2126142.61149091
             when QUA = 4 then 4864572.36436558
            end  as goods_value_target_s
            ,case when QUA = 1 then 2575572
             when QUA = 2 then 1818415.49089732
             when QUA = 3 then 2023786.09154084
             when QUA = 4 then 4623369.20580062
            end  as goods_value_target_a
            ,case when QUA = 1 then 2489133
             when QUA = 2 then 1720977.11195299
             when QUA = 3 then 1947935.68554358
             when QUA = 4 then 4445608.964042
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,盈利额_凯尔 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 1445236
        when  to_char(stat_month,'mm') = '02' then 491252
        when  to_char(stat_month,'mm') = '03' then 753252
        when  to_char(stat_month,'mm') = '04' then 851503
        when  to_char(stat_month,'mm') = '05' then 686833.89459481
        when  to_char(stat_month,'mm') = '06' then 652314.253736292
        when  to_char(stat_month,'mm') = '07' then 415196.408585182
        when  to_char(stat_month,'mm') = '08' then 648784.960668947
        when  to_char(stat_month,'mm') = '09' then 1062161.24223678
        when  to_char(stat_month,'mm') = '10' then 1442032.31782879
        when  to_char(stat_month,'mm') = '11' then 1762768.37619523
        when  to_char(stat_month,'mm') = '12' then 1659771.67034156
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 1394868
        when  to_char(stat_month,'mm') = '02' then 466067
        when  to_char(stat_month,'mm') = '03' then 714637
        when  to_char(stat_month,'mm') = '04' then 807850
        when  to_char(stat_month,'mm') = '05' then 636749.602885848
        when  to_char(stat_month,'mm') = '06' then 613999.626245504
        when  to_char(stat_month,'mm') = '07' then 387499.76640344
        when  to_char(stat_month,'mm') = '08' then 613703.778245819
        when  to_char(stat_month,'mm') = '09' then 1022582.54689158
        when  to_char(stat_month,'mm') = '10' then 1370901.03372907
        when  to_char(stat_month,'mm') = '11' then 1677785.40944434
        when  to_char(stat_month,'mm') = '12' then 1574682.76262721
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 1356733
        when  to_char(stat_month,'mm') = '02' then 447000
        when  to_char(stat_month,'mm') = '03' then 685400
        when  to_char(stat_month,'mm') = '04' then 774800
        when  to_char(stat_month,'mm') = '05' then 593972.402050348
        when  to_char(stat_month,'mm') = '06' then 582562.260753268
        when  to_char(stat_month,'mm') = '07' then 364101.412970793
        when  to_char(stat_month,'mm') = '08' then 586359.515571114
        when  to_char(stat_month,'mm') = '09' then 997474.757001669
        when  to_char(stat_month,'mm') = '10' then 1329475.50409192
        when  to_char(stat_month,'mm') = '11' then 1605872.59425938
        when  to_char(stat_month,'mm') = '12' then 1510260.8656907
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '货：供给 杭州市场&平湖市场' as 部门
	 ,'凯尔' as 负责人
	 ,stat_month as 季度or月份
	 ,'盈利额_凯尔' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"自然月平台化收入与买手议价金额" as  指标定义
	 ,0.15 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '货：供给 杭州市场&平湖市场' as 部门
	 ,'凯尔' as 负责人
	 ,stat_month  as 季度or月份
	 ,'盈利额_凯尔' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"自然月平台化收入与买手议价金额" as  指标定义
	 ,0.15 as 权重
	 from t2
;




--  货：供给  -->  南油&新品类 --> 妮娜    实际GMV -- 供给部门 筛选: 4深圳南油
with t1 as ( 
    select to_char(special_date, 'yyyy-mm')          as stat_month,
       sum(real_buy_amount)                        实际GMV,
       QUARTER(to_char(special_date, 'yyyy-mm')) as QUA
    from yishou_data.dwm_goods_sp_up_info_dt
    where dt >= '20230101'
    and department = '4深圳南油'
    GROUP BY to_char(special_date, 'yyyy-mm')
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(实际GMV) as 当前实际值
            ,QUA
            ,case when QUA = 1 then 58770073.09
             when QUA = 2 then 67675948.66
             when QUA = 3 then 62893580.43
             when QUA = 4 then 100766321.33
            end  as goods_value_target_s
            ,case when QUA = 1 then 54490808.41
             when QUA = 2 then 62662915.42
             when QUA = 3 then 58234796.69
             when QUA = 4 then 93302149.39
            end  as goods_value_target_a
            ,case when QUA = 1 then 50211543.74
             when QUA = 2 then 57649882.19
             when QUA = 3 then 53576012.96
             when QUA = 4 then 85837977.44
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,实际GMV as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 19745241.64
        when  to_char(stat_month,'mm') = '02' then 14391149.98
        when  to_char(stat_month,'mm') = '03' then 24633681.47
        when  to_char(stat_month,'mm') = '04' then 23109069.82
        when  to_char(stat_month,'mm') = '05' then 23627656.88
        when  to_char(stat_month,'mm') = '06' then 20939221.96
        when  to_char(stat_month,'mm') = '07' then 16476223.31
        when  to_char(stat_month,'mm') = '08' then 20410072.24
        when  to_char(stat_month,'mm') = '09' then 26007284.88
        when  to_char(stat_month,'mm') = '10' then 30910756.16
        when  to_char(stat_month,'mm') = '11' then 36590529.11
        when  to_char(stat_month,'mm') = '12' then 33265036.06
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 18356705.22
        when  to_char(stat_month,'mm') = '02' then 13325138.87
        when  to_char(stat_month,'mm') = '03' then 22808964.32
        when  to_char(stat_month,'mm') = '04' then 21397286.87
        when  to_char(stat_month,'mm') = '05' then 21877460.07
        when  to_char(stat_month,'mm') = '06' then 19388168.48
        when  to_char(stat_month,'mm') = '07' then 15255762.32
        when  to_char(stat_month,'mm') = '08' then 18898215.04
        when  to_char(stat_month,'mm') = '09' then 24080819.33
        when  to_char(stat_month,'mm') = '10' then 28621070.52
        when  to_char(stat_month,'mm') = '11' then 33880119.55
        when  to_char(stat_month,'mm') = '12' then 30800959.32
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 16968168.8
        when  to_char(stat_month,'mm') = '02' then 12259127.76
        when  to_char(stat_month,'mm') = '03' then 20984247.18
        when  to_char(stat_month,'mm') = '04' then 19685503.92
        when  to_char(stat_month,'mm') = '05' then 20127263.27
        when  to_char(stat_month,'mm') = '06' then 17837115
        when  to_char(stat_month,'mm') = '07' then 14035301.34
        when  to_char(stat_month,'mm') = '08' then 17386357.84
        when  to_char(stat_month,'mm') = '09' then 22154353.78
        when  to_char(stat_month,'mm') = '10' then 26331384.88
        when  to_char(stat_month,'mm') = '11' then 31169709.99
        when  to_char(stat_month,'mm') = '12' then 28336882.57
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '货：供给 南油&新品类' as 部门
	 ,'妮娜' as 负责人
	 ,stat_month as 季度or月份
	 ,'实际GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"自然月对应市场团队的总实际GMV" as  指标定义
	 ,case when SUBSTR(stat_month,5,2) BETWEEN '01' and '04' THEN  0.5
	    ELSE  0.3 end as 权重
	 from t3
    UNION ALL 
    SELECT 
     '货：供给 南油&新品类' as 部门
	 ,'妮娜' as 负责人
	 ,stat_month  as 季度or月份
	 ,'实际GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"自然月对应市场团队的总实际GMV" as  指标定义
	 ,case when QUA = 1 then 0.5 
	  when QUA = 2 then 0.5/3 + 0.3/3 + 0.3/3  -- 从左到右分别代表: 4月 5月 6月
	  else 0.3 end as 权重
	 from t2
;



-- 货：供给 南油&新品类	妮娜	潜力商家
WITH t1 as (
	SELECT  to_date1(月份,'yyyymm') as stat_month 
			-- , max(供应商ID)
			-- , max(潜力类型)
			-- , min(潜力类型)
			, sum(潜力商家得分) as 潜力商家得分
			-- ,count(1)
			-- ,count(DISTINCT  供应商ID)
			,QUARTER(to_date1(月份,'yyyymm') ) as QUA
	from (SELECT mt                                                                           月份
			, supply_id                                                                    供应商ID
			, supply_name                                                                  供应商名称
			, department                                                                   责任部门
			, primary_market_name                                                          一级市场
			, big_market                                                                   大市场
			, big_market_ql                                                                对比大市场
			, poten_score                                                                  潜力商家得分
			, case
					when real_buy_amount <= 500000 AND poten_score > 0 then '达成双潜力'
					when poten_score > 0 and real_buy_amount > 500000 then '达成50W潜力' end 潜力类型
			, uv_value                                                                     当月UV价值
			, big_market_ql_uv_value                                                       当月对照大市场UV价值
			, big_market_uv_value                                                          当月大市场UV价值
			, department_uv_value                                                          当月部门UV价值
			, buy_amount                                                                   当月gmv
			, real_buy_amount                                                              当月实际gmv
			, poten_score_this_mon                                                         当月是否符合要求
			, poten_score_11_mons                                                          过去11个月是否符合要求
		FROM yishou_daily.finebi_dwd_poten_supply_summary_mt t1
		where mt >= 202301) a
	where 责任部门 = '4深圳南油'
	GROUP BY 月份
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(潜力商家得分) as 当前实际值
            ,QUA
            ,case when QUA = 1 then 9
             when QUA = 2 then 10
             when QUA = 3 then 10
             when QUA = 4 then 11
            end  as goods_value_target_s
            ,case when QUA = 1 then 8
             when QUA = 2 then 9
             when QUA = 3 then 9
             when QUA = 4 then 10
            end  as goods_value_target_a
            ,case when QUA = 1 then 7
             when QUA = 2 then 8
             when QUA = 3 then 8
             when QUA = 4 then 9
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,潜力商家得分 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 3
        when  to_char(stat_month,'mm') = '02' then 3
        when  to_char(stat_month,'mm') = '03' then 3
        when  to_char(stat_month,'mm') = '04' then 4
        when  to_char(stat_month,'mm') = '05' then 3
        when  to_char(stat_month,'mm') = '06' then 3
        when  to_char(stat_month,'mm') = '07' then 3
        when  to_char(stat_month,'mm') = '08' then 3
        when  to_char(stat_month,'mm') = '09' then 4
        when  to_char(stat_month,'mm') = '10' then 3
        when  to_char(stat_month,'mm') = '11' then 5
        when  to_char(stat_month,'mm') = '12' then 3
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 2
        when  to_char(stat_month,'mm') = '02' then 3
        when  to_char(stat_month,'mm') = '03' then 3
        when  to_char(stat_month,'mm') = '04' then 3
        when  to_char(stat_month,'mm') = '05' then 3
        when  to_char(stat_month,'mm') = '06' then 3
        when  to_char(stat_month,'mm') = '07' then 2
        when  to_char(stat_month,'mm') = '08' then 3
        when  to_char(stat_month,'mm') = '09' then 4
        when  to_char(stat_month,'mm') = '10' then 2
        when  to_char(stat_month,'mm') = '11' then 5
        when  to_char(stat_month,'mm') = '12' then 3
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 2
        when  to_char(stat_month,'mm') = '02' then 2
        when  to_char(stat_month,'mm') = '03' then 3
        when  to_char(stat_month,'mm') = '04' then 3
        when  to_char(stat_month,'mm') = '05' then 2
        when  to_char(stat_month,'mm') = '06' then 3
        when  to_char(stat_month,'mm') = '07' then 2
        when  to_char(stat_month,'mm') = '08' then 2
        when  to_char(stat_month,'mm') = '09' then 4
        when  to_char(stat_month,'mm') = '10' then 2
        when  to_char(stat_month,'mm') = '11' then 4
        when  to_char(stat_month,'mm') = '12' then 3
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '货：供给 南油&新品类' as 部门
	 ,'妮娜' as 负责人
	 ,stat_month as 季度or月份
	 ,'潜力商家' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s and goods_value_target_s > goods_value_target_a then 'S'
            when t3.当前实际值 >= goods_value_target_a and goods_value_target_a > goods_value_target_b then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"累计12个月实际GMV首次达到20万+、50万+的商家总数量；若直接从20万以下直接做到50万的，算2家；实际UV价值大于所在市场的实际UV价值平均值" as  指标定义
	 ,case when SUBSTR(stat_month,5,2) BETWEEN '01' and '04' THEN  0.2 
	    ELSE  0.1 end as 权重
	 from t3
    UNION ALL 
    SELECT 
     '货：供给 南油&新品类' as 部门
	 ,'妮娜' as 负责人
	 ,stat_month  as 季度or月份
	 ,'潜力商家' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s and goods_value_target_s > goods_value_target_a then 'S'
            when t2.当前实际值 >= goods_value_target_a and goods_value_target_a > goods_value_target_b then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"累计12个月实际GMV首次达到20万+、50万+的商家总数量；若直接从20万以下直接做到50万的，算2家；实际UV价值大于所在市场的实际UV价值平均值" as  指标定义
	 ,case when QUA = 1 then 0.2
	  when QUA = 2 then 0.2/3 + 0.1/3 + 0.1/3 -- 从左到右分别代表: 4月 5月 6月
	  else 0.1 end as 权重
	 from t2
;

-- 货：供给 南油&新品类	妮娜	盈利额 -- 责任归属 筛选 '4深圳南油'
with earning as (
    select 年月
    ,责任归属
    ,大市场
    ,sum(佣金)+sum(退换货服务费)-sum(佣金返还) 平台化收益
    from yishou_daily.finebi_cwf_supplier_earnings_monthly 
    WHERE 责任归属 = '4深圳南油'
    group by 年月,责任归属,大市场)
,bargain as (
    select 年月
    ,责任归属
    ,大市场
    ,sum(议价金额) 议价金额
    from yishou_daily.finebi_cwf_maishouyijia_monthly 
    WHERE 责任归属 = '4深圳南油'
    group by 年月,责任归属,大市场
)
, t1 as (
	select to_char(to_date1(b.年月,'yyyymm'),'yyyy-mm') as stat_month
	-- ,b.大市场
	,b.责任归属
	,sum(b.平台化收益) + sum(c.议价金额) as 盈利额_妮娜
	,QUARTER(to_date1(b.年月,'yyyymm')) as QUA
	from     earning b 
	left join bargain c on b.年月=c.年月 and b.责任归属=c.责任归属 and b.大市场=c.大市场
	where b.年月>= 202301
	GROUP BY b.年月,b.责任归属
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(盈利额_妮娜) as 当前实际值
            ,QUA
            ,case 
                 when QUA = 1 then 947098
                 when QUA = 2 then 1428675
                 when QUA = 3 then 1175244
                 when QUA = 4 then 1818949
            end  as goods_value_target_s
            ,case 
                 when QUA = 1 then 893920
                 when QUA = 2 then 1322226
                 when QUA = 3 then 1043895
                 when QUA = 4 then 1709086
            end  as goods_value_target_a
            ,case 
                 when QUA = 1 then 836400
                 when QUA = 2 then 1234857
                 when QUA = 3 then 994560
                 when QUA = 4 then 1620248
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,盈利额_妮娜 as 当前实际值
        ,case 
            when  to_char(stat_month,'mm') = '01' then 375000
            when  to_char(stat_month,'mm') = '02' then 198000
            when  to_char(stat_month,'mm') = '03' then 374098
            when  to_char(stat_month,'mm') = '04' then 490766
            when  to_char(stat_month,'mm') = '05' then 535966
            when  to_char(stat_month,'mm') = '06' then 401943
            when  to_char(stat_month,'mm') = '07' then 300866
            when  to_char(stat_month,'mm') = '08' then 400180
            when  to_char(stat_month,'mm') = '09' then 474198
            when  to_char(stat_month,'mm') = '10' then 610877
            when  to_char(stat_month,'mm') = '11' then 672525
            when  to_char(stat_month,'mm') = '12' then 535547
        end as goods_value_target_s,
        case 
            when  to_char(stat_month,'mm') = '01' then 361000
            when  to_char(stat_month,'mm') = '02' then 178000
            when  to_char(stat_month,'mm') = '03' then 354920
            when  to_char(stat_month,'mm') = '04' then 439400
            when  to_char(stat_month,'mm') = '05' then 503000
            when  to_char(stat_month,'mm') = '06' then 379826
            when  to_char(stat_month,'mm') = '07' then 251859
            when  to_char(stat_month,'mm') = '08' then 345326
            when  to_char(stat_month,'mm') = '09' then 446710
            when  to_char(stat_month,'mm') = '10' then 574399
            when  to_char(stat_month,'mm') = '11' then 632007
            when  to_char(stat_month,'mm') = '12' then 502680
        end as goods_value_target_a,
        case 
            when  to_char(stat_month,'mm') = '01' then 348000
            when  to_char(stat_month,'mm') = '02' then 148000
            when  to_char(stat_month,'mm') = '03' then 340400
            when  to_char(stat_month,'mm') = '04' then 413000
            when  to_char(stat_month,'mm') = '05' then 473000
            when  to_char(stat_month,'mm') = '06' then 348857
            when  to_char(stat_month,'mm') = '07' then 240480
            when  to_char(stat_month,'mm') = '08' then 329264
            when  to_char(stat_month,'mm') = '09' then 424816
            when  to_char(stat_month,'mm') = '10' then 545024
            when  to_char(stat_month,'mm') = '11' then 599272
            when  to_char(stat_month,'mm') = '12' then 475952
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '货：供给 南油&新品类' as 部门
	 ,'妮娜' as 负责人
	 ,stat_month as 季度or月份
	 ,'盈利额_妮娜' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"自然月平台化收入与买手议价金额" as  指标定义
	 ,0.15 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '货：供给 南油&新品类' as 部门
	 ,'妮娜' as 负责人
	 ,stat_month  as 季度or月份
	 ,'盈利额_妮娜' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"自然月平台化收入与买手议价金额" as  指标定义
	 ,0.15  as 权重	
	 from t2
;




-- 货：供给 濮院市场	花花    实际GMV     部门筛选:'6新意法&濮院'
with t1 as (
    select to_char(special_date, 'yyyy-mm')          as stat_month,
      sum(real_buy_amount)                        实际GMV,
      QUARTER(to_char(special_date, 'yyyy-mm')) as QUA
    from yishou_data.dwm_goods_sp_up_info_dt
    where dt >= '20240401'
    and  department = '6新意法&濮院'
    GROUP BY to_char(special_date, 'yyyy-mm')
)
,t2 as  (
    SELECT   concat(to_char(max(stat_month),'yyyy'),'Q',QUA) as stat_month
            ,sum(实际GMV) as 当前实际值
            ,QUA
            ,case 
                 when QUA = 1 then NULL
                 when QUA = 2 then 41328789.1640938
                 when QUA = 3 then 51065340.9191872
                 when QUA = 4 then 169556301.547733
            end  as goods_value_target_s
            ,case 
                 when QUA = 1 then NULL
                 when QUA = 2 then 38664991.7639462
                 when QUA = 3 then 47643932.4171288
                 when QUA = 4 then 157861396.246434
            end  as goods_value_target_a
            ,case 
                 when QUA = 1 then NULL
                 when QUA = 2 then 36001194.3637985
                 when QUA = 3 then 44222523.9150705
                 when QUA = 4 then 146166490.945135
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,实际GMV as 当前实际值
        ,case 
            when  to_char(stat_month,'mm') = '01' then NULL
            when  to_char(stat_month,'mm') = '02' then NULL
            when  to_char(stat_month,'mm') = '03' then NULL
            when  to_char(stat_month,'mm') = '04' then 15157301.7298826
            when  to_char(stat_month,'mm') = '05' then 13564594.3431097
            when  to_char(stat_month,'mm') = '06' then 12606893.0911015
            when  to_char(stat_month,'mm') = '07' then 8531542.56063176
            when  to_char(stat_month,'mm') = '08' then 14733968.5253699
            when  to_char(stat_month,'mm') = '09' then 27799829.8331855
            when  to_char(stat_month,'mm') = '10' then 49346874.8915767
            when  to_char(stat_month,'mm') = '11' then 60453977.1718788
            when  to_char(stat_month,'mm') = '12' then 59755449.4842774
        end as goods_value_target_s,
        case 
            when  to_char(stat_month,'mm') = '01' then NULL
            when  to_char(stat_month,'mm') = '02' then NULL
            when  to_char(stat_month,'mm') = '03' then NULL
            when  to_char(stat_month,'mm') = '04' then 14199230.052432
            when  to_char(stat_month,'mm') = '05' then 12699435.3824127
            when  to_char(stat_month,'mm') = '06' then 11766326.3291015
            when  to_char(stat_month,'mm') = '07' then 7956973.35185906
            when  to_char(stat_month,'mm') = '08' then 13805463.7499943
            when  to_char(stat_month,'mm') = '09' then 25881495.3152754
            when  to_char(stat_month,'mm') = '10' then 45921746.1215042
            when  to_char(stat_month,'mm') = '11' then 56283294.6733914
            when  to_char(stat_month,'mm') = '12' then 55656355.4515383
        end as goods_value_target_a,
        case 
            when  to_char(stat_month,'mm') = '01' then NULL
            when  to_char(stat_month,'mm') = '02' then NULL
            when  to_char(stat_month,'mm') = '03' then NULL
            when  to_char(stat_month,'mm') = '04' then 13241158.3749815
            when  to_char(stat_month,'mm') = '05' then 11834276.4217157
            when  to_char(stat_month,'mm') = '06' then 10925759.5671013
            when  to_char(stat_month,'mm') = '07' then 7382404.14308633
            when  to_char(stat_month,'mm') = '08' then 12876958.9746188
            when  to_char(stat_month,'mm') = '09' then 23963160.7973654
            when  to_char(stat_month,'mm') = '10' then 42496617.3514318
            when  to_char(stat_month,'mm') = '11' then 52112612.174904
            when  to_char(stat_month,'mm') = '12' then 51557261.4187992
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '货：供给 濮院市场' as 部门
	 ,'花花' as 负责人
	 ,stat_month as 季度or月份
	 ,'实际GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"自然月对应市场团队的总实际GMV" as  指标定义
	 ,case when SUBSTR(stat_month,5,2) BETWEEN '01' and '04' THEN  0.50
	    ELSE  0.30 end as 权重
	 from t3
    UNION ALL 
    SELECT 
     '货：供给 濮院市场' as 部门
	 ,'花花' as 负责人
	 ,stat_month  as 季度or月份
	 ,'实际GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"自然月对应市场团队的总实际GMV" as  指标定义
	 ,case when QUA = 1 then 0.50
	  when QUA = 2 then 0.50/3 + 0.30/3 +0.30/3
	  else 0.30 end as 权重	
	 from t2
;


-- 货：供给 濮院市场	花花    潜力商家    部门筛选:'6新意法&濮院'
WITH t1 as (
	SELECT  to_date1(月份,'yyyymm') as stat_month 
			-- , max(供应商ID)
			-- , max(潜力类型)
			-- , min(潜力类型)
			, sum(潜力商家得分) as 潜力商家得分
			-- ,count(1)
			-- ,count(DISTINCT  供应商ID)
			,QUARTER(to_date1(月份,'yyyymm') ) as QUA
	from (SELECT mt                                                                           月份
			, supply_id                                                                    供应商ID
			, supply_name                                                                  供应商名称
			, department                                                                   责任部门
			, primary_market_name                                                          一级市场
			, big_market                                                                   大市场
			, big_market_ql                                                                对比大市场
			, poten_score                                                                  潜力商家得分
			, case
					when real_buy_amount <= 500000 AND poten_score > 0 then '达成双潜力'
					when poten_score > 0 and real_buy_amount > 500000 then '达成50W潜力' end 潜力类型
			, uv_value                                                                     当月UV价值
			, big_market_ql_uv_value                                                       当月对照大市场UV价值
			, big_market_uv_value                                                          当月大市场UV价值
			, department_uv_value                                                          当月部门UV价值
			, buy_amount                                                                   当月gmv
			, real_buy_amount                                                              当月实际gmv
			, poten_score_this_mon                                                         当月是否符合要求
			, poten_score_11_mons                                                          过去11个月是否符合要求
		FROM yishou_daily.finebi_dwd_poten_supply_summary_mt t1
		where mt >= 202404) a
	where 责任部门 = '6新意法&濮院'
	GROUP BY 月份
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(潜力商家得分) as 当前实际值
            ,QUA
            ,case 
                 when QUA = 1 then NULL
                 when QUA = 2 then 9
                 when QUA = 3 then 12
                 when QUA = 4 then 26
            end  as goods_value_target_s
            ,case 
                 when QUA = 1 then NULL
                 when QUA = 2 then 9
                 when QUA = 3 then 11
                 when QUA = 4 then 26
            end  as goods_value_target_a
            ,case 
                 when QUA = 1 then NULL
                 when QUA = 2 then 8
                 when QUA = 3 then 10
                 when QUA = 4 then 25
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,潜力商家得分 as 当前实际值
        ,case
            when  to_char(stat_month,'mm') = '01' then NULL
            when  to_char(stat_month,'mm') = '02' then NULL
            when  to_char(stat_month,'mm') = '03' then NULL
            when  to_char(stat_month,'mm') = '04' then 3
            when  to_char(stat_month,'mm') = '05' then 4
            when  to_char(stat_month,'mm') = '06' then 2
            when  to_char(stat_month,'mm') = '07' then 1
            when  to_char(stat_month,'mm') = '08' then 5
            when  to_char(stat_month,'mm') = '09' then 6
            when  to_char(stat_month,'mm') = '10' then 6
            when  to_char(stat_month,'mm') = '11' then 9
            when  to_char(stat_month,'mm') = '12' then 11
        end as goods_value_target_s,
        case 
            when  to_char(stat_month,'mm') = '01' then NULL
            when  to_char(stat_month,'mm') = '02' then NULL
            when  to_char(stat_month,'mm') = '03' then NULL
            when  to_char(stat_month,'mm') = '04' then 3
            when  to_char(stat_month,'mm') = '05' then 4
            when  to_char(stat_month,'mm') = '06' then 2
            when  to_char(stat_month,'mm') = '07' then 1
            when  to_char(stat_month,'mm') = '08' then 4
            when  to_char(stat_month,'mm') = '09' then 6
            when  to_char(stat_month,'mm') = '10' then 6
            when  to_char(stat_month,'mm') = '11' then 9
            when  to_char(stat_month,'mm') = '12' then 11
        end as goods_value_target_a,
        case 
            when  to_char(stat_month,'mm') = '01' then NULL
            when  to_char(stat_month,'mm') = '02' then NULL
            when  to_char(stat_month,'mm') = '03' then NULL
            when  to_char(stat_month,'mm') = '04' then 3
            when  to_char(stat_month,'mm') = '05' then 3
            when  to_char(stat_month,'mm') = '06' then 2
            when  to_char(stat_month,'mm') = '07' then 1
            when  to_char(stat_month,'mm') = '08' then 3
            when  to_char(stat_month,'mm') = '09' then 6
            when  to_char(stat_month,'mm') = '10' then 6
            when  to_char(stat_month,'mm') = '11' then 8
            when  to_char(stat_month,'mm') = '12' then 11
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '货：供给 濮院市场' as 部门
	 ,'花花' as 负责人
	 ,stat_month as 季度or月份
	 ,'潜力商家' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s and goods_value_target_s > goods_value_target_a then 'S'
            when t3.当前实际值 >= goods_value_target_a and goods_value_target_a > goods_value_target_b then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"累计12个月实际GMV首次达到20万+、50万+的商家总数量；若直接从20万以下直接做到50万的，算2家；实际UV价值大于所在市场的实际UV价值平均值" as  指标定义
	 ,case when SUBSTR(stat_month,5,2) BETWEEN '01' and '04' THEN  0.2 
	    ELSE  0.1 end as 权重
	 from t3
    UNION ALL 
    SELECT 
     '货：供给 濮院市场' as 部门
	 ,'花花' as 负责人
	 ,stat_month  as 季度or月份
	 ,'潜力商家' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s and goods_value_target_s > goods_value_target_a then 'S'
            when t2.当前实际值 >= goods_value_target_a and goods_value_target_a > goods_value_target_b then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"累计12个月实际GMV首次达到20万+、50万+的商家总数量；若直接从20万以下直接做到50万的，算2家；实际UV价值大于所在市场的实际UV价值平均值" as  指标定义
	 ,case when QUA = 1 then 0.2
	  when QUA = 2 then 0.2/3 + 0.1/3 + 0.1/3 -- 从左到右分别代表: 4月 5月 6月
	  else 0.1 end as 权重
	 from t2
;

-- 货：供给 濮院市场	花花    盈利额 -- 部门筛选: '6新意法&濮院'
with earning as (
    select 年月
    ,责任归属
    ,大市场
    ,sum(佣金)+sum(退换货服务费)-sum(佣金返还) 平台化收益
    from yishou_daily.finebi_cwf_supplier_earnings_monthly 
    WHERE 责任归属 = '6新意法&濮院'
    group by 年月,责任归属,大市场)
,bargain as (
    select 年月
    ,责任归属
    ,大市场
    ,sum(议价金额) 议价金额
    from yishou_daily.finebi_cwf_maishouyijia_monthly 
    WHERE 责任归属 = '6新意法&濮院'
    group by 年月,责任归属,大市场
)
, t1 as (
	select to_char(to_date1(b.年月,'yyyymm'),'yyyy-mm') as stat_month
	-- ,b.大市场
	,b.责任归属
	,sum(b.平台化收益) + sum(c.议价金额) as 盈利额_妮娜
	,QUARTER(to_date1(b.年月,'yyyymm')) as QUA
	from     earning b 
	left join bargain c on b.年月=c.年月 and b.责任归属=c.责任归属 and b.大市场=c.大市场
	where b.年月>= 202404   
	GROUP BY b.年月,b.责任归属
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(盈利额_妮娜) as 当前实际值
            ,QUA
            ,case 
                 when QUA = 1 then NULL
                 when QUA = 2 then 819765.481297088
                 when QUA = 3 then 919506.570475541
                 when QUA = 4 then 3390243.96479524
            end  as goods_value_target_s
            ,case 
                 when QUA = 1 then NULL
                 when QUA = 2 then 769384.789092773
                 when QUA = 3 then 873855.230781333
                 when QUA = 4 then 3222143.36080288
            end  as goods_value_target_a
            ,case 
                 when QUA = 1 then NULL
                 when QUA = 2 then 728157.903923297
                 when QUA = 3 then 839418.820235828
                 when QUA = 4 then 3098257.73599086
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,盈利额_妮娜 as 当前实际值
        ,case 
            when  to_char(stat_month,'mm') = '01' then NULL
            when  to_char(stat_month,'mm') = '02' then NULL
            when  to_char(stat_month,'mm') = '03' then NULL
            when  to_char(stat_month,'mm') = '04' then 253162.182532139
            when  to_char(stat_month,'mm') = '05' then 290604.404647825
            when  to_char(stat_month,'mm') = '06' then 275998.894117124
            when  to_char(stat_month,'mm') = '07' then 175672.613245147
            when  to_char(stat_month,'mm') = '08' then 294425.828322735
            when  to_char(stat_month,'mm') = '09' then 449408.128907659
            when  to_char(stat_month,'mm') = '10' then 1004988.92736615
            when  to_char(stat_month,'mm') = '11' then 1228518.02812212
            when  to_char(stat_month,'mm') = '12' then 1156737.00930697
        end as goods_value_target_s,
        case 
            when  to_char(stat_month,'mm') = '01' then NULL
            when  to_char(stat_month,'mm') = '02' then NULL
            when  to_char(stat_month,'mm') = '03' then NULL
            when  to_char(stat_month,'mm') = '04' then 240183.714486708
            when  to_char(stat_month,'mm') = '05' then 269413.377401161
            when  to_char(stat_month,'mm') = '06' then 259787.697204904
            when  to_char(stat_month,'mm') = '07' then 163953.962964037
            when  to_char(stat_month,'mm') = '08' then 277239.172103463
            when  to_char(stat_month,'mm') = '09' then 432662.095713833
            when  to_char(stat_month,'mm') = '10' then 955415.729854738
            when  to_char(stat_month,'mm') = '11' then 1169291.24135498
            when  to_char(stat_month,'mm') = '12' then 1097436.38959316
        end as goods_value_target_a,
        case 
            when  to_char(stat_month,'mm') = '01' then NULL
            when  to_char(stat_month,'mm') = '02' then NULL
            when  to_char(stat_month,'mm') = '03' then NULL
            when  to_char(stat_month,'mm') = '04' then 230357.550850626
            when  to_char(stat_month,'mm') = '05' then 251314.033325204
            when  to_char(stat_month,'mm') = '06' then 246486.319747467
            when  to_char(stat_month,'mm') = '07' then 154053.949842167
            when  to_char(stat_month,'mm') = '08' then 263326.062770881
            when  to_char(stat_month,'mm') = '09' then 422038.80762278
            when  to_char(stat_month,'mm') = '10' then 926545.226689946
            when  to_char(stat_month,'mm') = '11' then 1119173.37499161
            when  to_char(stat_month,'mm') = '12' then 1052539.1343093
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '货：供给 濮院市场' as 部门
	 ,'花花' as 负责人
	 ,stat_month as 季度or月份
	 ,'盈利额_花花' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"自然月平台化收入与买手议价金额" as  指标定义
	 ,0.15 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '货：供给 濮院市场' as 部门
	 ,'花花' as 负责人
	 ,stat_month  as 季度or月份
	 ,'盈利额_花花' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"自然月平台化收入与买手议价金额" as  指标定义
	 ,0.15 as 权重	
	 from t2
;


--------------------------------------------------------------------
-- 场：平台中心 企划  责任人:暂无  大盘实际GMV
WITH t1 (
    select to_char(special_date, 'yyyy-mm')          as stat_month,
       sum(real_buy_amount)                        GMV,
       QUARTER(to_char(special_date, 'yyyy-mm')) as QUA
    from yishou_data.dwm_goods_sp_up_info_dt
    where dt >= '20230101'
    GROUP BY to_char(special_date, 'yyyy-mm')
)
-- 季度
,t2 as  (
    SELECT   concat(to_char(max(stat_month),'yyyy'),'Q',QUA) as stat_month
            ,sum(GMV) as GMV
            ,case when QUA = 1 then 781584582 
             when QUA = 2 then 959832931 
             when QUA = 3 then 871137054 
             when QUA = 4 then 1421482117 
            end  as goods_value_target_s
            ,case when QUA = 1 then 719057816 
             when QUA = 2 then 888734196 
             when QUA = 3 then 806608383 
             when QUA = 4 then 1316187147 
            end  as goods_value_target_a
            ,case when QUA = 1 then 702280485.58 
             when QUA = 2 then 817635459 
             when QUA = 3 then 742079712 
             when QUA = 4 then 1210892175 
            end  as goods_value_target_b
            FROM t1 
            GROUP BY QUA 
)
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,GMV
        ,case when  to_char(stat_month,'mm') = '01' then 294712995
        when  to_char(stat_month,'mm') = '02' then 214185243
        when  to_char(stat_month,'mm') = '03' then 335213112 
        when  to_char(stat_month,'mm') = '04' then 348762122
        when  to_char(stat_month,'mm') = '05' then 329895871
        when  to_char(stat_month,'mm') = '06' then 281174938
        when  to_char(stat_month,'mm') = '07' then 194498030
        when  to_char(stat_month,'mm') = '08' then 301573423
        when  to_char(stat_month,'mm') = '09' then 375065601
        when  to_char(stat_month,'mm') = '10' then 457716347
        when  to_char(stat_month,'mm') = '11' then 501956322
        when  to_char(stat_month,'mm') = '12' then 461809448
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 272882402
        when  to_char(stat_month,'mm') = '02' then 198319669
        when  to_char(stat_month,'mm') = '03' then 310382511 
        when  to_char(stat_month,'mm') = '04' then 322927891
        when  to_char(stat_month,'mm') = '05' then 305459140
        when  to_char(stat_month,'mm') = '06' then 260347165
        when  to_char(stat_month,'mm') = '07' then 180090768
        when  to_char(stat_month,'mm') = '08' then 279234651
        when  to_char(stat_month,'mm') = '09' then 347282964
        when  to_char(stat_month,'mm') = '10' then 423811433
        when  to_char(stat_month,'mm') = '11' then 464774373
        when  to_char(stat_month,'mm') = '12' then 427601341
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 251051810
        when  to_char(stat_month,'mm') = '02' then 182454096
        when  to_char(stat_month,'mm') = '03' then 285551910 
        when  to_char(stat_month,'mm') = '04' then 297093659
        when  to_char(stat_month,'mm') = '05' then 281022408
        when  to_char(stat_month,'mm') = '06' then 239519392
        when  to_char(stat_month,'mm') = '07' then 165683507
        when  to_char(stat_month,'mm') = '08' then 256895879
        when  to_char(stat_month,'mm') = '09' then 319500326
        when  to_char(stat_month,'mm') = '10' then 389906518
        when  to_char(stat_month,'mm') = '11' then 427592423
        when  to_char(stat_month,'mm') = '12' then 393393234
        end as goods_value_target_b
    from t1
)
 INSERT INTO  yishou_daily.responsible_person_indicators
    SELECT 
     '场：平台中心 企划' as 部门
	 ,'暂无' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'大盘实际GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,GMV as 当前实际值
     , CASE when t3.GMV >= goods_value_target_s then 'S'
            when t3.GMV >= goods_value_target_a then 'A'
            when t3.GMV >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((GMV/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((GMV/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((GMV/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'指标释义：大盘实际GMV，不含专场结束前取消' as  指标定义
	 ,0.50 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '场：平台中心 企划' as 部门
	 ,'暂无' as 负责人
	 ,stat_month  as 季度or月份
	 ,'大盘实际GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,GMV as 当前实际值
     , CASE when t2.GMV >= goods_value_target_s then 'S'
            when t2.GMV >= goods_value_target_a then 'A'
            when t2.GMV >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((GMV/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((GMV/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((GMV/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'指标释义：大盘实际GMV，不含专场结束前取消' as  指标定义
	 ,0.50 as 权重
	 from t2;


-- 场：平台中心 企划 责任人:暂无   日新款动销款数
with t1 as (
    SELECT to_char(special_date, 'yyyy-mm')          as stat_month
         , round(count(DISTINCT goods_no)  / day(last_day(to_char(special_date, 'yyyy-mm'))),0)            as 日均新动销款
         , QUARTER(to_char(special_date, 'yyyy-mm')) as QUA
    from (SELECT special_date
               , goods_no          as goods_no
               , sum(real_buy_num) as real_buy_num
          from yishou_data.dwm_goods_sp_up_info_dt
          where dt >= '20230101'
            and is_new = 1
          GROUP BY special_date, goods_no) a
    WHERE real_buy_num > 0
    GROUP BY to_char(special_date, 'yyyy-mm')
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(日均新动销款) as 当前实际值
            ,case when QUA = 1 then 4220
             when QUA = 2 then 5205
             when QUA = 3 then 4289
             when QUA = 4 then 4523
            end  as goods_value_target_s
            ,case when QUA = 1 then 4127
             when QUA = 2 then 5091
             when QUA = 3 then 4196
             when QUA = 4 then 4423
            end  as goods_value_target_a
            ,case when QUA = 1 then 4017
             when QUA = 2 then 4954
             when QUA = 3 then 4083
             when QUA = 4 then 4304
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,日均新动销款 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 617
        when  to_char(stat_month,'mm') = '02' then 1618
        when  to_char(stat_month,'mm') = '03' then 1985
        when  to_char(stat_month,'mm') = '04' then 1934
        when  to_char(stat_month,'mm') = '05' then 1787
        when  to_char(stat_month,'mm') = '06' then 1484
        when  to_char(stat_month,'mm') = '07' then 1108
        when  to_char(stat_month,'mm') = '08' then 1605
        when  to_char(stat_month,'mm') = '09' then 1576
        when  to_char(stat_month,'mm') = '10' then 1638
        when  to_char(stat_month,'mm') = '11' then 1588
        when  to_char(stat_month,'mm') = '12' then 1297
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 604
        when  to_char(stat_month,'mm') = '02' then 1582
        when  to_char(stat_month,'mm') = '03' then 1941
        when  to_char(stat_month,'mm') = '04' then 1892
        when  to_char(stat_month,'mm') = '05' then 1748
        when  to_char(stat_month,'mm') = '06' then 1451
        when  to_char(stat_month,'mm') = '07' then 1084
        when  to_char(stat_month,'mm') = '08' then 1570
        when  to_char(stat_month,'mm') = '09' then 1542
        when  to_char(stat_month,'mm') = '10' then 1602
        when  to_char(stat_month,'mm') = '11' then 1553
        when  to_char(stat_month,'mm') = '12' then 1268
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 588
        when  to_char(stat_month,'mm') = '02' then 1540
        when  to_char(stat_month,'mm') = '03' then 1889
        when  to_char(stat_month,'mm') = '04' then 1841
        when  to_char(stat_month,'mm') = '05' then 1701
        when  to_char(stat_month,'mm') = '06' then 1412
        when  to_char(stat_month,'mm') = '07' then 1055
        when  to_char(stat_month,'mm') = '08' then 1528
        when  to_char(stat_month,'mm') = '09' then 1500
        when  to_char(stat_month,'mm') = '10' then 1559
        when  to_char(stat_month,'mm') = '11' then 1511
        when  to_char(stat_month,'mm') = '12' then 1234
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '场：平台中心 企划' as 部门
	 ,'暂无' as 负责人
	 ,stat_month as 季度or月份
	 ,'日新款动销款数' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"日销>0的新品数量，剔除专场结束前取消" as  指标定义
	 ,0.30 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '场：平台中心 企划' as 部门
	 ,'暂无' as 负责人
	 ,stat_month  as 季度or月份
	 ,'日新款动销款数' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"日销>0的新品数量，剔除专场结束前取消" as  指标定义
	 ,0.30 as 权重
	 from t2
;



-- 场：平台中心 企划  责任人:暂无  企划满意度
-- 暂无取数路径


-- 场：平台中心     商品&活动运营 责任人: 暂无  大盘实际GMV
-- 月份
WITH t1 (
    select to_char(special_date, 'yyyy-mm')          as stat_month,
       sum(real_buy_amount)                        GMV,
       QUARTER(to_char(special_date, 'yyyy-mm')) as QUA
    from yishou_data.dwm_goods_sp_up_info_dt
    where dt >= '20230101'
    GROUP BY to_char(special_date, 'yyyy-mm')
)
-- 季度
,t2 as  (
    SELECT   concat(to_char(max(stat_month),'yyyy'),'Q',QUA) as stat_month
            ,sum(GMV) as GMV
            ,case when QUA = 1 then 781584582 
             when QUA = 2 then 959832931 
             when QUA = 3 then 871137054 
             when QUA = 4 then 1421482117 
            end  as goods_value_target_s
            ,case when QUA = 1 then 719057816 
             when QUA = 2 then 888734196 
             when QUA = 3 then 806608383 
             when QUA = 4 then 1316187147 
            end  as goods_value_target_a
            ,case when QUA = 1 then 702280485.58 
             when QUA = 2 then 817635459 
             when QUA = 3 then 742079712 
             when QUA = 4 then 1210892175 
            end  as goods_value_target_b
            FROM t1 
            GROUP BY QUA 
)
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,GMV
        ,case when  to_char(stat_month,'mm') = '01' then 294712995
        when  to_char(stat_month,'mm') = '02' then 214185243
        when  to_char(stat_month,'mm') = '03' then 335213112 
        when  to_char(stat_month,'mm') = '04' then 348762122
        when  to_char(stat_month,'mm') = '05' then 329895871
        when  to_char(stat_month,'mm') = '06' then 281174938
        when  to_char(stat_month,'mm') = '07' then 194498030
        when  to_char(stat_month,'mm') = '08' then 301573423
        when  to_char(stat_month,'mm') = '09' then 375065601
        when  to_char(stat_month,'mm') = '10' then 457716347
        when  to_char(stat_month,'mm') = '11' then 501956322
        when  to_char(stat_month,'mm') = '12' then 461809448
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 272882402
        when  to_char(stat_month,'mm') = '02' then 198319669
        when  to_char(stat_month,'mm') = '03' then 310382511 
        when  to_char(stat_month,'mm') = '04' then 322927891
        when  to_char(stat_month,'mm') = '05' then 305459140
        when  to_char(stat_month,'mm') = '06' then 260347165
        when  to_char(stat_month,'mm') = '07' then 180090768
        when  to_char(stat_month,'mm') = '08' then 279234651
        when  to_char(stat_month,'mm') = '09' then 347282964
        when  to_char(stat_month,'mm') = '10' then 423811433
        when  to_char(stat_month,'mm') = '11' then 464774373
        when  to_char(stat_month,'mm') = '12' then 427601341
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 251051810
        when  to_char(stat_month,'mm') = '02' then 182454096
        when  to_char(stat_month,'mm') = '03' then 285551910 
        when  to_char(stat_month,'mm') = '04' then 297093659
        when  to_char(stat_month,'mm') = '05' then 281022408
        when  to_char(stat_month,'mm') = '06' then 239519392
        when  to_char(stat_month,'mm') = '07' then 165683507
        when  to_char(stat_month,'mm') = '08' then 256895879
        when  to_char(stat_month,'mm') = '09' then 319500326
        when  to_char(stat_month,'mm') = '10' then 389906518
        when  to_char(stat_month,'mm') = '11' then 427592423
        when  to_char(stat_month,'mm') = '12' then 393393234
        end as goods_value_target_b
    from t1
)
 INSERT INTO  yishou_daily.responsible_person_indicators
    SELECT 
     '场：平台中心 商品&活动运营' as 部门
	 ,'暂无' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'大盘实际GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,GMV as 当前实际值
     , CASE when t3.GMV >= goods_value_target_s then 'S'
            when t3.GMV >= goods_value_target_a then 'A'
            when t3.GMV >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((GMV/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((GMV/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((GMV/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'指标释义：大盘实际GMV，不含专场结束前取消' as  指标定义
	 ,0.30 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '场：平台中心 商品&活动运营' as 部门
	 ,'暂无' as 负责人
	 ,stat_month  as 季度or月份
	 ,'大盘实际GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,GMV as 当前实际值
     , CASE when t2.GMV >= goods_value_target_s then 'S'
            when t2.GMV >= goods_value_target_a then 'A'
            when t2.GMV >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((GMV/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((GMV/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((GMV/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'指标释义：大盘实际GMV，不含专场结束前取消' as  指标定义
	 ,0.30 as 权重
	 from t2;


-- 场：平台中心     商品&活动运营 责任人: 暂无  月爆款GMV
with t1 as (
SELECT 
	to_char(ym,'yyyy-mm') as stat_month,
    round(sum(cbk_real_gmv),4) as GMV,
    QUARTER(to_char(ym,'yyyy-mm')) as QUA
from cross_origin.finebi_kpi_cbk_lzj_ym
where ym >= '2023-01-01'
GROUP BY ym
)
-- 季度
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(GMV) as GMV
            ,case when QUA = 1 then 439089663 
             when QUA = 2 then 463559589 
             when QUA = 3 then 444069433 
             when QUA = 4 then 874384669 
            end  as goods_value_target_s
            ,case when QUA = 1 then 406351548 
             when QUA = 2 then 430176797 
             when QUA = 3 then 412958915 
             when QUA = 4 then 811729296 
            end  as goods_value_target_a
            ,case when QUA = 1 then 367454778 
             when QUA = 2 then 390514073 
             when QUA = 3 then 375995922 
             when QUA = 4 then 737287270 
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA 
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,GMV
        ,case when  to_char(stat_month,'mm') = '01' then 125126804
        when  to_char(stat_month,'mm') = '02' then 137280848
        when  to_char(stat_month,'mm') = '03' then 176682010 
        when  to_char(stat_month,'mm') = '04' then 178180259
        when  to_char(stat_month,'mm') = '05' then 158166920
        when  to_char(stat_month,'mm') = '06' then 127212410
        when  to_char(stat_month,'mm') = '07' then 83198036
        when  to_char(stat_month,'mm') = '08' then 158487759
        when  to_char(stat_month,'mm') = '09' then 202383638
        when  to_char(stat_month,'mm') = '10' then 262254088
        when  to_char(stat_month,'mm') = '11' then 318220867
        when  to_char(stat_month,'mm') = '12' then 293909713
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 119423823
        when  to_char(stat_month,'mm') = '02' then 96348429
        when  to_char(stat_month,'mm') = '03' then 163853194 
        when  to_char(stat_month,'mm') = '04' then 165335001
        when  to_char(stat_month,'mm') = '05' then 146780705
        when  to_char(stat_month,'mm') = '06' then 118061090
        when  to_char(stat_month,'mm') = '07' then 77221505
        when  to_char(stat_month,'mm') = '08' then 147363878
        when  to_char(stat_month,'mm') = '09' then 188373530
        when  to_char(stat_month,'mm') = '10' then 244124271
        when  to_char(stat_month,'mm') = '11' then 295442478
        when  to_char(stat_month,'mm') = '12' then 272162547
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 112648003
        when  to_char(stat_month,'mm') = '02' then 106195738
        when  to_char(stat_month,'mm') = '03' then 148611036 
        when  to_char(stat_month,'mm') = '04' then 150073308
        when  to_char(stat_month,'mm') = '05' then 133252530
        when  to_char(stat_month,'mm') = '06' then 107188234
        when  to_char(stat_month,'mm') = '07' then 70120677
        when  to_char(stat_month,'mm') = '08' then 134147387
        when  to_char(stat_month,'mm') = '09' then 171727857
        when  to_char(stat_month,'mm') = '10' then 222583894
        when  to_char(stat_month,'mm') = '11' then 268379044
        when  to_char(stat_month,'mm') = '12' then 246324331
        end as goods_value_target_b
    from t1
)
 INSERT INTO  yishou_daily.responsible_person_indicators
    SELECT 
     '场：平台中心 商品&活动运营' as 部门
	 ,'暂无' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'月爆款GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,GMV as 当前实际值
     , CASE when t3.GMV >= goods_value_target_s then 'S'
            when t3.GMV >= goods_value_target_a then 'A'
            when t3.GMV >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((GMV/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((GMV/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((GMV/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'月爆款定义：	2~7月，月实际gmv>5000	8~1月，月实际gmv>6000	且实际uv价值>大盘的实际uv价值均值*80%的商品	(23年业绩占比52%左右)	（包含未上架专场商品曝光、多渠道曝光不去重）' as  指标定义
	 ,0.25 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '场：平台中心 商品&活动运营' as 部门
	 ,'暂无' as 负责人
	 ,stat_month  as 季度or月份
	 ,'月爆款GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,GMV as 当前实际值
     , CASE when t2.GMV >= goods_value_target_s then 'S'
            when t2.GMV >= goods_value_target_a then 'A'
            when t2.GMV >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((GMV/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((GMV/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((GMV/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'月爆款定义：	2~7月，月实际gmv>5000	8~1月，月实际gmv>6000	且实际uv价值>大盘的实际uv价值均值*80%的商品	(23年业绩占比52%左右)	（包含未上架专场商品曝光、多渠道曝光不去重）' as  指标定义
	 ,0.25 as 权重
	 from t2;


-- 场：平台中心     商品&活动运营 责任人: 暂无  日均动销款数
WITH t1 as (
    SELECT to_char(special_date, 'yyyy-mm')          as stat_month
         , round(count(goods_no) / day(last_day(to_char(special_date, 'yyyy-mm'))),0)            as 日均动销款
         , QUARTER(to_char(special_date, 'yyyy-mm')) as QUA
        --  ,count(DISTINCT goods_no)
        --  ,count( goods_no)
    from (SELECT special_date
               , goods_no          as goods_no
               , sum(real_buy_num) as real_buy_num
          from yishou_data.dwm_goods_sp_up_info_dt
          where dt >= '20230101'
            -- and is_new = 1
          GROUP BY special_date, goods_no) a
    WHERE real_buy_num > 0   -- 实际销售件数 > 0
    GROUP BY to_char(special_date, 'yyyy-mm')
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(日均动销款) as 当前实际值
            ,case when QUA = 1 then 75226
             when QUA = 2 then 109813
             when QUA = 3 then 86162
             when QUA = 4 then 108954
            end  as goods_value_target_s
            ,case when QUA = 1 then 73931
             when QUA = 2 then 107924
             when QUA = 3 then 84680
             when QUA = 4 then 107080
            end  as goods_value_target_a
            ,case when QUA = 1 then 72378
             when QUA = 2 then 105656
             when QUA = 3 then 82901
             when QUA = 4 then 104830
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,日均动销款 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 26305
        when  to_char(stat_month,'mm') = '02' then 14875
        when  to_char(stat_month,'mm') = '03' then 34046
        when  to_char(stat_month,'mm') = '04' then 37562
        when  to_char(stat_month,'mm') = '05' then 38345
        when  to_char(stat_month,'mm') = '06' then 33907
        when  to_char(stat_month,'mm') = '07' then 25808
        when  to_char(stat_month,'mm') = '08' then 27782
        when  to_char(stat_month,'mm') = '09' then 32572
        when  to_char(stat_month,'mm') = '10' then 35331
        when  to_char(stat_month,'mm') = '11' then 37307
        when  to_char(stat_month,'mm') = '12' then 36317
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 25853
        when  to_char(stat_month,'mm') = '02' then 14619
        when  to_char(stat_month,'mm') = '03' then 33460
        when  to_char(stat_month,'mm') = '04' then 36915
        when  to_char(stat_month,'mm') = '05' then 37685
        when  to_char(stat_month,'mm') = '06' then 33324
        when  to_char(stat_month,'mm') = '07' then 25364
        when  to_char(stat_month,'mm') = '08' then 27304
        when  to_char(stat_month,'mm') = '09' then 32012
        when  to_char(stat_month,'mm') = '10' then 34723
        when  to_char(stat_month,'mm') = '11' then 36665
        when  to_char(stat_month,'mm') = '12' then 35692
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 25309
        when  to_char(stat_month,'mm') = '02' then 14312
        when  to_char(stat_month,'mm') = '03' then 32757
        when  to_char(stat_month,'mm') = '04' then 36140
        when  to_char(stat_month,'mm') = '05' then 36893
        when  to_char(stat_month,'mm') = '06' then 32624
        when  to_char(stat_month,'mm') = '07' then 24831
        when  to_char(stat_month,'mm') = '08' then 26730
        when  to_char(stat_month,'mm') = '09' then 31339
        when  to_char(stat_month,'mm') = '10' then 33993
        when  to_char(stat_month,'mm') = '11' then 35895
        when  to_char(stat_month,'mm') = '12' then 34942
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '场：平台中心 商品&活动运营' as 部门
	 ,'暂无' as 负责人
	 ,stat_month as 季度or月份
	 ,'日均动销款数' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"日销>0的商品数量，剔除专场结束前取消" as  指标定义
	 ,0.20 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '场：平台中心 商品&活动运营' as 部门
	 ,'暂无' as 负责人
	 ,stat_month  as 季度or月份
	 ,'日均动销款数' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"日销>0的商品数量，剔除专场结束前取消" as  指标定义
	 ,0.20 as 权重
	 from t2
;


-- 场：平台中心 商品&活动运营 责任人: 暂无  潜力商家
-- 有重复指标
WITH t1 as (
	SELECT  to_date1(月份,'yyyymm') as stat_month 
			-- , max(供应商ID)
			-- , max(潜力类型)
			-- , min(潜力类型)
			, sum(潜力商家得分) as 潜力商家得分
			-- ,count(1)
			-- ,count(DISTINCT  供应商ID)
			,QUARTER(to_date1(月份,'yyyymm') ) as QUA
	from (SELECT mt                                                                           月份
			, supply_id                                                                    供应商ID
			, supply_name                                                                  供应商名称
			, department                                                                   责任部门
			, primary_market_name                                                          一级市场
			, big_market                                                                   大市场
			, big_market_ql                                                                对比大市场
			, poten_score                                                                  潜力商家得分
			, case
					when real_buy_amount <= 500000 AND poten_score > 0 then '达成双潜力'
					when poten_score > 0 and real_buy_amount > 500000 then '达成50W潜力' end 潜力类型
			, uv_value                                                                     当月UV价值
			, big_market_ql_uv_value                                                       当月对照大市场UV价值
			, big_market_uv_value                                                          当月大市场UV价值
			, department_uv_value                                                          当月部门UV价值
			, buy_amount                                                                   当月gmv
			, real_buy_amount                                                              当月实际gmv
			, poten_score_this_mon                                                         当月是否符合要求
			, poten_score_11_mons                                                          过去11个月是否符合要求
		FROM yishou_daily.finebi_dwd_poten_supply_summary_mt t1
		where mt >= 202301) a
-- 	where 责任部门 = '1十三行'
	GROUP BY 月份
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(潜力商家得分) as 当前实际值
            ,case when QUA = 1 then 82
             when QUA = 2 then 86
             when QUA = 3 then 84
             when QUA = 4 then 163
            end  as goods_value_target_s
            ,case when QUA = 1 then 72
             when QUA = 2 then 79
             when QUA = 3 then 77
             when QUA = 4 then 156
            end  as goods_value_target_a
            ,case when QUA = 1 then 65
             when QUA = 2 then 72
             when QUA = 3 then 70
             when QUA = 4 then 149
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,潜力商家得分 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 24
        when  to_char(stat_month,'mm') = '02' then 30
        when  to_char(stat_month,'mm') = '03' then 28
        when  to_char(stat_month,'mm') = '04' then 30
        when  to_char(stat_month,'mm') = '05' then 29
        when  to_char(stat_month,'mm') = '06' then 27
        when  to_char(stat_month,'mm') = '07' then 17
        when  to_char(stat_month,'mm') = '08' then 32
        when  to_char(stat_month,'mm') = '09' then 35
        when  to_char(stat_month,'mm') = '10' then 45
        when  to_char(stat_month,'mm') = '11' then 65
        when  to_char(stat_month,'mm') = '12' then 53
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 20
        when  to_char(stat_month,'mm') = '02' then 27
        when  to_char(stat_month,'mm') = '03' then 25
        when  to_char(stat_month,'mm') = '04' then 29
        when  to_char(stat_month,'mm') = '05' then 26
        when  to_char(stat_month,'mm') = '06' then 24
        when  to_char(stat_month,'mm') = '07' then 16
        when  to_char(stat_month,'mm') = '08' then 29
        when  to_char(stat_month,'mm') = '09' then 32
        when  to_char(stat_month,'mm') = '10' then 43
        when  to_char(stat_month,'mm') = '11' then 62
        when  to_char(stat_month,'mm') = '12' then 51
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 17
        when  to_char(stat_month,'mm') = '02' then 23
        when  to_char(stat_month,'mm') = '03' then 25
        when  to_char(stat_month,'mm') = '04' then 26
        when  to_char(stat_month,'mm') = '05' then 22
        when  to_char(stat_month,'mm') = '06' then 24
        when  to_char(stat_month,'mm') = '07' then 13
        when  to_char(stat_month,'mm') = '08' then 25
        when  to_char(stat_month,'mm') = '09' then 32
        when  to_char(stat_month,'mm') = '10' then 40
        when  to_char(stat_month,'mm') = '11' then 58
        when  to_char(stat_month,'mm') = '12' then 51
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '场：平台中心 商品&活动运营' as 部门
	 ,'暂无' as 负责人
	 ,stat_month as 季度or月份
	 ,'潜力商家' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s and goods_value_target_s > goods_value_target_a then 'S'
            when t3.当前实际值 >= goods_value_target_a and goods_value_target_a > goods_value_target_b then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"累计12个月实际GMV首次达到20万+、50万+的商家总数量；若直接从20万以下直接做到50万的，算2家；实际UV价值大于所在市场的实际UV价值平均值" as  指标定义
	 ,0.15 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '场：平台中心 商品&活动运营' as 部门
	 ,'暂无' as 负责人
	 ,stat_month  as 季度or月份
	 ,'潜力商家' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     ,CASE when t2.当前实际值 >= goods_value_target_s and goods_value_target_s > goods_value_target_a then 'S'
            when t2.当前实际值 >= goods_value_target_a and goods_value_target_a > goods_value_target_b then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"累计12个月实际GMV首次达到20万+、50万+的商家总数量；若直接从20万以下直接做到50万的，算2家；实际UV价值大于所在市场的实际UV价值平均值" as  指标定义
	 ,0.15 as 权重
	 from t2
;




-- 货：供给 履约 浩兵 最终配货率-15天
with t1 as (
	select
		stat_date_str,
		date_type,
		to_char(to_date1(stat_date_str,'yyyymm'),'yyyy-mm') as stat_month,
		-- sum(h24_allot_amount)/sum(end_pay_amount) h24配率,
		coalesce(sum(d15_allot_amount)/coalesce(sum(d15_end_pay_amount),0.0),0.0) d15最终配率,
		CASE WHEN  date_type = '季度' then SUBSTR(stat_date_str,6,1) ELSE NULL end as QUA
	from yishou_daily.finebi_performance_kpi_2024_allot_data a
	WHERE date_type = '季度' or date_type = '月度'
	group by stat_date_str,date_type
)
,t2 as  (
    SELECT   stat_date_str as stat_month
            ,d15最终配率 * 100 as 当前实际值
            ,case when QUA = 1 then 90.71
             when QUA = 2 then 92.92
             when QUA = 3 then 93.18
             when QUA = 4 then 92.99
            end  as goods_value_target_s
            ,case when QUA = 1 then 90.21
             when QUA = 2 then 92.42
             when QUA = 3 then 92.68
             when QUA = 4 then 92.49
            end  as goods_value_target_a
            ,case when QUA = 1 then 89.21
             when QUA = 2 then 91.42
             when QUA = 3 then 91.68
             when QUA = 4 then 91.49
            end  as goods_value_target_b
            FROM t1 
            WHERE date_type = '季度'
)
-- 月份
,t3 as (
    select
        stat_date_str as stat_month 
        ,d15最终配率 * 100 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 88.51
        when  to_char(stat_month,'mm') = '02' then 89.51
        when  to_char(stat_month,'mm') = '03' then 93.42
        when  to_char(stat_month,'mm') = '04' then 92.96
        when  to_char(stat_month,'mm') = '05' then 93.94
        when  to_char(stat_month,'mm') = '06' then 91.68
        when  to_char(stat_month,'mm') = '07' then 91.38
        when  to_char(stat_month,'mm') = '08' then 94
        when  to_char(stat_month,'mm') = '09' then 93.45
        when  to_char(stat_month,'mm') = '10' then 93.81
        when  to_char(stat_month,'mm') = '11' then 92.71
        when  to_char(stat_month,'mm') = '12' then 92.5
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 88.01
        when  to_char(stat_month,'mm') = '02' then 89.01
        when  to_char(stat_month,'mm') = '03' then 92.92
        when  to_char(stat_month,'mm') = '04' then 92.46
        when  to_char(stat_month,'mm') = '05' then 93.44
        when  to_char(stat_month,'mm') = '06' then 91.18
        when  to_char(stat_month,'mm') = '07' then 90.88
        when  to_char(stat_month,'mm') = '08' then 93.5
        when  to_char(stat_month,'mm') = '09' then 92.95
        when  to_char(stat_month,'mm') = '10' then 93.31
        when  to_char(stat_month,'mm') = '11' then 92.21
        when  to_char(stat_month,'mm') = '12' then 92
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 87.01
        when  to_char(stat_month,'mm') = '02' then 88.01
        when  to_char(stat_month,'mm') = '03' then 91.92
        when  to_char(stat_month,'mm') = '04' then 91.46
        when  to_char(stat_month,'mm') = '05' then 92.44
        when  to_char(stat_month,'mm') = '06' then 90.18
        when  to_char(stat_month,'mm') = '07' then 89.88
        when  to_char(stat_month,'mm') = '08' then 92.5
        when  to_char(stat_month,'mm') = '09' then 91.95
        when  to_char(stat_month,'mm') = '10' then 92.31
        when  to_char(stat_month,'mm') = '11' then 91.21
        when  to_char(stat_month,'mm') = '12' then 91
        end as goods_value_target_b
    from t1
    where date_type = '月度'
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '货：供给 履约' as 部门
	 ,'浩兵' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'最终配货率-15天' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'统计周期（专场日期），15天配货金额/应开单金额' as  指标定义
	 ,0.20 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '货：供给 履约' as 部门
	 ,'浩兵' as 负责人
	 ,stat_month  as 季度or月份
	 ,'最终配货率-15天' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'统计周期（专场日期），15天配货金额/应开单金额' as  指标定义
	 ,0.20 as 权重
	 from t2;



-- 货：供给 履约 浩兵 现货GMV
WITH t1 as (
    SELECT to_char(stat_date, 'yyyy-mm') as stat_month,
           sum(spots_clean_gmv)                   as spot_gmv,
           QUARTER(to_char(stat_date, 'yyyy-mm')) as QUA
    from yishou_daily.finebi_performance_kpi_2024_gmv_data
    GROUP BY to_char(stat_date, 'yyyy-mm')
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(spot_gmv) as 当前实际值
            ,case when QUA = 1 then 197950385.29
             when QUA = 2 then 268104914.02
             when QUA = 3 then 273534989.09
             when QUA = 4 then 570732754.93
            end  as goods_value_target_s
            ,case when QUA = 1 then 143239464.51
             when QUA = 2 then 205893520.35
             when QUA = 3 then 217072402.28
             when QUA = 4 then 478599654.69
            end  as goods_value_target_a
            ,case when QUA = 1 then 88528543.74
             when QUA = 2 then 143682126.67
             when QUA = 3 then 160609815.46
             when QUA = 4 then 386466554.45
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,spot_gmv as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 69711503.94
        when  to_char(stat_month,'mm') = '02' then 47248236.5
        when  to_char(stat_month,'mm') = '03' then 80990644.86
        when  to_char(stat_month,'mm') = '04' then 95081967.48
        when  to_char(stat_month,'mm') = '05' then 92759529.46
        when  to_char(stat_month,'mm') = '06' then 80263417.09
        when  to_char(stat_month,'mm') = '07' then 65322986.32
        when  to_char(stat_month,'mm') = '08' then 95202031.15
        when  to_char(stat_month,'mm') = '09' then 113009971.63
        when  to_char(stat_month,'mm') = '10' then 181886752.92
        when  to_char(stat_month,'mm') = '11' then 200777965.7
        when  to_char(stat_month,'mm') = '12' then 188068036.31
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 50609735.77
        when  to_char(stat_month,'mm') = '02' then 33365859.66
        when  to_char(stat_month,'mm') = '03' then 59263869.08
        when  to_char(stat_month,'mm') = '04' then 72477015.13
        when  to_char(stat_month,'mm') = '05' then 71377389.68
        when  to_char(stat_month,'mm') = '06' then 62039115.54
        when  to_char(stat_month,'mm') = '07' then 52716632.53
        when  to_char(stat_month,'mm') = '08' then 75655605.56
        when  to_char(stat_month,'mm') = '09' then 88700164.18
        when  to_char(stat_month,'mm') = '10' then 152219952.64
        when  to_char(stat_month,'mm') = '11' then 168243759.62
        when  to_char(stat_month,'mm') = '12' then 158135942.43
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 31507967.6
        when  to_char(stat_month,'mm') = '02' then 19483482.83
        when  to_char(stat_month,'mm') = '03' then 37537093.31
        when  to_char(stat_month,'mm') = '04' then 49872062.78
        when  to_char(stat_month,'mm') = '05' then 49995249.91
        when  to_char(stat_month,'mm') = '06' then 43814813.99
        when  to_char(stat_month,'mm') = '07' then 40110278.75
        when  to_char(stat_month,'mm') = '08' then 56109179.98
        when  to_char(stat_month,'mm') = '09' then 64390356.74
        when  to_char(stat_month,'mm') = '10' then 122553152.36
        when  to_char(stat_month,'mm') = '11' then 135709553.54
        when  to_char(stat_month,'mm') = '12' then 128203848.55
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '货：供给 履约' as 部门
	 ,'浩兵' as 负责人
	 ,stat_month as 季度or月份
	 ,'现货GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"统计口径: 专场结束后，前置仓+备货+未来确定性库存的总实发GMV" as  指标定义
	 ,0.20 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '货：供给 履约' as 部门
	 ,'浩兵' as 负责人
	 ,stat_month  as 季度or月份
	 ,'现货GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"统计口径: 专场结束后，前置仓+备货+未来确定性库存的总实发GMV" as  指标定义
	 ,0.20 as 权重
	 from t2
;



-- 货：供给 履约 浩兵 买货损失率
with t1 as (
	SELECT 
		to_char(统计日期,'yyyy-mm') as stat_month
		,(sum(现货专场30天内销售采购额) * 1.1 - sum(现货专场30天内销售金额) + 1.1 * sum(现货专场30天内销售数量) + sum(周转超30天采购金额) * 0.3) as 累计亏损额
		,sum(入库金额) as 入库金额
		,(sum(现货专场30天内销售采购额) * 1.1 - sum(现货专场30天内销售金额) + 1.1 * sum(现货专场30天内销售数量) + sum(周转超30天采购金额) * 0.3) / sum(入库金额)  as 亏损率
		,'月份' as 维度
		,QUARTER(to_char(统计日期,'yyyy-mm')) as QUA
	from (
	select
		stat_date 统计日期,
		stock_in_amount 入库金额,
		day30_spot_sale_amount 现货专场30天内销售金额,
		day30_spot_sale_num 现货专场30天内销售数量,
		day30_spot_sale_pur_amount 现货专场30天内销售采购额,
		over_day30_pur_amount 周转超30天采购金额
	from yishou_daily.dws_supply_chain_stock_in_sale_detail_full_dt
	where stat_date  >= '2023-01-01'
	and p_type_name in ('手动计划_备货采购','手动计划_当天备货','手动计划_邀约配货','提前采购')	
	)
	GROUP BY to_char(统计日期,'yyyy-mm')
	UNION ALL 
	SELECT 
		concat(to_char(统计日期,'yyyy'),'Q',QUARTER(to_char(统计日期,'yyyy-mm'))) as stat_month
		,(sum(现货专场30天内销售采购额) * 1.1 - sum(现货专场30天内销售金额) + 1.1 * sum(现货专场30天内销售数量) + sum(周转超30天采购金额) * 0.3) as 累计亏损额
		,sum(入库金额) as 入库金额
		,(sum(现货专场30天内销售采购额) * 1.1 - sum(现货专场30天内销售金额) + 1.1 * sum(现货专场30天内销售数量) + sum(周转超30天采购金额) * 0.3) / sum(入库金额)  as 亏损率
		,'季度' as 维度
		,QUARTER(to_char(统计日期,'yyyy-mm')) as QUA
	from (
	select
		stat_date 统计日期,
		stock_in_amount 入库金额,
		day30_spot_sale_amount 现货专场30天内销售金额,
		day30_spot_sale_num 现货专场30天内销售数量,
		day30_spot_sale_pur_amount 现货专场30天内销售采购额,
		over_day30_pur_amount 周转超30天采购金额
	from yishou_daily.dws_supply_chain_stock_in_sale_detail_full_dt
	where stat_date  >= '2023-01-01'
	and p_type_name in ('手动计划_备货采购','手动计划_当天备货','手动计划_邀约配货','提前采购')	
	)
	GROUP BY to_char(统计日期,'yyyy'),QUARTER(to_char(统计日期,'yyyy-mm'))
)
,t2 as  (
    SELECT   stat_month as stat_month
            ,亏损率 * 100 as 当前实际值
            ,case when QUA = 1 then 0.75
             when QUA = 2 then 0.75
             when QUA = 3 then 0.75
             when QUA = 4 then 0.75
            end  as goods_value_target_s
            ,case when QUA = 1 then 1.13
             when QUA = 2 then 1.13
             when QUA = 3 then 1.13
             when QUA = 4 then 1.13
            end  as goods_value_target_a
            ,case when QUA = 1 then 1.8
             when QUA = 2 then 1.8
             when QUA = 3 then 1.8
             when QUA = 4 then 1.8
            end  as goods_value_target_b
            FROM t1 
            where 维度 = '季度'
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,亏损率 * 100 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 0.75
        when  to_char(stat_month,'mm') = '02' then 0.75
        when  to_char(stat_month,'mm') = '03' then 0.75
        when  to_char(stat_month,'mm') = '04' then 0.75
        when  to_char(stat_month,'mm') = '05' then 0.75
        when  to_char(stat_month,'mm') = '06' then 0.75
        when  to_char(stat_month,'mm') = '07' then 0.75
        when  to_char(stat_month,'mm') = '08' then 0.75
        when  to_char(stat_month,'mm') = '09' then 0.75
        when  to_char(stat_month,'mm') = '10' then 0.75
        when  to_char(stat_month,'mm') = '11' then 0.75
        when  to_char(stat_month,'mm') = '12' then 0.75
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 1.13
        when  to_char(stat_month,'mm') = '02' then 1.13
        when  to_char(stat_month,'mm') = '03' then 1.13
        when  to_char(stat_month,'mm') = '04' then 1.13
        when  to_char(stat_month,'mm') = '05' then 1.13
        when  to_char(stat_month,'mm') = '06' then 1.13
        when  to_char(stat_month,'mm') = '07' then 1.13
        when  to_char(stat_month,'mm') = '08' then 1.13
        when  to_char(stat_month,'mm') = '09' then 1.13
        when  to_char(stat_month,'mm') = '10' then 1.13
        when  to_char(stat_month,'mm') = '11' then 1.13
        when  to_char(stat_month,'mm') = '12' then 1.13
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 1.8
        when  to_char(stat_month,'mm') = '02' then 1.8
        when  to_char(stat_month,'mm') = '03' then 1.8
        when  to_char(stat_month,'mm') = '04' then 1.8
        when  to_char(stat_month,'mm') = '05' then 1.8
        when  to_char(stat_month,'mm') = '06' then 1.8
        when  to_char(stat_month,'mm') = '07' then 1.8
        when  to_char(stat_month,'mm') = '08' then 1.8
        when  to_char(stat_month,'mm') = '09' then 1.8
        when  to_char(stat_month,'mm') = '10' then 1.8
        when  to_char(stat_month,'mm') = '11' then 1.8
        when  to_char(stat_month,'mm') = '12' then 1.8
        end as goods_value_target_b
    from t1
    WHERE 维度 = '月份'
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '货：供给 履约' as 部门
	 ,'浩兵' as 负责人
	 ,stat_month as 季度or月份
	 ,'买货损失率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE 
        when (goods_value_target_s = goods_value_target_a and goods_value_target_a = goods_value_target_b) then 
            case    WHEN  t3.当前实际值 >= goods_value_target_b then 'C' 
                    ELSE 'B' 
                    end 
            when t3.当前实际值 < goods_value_target_s then 'S'
            when t3.当前实际值 < goods_value_target_a then 'A'
            when t3.当前实际值 < goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/当前实际值) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/当前实际值) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/当前实际值) * 100,2)),'%') as B级达成率
	 ,'（30天周转损失+30天后库存*0.3）/买货额' as  指标定义
	 ,0.10 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '货：供给 履约' as 部门
	 ,'浩兵' as 负责人
	 ,stat_month  as 季度or月份
	 ,'买货损失率' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 < goods_value_target_s then 'S'
            when t2.当前实际值 < goods_value_target_a then 'A'
            when t2.当前实际值 < goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((goods_value_target_s/当前实际值) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((goods_value_target_a/当前实际值) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((goods_value_target_b/当前实际值) * 100,2)),'%') as B级达成率
	 ,'（30天周转损失+30天后库存*0.3）/买货额' as  指标定义
	 ,0.10 as 权重
	 from t2
;

-- 场：平台中心 责任人: 苏苏  月爆款GMV
with t1 as (
SELECT 
	to_char(ym,'yyyy-mm') as stat_month,
    round(sum(cbk_real_gmv),4) as GMV,
    QUARTER(to_char(ym,'yyyy-mm')) as QUA
from cross_origin.finebi_kpi_cbk_lzj_ym
where ym >= '2023-01-01'
GROUP BY ym
)
-- 季度
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(GMV) as GMV
            ,QUA
            ,case when QUA = 1 then 439089663 
             when QUA = 2 then 463559589 
             when QUA = 3 then 444069433 
             when QUA = 4 then 874384669 
            end  as goods_value_target_s
            ,case when QUA = 1 then 406351548 
             when QUA = 2 then 430176797 
             when QUA = 3 then 412958915 
             when QUA = 4 then 811729296 
            end  as goods_value_target_a
            ,case when QUA = 1 then 367454778 
             when QUA = 2 then 390514073 
             when QUA = 3 then 375995922 
             when QUA = 4 then 737287270 
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA 
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,GMV
        ,case when  to_char(stat_month,'mm') = '01' then 125126804
        when  to_char(stat_month,'mm') = '02' then 137280848
        when  to_char(stat_month,'mm') = '03' then 176682010 
        when  to_char(stat_month,'mm') = '04' then 178180259
        when  to_char(stat_month,'mm') = '05' then 158166920
        when  to_char(stat_month,'mm') = '06' then 127212410
        when  to_char(stat_month,'mm') = '07' then 83198036
        when  to_char(stat_month,'mm') = '08' then 158487759
        when  to_char(stat_month,'mm') = '09' then 202383638
        when  to_char(stat_month,'mm') = '10' then 262254088
        when  to_char(stat_month,'mm') = '11' then 318220867
        when  to_char(stat_month,'mm') = '12' then 293909713
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 119423823
        when  to_char(stat_month,'mm') = '02' then 96348429
        when  to_char(stat_month,'mm') = '03' then 163853194 
        when  to_char(stat_month,'mm') = '04' then 165335001
        when  to_char(stat_month,'mm') = '05' then 146780705
        when  to_char(stat_month,'mm') = '06' then 118061090
        when  to_char(stat_month,'mm') = '07' then 77221505
        when  to_char(stat_month,'mm') = '08' then 147363878
        when  to_char(stat_month,'mm') = '09' then 188373530
        when  to_char(stat_month,'mm') = '10' then 244124271
        when  to_char(stat_month,'mm') = '11' then 295442478
        when  to_char(stat_month,'mm') = '12' then 272162547
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 112648003
        when  to_char(stat_month,'mm') = '02' then 106195738
        when  to_char(stat_month,'mm') = '03' then 148611036 
        when  to_char(stat_month,'mm') = '04' then 150073308
        when  to_char(stat_month,'mm') = '05' then 133252530
        when  to_char(stat_month,'mm') = '06' then 107188234
        when  to_char(stat_month,'mm') = '07' then 70120677
        when  to_char(stat_month,'mm') = '08' then 134147387
        when  to_char(stat_month,'mm') = '09' then 171727857
        when  to_char(stat_month,'mm') = '10' then 222583894
        when  to_char(stat_month,'mm') = '11' then 268379044
        when  to_char(stat_month,'mm') = '12' then 246324331
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
    SELECT 
     '场：平台中心' as 部门
	 ,'苏苏' as 负责人
	 ,t3.stat_month as 季度or月份
	 ,'月爆款GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,GMV as 当前实际值
     , CASE when t3.GMV >= goods_value_target_s then 'S'
            when t3.GMV >= goods_value_target_a then 'A'
            when t3.GMV >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((GMV/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((GMV/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((GMV/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'月爆款定义：	2~7月，月实际gmv>5000	8~1月，月实际gmv>6000	且实际uv价值>大盘的实际uv价值均值*80%的商品	(23年业绩占比52%左右)	（包含未上架专场商品曝光、多渠道曝光不去重）' as  指标定义
	 ,case when SUBSTR(stat_month,5,2) BETWEEN '01' and '04' THEN  0.30
	    ELSE  0.25 end as 权重
	 from t3
    UNION ALL 
    SELECT 
     '场：平台中心' as 部门
	 ,'苏苏' as 负责人
	 ,stat_month  as 季度or月份
	 ,'月爆款GMV' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,GMV as 当前实际值
     , CASE when t2.GMV >= goods_value_target_s then 'S'
            when t2.GMV >= goods_value_target_a then 'A'
            when t2.GMV >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((GMV/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((GMV/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((GMV/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,'月爆款定义：	2~7月，月实际gmv>5000	8~1月，月实际gmv>6000	且实际uv价值>大盘的实际uv价值均值*80%的商品	(23年业绩占比52%左右)	（包含未上架专场商品曝光、多渠道曝光不去重）' as  指标定义
	 ,case when QUA = 1 then 0.30 
	  when QUA = 2 then 0.30/3 + 0.25/3 + 0.25/3
	  else 0.25 end as 权重	
from t2;


-- 场：平台中心 苏苏  日动销款数 
WITH t1 as (
    SELECT to_char(special_date, 'yyyy-mm')          as stat_month
         , round(count(goods_no) / day(last_day(to_char(special_date, 'yyyy-mm'))),0)            as 日均动销款
         , QUARTER(to_char(special_date, 'yyyy-mm')) as QUA
        --  ,count(DISTINCT goods_no)
        --  ,count( goods_no)
    from (SELECT special_date
               , goods_no          as goods_no
               , sum(real_buy_num) as real_buy_num
          from yishou_data.dwm_goods_sp_up_info_dt
          where dt >= '20230101'
            -- and is_new = 1
          GROUP BY special_date, goods_no) a
    WHERE real_buy_num > 0   -- 实际销售件数 > 0
    GROUP BY to_char(special_date, 'yyyy-mm')
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(日均动销款) as 当前实际值
            ,case when QUA = 1 then 75226
             when QUA = 2 then 109813
             when QUA = 3 then 86162
             when QUA = 4 then 108954
            end  as goods_value_target_s
            ,case when QUA = 1 then 73931
             when QUA = 2 then 107924
             when QUA = 3 then 84680
             when QUA = 4 then 107080
            end  as goods_value_target_a
            ,case when QUA = 1 then 72378
             when QUA = 2 then 105656
             when QUA = 3 then 82901
             when QUA = 4 then 104830
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,日均动销款 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 26305
        when  to_char(stat_month,'mm') = '02' then 14875
        when  to_char(stat_month,'mm') = '03' then 34046
        when  to_char(stat_month,'mm') = '04' then 37562
        when  to_char(stat_month,'mm') = '05' then 38345
        when  to_char(stat_month,'mm') = '06' then 33907
        when  to_char(stat_month,'mm') = '07' then 25808
        when  to_char(stat_month,'mm') = '08' then 27782
        when  to_char(stat_month,'mm') = '09' then 32572
        when  to_char(stat_month,'mm') = '10' then 35331
        when  to_char(stat_month,'mm') = '11' then 37307
        when  to_char(stat_month,'mm') = '12' then 36317
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 25853
        when  to_char(stat_month,'mm') = '02' then 14619
        when  to_char(stat_month,'mm') = '03' then 33460
        when  to_char(stat_month,'mm') = '04' then 36915
        when  to_char(stat_month,'mm') = '05' then 37685
        when  to_char(stat_month,'mm') = '06' then 33324
        when  to_char(stat_month,'mm') = '07' then 25364
        when  to_char(stat_month,'mm') = '08' then 27304
        when  to_char(stat_month,'mm') = '09' then 32012
        when  to_char(stat_month,'mm') = '10' then 34723
        when  to_char(stat_month,'mm') = '11' then 36665
        when  to_char(stat_month,'mm') = '12' then 35692
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 25309
        when  to_char(stat_month,'mm') = '02' then 14312
        when  to_char(stat_month,'mm') = '03' then 32757
        when  to_char(stat_month,'mm') = '04' then 36140
        when  to_char(stat_month,'mm') = '05' then 36893
        when  to_char(stat_month,'mm') = '06' then 32624
        when  to_char(stat_month,'mm') = '07' then 24831
        when  to_char(stat_month,'mm') = '08' then 26730
        when  to_char(stat_month,'mm') = '09' then 31339
        when  to_char(stat_month,'mm') = '10' then 33993
        when  to_char(stat_month,'mm') = '11' then 35895
        when  to_char(stat_month,'mm') = '12' then 34942
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '场：平台中心' as 部门
	 ,'苏苏' as 负责人
	 ,stat_month as 季度or月份
	 ,'日均动销款数' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s then 'S'
            when t3.当前实际值 >= goods_value_target_a then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"日销>0的商品数量，剔除专场结束前取消" as  指标定义
	 ,0.20 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '场：平台中心' as 部门
	 ,'苏苏' as 负责人
	 ,stat_month  as 季度or月份
	 ,'日均动销款数' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s then 'S'
            when t2.当前实际值 >= goods_value_target_a then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"日销>0的商品数量，剔除专场结束前取消" as  指标定义
	 ,0.20 as 权重
	 from t2
;

-- 场：平台中心 责任人: 苏苏  潜力商家
WITH t1 as (
	SELECT  to_date1(月份,'yyyymm') as stat_month 
			-- , max(供应商ID)
			-- , max(潜力类型)
			-- , min(潜力类型)
			, sum(潜力商家得分) as 潜力商家得分
			-- ,count(1)
			-- ,count(DISTINCT  供应商ID)
			,QUARTER(to_date1(月份,'yyyymm') ) as QUA
	from (SELECT mt                                                                           月份
			, supply_id                                                                    供应商ID
			, supply_name                                                                  供应商名称
			, department                                                                   责任部门
			, primary_market_name                                                          一级市场
			, big_market                                                                   大市场
			, big_market_ql                                                                对比大市场
			, poten_score                                                                  潜力商家得分
			, case
					when real_buy_amount <= 500000 AND poten_score > 0 then '达成双潜力'
					when poten_score > 0 and real_buy_amount > 500000 then '达成50W潜力' end 潜力类型
			, uv_value                                                                     当月UV价值
			, big_market_ql_uv_value                                                       当月对照大市场UV价值
			, big_market_uv_value                                                          当月大市场UV价值
			, department_uv_value                                                          当月部门UV价值
			, buy_amount                                                                   当月gmv
			, real_buy_amount                                                              当月实际gmv
			, poten_score_this_mon                                                         当月是否符合要求
			, poten_score_11_mons                                                          过去11个月是否符合要求
		FROM yishou_daily.finebi_dwd_poten_supply_summary_mt t1
		where mt >= 202301) a
-- 	where 责任部门 = '1十三行'
	GROUP BY 月份
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(潜力商家得分) as 当前实际值
            ,QUA
            ,case when QUA = 1 then 82
             when QUA = 2 then 86
             when QUA = 3 then 84
             when QUA = 4 then 163
            end  as goods_value_target_s
            ,case when QUA = 1 then 72
             when QUA = 2 then 79
             when QUA = 3 then 77
             when QUA = 4 then 156
            end  as goods_value_target_a
            ,case when QUA = 1 then 65
             when QUA = 2 then 72
             when QUA = 3 then 70
             when QUA = 4 then 149
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,潜力商家得分 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 24
        when  to_char(stat_month,'mm') = '02' then 30
        when  to_char(stat_month,'mm') = '03' then 28
        when  to_char(stat_month,'mm') = '04' then 30
        when  to_char(stat_month,'mm') = '05' then 29
        when  to_char(stat_month,'mm') = '06' then 27
        when  to_char(stat_month,'mm') = '07' then 17
        when  to_char(stat_month,'mm') = '08' then 32
        when  to_char(stat_month,'mm') = '09' then 35
        when  to_char(stat_month,'mm') = '10' then 45
        when  to_char(stat_month,'mm') = '11' then 65
        when  to_char(stat_month,'mm') = '12' then 53
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 20
        when  to_char(stat_month,'mm') = '02' then 27
        when  to_char(stat_month,'mm') = '03' then 25
        when  to_char(stat_month,'mm') = '04' then 29
        when  to_char(stat_month,'mm') = '05' then 26
        when  to_char(stat_month,'mm') = '06' then 24
        when  to_char(stat_month,'mm') = '07' then 16
        when  to_char(stat_month,'mm') = '08' then 29
        when  to_char(stat_month,'mm') = '09' then 32
        when  to_char(stat_month,'mm') = '10' then 43
        when  to_char(stat_month,'mm') = '11' then 62
        when  to_char(stat_month,'mm') = '12' then 51
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 17
        when  to_char(stat_month,'mm') = '02' then 23
        when  to_char(stat_month,'mm') = '03' then 25
        when  to_char(stat_month,'mm') = '04' then 26
        when  to_char(stat_month,'mm') = '05' then 22
        when  to_char(stat_month,'mm') = '06' then 24
        when  to_char(stat_month,'mm') = '07' then 13
        when  to_char(stat_month,'mm') = '08' then 25
        when  to_char(stat_month,'mm') = '09' then 32
        when  to_char(stat_month,'mm') = '10' then 40
        when  to_char(stat_month,'mm') = '11' then 58
        when  to_char(stat_month,'mm') = '12' then 51
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '场：平台中心' as 部门
	 ,'苏苏' as 负责人
	 ,stat_month as 季度or月份
	 ,'潜力商家' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s and goods_value_target_s > goods_value_target_a then 'S'
            when t3.当前实际值 >= goods_value_target_a and goods_value_target_a > goods_value_target_b then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"累计12个月实际GMV首次达到20万+、50万+的商家总数量；若直接从20万以下直接做到50万的，算2家；实际UV价值大于所在市场的实际UV价值平均值" as  指标定义
	 ,case when SUBSTR(stat_month,5,2) BETWEEN '01' and '04' THEN  0.10
	    ELSE  0.00 end as 权重
	 from t3
    UNION ALL 
    SELECT 
     '场：平台中心' as 部门
	 ,'苏苏' as 负责人
	 ,stat_month  as 季度or月份
	 ,'潜力商家' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     ,CASE when t2.当前实际值 >= goods_value_target_s and goods_value_target_s > goods_value_target_a then 'S'
            when t2.当前实际值 >= goods_value_target_a and goods_value_target_a > goods_value_target_b then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"累计12个月实际GMV首次达到20万+、50万+的商家总数量；若直接从20万以下直接做到50万的，算2家；实际UV价值大于所在市场的实际UV价值平均值" as  指标定义
	 ,case when QUA = 1 then 0.10
	  when QUA = 2 then 0.10/3 + 0.00/3 +0.00/3
	  else 0.00 end as 权重	
	 from t2
;






--  创新业务-产业互联网项目部 衣者 闵宇 新增活跃用户数
WITH t1 as (
	SELECT  to_char(record_date,'yyyy-mm') as stat_month
			,coalesce(lag(current_active) OVER(ORDER BY id),0) as last_month_active
			,current_active
			,coalesce(current_active - lag(current_active) OVER(ORDER BY id) , 0) as 新增活跃用户数
			,QUARTER(record_date) as QUA
	from yishou_data.all_yz_month_data_mt
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,sum(新增活跃用户数) as 当前实际值
            ,case when QUA = 1 then 18
             when QUA = 2 then 26
             when QUA = 3 then 28
             when QUA = 4 then 20
            end  as goods_value_target_s
            ,case when QUA = 1 then 13
             when QUA = 2 then 17
             when QUA = 3 then 17
             when QUA = 4 then 13
            end  as goods_value_target_a
            ,case when QUA = 1 then 10
             when QUA = 2 then 14
             when QUA = 3 then 13
             when QUA = 4 then 10
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,新增活跃用户数 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 4
        when  to_char(stat_month,'mm') = '02' then 4
        when  to_char(stat_month,'mm') = '03' then 10
        when  to_char(stat_month,'mm') = '04' then 7
        when  to_char(stat_month,'mm') = '05' then 10
        when  to_char(stat_month,'mm') = '06' then 9
        when  to_char(stat_month,'mm') = '07' then 9
        when  to_char(stat_month,'mm') = '08' then 9
        when  to_char(stat_month,'mm') = '09' then 10
        when  to_char(stat_month,'mm') = '10' then 7
        when  to_char(stat_month,'mm') = '11' then 9
        when  to_char(stat_month,'mm') = '12' then 4
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 3
        when  to_char(stat_month,'mm') = '02' then 3
        when  to_char(stat_month,'mm') = '03' then 7
        when  to_char(stat_month,'mm') = '04' then 6
        when  to_char(stat_month,'mm') = '05' then 6
        when  to_char(stat_month,'mm') = '06' then 5
        when  to_char(stat_month,'mm') = '07' then 5
        when  to_char(stat_month,'mm') = '08' then 5
        when  to_char(stat_month,'mm') = '09' then 7
        when  to_char(stat_month,'mm') = '10' then 4
        when  to_char(stat_month,'mm') = '11' then 6
        when  to_char(stat_month,'mm') = '12' then 3
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 3
        when  to_char(stat_month,'mm') = '02' then 2
        when  to_char(stat_month,'mm') = '03' then 5
        when  to_char(stat_month,'mm') = '04' then 4
        when  to_char(stat_month,'mm') = '05' then 5
        when  to_char(stat_month,'mm') = '06' then 5
        when  to_char(stat_month,'mm') = '07' then 3
        when  to_char(stat_month,'mm') = '08' then 4
        when  to_char(stat_month,'mm') = '09' then 6
        when  to_char(stat_month,'mm') = '10' then 3
        when  to_char(stat_month,'mm') = '11' then 5
        when  to_char(stat_month,'mm') = '12' then 2
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '创新业务-产业互联网项目部 衣者' as 部门
	 ,'闵宇' as 负责人
	 ,stat_month as 季度or月份
	 ,'新增活跃用户数' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t3.当前实际值 >= goods_value_target_s and goods_value_target_s > goods_value_target_a then 'S'
            when t3.当前实际值 >= goods_value_target_a and goods_value_target_a > goods_value_target_b then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"当月活跃用户数－上月已活跃用户数=新增活跃用户数 活跃用户数：实施时间不为空，且近90天内创建的样板数≥100或者创建的订单数≥100" as  指标定义
	 ,0.50 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '创新业务-产业互联网项目部 衣者' as 部门
	 ,'闵宇' as 负责人
	 ,stat_month  as 季度or月份
	 ,'新增活跃用户数' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s and goods_value_target_s > goods_value_target_a then 'S'
            when t2.当前实际值 >= goods_value_target_a and goods_value_target_a > goods_value_target_b then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"当月活跃用户数－上月已活跃用户数=新增活跃用户数 活跃用户数：实施时间不为空，且近90天内创建的样板数≥100或者创建的订单数≥100" as  指标定义
	 ,0.50 as 权重
	 from t2
;



--  创新业务-产业互联网项目部 衣者 累计活跃用户数
WITH t1 as (
	SELECT  to_char(record_date,'yyyy-mm') as stat_month
			,current_active as 累计活跃用户数
			,QUARTER(record_date) as QUA
	from yishou_data.all_yz_month_data_mt
)
,t2 as  (
    SELECT   concat(to_char(stat_month,'yyyy'),'Q',QUA) as stat_month
            ,max(累计活跃用户数) as 当前实际值
            ,case when QUA = 1 then 75
             when QUA = 2 then 98
             when QUA = 3 then 123
             when QUA = 4 then 140
            end  as goods_value_target_s
            ,case when QUA = 1 then 70
             when QUA = 2 then 84
             when QUA = 3 then 98
             when QUA = 4 then 108
            end  as goods_value_target_a
            ,case when QUA = 1 then 67
             when QUA = 2 then 78
             when QUA = 3 then 88
             when QUA = 4 then 95
            end  as goods_value_target_b
            FROM t1 
            GROUP BY to_char(stat_month,'yyyy'),QUA
)
-- 月份
,t3 as (
    select
        to_char(stat_month,'yyyymm') as stat_month 
        ,累计活跃用户数 as 当前实际值
        ,case when  to_char(stat_month,'mm') = '01' then 63
        when  to_char(stat_month,'mm') = '02' then 66
        when  to_char(stat_month,'mm') = '03' then 75
        when  to_char(stat_month,'mm') = '04' then 81
        when  to_char(stat_month,'mm') = '05' then 90
        when  to_char(stat_month,'mm') = '06' then 98
        when  to_char(stat_month,'mm') = '07' then 106
        when  to_char(stat_month,'mm') = '08' then 114
        when  to_char(stat_month,'mm') = '09' then 123
        when  to_char(stat_month,'mm') = '10' then 129
        when  to_char(stat_month,'mm') = '11' then 137
        when  to_char(stat_month,'mm') = '12' then 140
        end as goods_value_target_s,
        case when  to_char(stat_month,'mm') = '01' then 62
        when  to_char(stat_month,'mm') = '02' then 64
        when  to_char(stat_month,'mm') = '03' then 70
        when  to_char(stat_month,'mm') = '04' then 75
        when  to_char(stat_month,'mm') = '05' then 80
        when  to_char(stat_month,'mm') = '06' then 84
        when  to_char(stat_month,'mm') = '07' then 88
        when  to_char(stat_month,'mm') = '08' then 92
        when  to_char(stat_month,'mm') = '09' then 98
        when  to_char(stat_month,'mm') = '10' then 101
        when  to_char(stat_month,'mm') = '11' then 106
        when  to_char(stat_month,'mm') = '12' then 108
        end as goods_value_target_a,
        case when  to_char(stat_month,'mm') = '01' then 62
        when  to_char(stat_month,'mm') = '02' then 63
        when  to_char(stat_month,'mm') = '03' then 67
        when  to_char(stat_month,'mm') = '04' then 70
        when  to_char(stat_month,'mm') = '05' then 74
        when  to_char(stat_month,'mm') = '06' then 78
        when  to_char(stat_month,'mm') = '07' then 80
        when  to_char(stat_month,'mm') = '08' then 83
        when  to_char(stat_month,'mm') = '09' then 88
        when  to_char(stat_month,'mm') = '10' then 90
        when  to_char(stat_month,'mm') = '11' then 94
        when  to_char(stat_month,'mm') = '12' then 95
        end as goods_value_target_b
    from t1
)
INSERT INTO  yishou_daily.responsible_person_indicators
SELECT 
     '创新业务-产业互联网项目部 衣者' as 部门
	 ,'闵宇' as 负责人
	 ,stat_month as 季度or月份
	 ,'累计活跃用户数' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     ,  CASE when t3.当前实际值 >= goods_value_target_s and goods_value_target_s > goods_value_target_a  then 'S'
            when t3.当前实际值 >= goods_value_target_a  and goods_value_target_a > goods_value_target_b  then 'A'
            when t3.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"当月活跃用户数－上月已活跃用户数=新增活跃用户数 活跃用户数：实施时间不为空，且近90天内创建的样板数≥100或者创建的订单数≥100" as  指标定义
	 ,0.30 as 权重
	 from t3
    UNION ALL 
    SELECT 
     '创新业务-产业互联网项目部 衣者' as 部门
	 ,'闵宇' as 负责人
	 ,stat_month  as 季度or月份
	 ,'累计活跃用户数' as  指标名称
	 ,goods_value_target_s as S级目标值	
     ,goods_value_target_a as A级目标值
     ,goods_value_target_b as  B级目标值
     ,当前实际值 as 当前实际值
     , CASE when t2.当前实际值 >= goods_value_target_s and goods_value_target_s > goods_value_target_a  then 'S'
            when t2.当前实际值 >= goods_value_target_a  and goods_value_target_a > goods_value_target_b  then 'A'
            when t2.当前实际值 >= goods_value_target_b then 'B'
         ELSE 'C'
     end as 指标达成等级
     ,concat(to_char(round((当前实际值/goods_value_target_s) * 100,2)),'%') as S级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_a) * 100,2)),'%') as A级达成率
     ,concat(to_char(round((当前实际值/goods_value_target_b) * 100,2)),'%') as B级达成率
	 ,"当月活跃用户数－上月已活跃用户数=新增活跃用户数 活跃用户数：实施时间不为空，且近90天内创建的样板数≥100或者创建的订单数≥100" as  指标定义
	 ,0.30 as 权重
	 from t2
;
