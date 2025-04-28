-- ALTER TABLE yishou_daily.dtl_new_user_feature
-- add columns (looklike_c_user BIGINT COMMENT '疑似c端')
-- ;

------验证调整：----根据历史所有的购买档次数据进行建模
-- drop table  if exists  temp_user_grade_steddev_daily_20210601 ;
-- create TABLE temp_user_grade_steddev_daily_20210601   as 
-- SELECT 
--  a.user_id,count(distinct order_sn) account_order_num,sum(buy_num) account_buyer_num
-- ,stddev(b.grade) standard_deviation,count(distinct b.grade) buyer_level_num
-- ,${bdp.system.bizdate} as update_time
-- from  yishou_data.dwd_sale_order_info_dt a
-- left join yishou_data.dim_goods_id_info b 
-- on a.goods_id=b.goods_id
-- where a.dt >= to_char(dateadd(to_date1(${bdp.system.bizdate},'yyyymmdd'),-4,'yyyy'),'yyyymmdd') and 
-- a.pay_status=1 and a.user_id NOT IN (2, 10, 17, 387836) 
-- group by  a.user_id
-- ;

-- drop  table if exists temp_daisy_grade_detail_daily_20210601;
-- create table temp_daisy_grade_detail_daily_20210601 as 
-- SELECT 
--  a.user_id,account_order_num,account_buyer_num,standard_deviation,buyer_level_num
--  ,b.grade,sum(buy_num) buy_num ,sum(a.buy_num*a.market_price) buy_amount
--  ,row_number() over(partition by a.user_id order by grade) grade_rank ---是1则是买的最高档次
--  ,${bdp.system.bizdate} as update_time
-- from  yishou_data.dwd_sale_order_info_dt a
-- left join yishou_data.dim_goods_id_info  b 
-- on a.goods_id=b.goods_id
-- left join  temp_user_grade_steddev_daily_20210601 c on a.user_id=c.user_id
-- where a.dt >= to_char(dateadd(to_date1(${bdp.system.bizdate},'yyyymmdd'),-4,'yyyy'),'yyyymmdd') and a.pay_status=1 and a.user_id NOT IN (2, 10, 17, 387836) 
-- group by a.user_id,account_order_num,account_buyer_num,standard_deviation,buyer_level_num,b.grade
-- ;

-- drop  table if exists temp_user_grade_steddev_daisy_20210601;
-- create TABLE temp_user_grade_steddev_daisy_20210601  as 
-- SELECT 
--  a.user_id,count(distinct order_sn) account_order_num,sum(buy_num) account_buyer_num
-- ,stddev(b.grade) standard_deviation,count(distinct b.grade) buyer_level_num
-- from  yishou_data.dwd_sale_order_info_dt a
-- left join yishou_data.dim_goods_id_info b 
-- on a.goods_id=b.goods_id
-- where a.dt >= 20190101 and a.pay_status=1 and a.user_id NOT IN (2, 10, 17, 387836) and 
-- TO_CHAR(a.special_start_time ,'yyyymmdd') >= to_char(dateadd(to_date1(${bdp.system.bizdate},'yyyymmdd'),-4,'yyyy'),'yyyymmdd')
-- group by  a.user_id
-- ;

-- drop  table if exists temp_daisy_grade_detail_daisy_20210601;
-- create table temp_daisy_grade_detail_daisy_20210601 as 
-- SELECT 
--  a.user_id,account_order_num,account_buyer_num,standard_deviation,buyer_level_num
--  ,b.grade,sum(buy_num) buy_num ,sum(a.buy_num*a.market_price) buy_amount
--  ,row_number() over(partition by a.user_id order by grade) grade_rank ---是1则是买的最高档次
-- from  yishou_data.dwd_sale_order_info_dt a
-- left join yishou_data.dim_goods_id_info b 
-- on a.goods_id=b.goods_id
-- left join  temp_user_grade_steddev_daisy_20210601 c on a.user_id=c.user_id
-- where a.dt >= to_char(dateadd(to_date1(${bdp.system.bizdate},'yyyymmdd'),-4,'yyyy'),'yyyymmdd') and a.pay_status=1 and a.user_id NOT IN (2, 10, 17, 387836) and 
-- TO_CHAR(a.special_start_time ,'yyyymmdd') >= to_char(dateadd(to_date1(${bdp.system.bizdate},'yyyymmdd'),-4,'yyyy'),'yyyymmdd')
-- group by a.user_id,account_order_num,account_buyer_num,standard_deviation,buyer_level_num,b.grade
-- ;



-- -----V1.O 根据用户近一年的购买档次数据进行推荐+未购买用户根据填写档次进行推荐,只提供档次名，暂不提供比例
-- drop table if exists  temp_user_grade_recommend_20210601;
-- create TABLE temp_user_grade_recommend_20210601 as 
--       ---购买一个档次，且购买件数少
-- select user_id,grade+1 as recmd_grade,buyer_level_num,'01向下一个档次推荐'  as recmd_type,${bdp.system.bizdate} as update_time
-- from  temp_daisy_grade_detail_daily_20210601 
-- where buyer_level_num=1 and account_buyer_num<=3  and grade<=4
-- union  --------购买一个档次
-- select user_id,grade as recmd_grade,buyer_level_num,'02已购买档次推荐'  as recmd_type,${bdp.system.bizdate} as update_time
-- from  temp_daisy_grade_detail_daily_20210601 
-- where buyer_level_num=1 
-- union ---------购买的两个档次是相连的
-- select t1.user_id,grade as recmd_grade,buyer_level_num,'03已购买档次推荐'  as recmd_type,${bdp.system.bizdate} as update_time
-- from  temp_daisy_grade_detail_daily_20210601 t1
-- JOIN 
-- (SELECT user_id,max(grade) as max_low_level,min(grade)  as max_hight_level
-- FROM temp_daisy_grade_detail_daisy_20210601
-- WHERE buyer_level_num =2 
-- GROUP BY user_id 
-- ) w ON t1.user_id = w.user_id 
-- where max_hight_level-max_low_level=1 
-- union ----------购买两个间隔档次，推荐最低档次和中间档次
-- SELECT 
--     t1.user_id
--     ,case when grade=max_low_level  then grade  
--           when grade>max_low_level  then grade-1 end as recmd_grade
--     ,buyer_level_num
--     ,case when grade=max_low_level  then '04已购买档次推荐'  
--           when grade>max_low_level  then '05最低档次向上推荐' end  as recmd_type
--     ,${bdp.system.bizdate} as update_time
-- FROM temp_daisy_grade_detail_daily_20210601 t1
-- JOIN 
--     (SELECT user_id,max(grade) max_low_level,min(grade)  as max_hight_level
--     FROM temp_daisy_grade_detail_daisy_20210601 
--     WHERE buyer_level_num =2 
--     GROUP BY user_id 
-- ) w  ON t1.user_id = w.user_id
-- WHERE buyer_level_num =2 and max_hight_level-max_low_level=2
-- ----------购买两个间隔档次且累计购买件数>25 ,加上推荐最高档次
-- union 
-- SELECT t1.user_id,grade as recmd_grade,buyer_level_num,'06已购买档次推荐'  as recmd_type,${bdp.system.bizdate} as update_time
-- FROM  temp_daisy_grade_detail_daily_20210601 t1
-- JOIN 
-- (SELECT user_id,max(grade) max_low_level,min(grade)  as max_hight_level
-- FROM temp_daisy_grade_detail_daisy_20210601
-- WHERE buyer_level_num =2 
-- GROUP BY user_id 
-- ) w  ON t1.user_id = w.user_id and t1.grade=max_hight_level
-- WHERE buyer_level_num=2  and max_hight_level-max_low_level=2 and account_buyer_num>25  
-- ----------购买三个档次且标准差小于1
-- union 
-- SELECT user_id,grade as recmd_grade,buyer_level_num,'07已购买档次推荐'  as recmd_type,${bdp.system.bizdate} as update_time
-- FROM  temp_daisy_grade_detail_daily_20210601 
-- WHERE buyer_level_num =3 AND standard_deviation<1
-- group by user_id,grade,buyer_level_num

-- -- ----------未购买用户的填写档次
-- -- union  
-- -- select  t1.user_id,median(t1.grade_pre) as recmd_grade,0 as 购买档次数,'填写档次推荐'  as recmd_type,${bdp.system.bizdate} as 数据更新时间
-- -- from  temp_grade_preference  t1 
-- -- left join  temp_user_grade_steddev_daisy_20210601  t2 on t2.user_id=t1.user_id 
-- -- where t1.grade_pre is not null  and t2.user_id is null
-- -- group by t1.user_id
-- -- 
-- ;

-- -----------------建模结果,在推荐档次上的转化
-- SELECT  购买档次数,recmd_type,
-- avg(商品曝光UV),avg(曝光DAU)
-- FROM (
--  SELECT  
--         TO_CHAR(ge.special_date,'yyyymmdd') as special_date,购买档次数,recmd_type
--         , count(distinct ge.user_id,ge.goods_id )  as 商品曝光UV
--         , count(distinct ge.user_id)  as 曝光DAU
--     FROM  yishou_data.dwd_log_goods_exposure_dt ge
--     join  yishou_data.dim_goods_id_info gii 
--     on  ge.goods_id = gii.goods_id  ------and ge.special_date = gii.special_date --不用限定当日上架商品
--     join   temp_user_grade_recommend_20210601  t2 on t2.user_id=ge.user_id  and  gii.grade=coalesce(t2.recmd_grade,0)
--     where ge.dt>='20201101' and  ge.dt<='20201201'and TO_CHAR(ge.special_date,'yyyymm')='202011'
--     group by   TO_CHAR(ge.special_date,'yyyymmdd'),购买档次数,recmd_type
-- )W
-- group by 购买档次数,recmd_type
-- ;

-- SELECT 购买档次数,recmd_type,
-- avg(商品支付UV) 日均商品支付UV,avg(支付DAU) 日均支付DAU--,avg(amount) 日均GMV
-- FROM (
--  SELECT  
--         TO_CHAR(ge.special_start_time,'yyyymmdd') as special_date,购买档次数,recmd_type
--     ,count(distinct ge.user_id,ge.goods_id) 商品支付UV
--     ,count(distinct ge.user_id) 支付DAU
--    -- ,SUM(ge.buy_num*ge.shop_price) amount ------一个用户多个档次，关联时会重复计算
--     FROM  yishou_data.dw_order_info_wt  ge
--     join  yishou_data.dim_goods_id_info gii 
--     on  ge.goods_id = gii.goods_id  
--     join   temp_user_grade_recommend_20210601  t2 on t2.user_id=ge.user_id  and  gii.grade=coalesce(t2.recmd_grade,0)
--     WHERE ge.pay_status = 1 AND ge.user_id not in(2,10,17,387836)
--     AND      TO_CHAR(ge.special_start_time,'yyyymm')='202011'  
--     group by TO_CHAR(ge.special_start_time,'yyyymmdd'),购买档次数,recmd_type
-- )W
-- group by 购买档次数,recmd_type
;
---------对比这些用户全部档次的转化率
-- SELECT  购买档次数,recmd_type,
-- avg(商品曝光UV) 日均商品曝光UV,avg(曝光DAU) 日均曝光DAU
-- FROM (
--  SELECT 
--         TO_CHAR(ge.special_date,'yyyymmdd') as special_date,购买档次数,recmd_type
--         , count(distinct ge.user_id,ge.goods_id )  as 商品曝光UV
--         , count(distinct ge.user_id)  as 曝光DAU
--     FROM  yishou_data.dwd_log_goods_exposure_dt ge
--     join  (select distinct 购买档次数,recmd_type,user_id from temp_user_grade_recommend_20210601 ) t2 on t2.user_id=ge.user_id 
--     where ge.dt>='20201101' and  ge.dt<='20201201'and TO_CHAR(ge.special_date,'yyyymm')='202011'
--     group by   TO_CHAR(ge.special_date,'yyyymmdd'),购买档次数,recmd_type
-- )W
-- group by 购买档次数,recmd_type
-- ;
-- --------------------实际应用--------------
-- SELECT 购买档次数,recmd_type,
-- avg(商品支付UV) 日均商品支付UV,avg(支付DAU) 日均支付DAU,avg(amount) 日均GMV
-- FROM (
--  SELECT  
--         TO_CHAR(ge.special_start_time,'yyyymmdd') as special_date,购买档次数,recmd_type
--     ,count(distinct ge.user_id,ge.goods_id) 商品支付UV
--     ,count(distinct ge.user_id) 支付DAU
--     ,SUM(ge.buy_num*ge.shop_price) amount 
--     FROM  yishou_data.dw_order_info_wt  ge
--     join  (select 购买档次数,recmd_type,user_id from temp_user_grade_recommend_20210601 
--     group by 购买档次数,recmd_type,user_id) t2 on t2.user_id=ge.user_id 
--     WHERE ge.pay_status = 1 AND ge.user_id not in(2,10,17,387836)
--     AND      TO_CHAR(ge.special_start_time,'yyyymm')='202011'  
--     group by TO_CHAR(ge.special_start_time,'yyyymmdd'),购买档次数,recmd_type
-- )W
-- group by 购买档次数,recmd_type
-- ;

-- -- 用户近60天活跃
-- DROP TABLE if EXISTS temp_user_last_login_day_60_20200321;
-- CREATE TABLE temp_user_last_login_day_60_20200321 as 
-- SELECT 
-- user_id, 
-- datediff1(to_date1(${bdp.system.bizdate}, 'yyyymmdd'), last_login_day, 'dd')  as last_login_day
-- FROM (
--     SELECT user_id, 
--     max(special_date) as last_login_day 
--     FROM yishou_data.dwd_log_app_wx_login_dt 
--     WHERE dt >= to_char(dateadd(to_date1(${bdp.system.bizdate}, 'yyyymmdd'), -60, 'dd'), 'yyyymmdd')
--     and is_number(user_id)
--     group by user_id
-- );

-- 新增6个字段
-- alter table yishou_daily.dtl_new_user_feature_d
-- add columns (
--     all_deliver_gmv double comment '累计发货GMV',
--     all_return_rate double comment '累计退货率',
--     all_sign_num bigint comment '近5年签收件数',
--     all_defective_rate double comment '近5年次品率',
--     recent_year_sign_num bigint comment '近1年签收件数',
--     recent_year_defective_rate double comment '近1年次品率'
-- ); 

-- alter table yishou_daily.dtl_new_user_feature
-- add columns (
--     all_deliver_gmv double comment '累计发货GMV',
--     all_return_rate double comment '累计退货率',
--     all_sign_num bigint comment '近5年签收件数',
--     all_defective_rate double comment '近5年次品率',
--     recent_year_sign_num bigint comment '近1年签收件数',
--     recent_year_defective_rate double comment '近1年次品率'
-- ); 


insert overwrite table yishou_daily.dtl_new_user_feature_d
SELECT 
    ul.user_id,
    CASE   
        WHEN ul.user_belong == '应用市场' THEN (CASE WHEN LOWER(ul.channel) in ('com.fumi.yishoupro','appstore' )  THEN  'iOS应用市场' ELSE '安卓应用市场' END )
        ELSE ul.user_belong    
    END AS user_belong,
    ul.dianzhu_level,
    coalesce(aaut.avg_amount_level_name, '新注册') as order_level_by_month,
    ut.is_maintain_by_other,
    ut.is_maintain_by_social,
    ut.is_maintain_by_vip,
    ut.is_maintain_by_wx,
    CASE 
        WHEN oi.avg_order_price is NULL THEN 0
        WHEN oi.avg_order_price > 0  AND oi.avg_order_price <= 100  THEN  '(0,100]'
        WHEN oi.avg_order_price > 100  AND oi.avg_order_price <= 200  THEN  '(100,200]'
        WHEN oi.avg_order_price > 200  AND oi.avg_order_price <= 300  THEN  '(200,300]'
        WHEN oi.avg_order_price > 300  AND oi.avg_order_price <= 400  THEN  '(300,400]'
        WHEN oi.avg_order_price > 400  AND oi.avg_order_price <= 500  THEN  '(400,500]'
        WHEN oi.avg_order_price > 500  AND oi.avg_order_price <= 600  THEN  '(500,600]'
        WHEN oi.avg_order_price > 600  AND oi.avg_order_price <= 700  THEN  '(600,700]'
        WHEN oi.avg_order_price > 700  AND oi.avg_order_price <= 800  THEN  '(700,800]'
        WHEN oi.avg_order_price > 800  AND oi.avg_order_price <= 900  THEN  '(800,900]'
        WHEN oi.avg_order_price > 900  AND oi.avg_order_price <= 1000  THEN  '(900,1000]'
        WHEN oi.avg_order_price > 1000  AND oi.avg_order_price <= 1500  THEN  '(1000,1500]'
        WHEN oi.avg_order_price > 1500  AND oi.avg_order_price <= 2000  THEN  '(1500,2000]'
        WHEN oi.avg_order_price > 2000  AND oi.avg_order_price <= 2500  THEN  '(2000,2500]'
        WHEN oi.avg_order_price > 2500  AND oi.avg_order_price <= 3000  THEN  '(2500,3000]'
        WHEN oi.avg_order_price > 3000 THEN  '(3000,)'
        ELSE ''  
    END AS  order_amount_level,
    CASE 
        WHEN ul.level =0  THEN  '0级'
        WHEN ul.level >= 1 AND ul.level < 4 THEN  '1-3级'
        WHEN ul.level >= 4 AND ul.level < 6 THEN '4-5级'
        WHEN ul.level >= 6 THEN '6级以上'
        ELSE ''     
    END AS order_level,
-- CASE 
--     WHEN ulld.last_login_day = 0 then '0'
--     WHEN ulld.last_login_day > 0 AND ulld.last_login_day < 2   THEN  '(0,2)'
--     WHEN ulld.last_login_day >= 2 AND ulld.last_login_day < 4 THEN  '[2,4)'
--     WHEN ulld.last_login_day > 4 AND ulld.last_login_day < 7   THEN  '[4,7)'
--     WHEN ulld.last_login_day >= 7 AND ulld.last_login_day < 15 THEN  '[7,15)'
--     WHEN ulld.last_login_day > 15 AND ulld.last_login_day < 30   THEN  '[15,30)'
--     WHEN ulld.last_login_day >= 30 AND ulld.last_login_day < 60 THEN  '[30,60)'
--     WHEN ulld.last_login_day >= 60 THEN  '[60,)'
--     ELSE ''     
-- END AS last_login_day,  
    dateadd(to_date1(${bdp.system.bizdate},'yyyymmdd'),-ulld.last_login_day,'dd') as last_login_day,
-- CASE 
--     WHEN uac.last_cart_day = 0 THEN '0'
--     WHEN uac.last_cart_day > 0 AND uac.last_cart_day < 3   THEN  '(0,3)'
--     WHEN uac.last_cart_day >= 3 AND uac.last_cart_day < 7 THEN  '[3,7)'
--     WHEN uac.last_cart_day >= 7 AND uac.last_cart_day < 15 THEN  '[7,15)'
--     WHEN uac.last_cart_day >= 15 AND uac.last_cart_day < 30 THEN  '[15,30)'
--     WHEN uac.last_cart_day >= 30 AND uac.last_cart_day < 60 THEN  '[30,60)'
--     WHEN uac.last_cart_day >= 60 THEN  '[60,)'
--     ELSE ''     
-- END AS last_cart_day,  
    dateadd(to_date1(${bdp.system.bizdate},'yyyymmdd'),-uac.last_cart_day,'dd') as last_cart_day,
-- CASE 
--     WHEN ul2.last_order_day = 0 THEN '0'
--     WHEN ul2.last_order_day > 0 AND ul2.last_order_day < 3   THEN  '(0,3)'
--     WHEN ul2.last_order_day >= 3 AND ul2.last_order_day < 7 THEN  '[3,7)'
--     WHEN ul2.last_order_day >= 7 AND ul2.last_order_day < 15 THEN  '[7,15)'
--     WHEN ul2.last_order_day >= 15 AND ul2.last_order_day < 30 THEN  '[15,30)'
--     WHEN ul2.last_order_day >= 30 AND ul2.last_order_day < 60 THEN  '[30,60)'
--     WHEN ul2.last_order_day >= 60 THEN  '[60,)'
--     ELSE ''     
-- END AS last_order_day  
    dateadd(to_date1(${bdp.system.bizdate},'yyyymmdd'),-ul2.last_order_day,'dd') as last_order_day,
-- , case 
--     when ul2.reg_day = 0 then '0'
--     when ul2.reg_day >= 1 and ul2.reg_day < 3 then '[1,3)'
--     when ul2.reg_day < 7 then '[3,7)'
--     when ul2.reg_day < 15 then '[7,15)'
--     when ul2.reg_day < 30 then '[15,30)'
--     when ul2.reg_day < 60 then '[30,60)'
--     when ul2.reg_day < 200 then '[60,200)'
--     when ul2.reg_day < 400 then '[200,400)'
--     when ul2.reg_day < 600 then '[400,600)'
--     when ul2.reg_day >= 600 then '[600,)'
--     else ''
-- end as reg_day  
    dateadd(to_date1(${bdp.system.bizdate},'yyyymmdd'),-ul2.reg_day,'dd') as reg_day
    , case user_value_level 
        when '0单' then 1 
        when '首单' then 2 
        when '2-5单' then 3 
        when 'A_低频' then 4 
        when 'A_中频' then 5 
        when 'A_高频' then 6 
        when 'B_低频' then 7 
        when 'B_中频' then 8 
        when 'B_高频' then 9 
        when 'C_低频' then 10 
        when 'C_中频' then 11 
        when 'C_高频' then 12 
        when 'D_低频' then 13 
        when 'D_中频' then 14 
        when 'D_高频' then 15 
        when '流失用户' then 16 
    end as value_level 
    , coalesce(round(b.order_amout_avg_3_8,2),0.00) as order_amout_avg_3_8
    , coalesce(round(b.order_amout_avg_9_2,2),0.00) as order_amout_avg_9_2
    , case when grade_recommend.recmd_grade is null then 0 else grade_recommend.recmd_grade end  as grade_recommend
    , case when label.freight_meet is null then false else label.freight_meet end
    , case when label.refund_sensitive is null then false else label.refund_sensitive end
    , case when label.good_category_grade_first_id    is null then 0 else label.good_category_grade_first_id end
    , case when label.good_category_grade_second_id   is null then 0 else  label.good_category_grade_second_id end 
    , case when label.good_category_grade_third_id   is null then 0 else label.good_category_grade_third_id end 
    , case when label.new_style_id   is null then 0 else label.new_style_id end 
    , case when label.buyer_id   is null then 0 else  label.buyer_id end 
    , case when label.supply_id   is null then 0 else label.supply_id end
    , case when label.anchor_id   is null then 0 else  label.anchor_id end
    , case when label.market_first_id   is null then 0 else label.market_first_id end
    , case when label.market_second_id   is null then 0 else  label.market_second_id end
    , case when label.pre_loss_flag   is null then false else label.pre_loss_flag end
    , case 
        WHEN c.first_time IS NULL or (c.first_time IS not NULL and (c.user_id is null OR c.累计订单数量 IS NULL OR c.累计销售额 is NULL )) then 2 -- '高潜期'
        WHEN c.first_time is NOT NULL 
        AND ((c.累计订单数量<=13 or  c.累计销售额<=4000))
        AND c.近30天订单量<>0 and c.近30天销售额<>0 
        AND (IF(c.近30_60天订单量=0,1,c.近30天订单量/c.近30_60天订单量)>0.6  or IF(c.近30_60天销售额=0,1,c.近30天销售额/c.近30_60天销售额)>0.6 )
        and c.last_time>=${bdp.system.bizdate} THEN 3 -- '成长期'
        when c.first_time is NOT NULL AND c.user_id is not null 
            AND (  ((c.累计订单数量<=27 AND c.累计订单数量>13) AND (c.累计销售额<=9000 AND c.累计销售额>4000)) or (c.累计订单数量>=27 AND  c.累计销售额<=9000) or (c.累计订单数量<=27 AND c.累计销售额>=9000)) 
            AND c.近30天订单量<>0 and c.近30天销售额<>0 
            AND (IF(c.近30_60天订单量=0,1,c.近30天订单量/c.近30_60天订单量)>0.6 or IF(c.近30_60天销售额=0,1,c.近30天销售额/c.近30_60天销售额)>0.6 )
            and c.last_time>=${bdp.system.bizdate} THEN 4 -- '稳定期'
        when c.first_time is NOT NULL AND c.user_id is not null AND c.累计订单数量>27 AND c.累计销售额>9000 AND c.近30天订单量<>0 and c.近30天销售额<>0 
            AND (IF(c.近30_60天订单量=0,1,c.近30天订单量/c.近30_60天订单量)>0.6 or IF(c.近30_60天销售额=0,1,c.近30天销售额/c.近30_60天销售额)>0.6 )
            and c.last_time>=${bdp.system.bizdate} THEN 5 -- '成熟期'
        WHEN c.近30天订单量<>0 and c.近30天销售额<>0 AND ((c.累计订单数量<=13 or  c.累计销售额<=4000)) 
            and (IF(c.近30_60天订单量=0,1,c.近30天订单量/c.近30_60天订单量)<=0.6 or IF(c.近30_60天销售额=0,1,c.近30天销售额/c.近30_60天销售额)<=0.6) 
            and c.last_time>=${bdp.system.bizdate} THEN 6 -- '成长_衰退期'
        WHEN c.近30天订单量<>0 and c.近30天销售额<>0 
            AND (((c.累计订单数量<=27 AND c.累计订单数量>13) AND (c.累计销售额<=9000 AND c.累计销售额>4000)) or (c.累计订单数量>=27 AND  c.累计销售额<=9000) or (c.累计订单数量<=27 AND c.累计销售额>=9000)) 
            and (IF(c.近30_60天订单量=0,1,c.近30天订单量/c.近30_60天订单量)<=0.6 or IF(c.近30_60天销售额=0,1,c.近30天销售额/c.近30_60天销售额)<=0.6) 
            and c.last_time>=${bdp.system.bizdate} THEN 7 -- '稳定_衰退期'
        WHEN c.近30天订单量<>0 and c.近30天销售额<>0 AND c.累计订单数量>27 AND c.累计销售额>9000
            and (IF(c.近30_60天订单量=0,1,c.近30天订单量/c.近30_60天订单量)<=0.6 or IF(c.近30_60天销售额=0,1,c.近30天销售额/c.近30_60天销售额)<=0.6) 
            and c.last_time>=${bdp.system.bizdate} THEN 8 --'成熟_衰退期'
        WHEN  (c.last_time<${bdp.system.bizdate} or c.近30天订单量=0 or c.近30天销售额=0)
            AND ((c.累计订单数量<=13 or  c.累计销售额<=4000)) THEN 9 --'成长_流失期'
        WHEN  (c.last_time<${bdp.system.bizdate} or c.近30天订单量=0 or c.近30天销售额=0)
            AND ( ((c.累计订单数量<=27 AND c.累计订单数量>13) AND (c.累计销售额<=9000 AND c.累计销售额>4000)) or (c.累计订单数量>=27 AND  c.累计销售额<=9000) or (c.累计订单数量<=27 AND c.累计销售额>=9000) )  THEN 10 --'稳定_流失期'
        WHEN (c.last_time<${bdp.system.bizdate} or c.近30天订单量=0 or c.近30天销售额=0 ) AND c.累计订单数量>27 AND c.累计销售额>9000 THEN 11 -- '成熟_流失期'
        else 1 -- '未知'
    end as each_life_cycle   -- 生命周期
    ,coalesce(e.user_type,0) as user_type   -- 清单类型万元户类型
    ,g.val as suspected_work_orders
    ,f.val as wong_work_orders
    ,coalesce(mmm.max_month_money,0) max_month_money --近一年最大的月拿货额
    ,case when uln.is_notication_open = 1 then 1 
        when uln.is_notication_open = 0 then 2
        else 0 end is_allow_push 
    ,case  
        when ufrr.last_month_refund_rate >= 0.9 then 9
        when ufrr.last_month_refund_rate >= 0.8 then 8
        when ufrr.last_month_refund_rate >= 0.7 then 7
        when ufrr.last_month_refund_rate >= 0.6 then 6
        when ufrr.last_month_refund_rate >= 0.5 then 5
        when ufrr.last_month_refund_rate >= 0.4 then 4
        when ufrr.last_month_refund_rate >= 0.3 then 3
        when ufrr.last_month_refund_rate >= 0.2 then 2
        when ufrr.last_month_refund_rate >= 0.1 then 1
        else 0 end last_month_refund_rate
    ,case  
        when ufrr.three_month_refund_rate >= 0.9 then 9
        when ufrr.three_month_refund_rate >= 0.8 then 8
        when ufrr.three_month_refund_rate >= 0.7 then 7
        when ufrr.three_month_refund_rate >= 0.6 then 6
        when ufrr.three_month_refund_rate >= 0.5 then 5
        when ufrr.three_month_refund_rate >= 0.4 then 4
        when ufrr.three_month_refund_rate >= 0.3 then 3
        when ufrr.three_month_refund_rate >= 0.2 then 2
        when ufrr.three_month_refund_rate >= 0.1 then 1
        else 0 end three_month_refund_rate
    ,case  
        when ufrr.one_year_refund_rate >= 0.9 then 9
        when ufrr.one_year_refund_rate >= 0.8 then 8
        when ufrr.one_year_refund_rate >= 0.7 then 7
        when ufrr.one_year_refund_rate >= 0.6 then 6
        when ufrr.one_year_refund_rate >= 0.5 then 5
        when ufrr.one_year_refund_rate >= 0.4 then 4
        when ufrr.one_year_refund_rate >= 0.3 then 3
        when ufrr.one_year_refund_rate >= 0.2 then 2
        when ufrr.one_year_refund_rate >= 0.1 then 1
        else 0 end one_year_refund_rate
    , 0 -- 作废 high_potential_users
    ,if(ul.probably_b='是',1,0) probably_b
    , coalesce(ul.r_level,0) r_level
    , case ul.r_table 
        when '高' then 2 
        when '低' then 1
        else 0 end r_table
    , case ul.f_table 
        when '高' then 2 
        when '低' then 1
        else 0 end f_table
    , case ul.m_table 
        when '高' then 2 
        when '低' then 1
        else 0 end m_table
    , case ul.value_table 
        when '重要发展用户' then 4
        when '重要挽留用户' then 3 
        when '一般发展客户' then 2 
        when '一般挽留用户' then 1
        else 0 end value_table
    ,coalesce(ul.is_b_port,0) is_b_port
    ,coalesce(ul.is_important_user,0) is_important_user
    ,coalesce(label.pre_loss_flag_v2,0) pre_loss_flag_v2
    , if(ucpp.cp_privilege=1,1,2) as cp_privilege
    , ul.first_time
    , coalesce(datediff1(current_date(),dateadd(to_date1(${bdp.system.bizdate},'yyyymmdd'),-ulld.last_login_day,'dd'), 'dd'),'') as last_login_day_count
    , coalesce(datediff1(current_date(),dateadd(to_date1(${bdp.system.bizdate},'yyyymmdd'),-uac.last_cart_day,'dd'), 'dd'),'') as last_cart_day_count
    , coalesce(datediff1(current_date(),dateadd(to_date1(${bdp.system.bizdate},'yyyymmdd'),-ul2.last_order_day,'dd'), 'dd'),'') as last_order_day_count
    , coalesce(datediff1(current_date(),ul.first_time, 'dd'),'') as first_order_day_count
    , coalesce(to_char(ul.first_time, 'yyyymm'),'') as first_order_month
    ,coalesce(oul.last_mon_order_cnt,0) as last_mon_order_cnt
	-- ,(case when to_char(yuod.fifth_time,'yyyymm') = to_char(${gmtdate},'yyyymm') then 1 else 0 end) as is_this_mon_5_order
	,(case when to_char(yuod.fifth_time,'yyyymm') = to_char(dateadd(to_date1('${gmtdate}','yyyymmdd'),-1,'mm'),'yyyymm') then 1 else 0 end) as is_last_mon_5_order
	,(case when to_char(yuod.fifth_time,'yyyymm') = to_char(dateadd(to_date1('${gmtdate}','yyyymmdd'),-2,'mm'),'yyyymm') then 1 else 0 end) as is_before_last_mon_5_order
	,coalesce(yuod.all_pay_order_num,0) as all_order_cnt
	,coalesce(umms.malice_mistake_send,0) as malice_mistake_send
	,coalesce(usmd.suspected_malice_defective,0) as suspected_malice_defective
	,coalesce(bround(dso.history_mon_max_gmv,0),0) as history_mon_max_gmv
	,coalesce(sod.normal_refund_money,0) normal_refund_money
    ,coalesce(sod.no_reason_refund_money,0) no_reason_refund_money
    ,if(fs7ad.sign_7_apply_cp_num>0 and fs7ad.sign_num>0,fs7ad.sign_7_apply_cp_num/fs7ad.sign_num,0) sign_7_apply_cp_rate
    ,coalesce(hdf.higt_defective_user,0) as higt_defective_user
    ,${bdp.system.bizdate} as dt
    ,if(cr.xh_buy_num > 0 ,coalesce(cr.xh_cancel_num,0) / cr.xh_buy_num , 0 ) stock_cacel_rate --现货取消率
    ,if(cr.buy_num > 0 ,coalesce(cr.cancel_num,0) / cr.buy_num , 0 ) order_cacel_rate --订单取消率
    ,coalesce(ada.defective_auto_pass,0) AS defective_auto_pass
    ,coalesce(c.all_deliver_gmv,0) as all_deliver_gmv -- 累计发货GMV
    ,coalesce(c.all_return_rate,0) as all_return_rate -- 累计退货率
    ,coalesce(fs7ad_5_year.all_sign_num,0) as all_sign_num -- 近5年签收件数
    ,coalesce(if(fs7ad_5_year.sign_7_apply_cp_num>0 and fs7ad_5_year.all_sign_num>0,fs7ad_5_year.sign_7_apply_cp_num/fs7ad_5_year.all_sign_num,0),0) all_defective_rate -- 近5年次品率
    ,coalesce(fs7ad_1_year.recent_year_sign_num,0) as recent_year_sign_num -- 近1年签收件数
    ,coalesce(if(fs7ad_1_year.sign_7_apply_cp_num>0 and fs7ad_1_year.recent_year_sign_num>0,fs7ad_1_year.sign_7_apply_cp_num/fs7ad_1_year.recent_year_sign_num,0),0) as recent_year_defective_rate -- 近1年次品率
FROM yishou_data.odps_user_label ul 
LEFT JOIN (
    SELECT DISTINCT ul.user_id, 
        case when sui.user_id is NOT  NULL  then 1  else 0 end as is_maintain_by_social,
        case when vut1.user_id is NOT  NULL then 1  else 0 end as is_maintain_by_vip,
        case when vut2.user_id is NOT  NULL then 1  else 0 end as is_maintain_by_wx,
        case when sui.user_id is NULL AND vut1.user_id is NULL AND vut2.user_id is NULL then 1  else 0 end as is_maintain_by_other
    FROM yishou_data.odps_user_label ul 
    LEFT JOIN (SELECT DISTINCT user_id FROM yishou_data.dwd_cs_association_user_detail_dt WHERE dt = ${bdp.system.bizdate}) sui on sui.user_id = ul.user_id 
    LEFT JOIN (SELECT DISTINCT user_id FROM yishou_data.apex_vip_user_type  WHERE dt = ${bdp.system.bizdate} and user_type='VIP')  vut1 on vut1.user_id = ul.user_id
    LEFT JOIN (SELECT DISTINCT user_id FROM yishou_data.apex_vip_user_type WHERE dt = ${bdp.system.bizdate} and user_type='微信')  vut2 on vut2.user_id = ul.user_id
    WHERE ul.dt = ${bdp.system.bizdate}
) ut on ul.user_id = ut.user_id
LEFT JOIN (
    SELECT user_id, 
    datediff1(to_date1(${bdp.system.bizdate}, 'yyyymmdd'), datetrunc(max_time, 'dd'), 'dd')  as last_order_day,
    datediff1(to_date1(${bdp.system.bizdate}, 'yyyymmdd'), datetrunc(reg_time, 'dd'), 'dd') as reg_day
    FROM yishou_data.odps_user_label WHERE dt = ${bdp.system.bizdate}
) ul2 on ul.user_id = ul2.user_id
LEFT JOIN (
    SELECT user_id, datediff1(to_date1(${bdp.system.bizdate}, 'yyyymmdd'), to_date1(last_cart_day, 'yyyymmdd'), 'dd')  as last_cart_day
    FROM (
        SELECT user_id, 
        max(dt) as last_cart_day 
        FROM yishou_data.dwd_log_add_cart_dt
        WHERE dt >= to_char(dateadd(to_date1(${bdp.system.bizdate}, 'yyyymmdd'), -60, 'dd'), 'yyyymmdd')
        group by user_id
    )
)uac on uac.user_id = ul.user_id
LEFT JOIN (
    SELECT 
    user_id, 
    SUM(shop_price * buy_num) / COUNT(DISTINCT order_id) as avg_order_price
    FROM yishou_data.dwd_sale_order_info_dt 
    WHERE dt  >= to_char(dateadd(to_date1(${bdp.system.bizdate}, 'yyyymmdd'), -15, 'dd'), 'yyyymmdd')
    and is_real_pay = 1
    GROUP BY user_id
) oi on oi.user_id = ul.user_id
LEFT JOIN yishou_data.dw_avg_amount_user_type aaut on aaut.user_id = ul.user_id AND aaut.dt = to_char(to_date1(${bdp.system.bizdate}, 'yyyymmdd'), 'yyyymm01')
LEFT JOIN temp_user_last_login_day_60_20200321 ulld on ulld.user_id = ul.user_id
left join (
    select 
        user_id
        , sum(case when month(special_date) in (3,4,5,6,7,8) then buy_amount end) as order_amout_avg_3_8
        , sum(case when month(special_date) in (1,2,9,10,11,12) then buy_amount end) as order_amout_avg_9_2
        -- , count(distinct case when month(special_date) in (3,4,5,6,7,8) then to_char(special_date,'yyyymm') end) as 春季购买月份个数
        -- , count(distinct case when month(special_date) in (1,2,9,10,11,12) then to_char(special_date,'yyyymm') end) as 冬季购买月份个数
        -- , COUNT(distinct to_char(special_date,'yyyymm')) AS 总共购买月份
    from yishou_data.dws_operation_sp_user_sale_dt 
    where substr(dt,1,6) between to_char(dateadd(to_date1(${bdp.system.cyctime},'yyyymmddhhmiss'),-12,'mm'),'yyyymm') 
        and to_char(dateadd(to_date1(${bdp.system.cyctime},'yyyymmddhhmiss'),-1,'mm'),'yyyymm')
    group by user_id
    -- having 总共购买月份 >12 or 春季购买月份个数>6 or 冬季购买月份个数>6
)b
on b.user_id=ul.user_id
LEFT JOIN (select 
	 user_id
	 ,max(recmd_grade) recmd_grade
	 ,max(buyer_level_num) buyer_level_num
	 ,max(recmd_type) recmd_type
	 ,max(update_time)  update_time
	 from  temp_user_grade_recommend_20210601
	group by 
	 user_id 
	) grade_recommend on grade_recommend.user_id = ul.user_id
LEFT JOIN (
  select user_id
  ,max(freight_meet) freight_meet 
  ,max(refund_sensitive)   refund_sensitive
  ,max(good_category_grade_first_id) good_category_grade_first_id
  ,max(good_category_grade_second_id) good_category_grade_second_id
  ,max(good_category_grade_third_id) good_category_grade_third_id
  ,max(new_style_id) new_style_id
  ,max(buyer_id) buyer_id
  ,max(supply_id) supply_id
  ,max(anchor_id) anchor_id
  ,max(market_first_id) market_first_id
  ,max(market_second_id) market_second_id
  ,max(pre_loss_flag) pre_loss_flag
  ,max(pre_loss_flag_v2) pre_loss_flag_v2
  from `yishou_data`.`dws_preferential_label` where dt=${bdp.system.bizdate}
  group by user_id
) label on label.user_id = ul.user_id
left join (
    SELECT fo.user_id
        , min(TO_CHAR(DATEADD(from_unixtime(add_time),-7,'hh'),'yyyymmdd')) as first_time
        , max(TO_CHAR(DATEADD(DATEADD(from_unixtime(add_time),-7,'hh'),+30,'dd'),'yyyymmdd')) as last_time
        , count(distinct fo.order_id) as 累计订单数量
        , sum(foi.sale_amount) as 累计销售额
        ,count(DISTINCT case when TO_CHAR(DATEADD(DATEADD(from_unixtime(add_time),-7,'hh'),+30,'dd'),'yyyymmdd')>=${bdp.system.bizdate} AND TO_CHAR(DATEADD(from_unixtime(add_time),-7,'hh'),'yyyymmdd')<=${bdp.system.bizdate} THEN fo.order_id END ) as 近30天订单量
        ,SUM( case when TO_CHAR(DATEADD(DATEADD(from_unixtime(add_time),-7,'hh'),+30,'dd'),'yyyymmdd')>=${bdp.system.bizdate} AND TO_CHAR(DATEADD(from_unixtime(add_time),-7,'hh'),'yyyymmdd')<=${bdp.system.bizdate} THEN sale_amount END ) as 近30天销售额
        ,count(DISTINCT case when TO_CHAR(DATEADD(DATEADD(from_unixtime(add_time),-7,'hh'),+60,'dd'),'yyyymmdd')>=${bdp.system.bizdate} AND TO_CHAR(DATEADD(DATEADD(from_unixtime(add_time),-7,'hh'),+30,'dd'),'yyyymmdd')<${bdp.system.bizdate} THEN fo.order_id END ) as 近30_60天订单量
        ,SUM( case when TO_CHAR(DATEADD(DATEADD(from_unixtime(add_time),-7,'hh'),+60,'dd'),'yyyymmdd')>=${bdp.system.bizdate} AND TO_CHAR(DATEADD(DATEADD(from_unixtime(add_time),-7,'hh'),+30,'dd'),'yyyymmdd')<${bdp.system.bizdate} THEN sale_amount END ) as 近30_60天销售额
        ,sum(foi.all_deliver_gmv) as all_deliver_gmv -- 累计发货GMV
        ,sum(foi.all_return_rate) as all_return_rate -- 累计退货率
        from yishou_data.all_fmys_order fo 
        left join (
            select 
                order_id
                , sum(buy_num) as buy_num 
                , sum(buy_num * shop_price) as sale_amount
                , sum(if(allot_status>0,buy_num * shop_price - refund_num * shop_price,0)) as all_deliver_gmv
                , sum(refund_money) / sum(if(allot_status>0,buy_num * shop_price - refund_num * shop_price,0)) as all_return_rate 
            from yishou_data.all_fmys_order_infos 
            group by order_id
        ) foi 
        on fo.order_id = foi.order_id
        WHERE user_id NOT IN (2,17,10,387836)
        AND pay_status=1
        and TO_CHAR(DATEADD(from_unixtime(add_time),-7,'hh'),'yyyymmdd') <= ${bdp.system.bizdate}
        group by user_id
)c
on c.user_id = ul.user_id
left join (
   select user_id,case when user_type between 1 and 5 then user_type else 0 end as user_type   from yishou_data.dws_ten_thousand_user_info_dt where dt='${bdp.system.bizdate}'
)e on e.user_id = ul.user_id
left join temp_deliver_goods_num_result_20210910 f on f.user_id=ul.user_id
left join temp_man_made_result_20210910 g on g.user_id=ul.user_id
left join (
    select user_id
        ,max(buy_amount) max_month_money
    from(
    select 
        user_id
        ,substr(dt,1,6)
        ,sum(buy_amount) buy_amount 
    from yishou_data.dws_operation_sp_user_sale_dt 
    where substr(dt,1,6) between to_char(dateadd(to_date1(${bdp.system.bizdate},'yyyymmdd'),-11,'mm'),'yyyymm') 
        and substr(${bdp.system.bizdate},1,6)
    group by user_id
            ,substr(dt,1,6)
    )group by user_id
)mmm
on mmm.user_id=ul.user_id
left join yishou_data.dwm_log_user_last_notication_dt uln
    on uln.user_id = ul.user_id and uln.dt = '${bdp.system.bizdate}'
left join yishou_daily.dtl_new_user_feature_refund_rate ufrr
    on ufrr.user_id = ul.user_id and ufrr.dt =  '${bdp.system.bizdate}'
--left join (select * from yishou_daily.dtl_user_cp_privilege where dt = '${bdp.system.bizdate}') ucpp on ucpp.user_id = a.user_id
left join yishou_daily.dtl_user_cp_privilege ucpp
    on  ucpp.user_id = ul.user_id and ucpp.dt = '${bdp.system.bizdate}'
left join (
    SELECT
        user_id,
        ifnull(pay_orders,0) as last_mon_order_cnt
    FROM yishou_data.odps_user_label 
    WHERE dt = to_char(last_day(dateadd(to_date1(${gmtdate},'yyyymmdd'),-1,'mm')),'yyyymmdd')
)oul
on oul.user_id = ul.user_id
left join yishou_data.dim_ys_user_order_data yuod
    on yuod.user_id = ul.user_id  and  to_char(yuod.fifth_time,'yyyymmdd') < ${gmtdate}
left join (
   SELECT
    	user_id,
    	max(mon_gmv) AS history_mon_max_gmv 
    FROM
    	(
    	SELECT
    		user_id,
    		SUBSTR(dt,1,6) mon,
    		sum(buy_num*shop_price) mon_gmv 
    	FROM
    		yishou_data.dwd_sale_order_info_dt 
    	WHERE
    		SUBSTR(dt,1,6) BETWEEN 201604 
    		AND SUBSTR(${bdp.system.bizdate},1,6) 
    		AND pay_status = 1
    	GROUP BY
    		user_id,
    		SUBSTR(dt,1,6) 
    	) 
    GROUP BY
    	user_id
) dso on dso.user_id = ul.user_id
left join yishou_daily.dtl_user_suspected_malice_defective usmd
    on usmd.user_id = ul.user_id and usmd.dt = '${bdp.system.bizdate}'
left join yishou_daily.dtl_user_malice_mistake_send umms
    on umms.user_id = ul.user_id and umms.dt = '${bdp.system.bizdate}'
left join (
    select  user_id
        ,sum(case when coalesce(service_order_type_v1,0) = 0 then refund_money end) normal_refund_money
        ,sum(case when service_order_type_v1 > 0 then refund_money end) no_reason_refund_money
    from yishou_data.dwd_as_service_order_detail w
    left join(
    SELECT p_id 
    from yishou_data.dwd_as_service_order_detail 
    where p_id <> 0
    group by p_id
    )b on w.id = b.p_id 
    where w.handle_status = 7
    and b.p_id is null -----存在一个rec_id，销售一件，但存在工单两个，为了只获取一条记录
    group by 1 
) sod
    on sod.user_id = ul.user_id
left join (
    select user_id
        ,sum(sign_7_apply_cp_num) sign_7_apply_cp_num
        ,sum(sign_num) sign_num
    from yishou_daily.finebi_sign_7days_application_defective
    where dt between to_char(dateadd(to_date1('${bdp.system.bizdate}','yyyymmdd'),-13,'dd'),'yyyymmdd') and to_char(dateadd(to_date1('${bdp.system.bizdate}','yyyymmdd'),-7,'dd'),'yyyymmdd')
    group by 1
)fs7ad
on fs7ad.user_id = ul.user_id
-- 取5年
left join (
    select 
        user_id
        ,sum(sign_7_apply_cp_num) sign_7_apply_cp_num
        ,sum(sign_num) all_sign_num
    from yishou_daily.finebi_sign_7days_application_defective
    where dt BETWEEN to_char(add_months(to_date('${bdp.system.bizdate}', 'yyyyMMdd'),-60),'yyyymmdd') AND '${bdp.system.bizdate}'
    group by user_id
) fs7ad_5_year
on fs7ad_5_year.user_id = ul.user_id
-- 取1年
left join (
    select 
        user_id
        ,sum(sign_7_apply_cp_num) sign_7_apply_cp_num
        ,sum(sign_num) recent_year_sign_num
    from yishou_daily.finebi_sign_7days_application_defective
    where dt BETWEEN to_char(add_months(to_date('${bdp.system.bizdate}', 'yyyyMMdd'),-12),'yyyymmdd') AND '${bdp.system.bizdate}'
    group by user_id
) fs7ad_1_year
on fs7ad_1_year.user_id = ul.user_id
LEFT JOIN (SELECT user_id,higt_defective_user FROM yishou_daily.ads_higt_defective_user  where dt = '${bdp.system.bizdate}')hdf on hdf.user_id = ul.user_id
LEFT JOIN (
select t1.user_id
    ,sum(if(t1.is_real_pay = 0 ,t1.buy_num,0)) cancel_num
    ,sum(t1.buy_num) buy_num --order_cacel_rate --近一个月件数取消率=>订单取消率
    ,sum(if(is_real_pay = 0 AND xh.goods_id is not null ,t1.buy_num ,0)) xh_cancel_num
    ,sum(if(xh.goods_id is not null ,t1.buy_num ,0)) xh_buy_num    --stock_cacel_rate --近一个月现货形式上架取消率=>现货取消率
    FROM yishou_data.dwd_sale_order_info_dt t1
    left join yishou_data.dim_goods_id_info xh
        on t1.goods_id = xh.goods_id and xh.is_in_stock = 1
    WHERE t1.dt >= to_char(dateadd(to_date1('${bdp.system.bizdate}','yyyymmdd'),-30,'dd'),'yyyymmdd') 
        and to_char(t1.special_start_time,'yyyymmdd') >= to_char(dateadd(to_date1('${bdp.system.bizdate}','yyyymmdd'),-30,'dd'),'yyyymmdd')
        and buy_num > 0
    GROUP BY 1
) cr
on ul.user_id = cr.user_id
LEFT JOIN (
    SELECT user_id,defective_auto_pass FROM yishou_daily.ads_defective_auto_pass  where dt = '${bdp.system.bizdate}'
)ada on ada.user_id = ul.user_id
WHERE ul.dt = ${bdp.system.bizdate} and label.user_id is not null
;

-- 用户对登录和支付的偏好得分统计
with 
user_login_cnt as (
    select
        user_id,
        weekday(time) wd,
        to_date(time) dy,
        hour(time) hr,
        hour(latest_login_time) latest_login_hr,
        count(*) login_cnt
    from(
        select
            user_id,
            time,
            max(time) over(partition by user_id) latest_login_time
        from yishou_data.dwd_log_app_wx_login_dt
        where dt >= to_char(dateadd(to_date('${bdp.system.bizdate}', 'yyyymmdd'),-29,'dd'),'yyyymmdd')
    )
    group by
        user_id,
        weekday(time),
        to_date(time),
        hour(time),
        hour(latest_login_time)
    having
        user_id <> 0
),
user_pay_cnt as (
    select
        user_id,
        weekday(pay_time) wd,
        to_date(pay_time) dy,
        hour(pay_time) hr,
        hour(latest_pay_time) latest_pay_hr,
        count(distinct order_id) pay_cnt
    from
        (
            select
                user_id,
                order_id,
                pay_time,
                max(pay_time) over(partition by user_id) latest_pay_time
            from
                yishou_daily.fin_base_matching_service_sales_order_details
            where
                pay_status = '已付款'
                and to_char(pay_time, 'yyyymmdd') >= to_char(
                    dateadd(
                        to_date('${bdp.system.bizdate}', 'yyyymmdd'),
                        -29,
                        'dd'
                    ),
                    'yyyymmdd'
                )
        )
    group by
        user_id,
        weekday(pay_time),
        to_date(pay_time),
        hour(pay_time),
        hour(latest_pay_time)
),
user_login_and_pay_record as (
    select
        user_id,
        hr,
        latest_login_hr,
        latest_pay_hr,
        sum(is_login) login_cnt,
        sum(is_pay) pay_cnt,
        sum(
            case
                when is_login = 1
                and is_pay = 1 then 1
                else 0
            end
        ) login_and_paid_cnt,
        sum(
            case
                when is_login = 1
                and is_pay = 0 then 1
                else 0
            end
        ) login_not_paid_cnt
    from
        (
            select
                coalesce(a.user_id, b.user_id) user_id,
                coalesce(a.dy, b.dy) dy,
                coalesce(a.hr, b.hr) hr,
                a.latest_login_hr,
                b.latest_pay_hr,
                case
                    when a.login_cnt is null
                    and b.pay_cnt > 0 then 1
                    when a.login_cnt > 0 then 1
                    else 0
                end is_login,
                case
                    when b.pay_cnt > 0 then 1
                    else 0
                end is_pay
            from
                user_login_cnt a full
                join user_pay_cnt b on a.user_id = b.user_id
                and a.dy = b.dy
                and a.hr = b.hr
        )
    group by
        user_id,
        hr,
        latest_login_hr,
        latest_pay_hr
),
user_login_and_pay_record_bucket as (
    select
        user_id,
        case
            when sum(login_cnt) over(partition by user_id) > 0 then '是'
            else '否'
        end as login_latest_30d,
        case
            when sum(pay_cnt) over(partition by user_id) > 0 then '是'
            else '否'
        end as pay_latest_30d,
        case
            when sum(login_and_paid_cnt) over(partition by user_id) > 0 then '是'
            else '否'
        end as login_and_pay_latest_30d,
        hr_bucket,
        latest_login_hr_bucket,
        latest_pay_hr_bucket,
        named_struct(
            '登录次数',
            login_cnt,
            '支付次数',
            pay_cnt,
            '登录且支付次数',
            login_and_paid_cnt,
            '登录未支付次数',
            login_not_paid_cnt
        ) bucket_info,
        login_cnt,
        pay_cnt,
        login_and_paid_cnt,
        login_not_paid_cnt
    from
        (
            select
                user_id,
                case
                    when hr >= 0 and hr < 1 then '[0,1)'
                    when hr >= 1 and hr < 7 then '[1,7)'
                    when hr >= 7 and hr < 8 then '[7,8)'
                    when hr >= 8 and hr < 9 then '[8,9)'
                    when hr >= 9 and hr < 10 then '[9,10)'
                    when hr >= 10 and hr < 11 then '[10,11)'
                    when hr >= 11 and hr < 12 then '[11,12)'
                    when hr >= 12 and hr < 13 then '[12,13)'
                    when hr >= 13 and hr < 14 then '[13,14)'
                    when hr >= 14 and hr < 15 then '[14,15)'
                    when hr >= 15 and hr < 16 then '[15,16)'
                    when hr >= 16 and hr < 17 then '[16,17)'
                    when hr >= 17 and hr < 18 then '[17,18)'
                    when hr >= 18 and hr < 19 then '[18,19)'
                    when hr >= 19 and hr < 20 then '[19,20)'
                    when hr >= 20 and hr < 21 then '[20,21)'
                    when hr >= 21 and hr < 22 then '[21,22)'
                    when hr >= 22 and hr < 23 then '[22,23)'
                    when hr >= 23 and hr < 24 then '[23,24)'
                    else 'NA'
                end as hr_bucket,
                case
                    when latest_login_hr >= 0 and latest_login_hr < 1 then '[0,1)'
                    when latest_login_hr >= 1 and latest_login_hr < 7 then '[1,7)'
                    when latest_login_hr >= 7 and latest_login_hr < 8 then '[7,8)'
                    when latest_login_hr >= 8 and latest_login_hr < 9 then '[8,9)'
                    when latest_login_hr >= 9 and latest_login_hr < 10 then '[9,10)'
                    when latest_login_hr >= 10 and latest_login_hr < 11 then '[10,11)'
                    when latest_login_hr >= 11 and latest_login_hr < 12 then '[11,12)'
                    when latest_login_hr >= 12 and latest_login_hr < 13 then '[12,13)'
                    when latest_login_hr >= 13 and latest_login_hr < 14 then '[13,14)'
                    when latest_login_hr >= 14 and latest_login_hr < 15 then '[14,15)'
                    when latest_login_hr >= 15 and latest_login_hr < 16 then '[15,16)'
                    when latest_login_hr >= 16 and latest_login_hr < 17 then '[16,17)'
                    when latest_login_hr >= 17 and latest_login_hr < 18 then '[17,18)'
                    when latest_login_hr >= 18 and latest_login_hr < 19 then '[18,19)'
                    when latest_login_hr >= 19 and latest_login_hr < 20 then '[19,20)'
                    when latest_login_hr >= 20 and latest_login_hr < 21 then '[20,21)'
                    when latest_login_hr >= 21 and latest_login_hr < 22 then '[21,22)'
                    when latest_login_hr >= 22 and latest_login_hr < 23 then '[22,23)'
                    when latest_login_hr >= 23 and latest_login_hr < 24 then '[23,24)'
                    else 'NA'
                end as latest_login_hr_bucket,
                case
                    when latest_pay_hr >= 0 and latest_pay_hr < 1 then '[0,1)'
                    when latest_pay_hr >= 1 and latest_pay_hr < 7 then '[1,7)'
                    when latest_pay_hr >= 7 and latest_pay_hr < 8 then '[7,8)'
                    when latest_pay_hr >= 8 and latest_pay_hr < 9 then '[8,9)'
                    when latest_pay_hr >= 9 and latest_pay_hr < 10 then '[9,10)'
                    when latest_pay_hr >= 10 and latest_pay_hr < 11 then '[10,11)'
                    when latest_pay_hr >= 11 and latest_pay_hr < 12 then '[11,12)'
                    when latest_pay_hr >= 12 and latest_pay_hr < 13 then '[12,13)'
                    when latest_pay_hr >= 13 and latest_pay_hr < 14 then '[13,14)'
                    when latest_pay_hr >= 14 and latest_pay_hr < 15 then '[14,15)'
                    when latest_pay_hr >= 15 and latest_pay_hr < 16 then '[15,16)'
                    when latest_pay_hr >= 16 and latest_pay_hr < 17 then '[16,17)'
                    when latest_pay_hr >= 17 and latest_pay_hr < 18 then '[17,18)'
                    when latest_pay_hr >= 18 and latest_pay_hr < 19 then '[18,19)'
                    when latest_pay_hr >= 19 and latest_pay_hr < 20 then '[19,20)'
                    when latest_pay_hr >= 20 and latest_pay_hr < 21 then '[20,21)'
                    when latest_pay_hr >= 21 and latest_pay_hr < 22 then '[21,22)'
                    when latest_pay_hr >= 22 and latest_pay_hr < 23 then '[22,23)'
                    when latest_pay_hr >= 23 and latest_pay_hr < 24 then '[23,24)'
                    else 'NA'
                end as latest_pay_hr_bucket,
                sum(login_cnt) login_cnt,
                sum(pay_cnt) pay_cnt,
                sum(login_and_paid_cnt) login_and_paid_cnt,
                sum(login_not_paid_cnt) login_not_paid_cnt
            from
                user_login_and_pay_record
            group by
                user_id,
                case
                    when hr >= 0 and hr < 1 then '[0,1)'
                    when hr >= 1 and hr < 7 then '[1,7)'
                    when hr >= 7 and hr < 8 then '[7,8)'
                    when hr >= 8 and hr < 9 then '[8,9)'
                    when hr >= 9 and hr < 10 then '[9,10)'
                    when hr >= 10 and hr < 11 then '[10,11)'
                    when hr >= 11 and hr < 12 then '[11,12)'
                    when hr >= 12 and hr < 13 then '[12,13)'
                    when hr >= 13 and hr < 14 then '[13,14)'
                    when hr >= 14 and hr < 15 then '[14,15)'
                    when hr >= 15 and hr < 16 then '[15,16)'
                    when hr >= 16 and hr < 17 then '[16,17)'
                    when hr >= 17 and hr < 18 then '[17,18)'
                    when hr >= 18 and hr < 19 then '[18,19)'
                    when hr >= 19 and hr < 20 then '[19,20)'
                    when hr >= 20 and hr < 21 then '[20,21)'
                    when hr >= 21 and hr < 22 then '[21,22)'
                    when hr >= 22 and hr < 23 then '[22,23)'
                    when hr >= 23 and hr < 24 then '[23,24)'
                    else 'NA'
                end,
                case
                    when latest_login_hr >= 0 and latest_login_hr < 1 then '[0,1)'
                    when latest_login_hr >= 1 and latest_login_hr < 7 then '[1,7)'
                    when latest_login_hr >= 7 and latest_login_hr < 8 then '[7,8)'
                    when latest_login_hr >= 8 and latest_login_hr < 9 then '[8,9)'
                    when latest_login_hr >= 9 and latest_login_hr < 10 then '[9,10)'
                    when latest_login_hr >= 10 and latest_login_hr < 11 then '[10,11)'
                    when latest_login_hr >= 11 and latest_login_hr < 12 then '[11,12)'
                    when latest_login_hr >= 12 and latest_login_hr < 13 then '[12,13)'
                    when latest_login_hr >= 13 and latest_login_hr < 14 then '[13,14)'
                    when latest_login_hr >= 14 and latest_login_hr < 15 then '[14,15)'
                    when latest_login_hr >= 15 and latest_login_hr < 16 then '[15,16)'
                    when latest_login_hr >= 16 and latest_login_hr < 17 then '[16,17)'
                    when latest_login_hr >= 17 and latest_login_hr < 18 then '[17,18)'
                    when latest_login_hr >= 18 and latest_login_hr < 19 then '[18,19)'
                    when latest_login_hr >= 19 and latest_login_hr < 20 then '[19,20)'
                    when latest_login_hr >= 20 and latest_login_hr < 21 then '[20,21)'
                    when latest_login_hr >= 21 and latest_login_hr < 22 then '[21,22)'
                    when latest_login_hr >= 22 and latest_login_hr < 23 then '[22,23)'
                    when latest_login_hr >= 23 and latest_login_hr < 24 then '[23,24)'
                    else 'NA'
                end,
                case
                    when latest_pay_hr >= 0 and latest_pay_hr < 1 then '[0,1)'
                    when latest_pay_hr >= 1 and latest_pay_hr < 7 then '[1,7)'
                    when latest_pay_hr >= 7 and latest_pay_hr < 8 then '[7,8)'
                    when latest_pay_hr >= 8 and latest_pay_hr < 9 then '[8,9)'
                    when latest_pay_hr >= 9 and latest_pay_hr < 10 then '[9,10)'
                    when latest_pay_hr >= 10 and latest_pay_hr < 11 then '[10,11)'
                    when latest_pay_hr >= 11 and latest_pay_hr < 12 then '[11,12)'
                    when latest_pay_hr >= 12 and latest_pay_hr < 13 then '[12,13)'
                    when latest_pay_hr >= 13 and latest_pay_hr < 14 then '[13,14)'
                    when latest_pay_hr >= 14 and latest_pay_hr < 15 then '[14,15)'
                    when latest_pay_hr >= 15 and latest_pay_hr < 16 then '[15,16)'
                    when latest_pay_hr >= 16 and latest_pay_hr < 17 then '[16,17)'
                    when latest_pay_hr >= 17 and latest_pay_hr < 18 then '[17,18)'
                    when latest_pay_hr >= 18 and latest_pay_hr < 19 then '[18,19)'
                    when latest_pay_hr >= 19 and latest_pay_hr < 20 then '[19,20)'
                    when latest_pay_hr >= 20 and latest_pay_hr < 21 then '[20,21)'
                    when latest_pay_hr >= 21 and latest_pay_hr < 22 then '[21,22)'
                    when latest_pay_hr >= 22 and latest_pay_hr < 23 then '[22,23)'
                    when latest_pay_hr >= 23 and latest_pay_hr < 24 then '[23,24)'
                    else 'NA'
                end
        )
)
-- 将结果插入到临时表中
insert overwrite table yishou_daily.dtl_new_user_feature_temp_pay_and_login_prefer_bucket
select
    user_id,
    login_latest_30d,
    pay_latest_30d,
    login_and_pay_latest_30d,
    concat_ws(',', collect_set(login_and_pay_record)) as login_and_pay_record,
    regexp_replace(max(prefer_login_bucket), 'NA', '')  prefer_login_bucket,
    regexp_replace(max(prefer_pay_bucket), 'NA', '') prefer_pay_bucket,
    max(prefer_login_and_pay_bucket) prefer_login_and_pay_bucket
from
    (
        select
            user_id,
            login_latest_30d,
            pay_latest_30d,
            login_and_pay_latest_30d,
            to_json(struct(hr_bucket as 时段, bucket_info as 登录和支付信息)) login_and_pay_record,
            case
                when max(login_cnt) over(partition by user_id) = min(login_cnt) over(partition by user_id) then (
                    case
                        when login_cnt = max(login_cnt) over(partition by user_id) then hr_bucket
                    end
                )
                else latest_login_hr_bucket
            end as prefer_login_bucket,
            case
                when pay_latest_30d = '否' then 'NA'
                when max(pay_cnt) over(partition by user_id) = min(pay_cnt) over(partition by user_id) then (
                    case
                        when pay_cnt = max(pay_cnt) over(partition by user_id) then hr_bucket
                    end
                )
                else latest_pay_hr_bucket
            end as prefer_pay_bucket,
            case
                when pay_latest_30d = '否' then 'NA'
                when max(login_and_paid_cnt) over(partition by user_id) = min(login_and_paid_cnt) over(partition by user_id) then (
                    case
                        when login_and_paid_cnt = max(login_and_paid_cnt) over(partition by user_id) then hr_bucket
                    end
                )
                else latest_pay_hr_bucket
            end as prefer_login_and_pay_bucket
        from
            user_login_and_pay_record_bucket
    )
group by
    user_id,
    login_latest_30d,
    pay_latest_30d,
    login_and_pay_latest_30d
;



-- 数据汇总
insert overwrite table yishou_daily.dtl_new_user_feature PARTITION(dt)
select
    a.user_id
    , a.user_belong
    , a.dianzhu_level
    , a.order_level_by_month
    , a.is_maintain_by_other
    , a.is_maintain_by_social
    , a.is_maintain_by_vip
    , a.is_maintain_by_wx
    , a.order_amount_level
    , a.order_level
    , a.last_login_day
    , a.last_cart_day
    , a.last_order_day
    , a.reg_day
    , a.value_level
    , a.order_amout_avg_3_8
    , a.order_amout_avg_9_2
    , coalesce(grade_table.grade, 0) as goods_preference_level
    , a.freight_meet
    , a.refund_sensitive
    , a.good_category_grade_first_id
    , a.good_category_grade_second_id
    , a.good_category_grade_third_id
    , coalesce(style_table.style_id, 0) new_style_id
    , a.buyer_id
    , a.supply_id
    , a.anchor_id
    , a.market_first_id
    , a.market_second_id
    , a.pre_loss_flag
    , a.each_life_cycle
    , a.user_type
    , a.suspected_work_orders
    , a.wong_work_orders
    , coalesce(b.city_id, ugeo.city_id,0) as city_id
    , coalesce(a.max_month_money,0) as max_month_money
    , coalesce(nufo.user_net_worth,'106') as user_net_worth
    , coalesce(pay_and_login_prefer_bucket_table.prefer_login_bucket, '') as like_login_time
    , coalesce(pay_and_login_prefer_bucket_table.prefer_pay_bucket, '') as like_pay_time
    , a.is_allow_push  
    , a.last_month_refund_rate
    , a.three_month_refund_rate
    , a.one_year_refund_rate
    , eval.user_service_label 
    , f_m_l.high_potential_users high_potential_users
    ,a.probably_b
    ,a.r_level
    ,a.r_table
    ,a.f_table
    ,a.m_table
    ,a.value_table
    ,a.is_b_port
    ,a.is_important_user
    ,a.pre_loss_flag_v2
    ,a.cp_privilege as cp_privilege
    ,a.first_time as first_time
    ,a.last_login_day_count as last_login_day_count
    ,a.last_cart_day_count as last_cart_day_count
    ,a.last_order_day_count as last_order_day_count
    ,a.first_order_day_count as first_order_day_count
    ,a.first_order_month as first_order_month
    ,a.last_mon_order_cnt as last_mon_order_cnt
    -- ,a.is_this_mon_5_order as is_this_mon_5_order
    ,a.is_last_mon_5_order as is_last_mon_5_order
    ,a.is_before_last_mon_5_order as is_before_last_mon_5_order
    ,a.all_order_cnt as all_order_cnt
    ,a.malice_mistake_send
    ,a.suspected_malice_defective
    ,a.history_mon_max_gmv
    ,a.normal_refund_money
    ,a.no_reason_refund_money
    ,a.sign_7_apply_cp_rate
    ,a.higt_defective_user 
    ,a.stock_cacel_rate
    ,a.order_cacel_rate
    ,a.defective_auto_pass 
    ,coalesce(epu.extreme_premium_user, 0) AS extreme_premium_user
    ,sergp.service_group
    ,coalesce(lcu.looklike_c_user, 0) AS looklike_c_user
    ,coalesce(a.all_deliver_gmv,0) as all_deliver_gmv -- 累计发货GMV
    ,coalesce(a.all_return_rate,0) as all_return_rate -- 累计退货率
    ,coalesce(a.all_sign_num,0) as all_sign_num -- 近5年签收件数
    ,coalesce(a.all_defective_rate,0) as all_defective_rate -- 近5年次品率
    ,coalesce(a.recent_year_sign_num,0) as recent_year_sign_num -- 近1年签收件数
    ,coalesce(a.recent_year_defective_rate,0) as recent_year_defective_rate -- 近1年次品率
    ,${bdp.system.bizdate} as dt
from yishou_daily.dtl_new_user_feature_d as a
left join (
    select
        user_id
        , city as city_id
    from (
        select user_id
            , city
            , order_cnt
            , last_order_time
            , ROW_NUMBER() OVER(PARTITION by user_id order by  order_cnt desc , last_order_time desc)  as rank
        from (
            select
                user_id as user_id
                , city as city
                , count(*) as order_cnt
                , max(add_time) as last_order_time
            from yishou_data.all_fmys_order
            group by user_id, city
        )
    )
    where rank = 1
) as b
on a.user_id = b.user_id

left join yishou_data.dtl_user_geo_info ugeo 
on a.user_id = ugeo.user_id

left join (
    select
        user_id,
        user_net_worth
    from
        yishou_daily.dtl_new_user_feature_order
    where
        dt = '${bdp.system.bizdate}'
) nufo 
on a.user_id = nufo.user_id

left join (

    select
        data_table.user_id
        , data_table.grade
        , data_table.score
        , data_table.row_number_id
    from(
        select
            user_id
        from yishou_recommendation_system.user_score_30day
        where score > 50
    ) as user_table
    
    INNER JOIN (
        select
            user_id
            , grade
            , score
            , row_number() over(partition by user_id order by score desc) row_number_id
        from yishou_recommendation_system.user_grade_score_180day
        having row_number_id = 1
    ) as data_table
    on user_table.user_id = data_table.user_id

) as grade_table
on a.user_id = grade_table.user_id

left join yishou_daily.dtl_new_user_feature_temp_pay_and_login_prefer_bucket as pay_and_login_prefer_bucket_table
on a.user_id = pay_and_login_prefer_bucket_table.user_id

left join (

    -- 在 风格id为 51 到 71之间，求取用户最偏好的风格
    select
        user_id
        , style_id
        , score
        , row_number() over(partition by user_id order by score desc) row_number_id
    from yishou_recommendation_system.user_style_score_180day
    where 
        dt = '${bdp.system.bizdate}'
        and style_id between 51 and 71
    having row_number_id = 1
    
) as style_table
on a.user_id = style_table.user_id
left join (select * from yishou_daily.dtl_user_malicious_feature_label where dt = '${bdp.system.bizdate}') eval on eval.user_id = a.user_id
left join (select * from yishou_daily.dtl_user_5000_more_feature_label where dt = '${bdp.system.bizdate}') f_m_l  on f_m_l.user_id = a.user_id
left join (select * from yishou_daily.dtl_extreme_premium_user_snap_dt where dt = '${bdp.system.bizdate}') epu  on epu.user_id = a.user_id
left join (SELECT * FROM yishou_daily.ads_looklike_c_user where dt = '${bdp.system.bizdate}') lcu  on lcu.user_id = a.user_id
left join (
select 
user_id
,test_tail_1
,case 
when test_tail_1 >= 0 and test_tail_1 <25 then '1'
when test_tail_1 >= 25 and test_tail_1 <50 then '2'
when test_tail_1 >= 50 and test_tail_1 <75 then '3'
when test_tail_1 >= 75 and test_tail_1 <100 then '4'
end service_group
from yishou_daily.dim_user_test_level_snap_dt
) sergp  on sergp.user_id = a.user_id
DISTRIBUTE BY floor(rand()*30)
;
