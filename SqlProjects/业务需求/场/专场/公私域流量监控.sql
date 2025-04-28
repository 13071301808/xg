-- DLI sql 
-- ******************************************************************** --
-- author: luozhengjie
-- create time: 2023/12/07 11:29:32 GMT+08:00
-- ******************************************************************** --

-- DROP TABLE `cross_origin`.`finebi_goods_source_exposure_dt_lzj`;
-- CREATE EXTERNAL TABLE cross_origin.finebi_goods_source_exposure_dt_lzj(
--     special_date     string comment '专场日期' ,
--     buy_num_level    string comment '销售件数分层' ,
--     uv_value_level   string comment 'uv价值分层' ,
--     款数             bigint comment '' ,
--     曝光uv           bigint comment '' ,
--     点击uv           bigint comment '' ,
--     加购uv           bigint comment '' ,
--     支付uv           bigint comment '' ,
--     实际支付uv       bigint comment '' ,
--     支付件数         bigint comment '' ,
--     实际支付件数     bigint comment '' ,
--     gmv              double comment '' ,
--     实际gmv          double comment '' ,
--     公域款数         bigint comment '' ,
--     公域曝光uv       bigint comment '' ,
--     公域点击uv       bigint comment '' ,
--     公域加购uv       bigint comment '' ,
--     公域支付uv       bigint comment '' ,
--     公域实际支付uv   bigint comment '' ,
--     公域销售件数     bigint comment '' ,
--     公域实际销售件数 bigint comment '' ,
--     公域销售额       double comment '' ,
--     公域实际销售额   double comment '' ,
--     私域款数         bigint comment '' ,
--     私域曝光         bigint comment '' ,
--     私域点击uv       bigint comment '' ,
--     私域加购uv       bigint comment '' ,
--     私域支付uv       bigint comment '' ,
--     私域实际支付uv   bigint comment '' ,
--     私域销售件数     bigint comment '' ,
--     私域实际销售件数 bigint comment '' ,
--     私域销售额       double comment '' ,
--     私域实际销售额   double comment ''
-- )COMMENT '公私域流量监控' partitioned by (`dt` string comment '统计日期')
-- ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' WITH SERDEPROPERTIES ('serialization.format' = '1') 
-- STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' 
-- LOCATION 'obs://yishou-bigdata/cross_origin.db/finebi_goods_source_exposure_dt_lzj' TBLPROPERTIES (
--     'hive.serialization.extend.nesting.levels' = 'true',
--     'orc.compress' = 'SNAPPY'
-- );




with t as 
(
select 
    coalesce(aaa.goods_no,bbb.goods_no,ccc.goods_no,ddd.goods_no)                   as goods_no ,
    coalesce(aaa.special_date,bbb.special_date,ccc.special_date,ddd.special_date)   as special_date ,
    coalesce(aaa.exposure_uv,0)                                                     as exposure_uv ,
    coalesce(bbb.click_uv,0)                                                        as click_uv ,
    coalesce(bbb.`公域点击uv`,0)                                                    as 公域点击 ,
    coalesce(bbb.`私域点击uv`,0)                                                    as 私域点击 ,
    coalesce(ccc.add_cart_uv,0)                                                     as add_cart_uv ,
    coalesce(ccc.`公域加购uv`,0)                                                    as 公域加购 ,
    coalesce(ccc.`私域加购uv`,0)                                                    as 私域加购 , 
    coalesce(ddd.pay_uv,0)                                                          as pay_uv ,
    coalesce(ddd.buy_amount,0)                                                      as buy_amount ,
    coalesce(ddd.buy_num,0)                                                         as buy_num ,
    coalesce(ddd.real_pay_uv,0)                                                     as real_pay_uv ,
    coalesce(ddd.real_buy_amount,0)                                                 as real_buy_amount ,
    coalesce(ddd.real_buy_num,0)                                                    as real_buy_num ,
    coalesce(ddd.buy_amount,0)/coalesce(aaa.exposure_uv,0)                          as uv_value ,
    coalesce(公域曝光,0)                                                            as 公域曝光 ,   
    coalesce(私域曝光,0)                                                            as 私域曝光 ,
    coalesce(公域销售件数,0)                                                        as 公域销售件数 ,
    coalesce(公域销售额,0)                                                          as 公域销售额 ,
    coalesce(公域支付,0)                                                            as 公域支付 ,
    coalesce(公域实际销售件数,0)                                                    as 公域实际销售件数 ,
    coalesce(公域实际销售额,0)                                                      as 公域实际销售额 ,
    coalesce(公域实际支付,0)                                                        as 公域实际支付 ,
    coalesce(私域销售件数,0)                                                        as 私域销售件数 ,
    coalesce(私域销售额,0)                                                          as 私域销售额 ,
    coalesce(私域支付,0)                                                            as 私域支付 ,
    coalesce(私域实际销售件数,0)                                                    as 私域实际销售件数 ,
    coalesce(私域实际销售额,0)                                                      as 私域实际销售额 ,
    coalesce(私域实际支付,0)                                                        as 私域实际支付
from
    (
        select 
            goods_no ,
            special_date ,
            sum(exposure_uv) exposure_uv ,
            sum(if(pid not in (17,30,31,34,35,26,44) or pid is null, exposure_uv, 0)) 公域曝光 ,
            sum(if(pid in (17,30,31,34,35,26,44), exposure_uv, 0)) 私域曝光
        from 
            (
                select 
                    case when a.goods_no = '' or not is_number(a.goods_no) then 0 else a.goods_no end as goods_no
                    ,a.special_date
                    ,pid
                    ,count(distinct case when user_id = '' or not is_number(user_id) then 0 else user_id end) as exposure_uv
                from yishou_data.dwd_log_goods_exposure_dt a
                    join yishou_data.dim_goods_id_info b on a.goods_id = b.goods_id and a.special_date = b.special_date
                -- where dt >= ${开始日期}
                --     and to_char(to_date1(special_time,'yyyymmdd'),'yyyymmdd') between ${开始日期} and ${结束日期}
                where dt between ${bizdate} and to_char(date_add(to_date1('${bizdate}','yyyymmdd'),1),'yyyymmdd')
                    and to_char(a.special_date,'yyyymmdd') = ${bizdate}
                group by 1,2,3
                union all
                select 
                    a.goods_no
                    ,a.special_date
                    ,'h5' pid
                    ,count(distinct user_id) as exposure_uv
                from yishou_data.dwd_log_h5_goods_list_exposure_dt a
                    join yishou_data.dim_goods_id_info b on a.goods_id = b.goods_id and a.special_date = b.special_date
                -- where dt >= ${开始日期}
                --     and to_char(special_date,'yyyymmdd') between ${开始日期} and ${结束日期}
                where dt between ${bizdate} and to_char(date_add(to_date1('${bizdate}','yyyymmdd'),1),'yyyymmdd')
                    and to_char(a.special_date,'yyyymmdd') = ${bizdate}
                group by 1,2,3
            )
        group by 1,2
    ) aaa
    full join 
    (
        --点击
        select
            aa.special_date
            ,bb.goods_no
            ,count(distinct aa.user_id,source) as click_uv 
            ,count(distinct if(source not in (1,2,3,6,9,11,12,13,14,16,27,30,37,49,52,53,58,59,66,67,68,70,71,72,74,77,80,82,84,85,86,87) or source is null, (aa.user_id,source), null)) as `公域点击uv` 
            ,count(distinct if(source in (1,2,3,6,9,11,12,13,14,16,27,30,37,49,52,53,58,59,66,67,68,70,71,72,74,77,80,82,84,85,86,87), (aa.user_id,source), null)) as `私域点击uv`
        from yishou_data.dwd_log_goods_detail_page_dt aa
            join yishou_data.dim_goods_id_info bb
                on aa.goods_id = bb.goods_id and aa.special_date = bb.special_date
        -- where aa.dt >= ${开始日期}
        --     and to_char(aa.special_date, 'yyyymmdd') between ${开始日期} and ${结束日期}
        where aa.dt between ${bizdate} and to_char(date_add(to_date1('${bizdate}','yyyymmdd'),1),'yyyymmdd')
            and to_char(aa.special_date,'yyyymmdd') = ${bizdate}
        group by 1,2
    ) bbb on aaa.special_date = bbb.special_date and aaa.goods_no = bbb.goods_no
    full join 
    (
        select
            aa.special_date
            ,bb.goods_no
            ,count(distinct aa.user_id,source) as add_cart_uv
            ,count(distinct if(source not in (1,2,3,6,9,11,12,13,14,16,27,30,37,49,52,53,58,59,66,67,68,70,71,72,74,77,80,82,84,85,86,87) or source is null, (aa.user_id,source), null)) as `公域加购uv` 
            ,count(distinct if(source in (1,2,3,6,9,11,12,13,14,16,27,30,37,49,52,53,58,59,66,67,68,70,71,72,74,77,80,82,84,85,86,87), (aa.user_id,source), null)) as `私域加购uv`
        from yishou_data.dwd_log_add_cart_dt aa
            join yishou_data.dim_goods_id_info bb
            on aa.goods_id = bb.goods_id and aa.special_date = bb.special_date
        -- where aa.dt >= ${开始日期} 
        --     and to_char(aa.special_date, 'yyyymmdd') between ${开始日期} and ${结束日期}
        where aa.dt between ${bizdate} and to_char(date_add(to_date1('${bizdate}','yyyymmdd'),1),'yyyymmdd')
            and to_char(aa.special_date,'yyyymmdd') = ${bizdate}
        group by 1,2
    )ccc on coalesce(aaa.special_date, bbb.special_date) = ccc.special_date and coalesce(aaa.goods_no, bbb.goods_no) = ccc.goods_no
    full join 
    (
        --支付
        select
            datetrunc(aa.special_start_time, 'dd') as special_date
            ,aa.goods_no
            ,sum(aa.shop_price * aa.buy_num) as buy_amount
            ,sum(if(is_real_pay = 1, aa.shop_price * aa.buy_num, 0)) as real_buy_amount
            ,sum(aa.buy_num) as buy_num
            ,sum(if(is_real_pay = 1, aa.buy_num, 0)) as real_buy_num
            ,count(distinct aa.user_id,sa_source) as pay_uv
            ,count(distinct if(is_real_pay = 1, (aa.user_id,sa_source), null)) as real_pay_uv
            ,sum(if(sa_source not in (1,2,3,6,9,11,12,13,14,16,27,30,37,49,52,53,58,59,66,67,68,70,71,72,74,77,80,82,84,85,86,87) or sa_source is null, buy_num, 0)) 公域销售件数 
            ,sum(if((sa_source not in (1,2,3,6,9,11,12,13,14,16,27,30,37,49,52,53,58,59,66,67,68,70,71,72,74,77,80,82,84,85,86,87) or sa_source is null) and is_real_pay = 1, buy_num, 0)) 公域实际销售件数 
            ,sum(if(sa_source not in (1,2,3,6,9,11,12,13,14,16,27,30,37,49,52,53,58,59,66,67,68,70,71,72,74,77,80,82,84,85,86,87) or sa_source is null, buy_num*shop_price, 0)) 公域销售额 
            ,sum(if((sa_source not in (1,2,3,6,9,11,12,13,14,16,27,30,37,49,52,53,58,59,66,67,68,70,71,72,74,77,80,82,84,85,86,87) or sa_source is null) and is_real_pay = 1, buy_num*shop_price, 0)) 公域实际销售额 
            ,count(distinct if(sa_source not in (1,2,3,6,9,11,12,13,14,16,27,30,37,49,52,53,58,59,66,67,68,70,71,72,74,77,80,82,84,85,86,87) or sa_source is null, (aa.user_id,sa_source), null)) 公域支付
            ,count(distinct if((sa_source not in (1,2,3,6,9,11,12,13,14,16,27,30,37,49,52,53,58,59,66,67,68,70,71,72,74,77,80,82,84,85,86,87) or sa_source is null) and is_real_pay = 1, (aa.user_id,sa_source), null)) 公域实际支付
            ,sum(if(sa_source in (1,2,3,6,9,11,12,13,14,16,27,30,37,49,52,53,58,59,66,67,68,70,71,72,74,77,80,82,84,85,86,87), buy_num, 0)) 私域销售件数 
            ,sum(if(sa_source in (1,2,3,6,9,11,12,13,14,16,27,30,37,49,52,53,58,59,66,67,68,70,71,72,74,77,80,82,84,85,86,87) and is_real_pay = 1, buy_num, 0)) 私域实际销售件数 
            ,sum(if(sa_source in (1,2,3,6,9,11,12,13,14,16,27,30,37,49,52,53,58,59,66,67,68,70,71,72,74,77,80,82,84,85,86,87), buy_num*shop_price, 0)) 私域销售额
            ,sum(if(sa_source in (1,2,3,6,9,11,12,13,14,16,27,30,37,49,52,53,58,59,66,67,68,70,71,72,74,77,80,82,84,85,86,87) and is_real_pay = 1, buy_num*shop_price, 0)) 私域实际销售额
            ,count(distinct if(sa_source in (1,2,3,6,9,11,12,13,14,16,27,30,37,49,52,53,58,59,66,67,68,70,71,72,74,77,80,82,84,85,86,87), (aa.user_id,sa_source), null)) 私域支付
            ,count(distinct if(sa_source in (1,2,3,6,9,11,12,13,14,16,27,30,37,49,52,53,58,59,66,67,68,70,71,72,74,77,80,82,84,85,86,87) and is_real_pay = 1, (aa.user_id,sa_source), null)) 私域实际支付
        from yishou_data.dwd_sale_order_info_dt aa
        -- where aa.dt >= ${开始日期}
        --     and to_char(aa.special_start_time, 'yyyymmdd') between ${开始日期} and ${结束日期}
        where aa.dt between ${bizdate} and to_char(date_add(to_date1('${bizdate}','yyyymmdd'),1),'yyyymmdd')
            and to_char(aa.special_start_time,'yyyymmdd') = ${bizdate}
            and pay_status = 1  
        group by 1,2
    )ddd on coalesce(aaa.special_date, bbb.special_date, ccc.special_date) = ddd.special_date and coalesce(aaa.goods_no, bbb.goods_no, ccc.goods_no) = ddd.goods_no
)

INSERT OVERWRITE TABLE cross_origin.finebi_goods_source_exposure_dt_lzj PARTITION(dt)
select 
    special_date ,
    case 
        when buy_num = 0 then '0件' 
        when buy_num = 1 then '1件'
        when buy_num between 2 and 4 then '2-4件'
        when buy_num between 5 and 9 then '5-9件'
        when buy_num between 10 and 30 then '10-30件'
        when buy_num > 30 then '30+件'
    end 销售件数分层 ,
    case 
        when exposure_uv = 0 and buy_amount > 0 then '0.4+'
        when uv_value is null or uv_value = 0 then '0'
        when uv_value <= 0.05 then '0-0.05'
        when uv_value <= 0.1 then '0.05-0.1'
        when uv_value <= 0.2 then '0.1-0.2'
        when uv_value <= 0.4 then '0.2-0.4'
        when uv_value > 0.4 then '0.4+'
    end uv价值分层 ,
    count(distinct goods_no) 款数 ,
    sum(exposure_uv) 曝光uv ,
    sum(click_uv) 点击uv ,
    sum(add_cart_uv) 加购uv ,
    sum(pay_uv) 支付uv ,
    sum(real_pay_uv) 实际支付uv ,
    sum(buy_num) 支付件数 ,
    sum(real_buy_num) 实际支付件数 ,
    sum(buy_amount) gmv ,
    sum(real_buy_amount) 实际gmv ,
    count(distinct if(公域曝光 > 0 or 公域销售额 > 0 or 公域点击 > 0, goods_no, null)) 公域款数 ,
    sum(公域曝光) 公域曝光uv ,
    sum(公域点击) 公域点击uv ,
    sum(公域加购) 公域加购uv ,
    sum(公域支付) 公域支付uv ,
    sum(公域实际支付) 公域实际支付uv ,
    sum(公域销售件数) 公域销售件数 ,
    sum(公域实际销售件数) 公域实际销售件数 ,
    sum(公域销售额) 公域销售额 ,
    sum(公域实际销售额) 公域实际销售额 ,
    count(distinct if(私域曝光 > 0 or 私域销售额 > 0 or 私域点击 > 0, goods_no, null)) 私域款数 ,
    sum(私域曝光) 私域曝光 ,
    sum(私域点击) 私域点击uv ,
    sum(私域加购) 私域加购uv ,
    sum(私域支付) 私域支付uv ,
    sum(私域实际支付) 私域实际支付uv ,
    sum(私域销售件数) 私域销售件数 ,
    sum(私域实际销售件数) 私域实际销售件数 ,
    sum(私域销售额) 私域销售额 ,
    sum(私域实际销售额) 私域实际销售额 ,
    to_char(special_date, 'yyyymmdd') dt
from t 
group by 1,2,3,dt
distribute by dt
;

