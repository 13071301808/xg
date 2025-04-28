-- DLI sql 
-- ******************************************************************** --
-- author: chenyiming
-- create time: 2023/12/27 16:05:59 GMT+08:00
-- ******************************************************************** --
-- create EXTERNAL table yishou_daily.dtl_report_supply_cat_price_range_dt
-- (
--     special_date      date comment '专场日期',
--     supply_id         bigint comment '供应商ID',
--     primary_cat_id    int comment '商品1级品类ID',
--     second_cat_id     int comment '商品2级品类ID',
--     third_cat_id      int comment '商品3级品类ID',
--     price_range_id    int comment '价格带ID',
--     price_range_name  string comment '价格带名称',
--     price_range_start int comment '价格带开始值',
--     price_range_end   int comment '价格带结束值',
--     buy_amount        double comment '销售金额',
--     buy_num           bigint comment '销售件数',
--     buy_user_num      bigint comment '支付人数',
--     sale_goods_num    bigint comment '商品动销款数'
-- )COMMENT '报表数据：供应商-品类-价格带' 
-- PARTITIONED BY (dt STRING COMMENT '日期分区') 
-- STORED AS parquet
--  LOCATION 'obs://yishou-bigdata/yishou_daily.db/dtl_report_supply_cat_price_range_dt' 
--  TBLPROPERTIES (
--         'hive.serialization.extend.nesting.levels' = 'true',
-- 		'parquet.compression' = 'SNAPPY'
-- );

INSERT OVERWRITE TABLE yishou_daily.dtl_report_supply_cat_price_range_dt PARTITION(dt)
select 
    to_date(to_date1('${gmtdate}','yyyymmdd')) special_date
    ,fgl.supply_id
    ,ci.primary_cat_id
    ,ci.second_cat_id
    ,ci.third_cat_id
    ,case 
        when foi.shop_price > 190 then 5
        when foi.shop_price > 100 then 4
        when foi.shop_price > 69 then 3
        when foi.shop_price > 40 then 2
        else 1 
    end as price_range_id
    ,case 
        when foi.shop_price > 190 then '190以上'
        when foi.shop_price > 100 then '100-190'
        when foi.shop_price > 69 then '69-100'
        when foi.shop_price > 40 then '40-69'
        else '0-40' 
    end as price_range_name
    ,case 
        when foi.shop_price > 190 then 190
        when foi.shop_price > 100 then 100
        when foi.shop_price > 69 then 69
        when foi.shop_price > 40 then 40
        else 0 
    end as price_range_start
    ,case 
        when foi.shop_price > 190 then 99999
        when foi.shop_price > 100 then 190
        when foi.shop_price > 69 then 100
        when foi.shop_price > 40 then 69
        else 40 
    end as price_range_end
    ,sum(if(fo.pay_status=1,foi.buy_num * foi.shop_price,0)) buy_amount
    ,sum(foi.buy_num) buy_num
    ,count(distinct foi.goods_no,foi.user_id) buy_user_num
    ,count(distinct foi.goods_no) sale_goods_num
    ,'${gmtdate}' as dt
from yishou_data.all_fmys_order_h fo
join yishou_data.all_fmys_order_infos_h foi on fo.order_id = foi.order_id
left join yishou_data.all_fmys_order_cancel_record_h cr on fo.order_id = cr.order_id and cr.cancel_type = 1
left join yishou_data.all_fmys_goods_lib_h fgl on foi.goods_no = fgl.goods_no
left join yishou_data.dim_cat_info ci on ci.cat_id=fgl.cat_id
where to_char(from_unixtime(fo.add_time),'yyyymmdd') = '${gmtdate}'
and (case when fo.pay_status = 1 and cr.order_id is null then 1 else 0 end) = 1
group by 1,2,3,4,5,6,7,8,9
DISTRIBUTE BY dt
;