-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/03/31 13:50:25 GMT+08:00
-- ******************************************************************** --
-- 专场一场多用
-- 需求prd:https://yb7ao262ru.feishu.cn/wiki/O5FqwnLrriKb8ekBDc7ch0DanLg

-- 逻辑
-- 1、特征维度
-- 折扣、是否新款、品类、档口、风格
-- 2、数据规则
-- a、折扣
-- 按照折扣字段对某个专场的商品做分组，
-- if ：低于1折的商品大于200个则折扣=1折
-- else：if 低于2折的商品大于200个则折扣=2折
-- else：if 低于3折的商品大于200个则折扣=3折
-- else：if 低于4折的商品大于200个则折扣=4折
-- else：if 低于5折的商品大于200个则折扣=5折
-- Else 折扣为 空值。
-- b、是否新款
-- 某个专场当日上新的商品大于200时，是否新款=新款
-- Else 为 空值
-- c、品类
-- 按照三级品类字段对某个专场商品做分组，
-- A、C、F品类对应的商品均超过200个，品类=[A,C,F]
-- Else 为 空值
-- d、档口
-- 按照档口字段对某个专场商品做分组
-- xx、ww、tt档口对应的商品均超过200个，档口=[xx、ww、tt]
-- Else 为 空值
-- e、风格
-- 按照风格字段对某个专场商品做分组
-- Tx、Ya、Ov风格对应的商品均超过200个，风格=[Tx、Ya、Ov]
-- Else 为 空值

-- 建表
-- CREATE EXTERNAL TABLE yishou_daily.dtl_special_label_multi_use_dt (
--     `special_id` string COMMENT '专场id',
--     `discount` string COMMENT '折扣',
--     `is_new` string COMMENT '是否新款',
--     `three_cat_lists` string COMMENT '三级品类标签列表',
--     `supply_lists` string COMMENT '档口标签列表',
--     `style_lists` string COMMENT '风格标签列表'
-- )
-- comment '专场标签-一场多用'
-- partitioned by (dt string)
-- STORED AS ORC LOCATION 'obs://yishou-bigdata/yishou_daily.db/dtl_special_label_multi_use_dt'
-- ;

-- 插数
insert overwrite table yishou_daily.dtl_special_label_multi_use_dt PARTITION(dt)
select 
    fs.special_id
    -- ,case 
    --     when goods.discount_1_goods > 200 then '1折'
    --     when goods.discount_2_goods > 200 then '2折'
    --     when goods.discount_3_goods > 200 then '3折'
    --     when goods.discount_4_goods > 200 then '4折'
    --     when goods.discount_5_goods > 200 then '5折'
    --     when goods.discount_6_goods > 200 then '6折'
    --     when goods.discount_7_goods > 200 then '7折'
    --     when goods.discount_8_goods > 200 then '8折'
    --     when goods.discount_9_goods > 200 then '9折'
    --     when goods.discount_10_goods > 200 then '10折'
    --     else null
    -- end as discount
    ,null as discount
    ,case when goods.new_goods_1 > 200 then '新款' else null end as is_new
    ,three_cat_lists
    ,supply_lists
    ,style_lists
    ,'${gmtdate}' as dt
from (
    -- 专场主表
    select 
        special_id
        ,to_char(FROM_UNIXTIME(special_start_time-25200),'yyyymmdd') as special_date
    from yishou_data.all_fmys_special_h
    where to_char(FROM_UNIXTIME(special_start_time-25200),'yyyymmdd') = '${gmtdate}'
) fs 
left join (
    -- 商品
    select 
        frsog.special_date
        ,frsog.special_id
        -- ,count(case when frsog.discount_ratio < 0.1 then frsog.goods_no end) as discount_1_goods
        -- ,count(case when frsog.discount_ratio < 0.2 then frsog.goods_no end) as discount_2_goods
        -- ,count(case when frsog.discount_ratio < 0.3 then frsog.goods_no end) as discount_3_goods
        -- ,count(case when frsog.discount_ratio < 0.4 then frsog.goods_no end) as discount_4_goods
        -- ,count(case when frsog.discount_ratio < 0.5 then frsog.goods_no end) as discount_5_goods
        -- ,count(case when frsog.discount_ratio < 0.6 then frsog.goods_no end) as discount_6_goods
        -- ,count(case when frsog.discount_ratio < 0.7 then frsog.goods_no end) as discount_7_goods
        -- ,count(case when frsog.discount_ratio < 0.8 then frsog.goods_no end) as discount_8_goods
        -- ,count(case when frsog.discount_ratio < 0.9 then frsog.goods_no end) as discount_9_goods
        -- ,count(case when frsog.discount_ratio < 1.0 then frsog.goods_no end) as discount_10_goods
        -- 近3天，例：24-26
        ,count(case when datediff(current_date(),new_goods_first_up_time) <= 2 then frsog.goods_no end) as new_goods_1
    from (
        select 
            a.* 
            -- 新款首次上架时间 
            ,to_char(coalesce(a.new_first_up_time,from_unixtime(unix_timestamp(current_date(), 'yyyy-MM-dd') + 7 * 3600)),'yyyy-mm-dd') as new_goods_first_up_time
            ,a.shop_price / t2.shop_price as discount_ratio
        from (
            select 
                special_date
                , goods_no
                , goods_id
                , special_id
                , shop_price
                , is_putaway
                , new_first_up_time
                ,ROW_NUMBER() over(PARTITION BY goods_id ORDER BY special_id desc) rank_num 
            from yishou_daily.finebi_realdata_special_onsale_goods
        ) a
        left join yishou_data.dim_goods_no_info_full_h t2 on a.goods_no = t2.goods_no
        where a.rank_num = 1
    ) frsog
    where frsog.special_date = '${gmtdate}' 
    and frsog.is_putaway = '上架'
    group by frsog.special_date,frsog.special_id
) goods on goods.special_id = fs.special_id and fs.special_date = goods.special_date
left join (
    -- 品类
    select 
        special_date
        ,special_id
        ,concat(
            '[',
            regexp_replace(
                concat_ws(
                    ',',
                    sort_array(collect_list(concat_ws(':',lpad(cast(rank_num as string),5,'0'),CONCAT('{"id":"',cat_id,'","name":"',third_cat_name,'"}'))))
                ),
                '\\d+\:',
                ''
            ),
            ']'
        ) as three_cat_lists
    from (
        select 
            to_char(dgif.special_date,'yyyymmdd') as special_date
            ,fs.special_id
            ,dci.cat_id
            ,dci.third_cat_name
            ,count(DISTINCT dgif.goods_no) as sale_kh
            ,ROW_NUMBER() OVER (PARTITION BY fs.special_id,to_char(dgif.special_date,'yyyymmdd') ORDER BY count(DISTINCT dgif.goods_no) DESC) AS rank_num
        from yishou_data.all_fmys_special_h fs
        left join yishou_data.dim_goods_id_info_full_h dgif on dgif.special_id = fs.special_id
        left join (
            SELECT 
                fc.cat_id
                , case when fc.grade = 3 then fc.cat_name end as third_cat_name
            from yishou_data.all_fmys_category_h fc 
            left join yishou_data.all_fmys_category_h fc1 on fc.parent_id = fc1.cat_id 
            left join yishou_data.all_fmys_category_h fc2 on fc1.parent_id = fc2.cat_id
        ) dci on dci.cat_id = dgif.cat_id
        where to_char(dgif.special_date,'yyyymmdd') = '${gmtdate}'
        and dgif.is_on_sale = 1
        group by 1,2,3,4
        having sale_kh > 200
    )
    group by 1,2
) cat_rank on cat_rank.special_id = fs.special_id and fs.special_date = cat_rank.special_date
left join (
    -- 供应商
    select 
        special_date
        ,special_id
        ,concat(
            '[',
            regexp_replace(
                concat_ws(
                    ',',
                    sort_array(collect_list(concat_ws(':',lpad(cast(rank_num as string),5,'0'),CONCAT('{"id":"',supply_id,'","name":"',brand_name,'"}'))))
                ),
                '\\d+\:',
                ''
            ),
            ']'
        ) as supply_lists
    from(
        select 
            to_char(dgif.special_date,'yyyymmdd') as special_date
            ,fs.special_id
            ,dsi.supply_id
            ,sb.brand_name
            ,count(distinct dgif.goods_no) as sale_kh
            ,ROW_NUMBER() OVER (PARTITION BY fs.special_id,to_char(dgif.special_date,'yyyymmdd') ORDER BY count(DISTINCT dgif.goods_no) DESC) AS rank_num
        from yishou_data.all_fmys_special_h fs
        left join yishou_data.dim_goods_id_info_full_h dgif on dgif.special_id = fs.special_id
        left join yishou_data.ods_fmys_supply_view dsi on dsi.supply_id = dgif.supply_id
        left join (	
            -- 主品牌
        	select
                supply_id
                , brand_id
                , brand_name
        	from (
                select 
                    sb.supply_id
                    , sb.brand_id
                    , fgb.brand_name
                    , row_number() over(partition by sb.supply_id order by sb.brand_id asc) as num   --剔除一个供应商2个主品牌现象
                from yishou_data.all_fmys_supply_brand_h sb
                left join yishou_data.all_fmys_goods_brand_h fgb on sb.brand_id = fgb.brand_id
                -- 启用主品牌
                where sb.is_main = 1 and sb.status <> 0
        	)
        	where num = 1
        ) sb 
        on dsi.supply_id = sb.supply_id
        where to_char(dgif.special_date,'yyyymmdd') = '${gmtdate}'
        and dgif.is_on_sale = 1
        group by 1,2,3,4
        having sale_kh > 200
    )
    group by 1,2
) supply_rank on supply_rank.special_id = fs.special_id and fs.special_date = supply_rank.special_date
left join (
    -- 风格
    select 
        special_date
        ,special_id
        ,concat(
            '[',
            regexp_replace(
                concat_ws(
                    ',',
                    sort_array(collect_list(concat_ws(':',lpad(cast(rank_num as string),5,'0'),CONCAT('{"id":"',style_id,'","name":"',style_name,'"}'))))
                ),
                '\\d+\:',
                ''
            ),
            ']'
        ) as style_lists
    from(
        select 
            to_char(dgif.special_date,'yyyymmdd') as special_date
            ,fs.special_id
            ,dgnif.style_id
            ,dgnif.style_name
            ,count(DISTINCT dgif.goods_no) as sale_kh
            ,ROW_NUMBER() OVER (PARTITION BY fs.special_id,to_char(dgif.special_date,'yyyymmdd') ORDER BY count(DISTINCT dgif.goods_no) DESC) AS rank_num
        from yishou_data.all_fmys_special_h fs
        left join yishou_data.dim_goods_id_info_full_h dgif on dgif.special_id = fs.special_id
        left join yishou_data.dim_goods_no_info_full_h dgnif on dgnif.goods_no = dgif.goods_no
        where to_char(dgif.special_date,'yyyymmdd') = '${gmtdate}'
        and dgif.is_on_sale = 1
        group by 1,2,3,4
        having sale_kh > 200
    )
    group by 1,2
) style_rank 
on style_rank.special_id = fs.special_id and fs.special_date = style_rank.special_date
DISTRIBUTE BY dt 
;

-- 打到redis
-- CREATE EXTERNAL TABLE yishou_daily.dtl_special_label_multi_use_redis_dt (
--     `key` string COMMENT '专场id',
--     `value` string COMMENT '标签汇总'
-- )
-- comment '专场标签-一场多用'
-- partitioned by (dt string)
-- STORED AS ORC LOCATION 'obs://yishou-bigdata/yishou_daily.db/dtl_special_label_multi_use_redis_dt'
-- ;

insert OVERWRITE table yishou_daily.dtl_special_label_multi_use_redis_dt
SELECT 
    special_id as key,
    CONCAT(
        '{',
        '"discount":"', IF(discount IS NULL, '', discount), '",',
        '"is_new":"', IF(is_new IS NULL, '', is_new), '",',
        '"cat_label":', IF(three_cat_lists IS NULL, '[]', three_cat_lists), ',',
        '"supply_label":', IF(supply_lists IS NULL, '[]', supply_lists), ',',
        '"style_label":', IF(style_lists IS NULL, '[]', style_lists),
        '}'
    ) AS value
    ,dt
FROM yishou_daily.dtl_special_label_multi_use_dt
where dt = '${gmtdate}'
DISTRIBUTE BY dt
;