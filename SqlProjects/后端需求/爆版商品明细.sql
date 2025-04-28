--odps sql 
--********************************************************************--
--author:常军磊
--create time:2020-12-29 17:18:41
--********************************************************************--
-- CREATE EXTERNAL TABLE `yishou_daily`.`dtl_data_goods_info`(
--     `special_date` STRING COMMENT '统计日期',
--     `supply_id` BIGINT COMMENT '供应商ID',
--     `goods_kh` STRING COMMENT '款号',
--     `goods_no` BIGINT COMMENT '货号',
--     `co_val` STRING COMMENT '颜色',
--     `si_val` STRING COMMENT '尺码',
--     `real_buy_user_num` BIGINT COMMENT '实际支付人数',
--     `real_buy_num` BIGINT COMMENT '实际销量',
--     `real_sale_amount` DOUBLE COMMENT '实际销售额',
--     `exposure_uv` INT COMMENT '曝光uv',
--     `chick_uv` INT COMMENT '点击uv',
--     `add_cart_uv` BIGINT COMMENT '加购uv',
--     `buy_uv` BIGINT COMMENT '支付uv',
--     `join_activity` STRING COMMENT '参与活动',
--     `discount_rate` DOUBLE COMMENT '折扣率',
--     `new_special_buy_level` STRING COMMENT '层级',
--     `supply_big_market` STRING COMMENT '供应商大市场'
--  ) COMMENT '爆版_商品明细' PARTITIONED BY (`dt` STRING COMMENT 'yyyymmdd') ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' WITH SERDEPROPERTIES ('serialization.format' = '1') STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION 'obs://yishou-bigdata/yishou_daily.db/dtl_data_goods_info' TBLPROPERTIES (
--     'hive.serialization.extend.nesting.levels' = 'true',
--     'transient_lastDdlTime' = '1642400572',
--     'parquet.compression' = 'SNAPPY'
--   );
--alter table dtl_data_goods_info add columns (exposure_uv int comment '曝光uv',chick_uv int comment '点击uv'); 


-- 爆版商品明细
with temp_dtl_data_goods_info_${one_day_ago} as (
    select 
        to_char(dateadd(from_unixtime(fo.add_time),-7,'hh'),'yyyymmdd') as special_date
        , foi.sku as sku_id
        , fgl.supply_id
        , fgl.goods_kh
        , fgl.goods_no
        , fc.co_val
        , fgs.si_val
        , count(distinct fo.user_id) as real_buy_user_num
        , SUM(foi.buy_num) as real_buy_num
        , round(sum(foi.buy_num*foi.shop_price),2) as real_sale_amount
    from  yishou_data.all_fmys_order_h fo
    join yishou_data.all_fmys_order_infos_h foi on fo.order_id = foi.order_id
    left join yishou_data.all_fmys_order_cancel_record_h cr on fo.order_id = cr.order_id and cr.cancel_type = 1
    left join yishou_data.all_fmys_goods_lib_h fgl on foi.goods_no = fgl.goods_no
    left join yishou_data.ods_fmys_goods_color_view fc on foi.co_id = fc.co_id
    left join yishou_data.ods_fmys_goods_size_view fgs on foi.si_id = fgs.si_id
    where to_char(dateadd(from_unixtime(fo.add_time),-7,'hh'),'yyyymmdd') = '${gmtdate}'
    and (case when fo.pay_status = 1 and cr.order_id is null then 1 else 0 end) = 1
    group by to_char(dateadd(from_unixtime(fo.add_time),-7,'hh'),'yyyymmdd')
        , foi.sku
        , fgl.supply_id
        , fgl.goods_kh
        , fgl.goods_no
        , fc.co_val
        , fgs.si_val
    having sum(foi.buy_num*foi.shop_price)>0
)
, temp_attend_activty_${one_day_ago} as (
    --  上线需要添加该临时表
    --  参与活动
    select 
        taa.goods_no
        ,concat_ws('\\',collect_set(taa.label)) as label  
    from (
        select 
           distinct a.goods_no,
             "直播款" as label
        from  yishou_data.dim_goods_id_info_full_h a
        left join yishou_data.all_fmys_goods_in_stock_h gis on a.goods_id = gis.goods_id
        where a.is_live=1 and gis.goods_id is null -- 去掉现货商品
        and to_char(dateadd(a.add_time,-7,'hh'),'yyyymmdd') = '${gmtdate}'
        union all
        select  
            distinct t1.goods_no,
            "大牌无忧" as label
        from yishou_data.dim_goods_id_info_full_h t1
        left join yishou_data.all_fmys_goods_in_stock_h gis on t1.goods_id = gis.goods_id
        join (
            SELECT 
                DISTINCT goods_no, 
                sale_type_name,
                status,
                started_at,
                ended_at
            FROM yishou_data.ods_fmys_activity_tag_view
            lateral view outer explode (split(goods_nos,',')) as goods_no
            WHERE goods_no is not null 
            and goods_no <> ''
        ) t3 
        on t1.goods_no=t3.goods_no and t3.status = '启用'
        where to_char(dateadd(t1.add_time,-7,'hh'),'yyyymmdd') = '${gmtdate}'
        and ((t3.sale_type_name = "以预售形式上架的商品" and t1.is_in_stock = 0)or(t3.sale_type_name = "以现货形式上架的商品" and t1.is_in_stock = 1)or t3.sale_type_name = "不限制上架形式的商品")
        and gis.goods_id is null -- 去掉现货商品
    )taa
    group by taa.goods_no
)
,app_goods_exposure as (
    -- APP商品曝光
    select
        case 
            when pid = 10 THEN 1
            when pid in (17,30,31,34,35,43) THEN 2
            when pid = 12 then 4
            when pid = 14 then 5
            when pid = 28 then 7
            when pid in (37,40) then 9
            when pid in (15,33) then 10
            when pid = 26 then 11
            when pid = 32 then 12
            when pid in (25,29) then 13
            else 8
        end channel_id
        ,user_id
        ,goods_id
    from yishou_data.dwd_app_goods_exposure_view
    where dt = '${gmtdate}'
    and from_unixtime((receive_time - 25200), 'yyyyMMdd') = from_unixtime(unix_timestamp() - 25200, 'yyyyMMdd')
    union ALL 
    select
        0 as channel_id
        ,user_id
        ,goods_id
    from yishou_data.dwd_app_goods_exposure_view
    where dt = '${gmtdate}'
    and from_unixtime((receive_time - 25200), 'yyyyMMdd') = from_unixtime(unix_timestamp() - 25200, 'yyyyMMdd')
)
,h5_goods_exposure as (
    -- H5商品曝光
    select
        case 
            when title = '首发新款' THEN 3
            else 6 
        end channel_id
        ,user_id
        ,goods_id
    from yishou_data.dwd_h5_goods_exposure_view
    where dt = '${gmtdate}'
    and from_unixtime((receive_time - 25200), 'yyyyMMdd') = from_unixtime(unix_timestamp() - 25200, 'yyyyMMdd')
    union all 
    select
        0 as channel_id
        ,user_id
        ,goods_id
    from yishou_data.dwd_h5_goods_exposure_view
    where dt = '${gmtdate}'
    and from_unixtime((receive_time - 25200), 'yyyyMMdd') = from_unixtime(unix_timestamp() - 25200, 'yyyyMMdd')
)
,app_goods_click as (
    -- APP商品点击
    select
        case 
            when source = 4 THEN 1
            when source in (11,79) THEN 2
            when source = 8 then 4
            when source = 15 then 5
            when source = 32 then 7
            when source = 75 then 9
            when source in (21,22) then 10
            when source = 2 then 11
            when source = 20 then 12
            when source in (28,29,43) then 13
            when source = 1495 then 3
            when length(source) >= 4 then 6
            else 8 
        end channel_id
        ,user_id
        ,goods_id
    from yishou_data.dwd_app_goods_click_view
    where dt = '${gmtdate}'
    and from_unixtime((receive_time - 25200), 'yyyyMMdd') = from_unixtime(unix_timestamp() - 25200, 'yyyyMMdd')
    union ALL 
    select
        0 as channel_id
        ,user_id
        ,goods_id
    from yishou_data.dwd_app_goods_click_view
    where dt = '${gmtdate}'
    and from_unixtime((receive_time - 25200), 'yyyyMMdd') = from_unixtime(unix_timestamp() - 25200, 'yyyyMMdd')
)
,goods_add_cart as (
    -- 全部商品加购
    select
        case 
            when source = 4 THEN 1
            when source in (11,79) THEN 2
            when source = 8 then 4
            when source = 15 then 5
            when source = 32 then 7
            when source = 75 then 9
            when source in (21,22) then 10
            when source = 2 then 11
            when source = 20 then 12
            when source in (28,29,43) then 13
            when source = 1495 then 3
            when length(source) >= 4 then 6
            else 8 
        end channel_id
        ,user_id
        ,goods_id
    from yishou_data.dwd_app_goods_add_cart_view
    where dt = '${gmtdate}'
    and from_unixtime((receive_time - 25200), 'yyyyMMdd') = from_unixtime(unix_timestamp() - 25200, 'yyyyMMdd')
    union ALL 
    select
        0 as channel_id
        ,user_id
        ,goods_id
    from yishou_data.dwd_app_goods_add_cart_view
    where dt = '${gmtdate}'
    and from_unixtime((receive_time - 25200), 'yyyyMMdd') = from_unixtime(unix_timestamp() - 25200, 'yyyyMMdd')
)
,h5_goods_click as (
    -- H5商品点击
    select
        case 
            when title = '首发新款' THEN 3
            else 6 
        end channel_id
        ,user_id
        ,goods_id
    from yishou_data.dwd_h5_goods_click_view
    where dt = '${gmtdate}'
    and from_unixtime((receive_time - 25200), 'yyyyMMdd') = from_unixtime(unix_timestamp() - 25200, 'yyyyMMdd')
    union ALL 
    select
        0 as channel_id
        ,user_id
        ,goods_id
    from yishou_data.dwd_h5_goods_click_view
    where dt = '${gmtdate}'
    and from_unixtime((receive_time - 25200), 'yyyyMMdd') = from_unixtime(unix_timestamp() - 25200, 'yyyyMMdd')
)
,flow_table as (
    -- 流量数据统计
    select
        coalesce(exposure_table.special_date,click_table.special_date,add_cart_table.special_date) as special_date
        , coalesce(exposure_table.channel_id,click_table.channel_id,add_cart_table.channel_id) as channel_id
        , coalesce(exposure_table.goods_no, click_table.goods_no) as goods_no
        , coalesce(exposure_table.exposure_pv, 0) as exposure_pv
        , coalesce(exposure_table.exposure_uv, 0) as exposure_uv
        , coalesce(click_table.click_pv, 0) as click_pv
        , coalesce(click_table.click_uv, 0) as click_uv
        , coalesce(add_cart_table.add_cart_pv, 0) as add_cart_pv
        , coalesce(add_cart_table.add_cart_uv, 0) as add_cart_uv
    from (
        select
            to_char(goods_id_info.special_date,'yyyymmdd') as special_date
            , main_table.channel_id
            , goods_id_info.goods_no
            , count(main_table.user_id) exposure_pv
            , count(distinct main_table.user_id) exposure_uv
        from (
            select
                channel_id
                , user_id
                , goods_id
            from app_goods_exposure
            union all 
            select
                channel_id
                , user_id
                , goods_id
            from h5_goods_exposure
        ) as main_table
        left join yishou_data.dwd_fmys_goods_in_stock_view as goods_in_stock
        on main_table.goods_id = goods_in_stock.goods_id
        left join yishou_data.dim_goods_id_info_full_h as goods_id_info
        on main_table.goods_id = goods_id_info.goods_id
        and goods_id_info.is_tail_dscn_goods = '否'  -- 2024.01.05去除尾货折销
        where goods_in_stock.goods_id is null
        group by to_char(goods_id_info.special_date,'yyyymmdd'),main_table.channel_id,goods_id_info.goods_no
    ) as exposure_table
    full join (
        select
            to_char(goods_id_info.special_date,'yyyymmdd') as special_date
            , main_table.channel_id
            , goods_id_info.goods_no
            , count(main_table.user_id) click_pv
            , count(distinct main_table.user_id) click_uv
        from (
            select
                channel_id
                , user_id
                , goods_id
            from app_goods_click
            union all 
            select
                channel_id
                , user_id
                , goods_id
            from h5_goods_click
        ) as main_table
        left join yishou_data.dwd_fmys_goods_in_stock_view as goods_in_stock
        on main_table.goods_id = goods_in_stock.goods_id
        left join yishou_data.dim_goods_id_info_full_h as goods_id_info
        on main_table.goods_id = goods_id_info.goods_id
        and goods_id_info.is_tail_dscn_goods = '否'  -- 2024.01.05去除尾货折销
        where goods_in_stock.goods_id is null
        group by to_char(goods_id_info.special_date,'yyyymmdd'),main_table.channel_id,goods_id_info.goods_no
    ) as click_table 
    on exposure_table.goods_no = click_table.goods_no and exposure_table.channel_id = click_table.channel_id
    full join (
        select
            to_char(goods_id_info.special_date,'yyyymmdd') as special_date
            , main_table.channel_id
            , goods_id_info.goods_no
            , count(main_table.user_id) add_cart_pv
            , count(distinct main_table.user_id) add_cart_uv
        from goods_add_cart as main_table
        left join yishou_data.dwd_fmys_goods_in_stock_view as goods_in_stock
        on main_table.goods_id = goods_in_stock.goods_id
        left join yishou_data.dim_goods_id_info_full_h as goods_id_info
        on main_table.goods_id = goods_id_info.goods_id
        and goods_id_info.is_tail_dscn_goods = '否'  -- 2024.01.05去除尾货折销
        where goods_in_stock.goods_id is null
        group by to_char(goods_id_info.special_date,'yyyymmdd'),main_table.channel_id,goods_id_info.goods_no
    ) as add_cart_table
    on coalesce(exposure_table.goods_no, click_table.goods_no) = add_cart_table.goods_no 
    and coalesce(exposure_table.channel_id,click_table.channel_id) = add_cart_table.channel_id
)
--  上线需要修改插入语句
insert overwrite table yishou_daily.dtl_data_goods_info partition (dt)
select 
    coalesce(dgi.special_date,flow_table.special_date) as special_date --这里的格式是yyyymmdd
    ,dgi.supply_id as supply_id
    ,coalesce(dgi.goods_kh,'') as goods_kh
    ,coalesce(dgi.goods_no,flow_table.goods_no) as goods_no
    ,coalesce(dgi.co_val,'') as co_val
    ,coalesce(dgi.si_val,'') as si_val
    ,coalesce(dgi.real_buy_user_num,0) as real_buy_user_num
    ,coalesce(dgi.real_buy_num,0) as real_buy_num
    ,coalesce(dgi.real_sale_amount,0) as real_sale_amount
    ,coalesce(flow_table.exposure_uv,0) as exposure_uv --曝光uv
    ,coalesce(flow_table.click_uv,0) as click_uv --点击uv
    ,coalesce(flow_table.add_cart_uv,0) as add_cart_uv --加购uv
    ,coalesce(dgi.real_buy_user_num,0) as buy_uv --支付uv
    ,case when taa.label is null then '无' else taa.label end as join_activity
    ,coalesce(saf.discount_rate,0) as discount_rate
    ,srr.new_special_buy_level
    ,case 
        when fbm.big_market_name='广州沙河南城' then '广州沙河南城'
        when fbm.big_market_name='广州十三行' then '广州十三行'
        when fbm.big_market_name in ('杭州市场','杭州平湖','杭州鞋包','濮院市场') then '杭濮市场'
        when fbm.big_market_name='深圳南油' then  '深圳市场'
        when fbm.big_market_name='鞋包配饰' then  '鞋包配饰'
        else '其它市场'
    end as supply_big_market
    ,coalesce(dgi.sku_id,0) as sku_id
    ,flow_table.channel_id
    ,${one_day_ago} as dt
from temp_dtl_data_goods_info_${one_day_ago} dgi
left join flow_table on dgi.goods_no = flow_table.goods_no and dgi.special_date = flow_table.special_date
left join yishou_data.all_fmys_supply_h fs on dgi.supply_id = fs.supply_id
left join yishou_data.all_fmys_market_h fm on fm.market_id = fs.market_id
left join yishou_data.all_fmys_market fmp on fm.parent_id = fmp.market_id
left join yishou_data.all_fmys_big_market_h fbm on (case when fm.parent_id = 0 then fm.big_market else fmp.big_market end)=fbm.big_market_id
left join (
    select 
        c.goods_no
        ,sum(a.supply_fee)
        ,sum(c.price* c.num)
        ,sum(a.supply_fee)/sum(c.price* c.num) as discount_rate
    from yishou_data.ods_fmys_supply_activity_fee_view a
    left join yishou_data.ods_fmys_order_plan_all_detail_view b on a.ade_id = b.ade_id
    left join yishou_data.ods_fmys_wcg_order_info_view c on b.woiid = c.woiid
    group by c.goods_no 
)saf on saf.goods_no = dgi.goods_no 
left join (
    select supply_id
        , max(new_special_buy_level) as new_special_buy_level
    from yishou_daily.daily_supply_level_mt
    where mt = to_char(dateadd(to_date1('${one_day_ago}','yyyymmdd'),-1,'mm'),'yyyymm')  -- 上个月
    group by supply_id
)srr on srr.supply_id=dgi.supply_id
left join temp_attend_activty_${one_day_ago} taa on taa.goods_no = dgi.goods_no
;
	