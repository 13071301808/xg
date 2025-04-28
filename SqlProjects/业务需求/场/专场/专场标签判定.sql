-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/12/03 16:52:39 GMT+08:00
-- ******************************************************************** --
-- 专场标签
-- 表头：https://yb7ao262ru.feishu.cn/sheets/JsATsu3P8heqIrtBDWTcZ6a5nSb

-- create EXTERNAL table yishou_daily.dtl_special_goods_label_choose_dt(
--     `special_date` string COMMENT '专场日期',
--     `special_id` string COMMENT '专场id',
--     `special_name` string COMMENT '专场名称',
--     `ini_middle_mid` string COMMENT '初始排序中部中档',
--     `special_style_type` string COMMENT '专场类型',
--     `big_market_1` string COMMENT '大市场1',
--     `big_market_2` string COMMENT '大市场2',
--     `department_1` string COMMENT '供给部门1',
--     `department_2` string COMMENT '供给部门2',
--     `cat_one` string COMMENT '品类1',
--     `cat_two` string COMMENT '品类2',
--     `cat_three` string COMMENT '品类3',
--     `special_style_type_name_1` string COMMENT '风格1',
--     `supply_id_1` string COMMENT '档口1',
--     `supply_id_2` string COMMENT '档口2',
--     `supply_id_3` string COMMENT '档口3',
--     `supply_sale_ratio_1` string COMMENT '档口1占比',
--     `supply_sale_ratio_2` string COMMENT '档口2占比',
--     `supply_sale_ratio_3` string COMMENT '档口3占比',
--     `cat_sale_ratio_1` string COMMENT '品类1占比',
--     `cat_sale_ratio_2` string COMMENT '品类2占比',
--     `cat_sale_ratio_3` string COMMENT '品类3占比',
--     `sale_goods_num` string COMMENT '上架状态商品货号款数',
--     `avg_goods_price` string COMMENT '平均商品售价',
--     `special_click_uv` string COMMENT '专场点击uv',
--     `special_exposure_uv` string COMMENT '专场曝光uv',
--     `special_real_gmv` string COMMENT '专场渠道实际销售额',
--     `new_sale_kh_1` string COMMENT '新款1',
--     `discount_ratio_1` string COMMENT '折扣率1',
--     `sp_g_price_madle` string COMMENT '近3天专场商品价格分位总均值'
-- )
-- comment '专场标签判定明细'
-- partitioned by (dt string)
-- STORED AS ORC LOCATION 'obs://yishou-bigdata/yishou_daily.db/dtl_special_goods_label_choose_dt'
-- ;

-- -- 20241224新增6个字段
-- alter table yishou_daily.dtl_special_goods_label_choose_dt
-- add columns (
--     big_market_id_1 string comment '大市场id1',
--     big_market_id_2 string comment '大市场id2',
--     cat_id_1 string comment '品类id1',
--     cat_id_2 string comment '品类id2',
--     cat_id_3 string comment '品类id3'
-- ); 
-- alter table yishou_daily.dtl_special_goods_label_choose_dt
-- add columns (
--     sku_num string comment 'sku总数'
-- ); 

insert overwrite table yishou_daily.dtl_special_goods_label_choose_dt partition(dt)
select 
    fs.special_date as special_date -- 专场日期
    ,fs.special_id as special_id -- 专场id
    ,fs.special_name as special_name -- 专场名称
    ,fss.ini_middle_mid as ini_middle_mid -- 初始排序中部中档
    ,fs.special_style_type_name as special_style_type_name -- 专场类型（要名称）
    ,dmi.big_market_1 as big_market_1 -- 大市场1
    ,dmi.big_market_2 as big_market_2 -- 大市场2
    ,dsi.department_1 as department_1 -- 供给部门1
    ,dsi.department_2 as department_2 -- 供给部门2
    ,cat_rank.cat_one as cat_one -- 品类1
    ,cat_rank.cat_two as cat_two -- 品类2
    ,cat_rank.cat_three as cat_three -- 品类3
    ,style_rank.style_name as style_name_1 -- 风格1
    ,supply_rank.supply_id_1 as supply_id_1 -- 档口1
    ,supply_rank.supply_id_2 as supply_id_2 -- 档口2
    ,supply_rank.supply_id_3 as supply_id_3 -- 档口3
    ,round(max(supply_rank.supply_sale_ratio_1),4) as supply_sale_ratio_1 -- 档口1占比
    ,round(max(supply_rank.supply_sale_ratio_2),4) as supply_sale_ratio_2 -- 档口2占比
    ,round(max(supply_rank.supply_sale_ratio_3),4) as supply_sale_ratio_3 -- 档口3占比
    ,round(max(cat_rank.cat_sale_ratio_1),4) as cat_sale_ratio_1 -- 品类1占比
    ,round(max(cat_rank.cat_sale_ratio_2),4) as cat_sale_ratio_2 -- 品类2占比
    ,round(max(cat_rank.cat_sale_ratio_3),4) as cat_sale_ratio_3 -- 品类3占比
    ,sum(goods.sale_goods_num) as sale_goods_num -- 上架状态商品货号款数
    ,round(avg(goods.avg_goods_price),4) as avg_goods_price -- 平均商品售价
    ,sum(special_click.click_uv) as special_click_uv -- 专场点击uv
    ,sum(special_exposure.exposure_uv) as special_exposure_uv -- 专场渠道曝光uv
    ,round(sum(gmv.special_real_gmv),4) as special_real_gmv -- 专场渠道实际销售额(路径归因)
    ,sum(goods.new_goods_1) as new_goods_1 -- 新款1
    ,round(max(goods.discount_ratio),4) as discount_ratio_1 -- 折扣率1
    ,round(avg(madle.sp_g_price_madle),4) as sp_g_price_madle -- 近3天专场商品价格分位总均值
    ,max(dmi.big_market_id_1) as big_market_id_1 -- 大市场id1
    ,max(dmi.big_market_id_2) as big_market_id_2 -- 大市场id2
    ,max(cat_rank.cat_id_1) as cat_id_1 -- 品类id1
    ,max(cat_rank.cat_id_2) as cat_id_2 -- 品类id2
    ,max(cat_rank.cat_id_3) as cat_id_3 -- 品类id3
    ,max(style_rank.style_id) as style_id_1 -- 风格id1
    ,sum(sku.sku_num) as sku_num -- sku总数
    ,'${gmtdate}' as dt
from (
    -- 专场主表
    select 
        special_id
        ,special_name
        ,to_char(FROM_UNIXTIME(special_start_time-25200),'yyyymmdd') as special_date
        ,supply_ids
        ,case 
            special_style_type
            when 1 then  '风格新款'
            when 2 then  '品类'
            when 3 then  '明星档口(作废)'
            when 22 then '明星档口-正价'
            when 23 then '明星档口-特价'
            when 4 then  '补货'
            when 5 then  '返场'
            when 6 then  '七日爆款'
            when 7 then  '买助全品类'
            when 8 then  '特价'
            when 9 then  '昨日TOP50爆款'
            when 10 then '畅销返场'
            when 11 then '周TOP'
            when 12 then '正价现货'
            when 13 then '特价现货'
            when 14 then '其他'
            when 15 then '直播-明星档口'
            when 16 then '返场专场-明星档口'
            when 17 then '直播-风格新款'
            when 18 then '返场专场-风格新款'
            when 19 then '直播-买助全品类'
            when 20 then '返场专场-买助全品类'
            when 21 then '直播-特价'
            when 24 then '直播-明星档口特价'
            when 25 then '返场专场-明星档口特价'
            when 26 then '直播-闪拼'
        end as special_style_type_name
    from yishou_data.all_fmys_special_h
    where to_char(FROM_UNIXTIME(special_start_time-25200),'yyyymmdd') = '${gmtdate}'
) fs
left join yishou_data.ods_fmys_special_sort_view fss on fs.special_id = fss.special_id
left join (
    -- 商品
    select 
        frsog.special_date
        ,frsog.special_id
        ,coalesce(count(frsog.goods_no),0) as sale_goods_num
        ,avg(frsog.shop_price) as avg_goods_price
        ,avg(frsog.discount_ratio) as discount_ratio
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
        where a.rank_num = 1 and a.is_putaway = '上架'
    ) frsog
    where frsog.special_date = '${gmtdate}'
    group by frsog.special_date,frsog.special_id
) goods on goods.special_id = fs.special_id and fs.special_date = goods.special_date
left join (
    -- 进入专场的点击uv
    SELECT  
        to_char(from_unixtime((time/1000)-25200),'yyyymmdd') AS special_date 
        ,special_id
        ,COUNT(DISTINCT user_id) AS click_uv
    FROM yishou_data.dcl_event_enter_special
    WHERE dt between to_char(dateadd(to_date1('${gmtdate}','yyyymmdd'),-1,'dd'),'yyyymmdd') and '${gmtdate}'
    and is_number(special_id)
    and to_char(from_unixtime((time/1000)-25200),'yyyymmdd') = '${gmtdate}'
    GROUP BY 1,2
) special_click 
on fs.special_id=special_click.special_id and fs.special_date = special_click.special_date
left join (
    -- 首页专场列表的曝光uv
    select 
        to_char(from_unixtime(time-25200),'yyyymmdd') as special_date
        , special_id 
        , count(DISTINCT user_id) as exposure_uv  
    from yishou_data.dcl_event_big_special_exposure_h 
    where special_id is not null and special_id <> ''
    and dt between to_char(dateadd(to_date1('${gmtdate}','yyyymmdd'),-1,'dd'),'yyyymmdd') and '${gmtdate}'
    and to_char(from_unixtime(time-25200),'yyyymmdd') = '${gmtdate}'
    group by 1,2
) special_exposure 
on fs.special_id=special_exposure.special_id and fs.special_date = special_exposure.special_date
left join (
    -- 专场渠道实际销售额(路径归因) -- 用了标哥那边的，刷不了
    select 
        to_char(sp.special_date,'yyyymmdd') as special_date
        ,goods.special_id
        ,sum(sp.buy_amount) as special_real_gmv
    from yishou_daily.dtl_special_horse_score_new_route_basic_temp sp 
    left join yishou_data.dim_goods_id_info_full_h goods on sp.goods_id = goods.goods_id
    where sp.dt between to_char(dateadd(to_date1('${gmtdate}','yyyymmdd'),-1,'dd'),'yyyymmdd') and '${gmtdate}'
    and to_char(sp.special_date,'yyyymmdd') = '${gmtdate}'
    and second_page_name = '首页专场'
    group by 1,2
) gmv on gmv.special_id = fs.special_id and fs.special_date = gmv.special_date
left join (
    -- 供应商排名
    select 
        special_date
        ,special_id
        ,max(case when rank_num = 1 then supply_id end) as supply_id_1
        ,max(case when rank_num = 2 then supply_id end) as supply_id_2
        ,max(case when rank_num = 3 then supply_id end) as supply_id_3
        ,max(case when rank_num = 1 then supply_sale_ratio end) as supply_sale_ratio_1
        ,max(case when rank_num = 2 then supply_sale_ratio end) as supply_sale_ratio_2
        ,max(case when rank_num = 3 then supply_sale_ratio end) as supply_sale_ratio_3
    from (
        select  
            special_date
            ,special_id
            ,supply_id
            ,ROW_NUMBER() OVER (PARTITION BY special_id,special_date ORDER BY sale_kh DESC) AS rank_num
            ,sum(sale_kh) over (PARTITION BY special_id,special_date,supply_id)/ sum(sale_kh) over (PARTITION BY special_id,special_date) as supply_sale_ratio
        from (         
            select 
                to_char(dgif.special_date,'yyyymmdd') as special_date
                ,fs.special_id
                ,dsi.supply_id
                ,count(distinct dgif.goods_no) as sale_kh
            from yishou_data.all_fmys_special_h fs
            left join yishou_data.dim_goods_id_info_full_h dgif on dgif.special_id = fs.special_id
            left join yishou_data.ods_fmys_supply_view dsi on dsi.supply_id = dgif.supply_id
            where to_char(dgif.special_date,'yyyymmdd') = '${gmtdate}'
            and dgif.is_on_sale = 1
            group by 1,2,3
        )
        having rank_num <= 3
    )
    group by 1,2
) supply_rank on supply_rank.special_id = fs.special_id and fs.special_date = supply_rank.special_date
left join (
    -- 供给部门排名
    select 
        special_date
        ,special_id
        ,max(case when rank_num = 1 then department end) as department_1
        ,max(case when rank_num = 2 then department end) as department_2
    from (
        select 
            special_date
            ,special_id
            ,department
            ,ROW_NUMBER() OVER (PARTITION BY special_id,special_date ORDER BY sale_kh DESC) AS rank_num
        from (
            select 
                to_char(dgif.special_date,'yyyymmdd') as special_date
                ,fs.special_id
                ,dgif.department
                ,count(DISTINCT dgif.goods_no) as sale_kh
            from yishou_data.all_fmys_special_h fs
            left join yishou_data.dim_goods_id_info_full_h dgif on dgif.special_id = fs.special_id
            where to_char(dgif.special_date,'yyyymmdd') = '${gmtdate}'
            and dgif.is_on_sale = 1
            group by 1,2,3
        )
        having rank_num <= 2
    )
    group by 1,2
)dsi on dsi.special_id = fs.special_id and fs.special_date = dsi.special_date
left join (
    -- 大市场排名
    select 
        special_date
        ,special_id
        ,max(case when rank_num = 1 then big_market end) as big_market_1
        ,max(case when rank_num = 2 then big_market end) as big_market_2
        ,max(case when rank_num = 1 then big_market_id end) as big_market_id_1
        ,max(case when rank_num = 2 then big_market_id end) as big_market_id_2
    from (
        select 
            special_date
            ,special_id
            ,big_market_id
            ,big_market
            ,ROW_NUMBER() OVER (PARTITION BY special_id,special_date ORDER BY sale_kh DESC) AS rank_num
        from (
            select 
                to_char(dgif.special_date,'yyyymmdd') as special_date
                ,fs.special_id
                ,fbm.big_market_id
                ,dgif.big_market
                ,count(DISTINCT dgif.goods_no) as sale_kh
            from yishou_data.all_fmys_special_h fs
            left join yishou_data.dim_goods_id_info_full_h dgif on dgif.special_id = fs.special_id
            left join yishou_data.all_fmys_big_market_h fbm on fbm.big_market_name = dgif.big_market
            where to_char(dgif.special_date,'yyyymmdd') = '${gmtdate}'
            and dgif.is_on_sale = 1
            group by 1,2,3,4
        )
        having rank_num <= 2
    )
    group by 1,2
) dmi on dmi.special_id = fs.special_id and dmi.special_date = fs.special_date
left join (
    -- 品类排名
    select 
        special_date
        ,special_id
        ,max(case when rank_num = 1 then cat_id end) as cat_id_1
        ,max(case when rank_num = 2 then cat_id end) as cat_id_2
        ,max(case when rank_num = 3 then cat_id end) as cat_id_3
        ,max(case when rank_num = 1 then third_cat_name end) as cat_one
        ,max(case when rank_num = 2 then third_cat_name end) as cat_two
        ,max(case when rank_num = 3 then third_cat_name end) as cat_three
        ,max(case when rank_num = 1 then cat_sale_ratio end) as cat_sale_ratio_1
        ,max(case when rank_num = 2 then cat_sale_ratio end) as cat_sale_ratio_2
        ,max(case when rank_num = 3 then cat_sale_ratio end) as cat_sale_ratio_3
    from (
        select  
            special_date
            ,special_id
            ,cat_id
            ,third_cat_name
            ,ROW_NUMBER() OVER (PARTITION BY special_id,special_date ORDER BY sale_kh DESC) AS rank_num
            ,sum(sale_kh) over (PARTITION BY special_id,special_date,third_cat_name)/ sum(sale_kh) over (PARTITION BY special_id,special_date) as cat_sale_ratio
        from (
            select 
                to_char(dgif.special_date,'yyyymmdd') as special_date
                ,fs.special_id
                ,dci.cat_id
                ,dci.third_cat_name
                ,count(DISTINCT dgif.goods_no) as sale_kh
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
        )
        having rank_num <= 3
    )
    group by 1,2
) cat_rank on cat_rank.special_id = fs.special_id and fs.special_date = cat_rank.special_date
left join (
    -- 风格排名
    select  
        special_date
        ,special_id
        ,style_id
        ,style_name
        ,ROW_NUMBER() OVER (PARTITION BY special_id,special_date ORDER BY sale_kh DESC) AS rank_num
    from (
        select 
            to_char(dgif.special_date,'yyyymmdd') as special_date
            ,fs.special_id
            ,dgnif.style_id
            ,dgnif.style_name
            ,count(DISTINCT dgif.goods_no) as sale_kh
        from yishou_data.all_fmys_special_h fs
        left join yishou_data.dim_goods_id_info_full_h dgif on dgif.special_id = fs.special_id
        left join yishou_data.dim_goods_no_info_full_h dgnif on dgnif.goods_no = dgif.goods_no
        where to_char(dgif.special_date,'yyyymmdd') = '${gmtdate}'
        and dgif.is_on_sale = 1
        group by 1,2,3,4
    )
    having rank_num = 1
) style_rank 
on style_rank.special_id = fs.special_id and fs.special_date = style_rank.special_date
left join (
    -- 当天同一专场下所有商品 / 近3天所有商品的分位总均值
    select
        special_id
        , sum(sp_g_ratio) / count(goods_no) as sp_g_price_madle -- (0.5+0.5+0.8)/3
    from (
        select
            a.special_id
            , a.goods_no
            , a.g_avg_rank / b.total_goods_num as sp_g_ratio     -- 2.5/5
        from (
            select 
                special_id
                , goods_no
                ,sum(sp_t_goods_rank) / count(goods_no) as g_avg_rank -- 2.5
            from (
                select 
                    special_id
                    ,goods_no
                    ,row_number() over(order by round(shop_price,0) asc) sp_t_goods_rank -- 5
                from yishou_data.dim_goods_id_info_full_h
                where to_char(special_date,'yyyymmdd') = '${gmtdate}'
            )
            group by special_id, goods_no
        ) as a
        left join (
            -- 当天所有商品
            select count(goods_no) as total_goods_num from yishou_data.dim_goods_id_info_full_h
            where to_char(special_date,'yyyymmdd') = '${gmtdate}'
        ) as b
        on 1 = 1
    )
    group by special_id
) madle on madle.special_id = fs.special_id
left join (
    -- sku统计
    select 
        to_char(dgif.special_date,'yyyymmdd') as special_date
        ,dgif.special_id
        ,count(DISTINCT fgk.sku) as sku_num
    from yishou_data.dim_goods_id_info_full_h dgif 
    left join yishou_data.dim_goods_no_info_full_h dgnif on dgif.goods_no = dgnif.goods_no
    left join yishou_data.ods_fmys_goods_sku_view fgk on fgk.goods_no = dgnif.goods_no
    where to_char(dgif.special_date,'yyyymmdd') = '${gmtdate}'
    and dgif.is_on_sale = 1
    group by 1,2
) sku on sku.special_id = fs.special_id and fs.special_date = sku.special_date
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
DISTRIBUTE BY dt
;


-- 小时表
insert overwrite table yishou_daily.dtl_special_goods_label_choose_ht partition(ht)
select
    *
    , concat(dt,hour(current_timestamp)) as ht
from yishou_daily.dtl_special_goods_label_choose_dt
where dt = '${gmtdate}'
DISTRIBUTE BY ht
;
