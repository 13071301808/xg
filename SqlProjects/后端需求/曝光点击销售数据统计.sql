-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/10/10 17:19:26 GMT+08:00
-- ******************************************************************** --
-- 跑数脚本

with

app_goods_exposure as (
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
    where dt between ${one_day_ago} and ${today}
    and from_unixtime((receive_time - 25200), 'yyyyMMdd') = from_unixtime(unix_timestamp() - 25200, 'yyyyMMdd')
    union ALL 
    select
        0 as channel_id
        ,user_id
        ,goods_id
    from yishou_data.dwd_app_goods_exposure_view
    where dt between ${one_day_ago} and ${today}
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
    where dt between ${one_day_ago} and ${today}
    and from_unixtime((receive_time - 25200), 'yyyyMMdd') = from_unixtime(unix_timestamp() - 25200, 'yyyyMMdd')
    union all
    select
        0 as channel_id
        ,user_id
        ,goods_id
    from yishou_data.dwd_h5_goods_exposure_view
    where dt between ${one_day_ago} and ${today}
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
    where dt between ${one_day_ago} and ${today}
    and from_unixtime((receive_time - 25200), 'yyyyMMdd') = from_unixtime(unix_timestamp() - 25200, 'yyyyMMdd')
    union ALL 
    select
        0 as channel_id
        ,user_id
        ,goods_id
    from yishou_data.dwd_app_goods_click_view
    where dt between ${one_day_ago} and ${today}
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
    where dt between ${one_day_ago} and ${today}
    and from_unixtime((receive_time - 25200), 'yyyyMMdd') = from_unixtime(unix_timestamp() - 25200, 'yyyyMMdd')
    union ALL 
    select
        0 as channel_id
        ,user_id
        ,goods_id
    from yishou_data.dwd_app_goods_add_cart_view
    where dt between ${one_day_ago} and ${today}
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
    where dt between ${one_day_ago} and ${today}
    and from_unixtime((receive_time - 25200), 'yyyyMMdd') = from_unixtime(unix_timestamp() - 25200, 'yyyyMMdd')
    union ALL 
    select
        0 as channel_id
        ,user_id
        ,goods_id
    from yishou_data.dwd_h5_goods_click_view
    where dt between ${one_day_ago} and ${today}
    and from_unixtime((receive_time - 25200), 'yyyyMMdd') = from_unixtime(unix_timestamp() - 25200, 'yyyyMMdd')
)
,flow_table as (
    -- 流量数据统计
    select
        coalesce(exposure_table.channel_id,click_table.channel_id,add_cart_table.channel_id) as channel_id
        , coalesce(exposure_table.goods_no, click_table.goods_no) as goods_no
        , coalesce(exposure_table.exposure_pv, 0) as exposure_pv
        , coalesce(exposure_table.exposure_uv, 0) as exposure_uv
        , coalesce(click_table.click_pv, 0) as click_pv
        , coalesce(click_table.click_uv, 0) as click_uv
        , coalesce(add_cart_table.add_cart_pv, 0) as add_cart_pv
        , coalesce(add_cart_table.add_cart_uv, 0) as add_cart_uv
    from (
        select
            main_table.channel_id
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
        group by main_table.channel_id,goods_id_info.goods_no
    ) as exposure_table
    full join (
        select
            main_table.channel_id
            ,goods_id_info.goods_no
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
        group by main_table.channel_id,goods_id_info.goods_no
    ) as click_table 
    on exposure_table.goods_no = click_table.goods_no and exposure_table.channel_id = click_table.channel_id
    full join (
        select
            main_table.channel_id
            ,goods_id_info.goods_no
            , count(main_table.user_id) add_cart_pv
            , count(distinct main_table.user_id) add_cart_uv
        from goods_add_cart as main_table
        left join yishou_data.dwd_fmys_goods_in_stock_view as goods_in_stock
        on main_table.goods_id = goods_in_stock.goods_id
        left join yishou_data.dim_goods_id_info_full_h as goods_id_info
        on main_table.goods_id = goods_id_info.goods_id
        and goods_id_info.is_tail_dscn_goods = '否'  -- 2024.01.05去除尾货折销
        where goods_in_stock.goods_id is null
        group by main_table.channel_id,goods_id_info.goods_no
    ) as add_cart_table
    on coalesce(exposure_table.goods_no, click_table.goods_no) = add_cart_table.goods_no 
    and coalesce(exposure_table.channel_id,click_table.channel_id) = add_cart_table.channel_id
)
,order_table as (
    -- 订单数据统计
    select
        main_table.sku_id
        , main_table.goods_id
        , main_table.goods_no
        , main_table.real_buy_user_num
        , main_table.real_buy_num
        , goods_id_info.shop_price
        , goods_id_info.market_price
        , main_table.real_buy_num * goods_id_info.market_price as real_sale_amount
        , goods_id_info.supply_id
        , goods_id_info.goods_kh
        , goods_sku_info.co_val
        , goods_sku_info.si_val
        , main_table.add_time
    from (
        select
            sku as sku_id
            , goods_id as goods_id
            , goods_no as goods_no
            , count(distinct user_id) as real_buy_user_num
            , sum(buy_num) as real_buy_num 
            , from_unixtime(min(add_time), 'yyyy-MM-dd HH:mm:ss') as add_time
        from yishou_data.dwd_order_infos_view
        where dt between ${one_day_ago} and ${today}
        and from_unixtime((create_time - 25200), 'yyyyMMdd') = from_unixtime(unix_timestamp() - 25200, 'yyyyMMdd')
        and order_status != 3
        and pay_status = 1
        group by sku, goods_id, goods_no
    ) as main_table
    left join yishou_data.dwd_fmys_goods_in_stock_view as goods_in_stock
    on main_table.goods_id = goods_in_stock.goods_id
    left join yishou_data.dim_goods_id_info_full_h as goods_id_info
    on main_table.goods_id = goods_id_info.goods_id
    and goods_id_info.is_tail_dscn_goods = '否'  -- 2024.01.05去除尾货折销
    left join yishou_data.dim_goods_sku_info_view as goods_sku_info
    on main_table.sku_id = goods_sku_info.sku_id
    where goods_in_stock.goods_id is null
)

-- 数据合并，添加维度，并写入到对应的结果表中
insert overwrite table yishou_daily.ads_actual_goods_info_mysql
select
    main_table.sku_id
    , main_table.goods_id
    , main_table.goods_no
    , goods_no_info.goods_kh
    , goods_no_info.supply_id
    , main_table.co_val
    , main_table.si_val
    , coalesce(main_table.click_uv, 0) as click_uv
    , coalesce(main_table.exposure_uv, 0) as exposure_uv
    , coalesce(main_table.real_buy_user_num, 0) as real_buy_user_num
    , coalesce(main_table.real_buy_num, 0) as real_buy_num
    , main_table.shop_price
    , main_table.market_price
    , main_table.real_sale_amount
    , TO_CHAR(DATEADD(to_date1(from_unixtime(unix_timestamp() - 25200, 'yyyyMMdd'),'yyyymmdd'),7,"hh"),"yyyy-mm-dd hh:mi:ss") as start_time
    , TO_CHAR(DATEADD(to_date1(from_unixtime(unix_timestamp() - 25200, 'yyyyMMdd'),'yyyymmdd'),31,"hh"),"yyyy-mm-dd hh:mi:ss") as end_time
    , from_unixtime(unix_timestamp() - 25200, 'yyyyMMdd') as special_date
    , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
    , main_table.add_time
    , 0 as is_stock
    , case
        when to_char(goods_no_info.first_up_new_time, 'yyyymmdd') = from_unixtime(unix_timestamp() - 25200, 'yyyyMMdd') then 1
        else 0
     end as is_new
    , coalesce(main_table.add_buy_num, 0) as add_buy_num
    , main_table.channel_id
from (
    select
        order_table.sku_id
        , order_table.goods_id
        , coalesce(order_table.goods_no, flow_table.goods_no) as goods_no
        , order_table.real_buy_user_num
        , order_table.real_buy_num
        , order_table.shop_price
        , order_table.market_price
        , order_table.real_sale_amount
        , order_table.supply_id
        , order_table.goods_kh
        , order_table.co_val
        , order_table.si_val
        , flow_table.channel_id
        , flow_table.exposure_pv
        , flow_table.exposure_uv
        , flow_table.click_pv
        , flow_table.click_uv
        , flow_table.add_cart_uv as add_buy_num
        , order_table.add_time
    from order_table
    full join flow_table
    on order_table.goods_no = flow_table.goods_no
) as main_table
left join yishou_data.dim_goods_no_info_full_h as goods_no_info
on main_table.goods_no = goods_no_info.goods_no
DISTRIBUTE BY floor(rand()*10)
;
