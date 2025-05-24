-- DLI sql
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/05/23 18:01:34 GMT+08:00
-- ******************************************************************** --
-- 商品上架数据货号维度表
-- 按专场日跑数脚本
-- drop table yishou_data.dws_goods_no_action_statistics_sp_dt_0425;
-- create table yishou_data.dws_goods_no_action_statistics_sp_dt_0425
-- as
with exposure_table as (
    -- 曝光数据
    select
        dt
        , goods_no
        -- 全渠道
        , count(distinct user_id) as exposure_uv
        , count(*) as exposure_pv
        -- 搜索渠道
        , count(
            distinct
            case
                when pid = '14' then user_id
            end
        ) as search_exposure_uv
        , count(
            case
                when pid = '14' then user_id
            end
        ) as search_exposure_pv
        -- 分类渠道
        , count(
            distinct
            case
                when pid = '12' then user_id
            end
        ) as classify_exposure_uv
        , count(
            case
                when pid = '12' then user_id
            end
        ) as classify_exposure_pv
        -- 个人中心
        , count(
            distinct
            case
                when pid = '11' then user_id
            end
        ) as per_center_exposure_uv
        , count(
            case
                when pid = '11' then user_id
            end
        ) as per_center_exposure_pv
        -- 购物车
        , count(
            distinct
            case
                when pid = '24' then user_id
            end
        ) as shop_cart_exposure_uv
        , count(
            case
                when pid = '24' then user_id
            end
        ) as shop_cart_exposure_pv
        -- 专场
        , count(
            distinct
            case
                when pid = '10' then user_id
            end
        ) as special_exposure_uv
        , count(
            case
                when pid = '10' then user_id
            end
        ) as special_exposure_pv
        -- 营销频道之今日特卖
        , count(
            distinct
            case
                when pid = '28' then user_id
            end
        ) as today_on_sale_exposure_uv
        , count(
            case
                when pid = '28' then user_id
            end
        ) as today_on_sale_exposure_pv
        -- 营销频道之首发新款
        , count(
            distinct
            case
                when title = '首发新款' then user_id
            end
        ) as first_style_exposure_uv
        , count(
            case
                when title = '首发新款' then user_id
            end
        ) as first_style_exposure_pv
        -- 档口详情
        , count(
            distinct
            case
                when pid in (17,30,31,34,35) then user_id
            end
        ) as stall_page_exposure_uv
        , count(
            case
                when pid in (17,30,31,34,35) then user_id
            end
        ) as stall_page_exposure_pv
    from (
        select
            main_table.dt
            , main_table.goods_no
            , main_table.user_id
            , '' as title
            , pid as pid
        from yishou_data.dcl_event_goods_exposure_d as main_table
        where main_table.dt between '${start}' and '${end}'
        and main_table.goods_id is not null
        union all
        select
            main_table.dt
            , main_table.goods_no
            , main_table.user_id
            , title as title
            , '' as pid
        from yishou_data.dwd_h5_goods_exposure_sp_dt as main_table
        where main_table.dt between '${start}' and '${end}'
        and main_table.goods_id is not null
    )
    group by dt, goods_no
)
insert overwrite table yishou_data.dws_goods_no_action_statistics_sp_dt partition(dt)
select
    main.goods_no as goods_no
    -- 维度属性
    , main.goods_name
    , main.goods_status
    , main.goods_tag
    , main.goods_weight
    , main.goods_desc
    , main.goods_img
    , main.goods_kh
    , main.goods_model_id
    , main.goods_model_setting
    , main.goods_from
    , main.goods_type
    , main.cat_id
    , main.primary_cat_id
    , main.primary_cat_name
    , main.second_cat_id
    , main.second_cat_name
    , main.third_cat_id
    , main.third_cat_name
    , main.style_id
    , main.supply_id
    , main.supply_name
    , main.pg_id
    , main.market_id
    , main.market_name
    , main.second_market_id
    , main.second_market_name
    , main.primary_market_id
    , main.primary_market_name
    , main.big_market_id
    , main.big_market_name
    , main.grade_id
    , main.brand_id
    , main.size_chart_id
    , main.size_chart
    , main.add_admin
    , main.is_on_sale
    , main.market_price
    , main.old_price
    , main.origin
    , main.picker_assist
    , main.picker_group_code
    , main.shop_price
    , main.shoppe_price
    , main.first_sale_period
    , main.is_advance_support
    , main.is_delete
    , main.is_no_reason_support
    , main.is_sole
    , main.is_stop_sale
    , main.off_sale_remark
    , main.is_new_style
    , main.one_hand
    , main.first_add_time
    , main.first_up_time
    , main.first_up_new_time
    , main.first_show_time
    , main.last_up_time
    -- 事实属性
    , coalesce(main.buy_num, 0) as buy_num
    , coalesce(main.real_buy_num, 0) as real_buy_num
    , coalesce(main.buy_user_num, 0) as buy_user_num
    , coalesce(main.real_buy_user_num, 0) as real_buy_user_num
    , coalesce(main.buy_amount, 0) as buy_amount
    , coalesce(main.real_buy_amount, 0) as real_buy_amount
    , coalesce(main.search_buy_num, 0) as search_buy_num
    , coalesce(main.search_real_buy_num, 0) as search_real_buy_num
    , coalesce(main.search_buy_user_num, 0) as search_buy_user_num
    , coalesce(main.search_real_buy_user_num, 0) as search_real_buy_user_num
    , coalesce(main.search_buy_amount, 0) as search_buy_amount
    , coalesce(main.search_real_buy_amount, 0) as search_real_buy_amount
    , coalesce(main.classify_buy_num, 0) as classify_buy_num
    , coalesce(main.classify_real_buy_num, 0) as classify_real_buy_num
    , coalesce(main.classify_buy_user_num, 0) as classify_buy_user_num
    , coalesce(main.classify_real_buy_user_num, 0) as classify_real_buy_user_num
    , coalesce(main.classify_buy_amount, 0) as classify_buy_amount
    , coalesce(main.classify_real_buy_amount, 0) as classify_real_buy_amount
    , coalesce(main.per_center_buy_num, 0) as per_center_buy_num
    , coalesce(main.per_center_real_buy_num, 0) as per_center_real_buy_num
    , coalesce(main.per_center_buy_user_num, 0) as per_center_buy_user_num
    , coalesce(main.per_center_real_buy_user_num, 0) as per_center_real_buy_user_num
    , coalesce(main.per_center_buy_amount, 0) as per_center_buy_amount
    , coalesce(main.per_center_real_buy_amount, 0) as per_center_real_buy_amount
    , coalesce(main.shop_cart_buy_num, 0) as shop_cart_buy_num
    , coalesce(main.shop_cart_real_buy_num, 0) as shop_cart_real_buy_num
    , coalesce(main.shop_cart_buy_user_num, 0) as shop_cart_buy_user_num
    , coalesce(main.shop_cart_real_buy_user_num, 0) as shop_cart_real_buy_user_num
    , coalesce(main.shop_cart_buy_amount, 0) as shop_cart_buy_amount
    , coalesce(main.shop_cart_real_buy_amount, 0) as shop_cart_real_buy_amount
    , coalesce(main.special_buy_num, 0) as special_buy_num
    , coalesce(main.special_real_buy_num, 0) as special_real_buy_num
    , coalesce(main.special_buy_user_num, 0) as special_buy_user_num
    , coalesce(main.special_real_buy_user_num, 0) as special_real_buy_user_num
    , coalesce(main.special_buy_amount, 0) as special_buy_amount
    , coalesce(main.special_real_buy_amount, 0) as special_real_buy_amount
    , coalesce(main.today_on_sale_buy_num, 0) as today_on_sale_buy_num
    , coalesce(main.today_on_sale_real_buy_num, 0) as today_on_sale_real_buy_num
    , coalesce(main.today_on_sale_buy_user_num, 0) as today_on_sale_buy_user_num
    , coalesce(main.today_on_sale_real_buy_user_num, 0) as today_on_sale_real_buy_user_num
    , coalesce(main.today_on_sale_buy_amount, 0) as today_on_sale_buy_amount
    , coalesce(main.today_on_sale_real_buy_amount, 0) as today_on_sale_real_buy_amount
    , coalesce(main.first_style_buy_num, 0) as first_style_buy_num
    , coalesce(main.first_style_real_buy_num, 0) as first_style_real_buy_num
    , coalesce(main.first_style_buy_user_num, 0) as first_style_buy_user_num
    , coalesce(main.first_style_real_buy_user_num, 0) as first_style_real_buy_user_num
    , coalesce(main.first_style_buy_amount, 0) as first_style_buy_amount
    , coalesce(main.first_style_real_buy_amount, 0) as first_style_real_buy_amount
    , coalesce(main.stall_page_buy_num, 0) as stall_page_buy_num
    , coalesce(main.stall_page_real_buy_num, 0) as stall_page_real_buy_num
    , coalesce(main.stall_page_buy_user_num, 0) as stall_page_buy_user_num
    , coalesce(main.stall_page_real_buy_user_num, 0) as stall_page_real_buy_user_num
    , coalesce(main.stall_page_buy_amount, 0) as stall_page_buy_amount
    , coalesce(main.stall_page_real_buy_amount, 0) as stall_page_real_buy_amount
    , coalesce(main.pay_user_num, 0) as pay_user_num
    , coalesce(main.real_pay_user_num, 0) as real_pay_user_num
    , coalesce(exposure_table.exposure_uv, 0) as exposure_uv
    , coalesce(exposure_table.exposure_pv, 0) as exposure_pv
    , coalesce(exposure_table.search_exposure_uv, 0) as search_exposure_uv
    , coalesce(exposure_table.search_exposure_pv, 0) as search_exposure_pv
    , coalesce(exposure_table.classify_exposure_uv, 0) as classify_exposure_uv
    , coalesce(exposure_table.classify_exposure_pv, 0) as classify_exposure_pv
    , coalesce(exposure_table.per_center_exposure_uv, 0) as per_center_exposure_uv
    , coalesce(exposure_table.per_center_exposure_pv, 0) as per_center_exposure_pv
    , coalesce(exposure_table.shop_cart_exposure_uv, 0) as shop_cart_exposure_uv
    , coalesce(exposure_table.shop_cart_exposure_pv, 0) as shop_cart_exposure_pv
    , coalesce(exposure_table.special_exposure_uv, 0) as special_exposure_uv
    , coalesce(exposure_table.special_exposure_pv, 0) as special_exposure_pv
    , coalesce(exposure_table.today_on_sale_exposure_uv, 0) as today_on_sale_exposure_uv
    , coalesce(exposure_table.today_on_sale_exposure_pv, 0) as today_on_sale_exposure_pv
    , coalesce(exposure_table.first_style_exposure_uv, 0) as first_style_exposure_uv
    , coalesce(exposure_table.first_style_exposure_pv, 0) as first_style_exposure_pv
    , coalesce(exposure_table.stall_page_exposure_uv, 0) as stall_page_exposure_uv
    , coalesce(exposure_table.stall_page_exposure_pv, 0) as stall_page_exposure_pv
    , coalesce(main.click_uv, 0) as click_uv
    , coalesce(main.click_pv, 0) as click_pv
    , coalesce(main.search_click_uv, 0) as search_click_uv
    , coalesce(main.search_click_pv, 0) as search_click_pv
    , coalesce(main.classify_click_uv, 0) as classify_click_uv
    , coalesce(main.classify_click_pv, 0) as classify_click_pv
    , coalesce(main.per_center_click_uv, 0) as per_center_click_uv
    , coalesce(main.per_center_click_pv, 0) as per_center_click_pv
    , coalesce(main.shop_cart_click_uv, 0) as shop_cart_click_uv
    , coalesce(main.shop_cart_click_pv, 0) as shop_cart_click_pv
    , coalesce(main.special_click_uv, 0) as special_click_uv
    , coalesce(main.special_click_pv, 0) as special_click_pv
    , coalesce(main.today_on_sale_click_uv, 0) as today_on_sale_click_uv
    , coalesce(main.today_on_sale_click_pv, 0) as today_on_sale_click_pv
    , coalesce(main.first_style_click_uv, 0) as first_style_click_uv
    , coalesce(main.first_style_click_pv, 0) as first_style_click_pv
    , coalesce(main.stall_page_click_uv, 0) as stall_page_click_uv
    , coalesce(main.stall_page_click_pv, 0) as stall_page_click_pv
    , coalesce(main.add_cart_uv, 0) as add_cart_uv
    , coalesce(main.add_cart_pv, 0) as add_cart_pv
    , coalesce(main.add_cart_num, 0) as add_cart_num
    , coalesce(main.add_cart_amount, 0) as add_cart_amount
    , coalesce(main.search_add_cart_uv, 0) as search_add_cart_uv
    , coalesce(main.search_add_cart_pv, 0) as search_add_cart_pv
    , coalesce(main.search_add_cart_num, 0) as search_add_cart_num
    , coalesce(main.search_add_cart_amount, 0) as search_add_cart_amount
    , coalesce(main.classify_add_cart_uv, 0) as classify_add_cart_uv
    , coalesce(main.classify_add_cart_pv, 0) as classify_add_cart_pv
    , coalesce(main.classify_add_cart_num, 0) as classify_add_cart_num
    , coalesce(main.classify_add_cart_amount, 0) as classify_add_cart_amount
    , coalesce(main.per_center_add_cart_uv, 0) as per_center_add_cart_uv
    , coalesce(main.per_center_add_cart_pv, 0) as per_center_add_cart_pv
    , coalesce(main.per_center_add_cart_num, 0) as per_center_add_cart_num
    , coalesce(main.per_center_add_cart_amount, 0) as per_center_add_cart_amount
    , coalesce(main.shop_cart_add_cart_uv, 0) as shop_cart_add_cart_uv
    , coalesce(main.shop_cart_add_cart_pv, 0) as shop_cart_add_cart_pv
    , coalesce(main.shop_cart_add_cart_num, 0) as shop_cart_add_cart_num
    , coalesce(main.shop_cart_add_cart_amount, 0) as shop_cart_add_cart_amount
    , coalesce(main.special_add_cart_uv, 0) as special_add_cart_uv
    , coalesce(main.special_add_cart_pv, 0) as special_add_cart_pv
    , coalesce(main.special_add_cart_num, 0) as special_add_cart_num
    , coalesce(main.special_add_cart_amount, 0) as special_add_cart_amount
    , coalesce(main.today_on_sale_add_cart_uv, 0) as today_on_sale_add_cart_uv
    , coalesce(main.today_on_sale_add_cart_pv, 0) as today_on_sale_add_cart_pv
    , coalesce(main.today_on_sale_add_cart_num, 0) as today_on_sale_add_cart_num
    , coalesce(main.today_on_sale_add_cart_amount, 0) as today_on_sale_add_cart_amount
    , coalesce(main.first_style_add_cart_uv, 0) as first_style_add_cart_uv
    , coalesce(main.first_style_add_cart_pv, 0) as first_style_add_cart_pv
    , coalesce(main.first_style_add_cart_num, 0) as first_style_add_cart_num
    , coalesce(main.first_style_add_cart_amount, 0) as first_style_add_cart_amount
    , coalesce(main.stall_page_add_cart_uv, 0) as stall_page_add_cart_uv
    , coalesce(main.stall_page_add_cart_pv, 0) as stall_page_add_cart_pv
    , coalesce(main.stall_page_add_cart_num, 0) as stall_page_add_cart_num
    , coalesce(main.stall_page_add_cart_amount, 0) as stall_page_add_cart_amount
    , coalesce(main.collect_num, 0) as collect_num
    , coalesce(main.search_collect_num, 0) as search_collect_num
    , coalesce(main.classify_collect_num, 0) as classify_collect_num
    , coalesce(main.per_center_collect_num, 0) as per_center_collect_num
    , coalesce(main.shop_cart_collect_num, 0) as shop_cart_collect_num
    , coalesce(main.special_collect_num, 0) as special_collect_num
    , coalesce(main.today_on_sale_collect_num, 0) as today_on_sale_collect_num
    , coalesce(main.first_style_collect_num, 0) as first_style_collect_num
    , coalesce(main.stall_page_collect_num, 0) as stall_page_collect_num
    , main.dt as dt
from yishou_data.dws_goods_no_action_statistics_sp_dt_backup main
left join exposure_table on main.goods_no = exposure_table.goods_no and main.dt = exposure_table.dt
where main.dt between '${start}' and '${end}'
DISTRIBUTE BY dt
;

-- -- 验数
-- select dt,sum(exposure_uv) exposure_uv from yishou_data.dws_goods_no_action_statistics_sp_dt_0425 where dt = '20250425' group by dt;
-- select dt,sum(exposure_uv) exposure_uv from yishou_data.dws_goods_no_action_statistics_sp_dt_backup where dt = '20250425' group by dt;