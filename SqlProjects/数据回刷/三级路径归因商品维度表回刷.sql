-- DLI sql
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/05/23 11:40:23 GMT+08:00
-- ******************************************************************** --
-- 一二三级页面常用商品指标
with ex as (
    select
        to_date1(regexp_replace(ex.special_date,'-',''),'yyyymmdd') special_date
        ,ex.goods_id
        ,ex.first_page_name
        ,ex.second_page_name
        ,ex.third_page_name
        ,count(distinct ex.user_id,ex.goods_id) as goods_exposure_uv
    from yishou_daily.route_all_event_exposure_detail_v2 ex
    where ex.dt between '${start}' and to_char(dateadd(to_date1('${end}','yyyymmdd'),1,'dd'),'yyyymmdd')
    group by 1,2,3,4,5
)
INSERT OVERWRITE TABLE yishou_daily.third_page_name_goods_dt partition(dt)
SELECT
    main.special_date
    , main.goods_id
    , main.special_id
    , main.goods_no
    , main.supply_id
    , main.supply_name
    , main.big_market
    , main.up_type
    , main.third_cat_name
    , main.pgm_name
    , main.is_new
    , main.department
    , main.goods_season
    , main.is_in_stock
    , main.is_bargain
    , main.grade_name
    , main.price_section
    , main.salenum_type
    , main.kol_class
    , main.first_page_name
    , main.second_page_name
    , main.third_page_name
    , main.special_show
    , main.special_name
    , main.special_style_type_name
    , main.goods_no_status
    , main.market_price
    , main.shop_price
    , ex.goods_exposure_uv
    , main.goods_click_uv
    , main.goods_add_uv
    , main.buy_user_num
    , main.real_buy_user_num
    , main.buy_amount
    , main.real_buy_amount
    , main.buy_num
    , main.real_buy_num
    , main.real_buy_num_all
    , main.is_bao
    , main.sale_age
    , main.is_live
    , main.is_platform
    , main.dt
from yishou_daily.third_page_name_goods_dt_backup1 main
left join ex on ex.special_date = main.special_date and ex.goods_id = main.goods_id
and ex.first_page_name = main.first_page_name and ex.second_page_name = main.second_page_name
and ex.third_page_name = main.third_page_name
where main.dt between '${start}' and '${end}'
DISTRIBUTE BY dt
;

-- select * from yishou_daily.third_page_name_goods_dt where dt = '20250522' and first_page_name = '首页' limit 10;



