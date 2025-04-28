with ex as (
    select 
        to_date1(regexp_replace(ex.special_date,'-',''),'yyyymmdd') special_date  
        ,ex.user_id ex_user_id 
        ,ex.first_page_name
        ,case 
            when substring(ex.first_home_index,1,2) in ('BL') then '首页市场下方banner' 
            when substring(ex.first_home_index,1,2) in ('BH') then '首页顶部图标'
            else ex.second_page_name
        end as second_page_name1
        ,ex.dt
        ,count(distinct ex.goods_id) goods_exposure_uv
        ,count(ex.user_id) as user_exposure_pv
    from yishou_daily.route_all_event_exposure_detail_v2 ex 
    where ex.dt between '20240618' and '20240918'
    -- where ex.dt = '20240918'
    and first_page_name = '首页' 
    group by 1,2,3,4,5
),
cl as (
    select 
        to_date1(regexp_replace(cl.special_date,'-',''),'yyyymmdd') special_date  
        ,cl.user_id cl_user_id
        ,cl.first_page_name
        ,case 
            when substring(cl.first_home_index,1,2) in ('BL') then '首页市场下方banner' 
            when substring(cl.first_home_index,1,2) in ('BH') then '首页顶部图标'
            else cl.second_page_name
        end as second_page_name1
        ,cl.dt 
        ,count(distinct cl.goods_id) goods_click_uv
        -- ,count(distinct cl.user_id) as user_click_uv
    from yishou_daily.route_all_event_click_add_cart_detail_v2 cl 
    where cl.dt between '20240618' and '20240918'
    -- where cl.dt = '20240918'
    and first_page_name = '首页' 
    and page_name = '商品详情'
    group by 1,2,3,4,5
),
ad as (
    select 
        to_date1(regexp_replace(ad.special_date,'-',''),'yyyymmdd') special_date
        ,ad.user_id ad_user_id
        ,ad.first_page_name
        ,case 
            when substring(ad.first_home_index,1,2) in ('BL') then '首页市场下方banner' 
            when substring(ad.first_home_index,1,2) in ('BH') then '首页顶部图标'
            else ad.second_page_name
        end as second_page_name1
        ,ad.dt
        ,count(distinct ad.goods_id)  goods_add_uv
        ,count(distinct case when is_join_add_cart = 1 then ad.goods_id end)  join_goods_add_uv
        ,sum(add_cart_num) add_cart_num
        ,sum(add_cart_amount) add_cart_amount
        ,count(add_cart_id) add_cart_pv
    from yishou_daily.route_all_event_click_add_cart_detail_v2 ad 
    where ad.dt between '20240618' and '20240918'
    -- where ad.dt = '20240918'
    and first_page_name = '首页' 
    and page_name = '加购'
    group by 1,2,3,4,5
),
pa as (
    select 
        to_date1(regexp_replace(pa.special_date,'-',''),'yyyymmdd') special_date
        ,pa.user_id pa_user_id
        ,pa.first_page_name
        ,case 
            when substring(pa.first_home_index,1,2) in ('BL') then '首页市场下方banner' 
            when substring(pa.first_home_index,1,2) in ('BH') then '首页顶部图标'
            else pa.second_page_name
        end as second_page_name1
        ,pa.dt 
        ,count(distinct pa.goods_id) goods_pay_uv
        ,sum(gmv) gmv
        ,sum(buy_num) buy_num  
        ,sum(real_gmv) real_gmv
        ,sum(real_buy_num) real_buy_num  
        ,count(distinct case when real_buy_num > 0 then pa.goods_id end ) real_goods_pay_uv
    from yishou_daily.route_all_event_gmv_detail_v2 pa 
    where pa.dt between '20240618' and '20240918'
    -- where pa.dt = '20240918'
    and first_page_name = '首页' 
    group by 1,2,3,4,5
),
t1 as (
    select 
        coalesce(ex.special_date,cl.special_date,ad.special_date,pa.special_date) special_date
        ,coalesce(ex.ex_user_id,cl.cl_user_id,ad.ad_user_id,pa.pa_user_id) user_id
        ,coalesce(ex.first_page_name,cl.first_page_name,ad.first_page_name,pa.first_page_name) first_page_name 
        ,coalesce(ex.second_page_name1,cl.second_page_name1,ad.second_page_name1,pa.second_page_name1) second_page_name 
        ,coalesce(ex.dt,cl.dt,ad.dt,pa.dt) dt
        ,sum(ex.user_exposure_pv) as user_exposure_pv
        ,sum(goods_exposure_uv) goods_exposure_uv
        ,sum(goods_click_uv) goods_click_uv
        ,sum(goods_add_uv) goods_add_uv
        ,sum(goods_pay_uv) goods_pay_uv
        ,sum(gmv) gmv
        ,sum(buy_num) buy_num 
        ,sum(join_goods_add_uv)  join_goods_add_uv
        ,sum(add_cart_num) add_cart_num
        ,sum(add_cart_amount) add_cart_amount
        ,sum(real_gmv) real_gmv
        ,sum(real_buy_num) real_buy_num  
        ,sum(add_cart_pv) add_cart_pv
        ,sum(real_goods_pay_uv) real_goods_pay_uv
    from ex 
    full join cl on ex.ex_user_id = cl.cl_user_id 
        and ex.special_date = cl.special_date 
        and ex.first_page_name =cl.first_page_name 
        and ex.second_page_name1 = cl.second_page_name1 
        and ex.dt = cl.dt 
    full join ad 
        on coalesce(ex.ex_user_id,cl.cl_user_id) = ad.ad_user_id 
        and coalesce(ex.special_date,cl.special_date) = ad.special_date 
        and coalesce(ex.first_page_name,cl.first_page_name) =ad.first_page_name 
        and coalesce(ex.second_page_name1,cl.second_page_name1) = ad.second_page_name1 
        and coalesce(ex.dt,cl.dt) = ad.dt 
    full join pa 
        on coalesce(ex.ex_user_id,cl.cl_user_id, ad.ad_user_id ) = pa.pa_user_id 
        and coalesce(ex.special_date,cl.special_date, ad.special_date ) = pa.special_date 
        and coalesce(ex.first_page_name,cl.first_page_name, ad.first_page_name ) = pa.first_page_name 
        and coalesce(ex.second_page_name1,cl.second_page_name1, ad.second_page_name1 ) = pa.second_page_name1 
        and coalesce(ex.dt,cl.dt, ad.dt ) = pa.dt 
    group by 1,2,3,4,5
)
SELECT 
    t1.special_date as 专场日期
    ,t1.second_page_name as 二级页面
    ,count(DISTINCT case when t1.goods_exposure_uv >0 then t1.user_id end) as 曝光用户数
    ,count(DISTINCT case when t1.goods_click_uv >0 then t1.user_id end) as 点击用户数
    ,count(DISTINCT case when t1.goods_add_uv >0 then t1.user_id end) as 加购用户数
    ,count(DISTINCT case when t1.goods_pay_uv > 0 then t1.user_id end) as 支付用户数
    ,sum(t1.goods_exposure_uv) 商品曝光uv
    ,sum(t1.goods_click_uv) 商品点击uv
    ,sum(t1.goods_add_uv) 商品加购uv
    ,sum(t1.add_cart_pv) 商品加购次数
    ,sum(t1.goods_pay_uv) 商品支付uv
    ,sum(t1.add_cart_amount) 加购金额
    ,sum(t1.add_cart_num) 加购件数
    ,sum(t1.gmv) 销售额
    ,sum(t1.buy_num) 销售件数
    ,SUM(t1.gmv)/SUM(t1.goods_exposure_uv) as UV价值
    ,sum(t1.real_gmv) 实际销售额
    ,sum(t1.real_buy_num) 实际销售件数  
from t1
where t1.dt between '20240618' and '20240918' and t1.user_id is not null
-- where t1.dt = '20240918' and t1.user_id is not null
group by t1.special_date,t1.second_page_name
;
