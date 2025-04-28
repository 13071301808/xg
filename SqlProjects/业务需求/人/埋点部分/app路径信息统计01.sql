with ex as (
    select 
        ex.dt
        ,ex.first_page_name
        ,ex.second_page_name
        ,ex.third_page_name
        ,count(ex.user_id) as exposure_pv
        ,count(DISTINCT ex.user_id) as exposure_uv
    from yishou_daily.route_all_event_exposure_detail_v2 ex 
    where ex.dt between '20240912' and '20240919'
    group by 1,2,3,4
),
cl as (
    select
        cl.dt
        ,cl.first_page_name
        ,cl.second_page_name
        ,cl.third_page_name
        ,count(distinct cl.user_id) click_uv
        ,count(cl.user_id) as click_pv
    from yishou_daily.route_all_event_click_add_cart_detail_v2 cl 
    where cl.dt between '20240912' and '20240919'
    and page_name = '商品详情'
    group by 1,2,3,4
)
select 
    coalesce(ex.dt,cl.dt) as dt
    ,coalesce(ex.first_page_name,cl.first_page_name) first_page_name 
    ,coalesce(ex.second_page_name,cl.second_page_name) second_page_name 
    ,coalesce(ex.third_page_name,cl.third_page_name) third_page_name 
    ,sum(ex.exposure_pv) as exposure_pv
    ,sum(ex.exposure_uv) as exposure_uv
    ,sum(cl.click_uv) as click_uv
    ,sum(cl.click_pv) as click_pv
from ex 
full join cl 
on ex.first_page_name =cl.first_page_name 
and ex.second_page_name = cl.second_page_name
and ex.third_page_name = cl.third_page_name
and ex.dt = cl.dt 
group by 1,2,3,4