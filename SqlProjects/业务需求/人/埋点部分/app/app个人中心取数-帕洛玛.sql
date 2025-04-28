-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/11/06 17:00:29 GMT+08:00
-- ******************************************************************** --
-- app个人中心取数-帕洛玛
select 
    dt as 日期
    ,case 
        when click_source = '待付款' then '待付款'
        when click_source = '待发货' then '待发货'
        when click_source = '待收货' then '待收货'
        when click_source = '发货进度' then '发货通知'
        when click_source = '全部订单' then '全部订单'
        when click_source = '售后进度' then '售后进度'
        when click_source = '待评价' then '待评价'
    end as 点击位置
    ,count(DISTINCT user_id) as `点击uv`
from yishou_data.dcl_event_user_center_click_d
where dt between '20240601' and '20241105'
and click_source in ('待付款','待发货','待收货','发货进度','售后进度','全部订单','待评价')
group by 1,2
;