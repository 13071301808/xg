-- 口径：https://yb7ao262ru.feishu.cn/wiki/JHQfwOYYcisyCikOlMJcXmWpndd
select 
    pay.special_date
    ,pay.user_id
    ,pay.order_sn
    ,pay.order_status
    ,sum(real_buy_amount) real_buy_amount
    ,sum(real_buy_num) real_buy_num
    ,sum(cancel_buy_num) cancel_buy_num
    ,sum(cancel_buy_amount) cancel_buy_amount
from (
    select 
        to_char(dateadd(pay.add_time,-7,'hh'),'yyyymmdd') as special_date
        ,pay.order_id
        ,pay.user_id
        ,pay.order_sn
        ,case when cr.order_id is not null then '已取消' else '已完成' end as order_status
        ,round(sum(case when pay.is_real_pay = 1 then shop_price * buy_num end),2) as real_buy_amount 
        ,round(sum(case when pay.is_real_pay = 1 then buy_num end),2) as real_buy_num 
        ,round(sum(case when cr.order_id is not null then buy_num end),2) as cancel_buy_num
        ,round(sum(case when cr.order_id is not null then shop_price * buy_num end),2) as cancel_buy_amount
    from yishou_data.dwd_sale_order_info_dt pay 
    left join yishou_data.all_fmys_order_cancel_record cr on pay.order_id = cr.order_id and cancel_type = 1
    where pay.dt between '20240826' and '20250426'
    and to_char(dateadd(pay.add_time,-7,'hh'),'yyyymmdd') between '20240826' and '20250426'
    group by 1,2,3,4,5
) pay
left join (
    select 
        to_char(from_unixtime(time-25200),'yyyymmdd') special_date
        ,user_id
        ,order_sn
    from yishou_data.dcl_event_cancel_order_detail_lock_exposure_d 
    where dt between '20240826' and '20250427'
    and to_char(from_unixtime(time-25200),'yyyymmdd') between '20240826' and '20250426'
    group by 1,2,3
) suo 
on suo.user_id = pay.user_id and suo.order_sn = pay.order_sn and suo.special_date = pay.special_date
where suo.user_id is not null and suo.order_sn is not null and suo.special_date is not null
group by 1,2,3,4
;