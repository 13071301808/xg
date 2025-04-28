-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/09/10 20:37:18 GMT+08:00
-- ******************************************************************** --
-- 口径
-- 单订单内对单个档口支付金额>=1000元，全部购买退货卡
-- 条件1：单订单内对单个档口支付金额>=1000元
-- 条件2：条件1内命中的商品全部购买退货卡
-- 预警文案格式：
-- 用户ID:，提交订单ID：，金额**元，商品件数**件；
-- 商家id：，商家名称：，金额**元，商品件数**件，全部购买退货卡

drop table if exists yishou_daily.temp_supply_refund_03;
create table yishou_daily.temp_supply_refund_03 
as
select
    fo.user_id
    ,fo.order_id
    ,fgl.supply_id
    ,su.supply_name
    ,round(sum(coalesce(foi.order_buy_num,rioi.refund_buy_num)),2) as goods_buy_num
    ,round(sum(foi.order_amount_price),2) as order_amount_price
from yishou_data.all_fmys_order_h fo
join (
    select 
        order_id
        ,goods_id
        ,goods_no
        ,sum(buy_num) as order_buy_num
        ,sum(buy_num * shop_price) as order_amount_price
    from yishou_data.all_fmys_order_infos_h
    group by 1,2,3
)foi on fo.order_id = foi.order_id
left join yishou_data.ods_fmys_goods_h fg on foi.goods_id = fg.goods_id
left join yishou_data.all_fmys_goods_lib_h fgl on foi.goods_no = fgl.goods_no
left join (
    select 
        order_id
        ,sum(buy_num) as refund_buy_num
    from yishou_data.ods_fmys_refund_insurance_order_infos_view
    group by order_id
) rioi on fo.order_id = rioi.order_id
left join yishou_data.dim_supply_info su on fgl.supply_id = su.supply_id
where fo.user_id not in (2, 10, 17, 387836) and fo.pay_status = 1 -- 有支付行为
-- 取1小时
and from_unixtime(fo.pay_time) >= date_format(dateadd('${todate_date}', -1 , 'HH'),'yyyy-MM-dd HH:00:00')
and from_unixtime(fo.pay_time) <  date_format(dateadd('${todate_date}',  0 , 'HH'),'yyyy-MM-dd HH:00:00')
-- 全部购买退货卡
and foi.order_buy_num = rioi.refund_buy_num
group by 1,2,3,4
having order_amount_price >= 1000
;

