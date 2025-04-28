-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/09/11 10:22:50 GMT+08:00
-- ******************************************************************** --
-- 口径
-- 规则四
-- 单个档口一个专场日内，销售金额大于3000元且退货卡购买率>=50%
-- 条件1：专场日内，某商家id所售商品退货卡购买率>=50%
-- 条件2：专场日内，某商家id所售商品金额>=3000元
-- 预警文案格式：
-- 商家ID:，商家名称：专场日期：，销售金额**元，商品件数**件；退货卡购买率**%，涉及**件商品

drop table if exists yishou_daily.temp_supply_refund_04;
create table yishou_daily.temp_supply_refund_04
as
select
    fgl.supply_id
    ,su.supply_name
    ,to_char(from_unixtime(fs.special_start_time),'yyyymmdd') as special_date
    ,round(sum(foi.order_amount_price),2) as order_amount_price
    ,round(sum(coalesce(foi.goods_buy_num,0)),2) as goods_buy_num
    ,concat(round((sum(coalesce(rioi.refund_buy_num,0)) / sum(coalesce(foi.goods_buy_num,0))*100),2),'%') as refund_rate
    ,round(sum(coalesce(rioi.refund_buy_num,0)),2) as refund_buy_num
from yishou_data.ods_fmys_order_h fo
join (
    select 
        special_id
        ,order_id
        ,goods_id
        ,goods_no
        ,sum(buy_num) as goods_buy_num
        ,sum(buy_num * shop_price) as order_amount_price
    from yishou_data.all_fmys_order_infos_h
    group by 1,2,3,4
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
left join yishou_data.all_fmys_special_h fs on foi.special_id = fs.special_id
where fo.user_id not in (2, 10, 17, 387836) and fo.pay_status = 1 -- 有支付行为
-- 取1小时
-- and from_unixtime(fo.pay_time) >= '2024-11-25 00:00:00'
and from_unixtime(fo.pay_time) >= date_format(dateadd('${todate_date}', -1 , 'HH'),'yyyy-MM-dd HH:00:00')
and from_unixtime(fo.pay_time) <  date_format(dateadd('${todate_date}',  0 , 'HH'),'yyyy-MM-dd HH:00:00')
group by 1,2,3
having order_amount_price >= 3000 
and refund_rate >= 0.5
;