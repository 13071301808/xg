-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/09/11 14:29:36 GMT+08:00
-- ******************************************************************** --
-- 规则五
-- 收件信息中电话号码与人工归属的退货黑名单用户一致
-- 条件1：下单账号与退货黑名单用户，收件信息中电话号码一致
-- 命中的商品全部购买退货卡，且订单购买的档口不超过2间
-- 条件2：订单金额>=1000元

-- 预警文案格式：
-- 黑名单小号--用户ID:，提交订单ID：，金额**元，商品件数**件；全部购买退货卡；关联用户ID：
drop table if exists yishou_daily.temp_supply_refund_05;
create table yishou_daily.temp_supply_refund_05
with t1 as (
    select 
        fo1.user_id
        ,fo.order_id
        ,fgrib.user_id as black_user_id
    -- 取出下单账号与退货黑名单为同一个用户的信息
    from yishou_data.ods_fmys_goods_refund_insurance_blacklist_view fgrib
    left join yishou_data.ods_fmys_order_h fo on fgrib.user_id = fo.user_id
    -- 取出下单账号与退货黑名单电话号码一致的信息
    left join yishou_data.ods_fmys_order_h fo1 on fo.mobile_encrypt = fo1.mobile_encrypt
    left join yishou_data.all_fmys_admin_h ad on fgrib.create_admin = ad.admin_id
    -- where from_unixtime(fo.pay_time) >= '2024-11-25 00:00:00'
    where from_unixtime(fo.pay_time) >= date_format(dateadd('${todate_date}', -1 , 'HH'),'yyyy-MM-dd HH:00:00')
    and from_unixtime(fo.pay_time) <  date_format(dateadd('${todate_date}',  0 , 'HH'),'yyyy-MM-dd HH:00:00')
    and ad.admin_name <> '系统'
    and fo1.user_id not in (2, 10, 17, 387836) and fo1.pay_status = 1 -- 有支付行为
    group by 1,2,3
)
select 
    t1.user_id
    ,t1.order_id
    ,t1.black_user_id
    ,round(sum(foi.buy_amount),2) as buy_amount
    ,sum(foi.goods_buy_num) as goods_buy_num
    ,count(DISTINCT fgl.supply_id) as buy_supply
from t1 
join (
    select 
        order_id
        ,goods_no
        ,sum(buy_num) as goods_buy_num
        ,round(sum(buy_num * shop_price),2) as buy_amount
    from yishou_data.all_fmys_order_infos_h
    group by order_id,goods_no
) foi on t1.order_id = foi.order_id
-- 供应商
left join yishou_data.all_fmys_goods_lib_h fgl on foi.goods_no = fgl.goods_no
-- 退货卡
left join (
    select 
        order_id
        ,sum(buy_num) as refund_buy_num
    from yishou_data.ods_fmys_refund_insurance_order_infos_view
    group by order_id
) rioi on foi.order_id = rioi.order_id
where foi.goods_buy_num = rioi.refund_buy_num
group by 1,2,3
HAVING buy_amount >= 1000 and buy_supply <= 2
;

