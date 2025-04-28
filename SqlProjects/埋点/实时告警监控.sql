-- CREATE EXTERNAL TABLE yishou_daily.ads_log_monitor_infos_ht (
--     `reg_user_num` string COMMENT '注册用户数',
--     `login_dau` string COMMENT '登录DAU',
--     `app_goods_exposure_uv` string  COMMENT 'app曝光uv',
--     `h5_goods_exposure_uv` string COMMENT 'h5曝光uv',
--     `app_goods_click_uv` string COMMENT 'app点击uv',
--     `h5_goods_click_uv` string COMMENT 'h5点击uv',
--     `app_add_cart_user_num` string COMMENT 'app加购人数',
--     `wx_add_cart_user_num` string COMMENT '小程序加购人数',
--     `buy_amount` string COMMENT '实际gmv',
--     `buy_num` string COMMENT '实际购买件数',
--     `dt` string COMMENT '日期'
-- )
-- comment '通用实时埋点监控告警'
-- partitioned by (ht string)
-- STORED AS ORC LOCATION 'obs://yishou-bigdata/yishou_daily.db/ads_log_monitor_infos_ht'
-- ;

insert overwrite table yishou_daily.ads_log_monitor_infos_ht PARTITION(ht)
select 
    reg.reg_user_num
    ,login.login_dau
    ,app_goods_exposure.goods_exposure_uv as app_goods_exposure_uv
    ,h5_goods_exposure.goods_exposure_uv as h5_goods_exposure_uv
    ,app_goods_click.goods_click_uv as app_goods_click_uv
    ,h5_goods_click.goods_click_uv as h5_goods_click_uv
    ,app_add_cart.add_cart_user_num as app_add_cart_user_num
    ,wx_add_cart.add_cart_user_num as wx_add_cart_user_num
    ,order_info.buy_amount
    ,order_info.buy_num
    ,'${gmtdate}' as dt
    ,FROM_UNIXTIME(UNIX_TIMESTAMP('${today_time}', 'yyyyMMddHHmmss') - 3600, 'yyyyMMddHH') as ht
from (
    -- 注册用户数
    select 
        count(DISTINCT user_id) as reg_user_num 
    from yishou_data.ods_fmys_users_view
    where from_unixtime(reg_time) >= date_format(dateadd('${todate_date}',-1,'HH'),'yyyy-MM-dd HH:00:00') 
    and from_unixtime(reg_time) < date_format(dateadd('${todate_date}',0,'HH'),'yyyy-MM-dd HH:00:00')
) reg
left join (
    -- 登录DAU
    select 
        count(DISTINCT user_id) as login_dau 
    from yishou_data.dwd_log_app_wx_login_h_dt 
    where dt = '${gmtdate}' 
    and time >= date_format(dateadd('${todate_date}',-1,'HH'),'yyyy-MM-dd HH:00:00') 
    and time < date_format(dateadd('${todate_date}',0,'HH'),'yyyy-MM-dd HH:00:00')
) login on 1=1
left join (
    -- app商品曝光uv
    select 
        count(DISTINCT user_id) as goods_exposure_uv
    from yishou_data.dwd_app_goods_exposure_view
    where dt = '${gmtdate}' 
    and from_unixtime(report_time) >= date_format(dateadd('${todate_date}',-1,'HH'),'yyyy-MM-dd HH:00:00') 
    and from_unixtime(report_time) < date_format(dateadd('${todate_date}',0,'HH'),'yyyy-MM-dd HH:00:00')
) app_goods_exposure on 1=1
left join (
    -- h5商品曝光uv
    select 
        count(DISTINCT user_id) as goods_exposure_uv
    from yishou_data.dwd_h5_goods_exposure_view
    where dt = '${gmtdate}' 
    and from_unixtime(time) >= date_format(dateadd('${todate_date}',-1,'HH'),'yyyy-MM-dd HH:00:00') 
    and from_unixtime(time) < date_format(dateadd('${todate_date}',0,'HH'),'yyyy-MM-dd HH:00:00')
) h5_goods_exposure on 1=1
left join (
    -- app商品点击uv
    select 
        count(DISTINCT user_id) as goods_click_uv
    from yishou_data.dwd_app_goods_click_view
    where dt = '${gmtdate}' 
    and from_unixtime(report_time) >= date_format(dateadd('${todate_date}',-1,'HH'),'yyyy-MM-dd HH:00:00') 
    and from_unixtime(report_time) < date_format(dateadd('${todate_date}',0,'HH'),'yyyy-MM-dd HH:00:00')
) app_goods_click on 1=1
left join (
    -- h5商品点击uv
    select 
        count(DISTINCT user_id) as goods_click_uv
    from yishou_data.dwd_h5_goods_click_view
    where dt = '${gmtdate}' 
    and from_unixtime(time) >= date_format(dateadd('${todate_date}',-1,'HH'),'yyyy-MM-dd HH:00:00') 
    and from_unixtime(time) < date_format(dateadd('${todate_date}',0,'HH'),'yyyy-MM-dd HH:00:00')
) h5_goods_click on 1=1
left join (
    -- app加购用户数
    select 
        count(DISTINCT user_id) as add_cart_user_num 
    from yishou_data.dcl_event_add_cart_d
    where dt = '${gmtdate}' 
    and from_unixtime(time / 1000) >= date_format(dateadd('${todate_date}',-1,'HH'),'yyyy-MM-dd HH:00:00') 
    and from_unixtime(time / 1000) < date_format(dateadd('${todate_date}',0,'HH'),'yyyy-MM-dd HH:00:00')
) app_add_cart on 1=1
left join (
    -- 小程序加购用户数
    select 
        count(DISTINCT user_id) as add_cart_user_num 
    from yishou_data.dcl_wx_add_cart_click_d
    where dt = '${gmtdate}' 
    and from_unixtime(time / 1000) >= date_format(dateadd('${todate_date}',-1,'HH'),'yyyy-MM-dd HH:00:00') 
    and from_unixtime(time / 1000) < date_format(dateadd('${todate_date}',0,'HH'),'yyyy-MM-dd HH:00:00')
) wx_add_cart on 1=1
left join (
    -- 订单
    select 
        round(sum(if(fo.pay_status=1,foi.buy_num * foi.shop_price,0)),2) buy_amount
        ,sum(foi.buy_num) as buy_num
    from yishou_data.all_fmys_order_h fo
    join yishou_data.all_fmys_order_infos_h foi on fo.order_id = foi.order_id
    left join yishou_data.all_fmys_order_cancel_record_h cr on fo.order_id = cr.order_id and cr.cancel_type = 1
    where from_unixtime(fo.add_time) >= date_format(dateadd('${todate_date}',-1,'HH'),'yyyy-MM-dd HH:00:00') 
    and from_unixtime(fo.add_time) < date_format(dateadd('${todate_date}',0,'HH'),'yyyy-MM-dd HH:00:00')
    and (case when fo.pay_status = 1 and cr.order_id is null then 1 else 0 end) = 1
)order_info on 1=1
DISTRIBUTE BY ht
;

