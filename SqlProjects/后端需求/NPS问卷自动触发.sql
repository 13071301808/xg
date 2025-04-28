-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/11/13 10:21:38 GMT+08:00
-- 口径逻辑：http://prd.yishou.com/newOS/cd5723/#id=62hn5z&p=nps%E9%97%AE%E5%8D%B7%E8%87%AA%E5%8A%A8%E8%A7%A6%E5%8F%91&g=1
-- ******************************************************************** --
-- 表结构
-- CREATE EXTERNAL TABLE yishou_data.ads_data_nps_auto_rules (
--   `user_id` bigint COMMENT '用户ID',
--   `auth_unplaced_order_days` bigint COMMENT '店主认证成功后内未下单天数',
--   `login_unplaced_order_days` bigint COMMENT '连续登录且未下单天数',
--   `unplaced_order_days` bigint COMMENT '连续专场日未下有效单天数',
--   `visit_supply_days` bigint COMMENT '同一个档口主页，用户10个自然日内访问天数',
--   `view_unadd_cart_nums` bigint COMMENT '任意分类页同一天浏览未加进货车款数',
--   `homepage_search_counts` bigint COMMENT '首页搜索框一天内搜索次数',
--   `first_order_sign_time` bigint COMMENT '首单任一包裹签收时间',
--   `after_first_order_days` bigint COMMENT '距首单下单天数',
--   `after_first_order_counts` bigint COMMENT '首单专场日结束后下的有效订单数',
--   `normal_order_sign_time` bigint  COMMENT '正常下单任一包裹签收时间',
--   `allot_order_cancel_success_time` bigint COMMENT '最近用户成功自主取消过一次排单的时间',
--   `stock_out_order_time` bigint COMMENT '用户2天内排单中被非用户断货的时间',
--   `order_cancel_success_time` bigint COMMENT '最近用户成功自主取消过一次订单时间',
--   `first_customer_service_time` bigint COMMENT '首次进线七鱼找客服时间',
--   `last_service_order_time` bigint COMMENT '最近用户创建过售后工单提交后时间',
--   `identify_up_order_counts` bigint COMMENT '用户会员等级升级后下的有效订单数',
--   `max_gmv` DOUBLE COMMENT '单个订单最大GMV',
--   `max_gmv_order_sign_time` bigint COMMENT '单个订单最大GMV中一个包裹签收时间',
--   `month_gmv_percent` DOUBLE COMMENT '实际GMV月环比',
--   `first_refund_card_service_time` bigint COMMENT '首次提交退货卡售后工单时间',
--   `first_month_card_coupon_time` bigint COMMENT '首次使用运费月卡的运费券时间'
-- ) 
-- comment 'NPS问卷自动触发'
-- partitioned by (dt string)
-- STORED AS ORC LOCATION 'obs://yishou-bigdata/yishou_data.db/ads_data_nps_auto_rules'
-- ;


insert overwrite table yishou_data.ads_data_nps_auto_rules PARTITION(dt)
select 
    fu.user_id
    ,case 
        -- 店主认证后正常未下单
        when fcac.user_id is not null and no_order_info.real_first_time is not null and datediff(no_order_info.real_first_time,fcac.first_start_time)>0 
        then datediff(no_order_info.real_first_time,fcac.first_start_time)
        -- 店主认证后异常未下单
        when fcac.user_id is not null and no_order_info.real_first_time is not null and datediff(no_order_info.real_first_time,fcac.first_start_time)<0 
        then datediff(CURRENT_DATE(),fcac.first_start_time)
        -- 店主认证后从未下单
        when fcac.user_id is not null and no_order_info.real_first_time is null 
        then datediff(CURRENT_DATE(),fcac.first_start_time)
        else 0
    end as auth_unplaced_order_days
    ,coalesce(login.max_login_num,0) as login_unplaced_order_days
    ,coalesce(no_order.max_no_order_num,0) as unplaced_order_days
    ,coalesce(stall.max_stall_uv,0) as visit_supply_days
    ,coalesce(classify_info.goods_uv,0) as view_unadd_cart_nums
    ,coalesce(search.search_pv,0) as homepage_search_counts
    ,coalesce(shipping.first_sign_time,0) as first_order_sign_time
    ,coalesce(order_info.after_first_order_days,0) as after_first_order_days
    ,coalesce(order_info.after_first_order_counts,0) as after_first_order_counts
    ,coalesce(shipping.last_sign_time,0) as normal_order_sign_time
    ,coalesce(cancel_allot_order.allot_order_cancel_success_time,0) as allot_order_cancel_success_time
    ,coalesce(cancel_allot_order.stock_out_order_time,0) as stock_out_order_time
    ,coalesce(cancel_order.order_cancel_success_time,0) as order_cancel_success_time
    ,coalesce(qiyu.first_customer_service_time,0) as first_customer_service_time
    ,coalesce(service_order.last_service_order_time,0) as last_service_order_time
    ,coalesce(identity.identify_up_order_counts,0) as identify_up_order_counts
    ,coalesce(gmv.max_gmv,0) as max_gmv
    ,coalesce(gmv.max_gmv_order_sign_time,0) as max_gmv_order_sign_time
    ,coalesce(gmv_month.month_gmv_percent,0) as month_gmv_percent
    ,coalesce(card_order.first_refund_card_service_time,0) as first_refund_card_service_time
    ,coalesce(card_order.first_month_card_coupon_time,0) as first_month_card_coupon_time
    ,'${gmtdate}' as dt
from yishou_data.ods_fmys_users_view fu
left join (
    -- 店主认证成功
    select 
        user_id
        -- 首次店主认证时间统一按次日的专场日算
        ,MIN(DATE_ADD(FROM_UNIXTIME(start_time-25200),1)) as first_start_time
    from(
        select 
            user_id
            ,start_time
            ,ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY start_time) AS ranking  
        from yishou_data.ods_fmys_check_account_channel_view
        where status = 1 
        having ranking = 1
    )
    group by user_id
) fcac ON fcac.user_id=fu.user_id
left join (
    select user_id,min(real_first_time) as real_first_time 
    from (
        select 
            fo.user_id
            -- 实际首单时间，统一按次日的专场日算
            ,DATE_ADD(MIN(FROM_UNIXTIME(fo.add_time-25200)) OVER(PARTITION BY fo.user_id,cr.order_id ORDER BY fo.add_time,fo.order_id),1) as real_first_time
        from yishou_data.all_fmys_order_h fo
        join yishou_data.all_fmys_order_infos_h foi on fo.order_id = foi.order_id
        -- 未下单
        left join yishou_data.all_fmys_order_cancel_record_h cr on fo.order_id = cr.order_id and cr.cancel_type = 1
        where cr.order_id is not null
    )
    group by user_id
) no_order_info on no_order_info.user_id = fu.user_id
left join (
    -- 用户在180天内连续登录未下单的最大天数
    SELECT 
        user_id, max(login_num) as max_login_num 
    FROM (
        SELECT 
            user_id, first_login_date, count(1) as login_num
        FROM (
            SELECT 
                user_id
                , date_sub(special_date,row_number() over(partition by user_id order by special_date)) first_login_date
            FROM (
                select 
                    dateadd(to_date(a.dt,'yyyyMMdd'),-7,'hh') as special_date
                    , a.user_id 
                from yishou_data.dwd_log_app_wx_login_h_dt a
                left join (
                    select 
                        from_unixtime(fo.add_time-25200) as special_date
                        ,fo.user_id
                    from yishou_data.all_fmys_order_h fo
                    join yishou_data.all_fmys_order_infos_h foi on fo.order_id = foi.order_id
                    left join yishou_data.all_fmys_order_cancel_record_h cr on fo.order_id = cr.order_id and cr.cancel_type = 1
                    -- 未下单
                    where cr.order_id is not null 
                    and to_char(from_unixtime(fo.add_time-25200),'yyyymmdd') between to_char(dateadd(to_date1('${gmtdate}','yyyymmdd'),-180,'dd'),'yyyymmdd') and '${gmtdate}'
                    group by 1,2
                ) b 
                on a.user_id = b.user_id and to_char(b.special_date,'yyyymmdd') = to_char(dateadd(to_date(a.dt,'yyyyMMdd'),-7,'hh'),'yyyymmdd')
                where a.dt between to_char(dateadd(to_date1('${gmtdate}','yyyymmdd'),-180,'dd'),'yyyymmdd') and '${gmtdate}'
                GROUP BY 1,2
            )
        )
        GROUP BY user_id, first_login_date
    )
    GROUP BY user_id
) login on login.user_id = fu.user_id
left join (
    -- 用户在180天内连续未下单的最大天数
    select 
        user_id,max(no_order_num) as max_no_order_num
    from (
        select 
            user_id, first_no_order_date, count(1) as no_order_num
        from (
            select
                user_id
                , date_sub(special_date,row_number() over(partition by user_id order by special_date)) first_no_order_date
            from (
                select 
                    from_unixtime(fo.add_time-25200) as special_date
                    ,fo.user_id
                from yishou_data.all_fmys_order_h fo
                join yishou_data.all_fmys_order_infos_h foi on fo.order_id = foi.order_id
                -- 未下单
                left join yishou_data.all_fmys_order_cancel_record_h cr on fo.order_id = cr.order_id and cr.cancel_type in (1,2)
                where cr.order_id is not null 
                and to_char(from_unixtime(fo.add_time-25200),'yyyymmdd') between to_char(dateadd(to_date1('${gmtdate}','yyyymmdd'),-180,'dd'),'yyyymmdd') and '${gmtdate}'
                group by 1,2
            )
        )
        group by user_id, first_no_order_date
    )
    group by user_id
) no_order on no_order.user_id = fu.user_id
left join (
    -- 在同一档口主页下，用户在10天内进入超7天
    select 
        user_id,max(stall_uv) as max_stall_uv
    from (
        -- app为实时
        select 
            user_id
            ,stall_id
            ,count(DISTINCT dt) as stall_uv
        from yishou_data.dwd_app_check_stall_dt_view
        where dt between to_char(dateadd(to_date1('${gmtdate}','yyyymmdd'),-9,'dd'),'yyyymmdd') and '${gmtdate}'
        and stall_id is not null
        group by user_id,stall_id
        having stall_uv > 7
        union ALL 
        -- 小程序为历史
        select 
            user_id
            ,stall_id
            ,count(DISTINCT dt) as stall_uv
        from yishou_data.dcl_wx_check_stall_d
        where dt between to_char(dateadd(to_date1('${bizdate}','yyyymmdd'),-9,'dd'),'yyyymmdd') and '${bizdate}'
        and stall_id is not null
        group by user_id,stall_id
        having stall_uv > 7
    )
    group by user_id
) stall on stall.user_id = fu.user_id
left join (
    -- 分类页面下同一天浏览款数超x款未加进货车
    -- app的，小程序暂无埋点
    select 
        a.user_id,count(DISTINCT a.goods_no) as goods_uv
    from yishou_data.dwd_app_big_goods_exposure_dt_view a
    left join (
        select 
            a.user_id,b.goods_no 
        from yishou_data.dwd_app_add_cart_dt_view a
        left join yishou_data.dim_goods_id_info_full_h b on a.goods_id = b.goods_id
        where a.dt = '${gmtdate}' and a.source = 8 
        group by a.user_id,b.goods_no
    ) b on a.user_id = b.user_id and a.goods_no = b.goods_no
    where a.pid = 12 and a.dt = '${gmtdate}' and b.goods_no is null
    group by a.user_id
) classify_info on classify_info.user_id = fu.user_id
left join (
    -- 用户在首页搜索框的搜索次数
    -- app,小程序无埋点
    select 
        user_id,count(user_id) as search_pv
    from yishou_data.dwd_app_search_dt_view
    where dt = '${gmtdate}' and search_source_name = '首页'
    group by user_id
) search on search.user_id = fu.user_id
left join (
    -- 用户首个包裹签收时间
    select 
        b.user_id
        ,MIN(a.sign_time) as first_sign_time
        ,max(a.sign_time) as last_sign_time
    FROM yishou_data.ods_fmys_shipping_detail_view a
    join yishou_data.all_fmys_order_h b on a.order_id=b.order_id and b.pay_status=1  
    WHERE a.sign_time > 0 and a.sign_time is not null
    group by b.user_id
) shipping on shipping.user_id = fu.user_id
left join (
    select 
        fo.user_id
        -- 用户在专场日内最后一次下单距离实际首单时间的天数
        ,DATEDIFF(max(from_unixtime(fo.add_time-25200)),first_orders.first_order_time) as after_first_order_days
        -- 用户在首单专场日后的下单的有效订单数
        ,COUNT(IF(FROM_UNIXTIME(fo.add_time-25200)>first_orders.first_order_time,fo.order_id,NULL)) as after_first_order_counts
    from yishou_data.all_fmys_order_h fo
    join yishou_data.all_fmys_order_infos_h foi on fo.order_id = foi.order_id
    -- 下单支付
    left join yishou_data.all_fmys_order_cancel_record_h cr on fo.order_id = cr.order_id and cr.cancel_type = 1
    JOIN (
        SELECT fo.user_id,MIN(FROM_UNIXTIME(fo.add_time-25200)) AS first_order_time
        FROM yishou_data.all_fmys_order_h fo
        left join yishou_data.all_fmys_order_cancel_record_h cr on fo.order_id = cr.order_id and cr.cancel_type = 1
        where fo.pay_status = 1 and cr.order_id is null
        GROUP BY fo.user_id
    ) AS first_orders ON fo.user_id = first_orders.user_id
    where fo.pay_status = 1 and cr.order_id is null
    group by fo.user_id,first_orders.first_order_time
) order_info on order_info.user_id = fu.user_id
left join (
    select 
        fo.user_id
        -- 用户在近x天内成功自主取消过一次排单的时间
        ,max(fgsl.create_at) as allot_order_cancel_success_time
        -- 用户在近x天内排单中商品断货且原因为非排单用户自主取消的时间
        ,max(fgsl_no_good.create_at) as stock_out_order_time
    from yishou_data.all_fmys_order_h fo
    join yishou_data.all_fmys_order_infos_h foi on fo.order_id = foi.order_id
    left join (
        -- 取消排单-用户手动操作
        select order_id,type,note,create_at from yishou_data.ods_fmys_goods_stock_log_view
        where type in (1,11) 
        -- and to_char(from_unixtime(create_at),'yyyymmdd') between to_char(DATEADD(to_date1('${gmtdate}','yyyymmdd'),-2,'dd'),'yyyymmdd') and '${gmtdate}'
    ) fgsl on fgsl.order_id = fo.order_id
    left join (
        -- 取消排单-商品断货
        select order_id,type,note,create_at from yishou_data.ods_fmys_goods_stock_log_view
        where type not in (1,11) and note != '排单用户自主取消'
    ) fgsl_no_good on fgsl_no_good.order_id = fo.order_id
    group by fo.user_id
) cancel_allot_order on cancel_allot_order.user_id = fu.user_id
left join (
    -- 用户在近x天内成功自主取消过一次订单的时间
    select 
        fo.user_id
        ,max(cr.cancel_time) as order_cancel_success_time 
    from yishou_data.all_fmys_order_h fo
    join yishou_data.all_fmys_order_infos_h foi on fo.order_id = foi.order_id
    left join yishou_data.all_fmys_order_cancel_record_h cr on fo.order_id = cr.order_id and cr.cancel_type = 1
    where cr.order_id is not null
    group by fo.user_id
) cancel_order on cancel_order.user_id = fu.user_id
left join (
    -- 用户首次进入七鱼客服的时间
    select 
        user_id
        ,min(time) as first_customer_service_time
    from yishou_data.dcl_event_qiyu_click_d 
    where dt >= '20190801' 
    group by user_id
) qiyu on qiyu.user_id = fu.user_id
left join (
    -- 用户在近x天内成功提交过任意售后工单
    select 
        b.user_id
        ,max(b.create_time) as last_service_order_time
    from yishou_data.all_fmys_service_order_h so
    left join yishou_data.ods_fmys_service_order_ext_view soe on so.id = soe.so_id 
    left join yishou_data.ods_fmys_service_order_by_app_view b on b.service_id=so.id
    where coalesce(soe.service_order_type,0) in ('0','1') 
    group by b.user_id
) service_order on service_order.user_id = fu.user_id
left join (
    -- 用户会员等级升级后下的第x个有效订单
    select 
        a.user_id
        ,count(foi.order_id) as identify_up_order_counts
    from yishou_data.ods_fmys_account_channel_view a
    left join yishou_data.all_fmys_order_infos_h foi on a.user_id = foi.user_id
    where a.last_identity < a.identity
    group by a.user_id
) identity on identity.user_id = fu.user_id
left join (
    select 
        a.user_id
        -- 用户单个订单的最大gmv,剔除专场结束前取消的
        ,max(a.gmv) as max_gmv
        -- 用户单个订单最大gmv对应的最早下单的订单签收时间
        ,max(fsd.sign_time) as max_gmv_order_sign_time
    from (
        select 
            user_id
            ,order_id
            ,gmv
            ,row_number() over (partition by user_id order by gmv desc,order_id asc) as rn
        from (
            select 
                fo.user_id
                ,fo.order_id
                ,sum(foi.buy_num*foi.shop_price) gmv
            from yishou_data.all_fmys_order_h fo
            join yishou_data.all_fmys_order_infos_h foi on fo.order_id = foi.order_id
            left join yishou_data.all_fmys_order_cancel_record_h cr on fo.order_id = cr.order_id and cr.cancel_type = 1
            where fo.pay_status = 1 and cr.order_id is null 
            group by fo.user_id,fo.order_id
        )
        HAVING rn = 1
    ) a
    left join yishou_data.ods_fmys_shipping_detail_view fsd on a.order_id = fsd.order_id
    group by a.user_id
) gmv on gmv.user_id = fu.user_id
left join (
    -- 用户的实际gmv月环比
    select 
        fo.user_id
        -- 本月
        ,sum(foi.buy_num*foi.shop_price) / sum(last_month.gmv) - 1 as month_gmv_percent
    from yishou_data.all_fmys_order_h fo
    join yishou_data.all_fmys_order_infos_h foi on fo.order_id = foi.order_id
    left join yishou_data.all_fmys_order_cancel_record_h cr on fo.order_id = cr.order_id and cr.cancel_type = 1
    left join (
        -- 上个月
        select 
            fo.user_id
            ,sum(foi.buy_num*foi.shop_price) gmv
        from yishou_data.all_fmys_order_h fo
        join yishou_data.all_fmys_order_infos_h foi on fo.order_id = foi.order_id
        left join yishou_data.all_fmys_order_cancel_record_h cr on fo.order_id = cr.order_id and cr.cancel_type = 1
        where fo.pay_status = 1 and cr.order_id is null 
        and to_char(from_unixtime(foi.create_time),'yyyymmdd') between to_char(dateadd(to_date1('${gmtdate}','yyyymmdd'),-1,'mm'),'yyyymm01') and to_char(dateadd(to_date1('${gmtdate}','yyyymmdd'),-1,'mm'),'yyyymmdd')
        group by fo.user_id
    ) last_month on last_month.user_id = fo.user_id
    where fo.pay_status = 1 and cr.order_id is null 
    and to_char(from_unixtime(foi.create_time),'yyyymmdd') between concat(substr('${gmtdate}',1,6),'01') and '${gmtdate}'
    group by fo.user_id
) gmv_month on gmv_month.user_id = fu.user_id
left join (
    select 
        co.user_id
        -- 首次提交退货卡售后工单时间
        ,min(coi.create_time) as first_refund_card_service_time
        -- 首次使用运费月卡的运费券时间
        ,min(if(co.pay_status=2,co.create_time,0)) as first_month_card_coupon_time
    from yishou_data.ods_fmys_card_order_view co
    join yishou_data.ods_fmys_card_order_infos_view coi on co.id = coi.card_order_id
    group by co.user_id
) card_order on card_order.user_id = fu.user_id
DISTRIBUTE BY dt
;