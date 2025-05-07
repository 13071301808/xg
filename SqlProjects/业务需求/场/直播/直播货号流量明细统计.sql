-- CREATE EXTERNAL TABLE yishou_daily.dwd_live_goods_flow_infos_dt (
--     `special_date` string COMMENT '专场日',
--     `supply_id` string COMMENT '供应商id',
--     `supply_name` string COMMENT '供应商名称',
--     `room_id` string COMMENT '直播间id',
--     `goods_no` string COMMENT '货号',
--     `goods_name` string COMMENT '商品名称',
--     `cat_name` string COMMENT '三级品类名称',
--     `is_live_fragment` string COMMENT '是否直播片段',
--     `goods_exposure_uv` string COMMENT '商品曝光uv',
--     `goods_click_uv` string COMMENT '商品点击uv',
--     `goods_addcart_uv` string COMMENT '商品加购uv',
--     `goods_addcart_num` string COMMENT '商品加购件数',
--     `goods_addcart_amount` string COMMENT '商品加购金额',
--     `room_buy_num` string COMMENT '实际购买件数',
--     `room_gmv` string COMMENT '实际gmv',
--     `all_buy_gmv` string COMMENT '大盘成交金额',
--     `room_gmv_ratio` string COMMENT '占比大盘'
-- )
-- comment '直播间货号流量明细分析'
-- partitioned by (dt string)
-- STORED AS ORC LOCATION 'obs://yishou-bigdata/yishou_daily.db/dwd_live_goods_flow_infos_dt'
-- ;

insert overwrite table yishou_daily.dwd_live_goods_flow_infos_dt PARTITION(dt)
select 
    main.special_date
    ,main.supply_id
    ,main.supply_name
    ,main.room_id
    ,main.goods_no
    ,main.goods_name
    ,main.cat_name
    ,main.is_live_fragment
    ,coalesce(count(DISTINCT ex.user_id),0) as goods_exposure_uv
    ,coalesce(count(DISTINCT cl.user_id),0) as goods_click_uv
    ,coalesce(count(DISTINCT cart.user_id),0) as goods_addcart_uv
    ,coalesce(sum(cart.goods_addcart_num),0) as goods_addcart_num
    ,coalesce(round(sum(cart.goods_addcart_amount),2),0) as goods_addcart_amount
    ,coalesce(sum(cart.room_buy_num),0) as room_buy_num
    ,coalesce(round(sum(cart.room_gmv),2),0) as room_gmv
    ,coalesce(round(max(all_goods.all_buy_gmv),2),0) as all_buy_gmv
    ,round(sum(cart.room_gmv) / max(all_goods.all_buy_gmv),4) as room_gmv_ratio
    ,main.special_date as dt
from (
    -- 主表
    select 
        to_char(lrp.special_date,'yyyymmdd') as special_date
        , lr.supply_id
        , lr.supply_name
        , lr.room_id
        , lrg.goods_no
        , dgnif.goods_name
        , dgnif.cat_name
        , case 
            when lrg.start_explain_time > 0 and lrg.goods_no is not null then '是' 
            else '否' 
        end as is_live_fragment
        , lrp.user_id
    from yishou_data.dim_live_room_info lr
    left join yishou_data.dwd_log_live_root_page_dt lrp on to_char(lr.start_special_date,'yyyymmdd') = to_char(lrp.special_date,'yyyymmdd') and lr.room_id = lrp.room_id
    left join yishou_data.all_fmys_live_room_goods lrg on lr.room_id = lrg.room_id 
    left join yishou_data.dim_goods_no_info_full_h dgnif on lrg.goods_no = dgnif.goods_no
    where lrp.dt between '${one_day_ago}' and '${gmtdate}'
    and to_char(lrp.special_date,'yyyymmdd') = '${one_day_ago}'
    and to_char(lr.start_special_date,'yyyymmdd') = '${one_day_ago}'
    and is_show = 1 and room_status = 5 
    and lrp.user_id > 0 
    and lr.title not like '%测试%' 
    and lr.supply_id not in (13854,13856)
    -- -- 验数
    -- and lr.room_id = '105567'
    -- and lr.supply_id = '25302'
    and lrg.goods_no is not null
    group by 1,2,3,4,5,6,7,8,9
) main
left join (
    -- 曝光商品用户
    select 
        gd.special_date
        , lrg.room_id
        , lrg.goods_no
        , gd.user_id
    from (
        select 
            to_char(report_special_date,'yyyymmdd') as special_date
            , goods_no
            , user_id
            , goods_id 
        from yishou_data.dwd_log_big_goods_exposure_dt
        where dt between '${one_day_ago}' and '${gmtdate}'
        and to_char(report_special_date,'yyyymmdd') = '${one_day_ago}'
        and user_id > 0
        -- 限制直播渠道
        and pid in (25,29,56,57)
        group by 1,2,3,4
    ) gd
    left join yishou_data.dim_goods_id_info_full_h gii on gd.goods_id = gii.goods_id and gii.is_live_room=1
    left join yishou_data.all_fmys_live_room_goods lrg on gd.goods_id = lrg.goods_id
    where gii.goods_id is not null and lrg.room_id is not null
    group by 1,2,3,4
) ex on ex.special_date = main.special_date and ex.goods_no = main.goods_no 
and ex.room_id = main.room_id and main.user_id = ex.user_id
left join (
    -- 点击商品用户
    select 
        to_char(gd.special_date,'yyyymmdd') as special_date
        , lrg.room_id
        , gd.user_id
        , gii.goods_no
    from yishou_data.dwd_log_goods_detail_page_dt gd
    join yishou_data.dim_goods_id_info gii on gd.goods_id = gii.goods_id
    join yishou_data.all_fmys_live_room_goods lrg on gd.goods_id = lrg.goods_id 
    where gd.dt between '${one_day_ago}' and '${gmtdate}'
    and to_char(gd.special_date, 'yyyymmdd') = '${one_day_ago}'
    and gd.user_id > 0 and gii.is_live_room=1
    group by 1,2,3,4
) cl on cl.special_date = main.special_date and cl.goods_no = main.goods_no 
and cl.room_id = main.room_id and main.user_id = cl.user_id
left join (
    -- 加购商品用户、商品
    select 
        cart.special_date
        , cart.room_id
        , lrg.goods_no
        , cart.user_id
        , sum(coalesce(pay.buy_num, 0)) as room_buy_num 
        , sum(coalesce(pay.buy_amount, 0)) as room_gmv
        , sum(coalesce(cart.goods_addcart_num, 0)) as goods_addcart_num
        , sum(coalesce(cart.goods_addcart_amount, 0)) as goods_addcart_amount
    from (
        -- 加购商品用户、商品
        select 
            ac.special_date
            , lrg.room_id
            , ac.user_id
            , ac.goods_id
            , sum(ac.goods_addcart_num) as goods_addcart_num
            , sum(ac.goods_addcart_amount) as goods_addcart_amount
        from (
            select 
                to_char(ac.special_date, 'yyyymmdd') as special_date
                ,ac.user_id
                ,ac.goods_id
                ,sum(ac.goods_number) as goods_addcart_num
                ,sum(ac.goods_number * ac.goods_price) as goods_addcart_amount
            from yishou_data.dwd_log_add_cart_dt ac
            where ac.dt between '${one_day_ago}' and '${gmtdate}'
            and to_char(ac.special_date, 'yyyymmdd') = '${one_day_ago}'
            and ac.user_id > 0
            group by 1,2,3
            union 
            select 
                to_char(ac.special_date, 'yyyymmdd') as special_date
                ,user_id
                ,goods_id
                ,sum(good_number) as goods_addcart_num
                ,sum(good_number * good_price) as goods_addcart_amount
            from yishou_data.dwd_log_wx_add_cart_dt ac
            where ac.dt between '${one_day_ago}' and '${gmtdate}'
            and to_char(ac.special_date, 'yyyymmdd') = '${one_day_ago}'
            and ac.user_id > 0
            group by 1,2,3
        ) ac
        join yishou_data.dim_goods_id_info gii on ac.goods_id = gii.goods_id and gii.is_live_room=1
        join yishou_data.all_fmys_live_room_goods lrg on ac.goods_id = lrg.goods_id 
        where gii.goods_id is not null
        group by 1,2,3,4
    ) cart 
    left join (
        -- 支付用户、商品
        select
            to_char(datetrunc(special_start_time,'dd'),'yyyymmdd') as special_date
            , user_id
            , goods_id
            , sum(is_real_pay*buy_num) as buy_num 
            , sum(is_real_pay*shop_price * buy_num) as buy_amount
        from yishou_data.dwd_sale_order_info_dt so 
        where dt between '${one_day_ago}' and '${gmtdate}'
        and to_char(special_start_time, 'yyyymmdd') = '${one_day_ago}'
        and pay_status = 1 
        and user_id not in (2,10,17,387836)
        group by 1,2,3
    ) pay on cart.special_date = pay.special_date and cart.user_id = pay.user_id and cart.goods_id = pay.goods_id
    left join yishou_data.all_fmys_live_room_goods lrg on cart.goods_id = lrg.goods_id 
    group by 1,2,3,4
) cart 
on cart.special_date = main.special_date and cart.goods_no = main.goods_no 
and cart.room_id = main.room_id and main.user_id = cart.user_id
left join (
    -- 大盘gmv
    select  
        to_char(dateadd(add_time,-7,'hh'),'yyyymmdd') as special_date
        ,goods_no
        ,sum(shop_price*buy_num) as all_buy_gmv
    from yishou_data.dwd_sale_order_info_dt
    where dt between '${one_day_ago}' and '${gmtdate}'
    and to_char(dateadd(add_time,-7,'hh'),'yyyymmdd') = '${one_day_ago}'
    and is_real_pay = 1
    group by 1,2
) all_goods on main.goods_no = all_goods.goods_no and all_goods.special_date = main.special_date
group by 1,2,3,4,5,6,7,8,18
DISTRIBUTE BY dt 
;