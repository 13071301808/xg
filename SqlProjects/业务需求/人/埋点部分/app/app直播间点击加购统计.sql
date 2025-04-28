-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/12/11 14:45:03 GMT+08:00
-- ******************************************************************** --
select 
    a.dt as 直播日期,
	a.room_id as 直播id,
	room.title as 直播标题,
	room.supply_id as 供应商id,
	room.supply_name as 供应商名称,
	case 
	    when user_level.user_group = '3_当月新客&7单内老客' and user_level.user_level = '8_[,0]' then '新客0单'
	    when user_level.user_group = '3_当月新客&7单内老客' and user_level.user_level = '9_小于7单' then '新客1-6单'
	    when user_level.user_group = '2_中小客' then '中小客'
	    when user_level.user_group = '1_大客&超大客' then '大客'
	end as 客群,
	sum(enter.total_enter_uv) as 总观看uv,
	sum(live.total_user) as 总观看人数,
	coalesce(sum(add_cart.add_cart_user),0) as 加购人数,
	coalesce(sum(pay.pay_user),0) as 支付人数
from yishou_daily.fmdes_user_live_info_d_temp a
left join yishou_data.dim_live_room_info room on a.room_id = room.room_id
left join (
	-- 进入直播间
	select
		room_id,
		user_id,
		count(DISTINCT user_id) as total_enter_uv
	from yishou_data.dcl_event_live_root_page_d
	-- 历史所有
	where dt between '20190101' and '${bizdate}' and is_number(user_id) and is_number(room_id)
	group by room_id,user_id
) enter on enter.room_id = a.room_id and a.user_id = enter.user_id
left join (
    -- 观看效果
	select
		room_id,
		user_id,
		sum(ptime) as total_time,
		count(DISTINCT user_id) as total_user
	from yishou_data.dcl_event_live_root_page_time_d
	-- 历史所有
	where dt between '20190101' and '${bizdate}' and is_number(user_id) and is_number(room_id)
	group by user_id,room_id
	having total_time > 60 
) live on a.room_id = live.room_id and a.user_id = live.user_id
left join (
    -- 客群
    select 
        dt,user_id,user_level,user_group
    from yishou_data.dim_user_superman_franky_snap_dt 
    where dt between '20241201' and '${bizdate}'
    group by 1,2,3,4
) user_level on user_level.user_id = a.user_id and a.dt = user_level.dt
left join (
    -- 直播间加购
    select 
        room_id
        ,user_id
        ,sum(shop_price * num) as add_amount
        ,sum(num) as add_num
        ,count(DISTINCT user_id) as add_cart_user
    from yishou_data.all_fmys_live_add_cart
    group by room_id,user_id
) add_cart 
on a.room_id = add_cart.room_id and a.user_id = add_cart.user_id
left join (
    -- 支付
    select 
        sa_room_id
        ,user_id
        -- 实际销售额
        -- ,sum(case when (cancel_reason = 0 or order_status not in ('3')) then shop_price * buy_num end) as real_sale_amount  
        ,count(DISTINCT user_id) as pay_user
    from yishou_data.dw_order_info_sa_wt
    where pay_status = 1 and is_number(sa_room_id)
    group by sa_room_id,user_id
)pay 
on a.room_id = pay.sa_room_id and a.user_id = pay.user_id
where room.supply_id = '24568'
and (
    case 
	    when user_level.user_group = '3_当月新客&7单内老客' and user_level.user_level = '8_[,0]' then '新客0单'
	    when user_level.user_group = '3_当月新客&7单内老客' and user_level.user_level = '9_小于7单' then '新客1-6单'
	    when user_level.user_group = '2_中小客' then '中小客'
	    when user_level.user_group = '1_大客&超大客' then '大客'
	end
) is not null
and a.dt between '20241201' and '${bizdate}'
group by 1,2,3,4,5,6