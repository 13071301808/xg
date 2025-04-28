-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/04/23 11:05:41 GMT+08:00
-- ******************************************************************** --
-- 口径：https://yb7ao262ru.feishu.cn/wiki/DD8MwuVmbixr8GkHEtucoqNcnOc
-- CREATE EXTERNAL TABLE yishou_daily.temp_route_all_event_gmv_detail_h5_dt (
--     special_date string comment '',
-- 	sa_add_cart_id string comment '',
-- 	first_page_name string comment '',
-- 	second_page_name string comment '',
-- 	third_page_name string comment '',
-- 	four_page_name string comment '',
-- 	gmv string comment '',
-- 	buy_num string comment '',
-- 	real_gmv string comment '',
-- 	real_buy_num string comment '',
-- 	goods_id string comment '',
-- 	user_id string comment ''
-- )
-- comment '首页h5路径归因-档口现货临时gmv'
-- partitioned by (dt string)
-- STORED AS ORC LOCATION 'obs://yishou-bigdata/yishou_daily.db/temp_route_all_event_gmv_detail_h5_dt'
-- ;

insert overwrite table yishou_daily.temp_route_all_event_gmv_detail_h5_dt PARTITION(dt)
select
	t1.special_date,
	t1.sa_add_cart_id,
	nvl(
		case
			when t2.first_page_name in ('首页', '进货车', '个人中心', '分类页', '订阅', '直播间列表', '有料') then t2.first_page_name
			when t2.first_page_name in ('首页分类页', '榜单中心') then '首页'
			when t2.first_page_name = '市场着陆详情页面' then '首页'
			when t2.first_page_name = '直播' then '直播'
			else '其他'
		end,
		'其他'
	) first_page_name,
	nvl(
		case
			when t2.first_page_name = '市场着陆详情页面' then '首页市场'
			when t2.first_page_name in ('首页分类页', '榜单中心') then '首页分类页_新'
			else second_page_name
		end,
		'其他'
	) second_page_name,
	t2.third_page_name,
	t2.four_page_name,
	t1.gmv,
	t1.buy_num,
	t1.real_gmv,
	t1.real_buy_num,
	t1.goods_id,
	t1.user_id,
	t1.dt
from (
	select
		to_char(special_start_time,'yyyy-mm-dd') special_date,
		sa_add_cart_id,
		case
			when sa_add_cart_id = '' or sa_add_cart_id is null then concat(round(rand() * 10, 5), '_空值避免数据倾斜')
			else sa_add_cart_id
		end sa_add_cart_id_rand,
		goods_id,
		user_id,
		to_char(special_start_time,'yyyymmdd') dt,
		sum(buy_num * shop_price) gmv,
		sum(buy_num) buy_num,
		sum(case when is_real_pay = 1 then buy_num * shop_price else 0 end) real_gmv,
		sum(case when is_real_pay = 1 then buy_num else 0 end) real_buy_num
	from yishou_data.dwd_sale_order_info_dt
	where dt between '${one_day_ago}' and '${gmtdate}'
	and to_char (special_start_time, 'yyyymmdd') = '${one_day_ago}'
	and pay_status = 1
	group by 1, 2, 3, 4, 5, 6
) t1
left join (
	select
		first_page_name,
		second_page_name,
		third_page_name,
		four_page_name,
		add_cart_id,
		row_number() over (partition by add_cart_id order by click_event_time) nu
	from yishou_daily.temp_route_all_event_clickaddcart_detail_h5_dt
	where dt >= TO_CHAR(DATEADD(to_date1('${one_day_ago}','yyyymmdd'),-6,'mm'),'yyyymmdd')
	and length(add_cart_id) > 0
) t2 on t1.sa_add_cart_id_rand = t2.add_cart_id and t2.nu = 1
where t2.four_page_name <> '其他' and t2.four_page_name is not null
DISTRIBUTE BY dt 
;