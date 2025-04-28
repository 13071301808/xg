SELECT
	t1.special_date 专场日期,
	t1.first_page_name 一级页面,
	t1.second_page_name 二级页面,
	t1.third_page_name 三级页面,
	t1.is_in_stock 是否现货,
	count(concat(t1.goods_no, t1.special_date)) as 上架款数,
	count(IF(IF(t1.real_buy_num>0,t1.goods_no,0)=0,null,(concat(IF(t1.real_buy_num>0,t1.goods_no,0),t1.special_date)))) as 实际动销款数,
	count(IF(IF(t1.real_buy_num>0,t1.goods_no,0)=0,null,(concat(IF(t1.real_buy_num>0,t1.goods_no,0),t1.special_date))))/count(concat(t1.goods_no, t1.special_date)) as 实际动销率,
	sum(t1.real_buy_amount) / sum(t1.real_buy_num) as 实际价单价,
	sum(t1.real_buy_amount) 实际销售额,
	sum(t1.real_buy_num) 实际销售件数,
	sum(t1.goods_exposure_uv) as 商品曝光uv,
	sum(t1.real_buy_amount) / sum(t1.goods_exposure_uv) as 实际uv价值,
	sum(if(t1.real_buy_num>0,t1.real_buy_num,t1.goods_exposure_uv)) / sum(t1.goods_exposure_uv) as 不动销商品流量占比,
	sum(t1.real_buy_user_num) / sum(t1.goods_exposure_uv) as 商品曝光实际支付率,
	sum(t1.goods_click_uv) as 点击uv,
	sum(t1.goods_click_uv) / sum(t1.goods_exposure_uv) as 商品曝光点击率,
	sum(t1.goods_add_uv) as 加购uv,
	sum(t1.goods_add_uv) / sum(t1.goods_click_uv) as 商品点击加购率,
	sum(t1.real_buy_user_num) as 商品实际支付uv,
	sum(t1.real_buy_user_num) / sum(t1.goods_add_uv) as 商品加购实际支付率,
	sum(if(t1.real_buy_num>0,t1.real_buy_num,t1.goods_exposure_uv)) as 不动销商品流量
from (select * from yishou_daily.third_page_name_goods_dt where dt between '20230101' and '20230630') t1
left join yishou_data.dim_supply_info t2 on t1.supply_id = t2.supply_id
left join (
	select
		*,DATE_FORMAT(DATE_ADD(TO_DATE(dt, 'yyyyMMdd'), 1),'yyyy-MM-dd') as sp
	from
		yishou_daily.dtl_goods_factor_online_temp_hot_inefficient
	where
		dt between '20221231' and '20230629'
) t3 on t1.special_date = t3.sp
and t1.goods_no = t3.goods_no
left join yishou_data.dim_goods_no_info_full_h t333 on t1.goods_no = t333.goods_no
WHERE
	t1.special_date BETWEEN '2023-01-01' and '2023-07-01'
group by
	t1.special_date,
	t1.first_page_name,
	t1.second_page_name,
	t1.third_page_name,
	t1.is_in_stock

