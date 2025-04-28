SELECT
    aa.*,
    GMV,
	铺货用户GMV,
	铺货商品GMV
FROM (
    SELECT
    	to_char (reg_time, 'yyyymm') `mt`,
    	COUNT(DISTINCT t1.user_id) `注册用户数`,
    	COUNT(DISTINCT IF (is_login = 1, t1.user_id, NULL)) `登录用户数`,
    	COUNT(DISTINCT IF (is_this_month_pay_user = 1, t1.user_id, NULL)) `支付用户数`,
    	COUNT(DISTINCT t2.user_id) `绑定铺货用户数`,
    	COUNT(DISTINCT t3.user_id) `铺货>0用户数`,
    	COUNT(DISTINCT t4.user_id) `绑定订单打通用户数`,
    	max(t5_user_id) `绑定订单且下单用户数`
    FROM
    	yishou_data.dws_user_label_summary_dt t1

    LEFT JOIN (
    	SELECT
    		user_id,
    		to_char (created_at, 'yyyymm') AS mt
    	FROM
    		yishou_data.all_fmys_sb_store_bind_result
    	WHERE
    		status = 1
    	GROUP BY
    		user_id,
    		to_char (created_at, 'yyyymm')
    ) t2 ON t1.user_id = t2.user_id
    AND to_char (reg_time, 'yyyymm') = t2.mt

    LEFT JOIN (
    	SELECT
    		user_id,
    		to_char (created_at, 'yyyymm') AS mt,
    		COUNT(DISTINCT task_id) distribution_cnt
    	FROM
    		yishou_data.all_fmys_sb_goods_distribute_result
    	WHERE
    		status = 3
    	GROUP BY
    		user_id,
    		to_char (created_at, 'yyyymm')
    ) t3 ON t2.user_id = t3.user_id
    AND to_char (reg_time, 'yyyymm') = t3.mt

    LEFT JOIN (
    	SELECT
    		to_char (created_at, 'yyyymm') mt,
    		user_id
    	FROM
    		yishou_data.ods_fmys_sb_km_store_bind_result_dt
    	WHERE
    		dt = max_pt ('yishou_data.ods_fmys_sb_km_store_bind_result_dt')
    	GROUP BY
    		1,
    		2
    ) t4 ON to_char (reg_time, 'yyyymm') = t4.mt
    AND t1.user_id = t4.user_id

    LEFT JOIN (
    	SELECT
    		to_char (created_at, 'yyyymm') mt,
    		count(DISTINCT user_id) t5_user_id
    	FROM
    		yishou_data.ods_fmys_sb_km_order_rel_dt
    	WHERE
    		dt = max_pt ('yishou_data.ods_fmys_sb_km_order_rel_dt')
    	GROUP BY
    		1
    ) t5 ON to_char (reg_time, 'yyyymm') = t5.mt

    WHERE
    	dt >= 20241231
    	AND to_char (reg_time, 'yyyymmdd') >= 20250101
    	AND user_type_2024 like '%网商%' = '网商/地摊'
    	AND to_char (FROM_UNIXTIME(UNIX_TIMESTAMP(reg_time) - 7 * 3600), 'yyyymm') = substring(dt, 1, 6)
    GROUP BY
    	1
) aa

LEFT JOIN (
    select
        to_char(from_unixtime(unix_timestamp(add_time, 'yyyy-MM-dd HH:mm:ss') - 7 * 3600),'yyyymm') mt, sum(shop_price*buy_num) `铺货用户GMV`
    from yishou_data.dwd_sale_order_info_dt so
    left join yishou_data.all_fmys_order_cancel_record d
    on so.order_id = d.order_id and d.cancel_type = 1
    join (SELECT DISTINCT to_char (created_at, 'yyyymm') mt, user_id FROM yishou_data.ods_fmys_sb_goods_distribute_result_dt WHERE dt = max_pt ('yishou_data.ods_fmys_sb_goods_distribute_result_dt')) ttt on so.user_id = ttt.user_id and to_char(from_unixtime(unix_timestamp(add_time, 'yyyy-MM-dd HH:mm:ss') - 7 * 3600),'yyyymm') = ttt.mt
    JOIN ( SELECT user_id, to_char (created_at, 'yyyymm') AS mt FROM yishou_data.all_fmys_sb_store_bind_result WHERE status = 1 GROUP BY user_id, to_char (created_at, 'yyyymm') ) ttt2 ON so.user_id = ttt2.user_id and to_char(from_unixtime(unix_timestamp(add_time, 'yyyy-MM-dd HH:mm:ss') - 7 * 3600),'yyyymm') = ttt2.mt
    where so.dt >= 20241231
        and so.pay_status=1 and so.special_id !=271880
        and d.order_id is null
        and to_char(from_unixtime(unix_timestamp(add_time, 'yyyy-MM-dd HH:mm:ss') - 7 * 3600),'yyyymmdd') >= 20250101
    group by 1
)bb
ON aa.mt = bb.mt

LEFT JOIN (
    select
        to_char(from_unixtime(unix_timestamp(add_time, 'yyyy-MM-dd HH:mm:ss') - 7 * 3600),'yyyymm') mt, sum(shop_price*buy_num) `铺货商品GMV`
    from yishou_data.dwd_sale_order_info_dt so
    left join yishou_data.all_fmys_order_cancel_record d
    on so.order_id = d.order_id and d.cancel_type = 1
    join ( SELECT DISTINCT to_char (created_at, 'yyyymm') mt, goods_no FROM yishou_data.ods_fmys_sb_goods_distribute_result_dt WHERE dt = max_pt ('yishou_data.ods_fmys_sb_goods_distribute_result_dt')) ttt on so.goods_no = ttt.goods_no and to_char(from_unixtime(unix_timestamp(add_time, 'yyyy-MM-dd HH:mm:ss') - 7 * 3600),'yyyymm') = ttt.mt
    JOIN ( SELECT user_id, to_char (created_at, 'yyyymm') AS mt FROM yishou_data.all_fmys_sb_store_bind_result WHERE status = 1 GROUP BY user_id, to_char (created_at, 'yyyymm') ) ttt2 ON so.user_id = ttt2.user_id and to_char(from_unixtime(unix_timestamp(add_time, 'yyyy-MM-dd HH:mm:ss') - 7 * 3600),'yyyymm') = ttt2.mt
    where so.dt >= 20241231
        and so.pay_status=1 and so.special_id !=271880
        and d.order_id is null
        and to_char(from_unixtime(unix_timestamp(add_time, 'yyyy-MM-dd HH:mm:ss') - 7 * 3600),'yyyymmdd') >= 20250101
    group by 1
)cc
ON aa.mt = cc.mt

LEFT JOIN (
    select
        to_char(from_unixtime(unix_timestamp(add_time, 'yyyy-MM-dd HH:mm:ss') - 7 * 3600),'yyyymm') mt, sum(shop_price*buy_num) `GMV`
    from yishou_data.dwd_sale_order_info_dt so
    left join yishou_data.all_fmys_order_cancel_record d
    on so.order_id = d.order_id and d.cancel_type = 1
    JOIN ( SELECT user_id, to_char (created_at, 'yyyymm') AS mt FROM yishou_data.all_fmys_sb_store_bind_result WHERE status = 1 GROUP BY user_id, to_char (created_at, 'yyyymm') ) ttt2 ON so.user_id = ttt2.user_id and to_char(from_unixtime(unix_timestamp(add_time, 'yyyy-MM-dd HH:mm:ss') - 7 * 3600),'yyyymm') = ttt2.mt
    where so.dt >= 20241231
        and so.pay_status=1 and so.special_id !=271880
        and d.order_id is null
        and to_char(from_unixtime(unix_timestamp(add_time, 'yyyy-MM-dd HH:mm:ss') - 7 * 3600),'yyyymmdd') >= 20250101
    group by 1
)dd
ON aa.mt = dd.mt