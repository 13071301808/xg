-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/10/11 13:52:54 GMT+08:00
-- ******************************************************************** --
-- 专场日期、货号、是否新款、售龄、供给部门、供应商id、供应商名称、当月商家销售层级、三级品类、供应商近7天GMV、近5天支付人数、近5天UV价值、近5天该款的GMV、近5天该款的加购UV、近5天该款的支付UV、近15天该款的销售件数、近7天该款的加购UV、近7天该款的销售件数

select 
    DISTINCT 
    sp.special_date as 专场日期
    ,sp.goods_no as 商品货号
    ,sp.is_new as 是否新款
    ,datediff(sp.special_date,dgn.first_up_new_time)+1 as 售龄
    ,dsi.department as 供给部门
    ,sp.supply_id_new as `供应商id`
    ,dsi.supply_name as 供应商名称
    ,coalesce(mon.supply_gmv_level, '1_[0-1w)') as `近30天商家实际销售额分层`
    ,sp.third_cat_name as 三级品类
    ,seven_day.seven_day_gmv as `供应商近7天GMV`
    ,five_day.five_buy_user_num as `近5天支付人数`
    ,five_day.five_uv_value as `近5天UV价值`
    ,five_day.five_goods_gmv as `近5天该款的GMV`
    ,five_day.five_add_cart_uv as `近5天该款的加购UV`
    ,five_day.five_pay_uv as `近5天该款的支付UV`
    ,fifth_day.fifth_buy_num as `近15天该款的销售件数`
    ,seven_day.seven_add_cart_uv as `近7天该款的加购UV`
    ,seven_day.seven_buy_num as `近7天该款的销售件数`
from (
    select 
        sp.special_date
        ,sp.goods_no
        ,sp.is_new
        ,case 
            when sp.department in ('5杭州&平湖','6新意法&濮院') and sp.supply_name like '%IP运营%' and to_char(sp.dt,'yyyymm') <= '202402' then vendor_supply_id 
            else sp.supply_id 
        end as supply_id_new
        ,case 
            when sp.third_cat_name='牛仔裤' then '牛仔裤'
            when sp.third_cat_name='毛衣' then '毛衣'
            when sp.third_cat_name='针织衫' then '针织衫'
            when sp.third_cat_name='针织开衫' then '针织开衫'
            when sp.third_cat_name='休闲裤' then '休闲裤'
            when sp.third_cat_name='套装' then '套装'
            when sp.third_cat_name='半身裙' then '半身裙'
            when sp.third_cat_name='短外套' then '短外套'
            when sp.third_cat_name='羊毛外套' then '羊毛外套'
            when sp.third_cat_name='衬衫' then '衬衫'
            when sp.third_cat_name='卫衣' then '卫衣'
            when sp.third_cat_name='长袖T恤' then '长袖T恤'
            when sp.third_cat_name='连衣裙' then '连衣裙'
            when sp.third_cat_name='打底衫' then '打底衫'
            when sp.third_cat_name='风衣' then '风衣'
            when sp.third_cat_name='皮衣' then '皮衣'
            when sp.third_cat_name='西装' then '西装'
            when sp.third_cat_name='羽绒服' then '羽绒服'
            when sp.third_cat_name='西裤' then '西裤'
            when sp.third_cat_name='针织马甲' then '针织马甲'
            when sp.third_cat_name='短袖T恤' then '短袖T恤'
            when sp.third_cat_name='羽绒马甲' then '羽绒马甲'
            when sp.third_cat_name='大衣' then '大衣'
            when sp.third_cat_name='毛衣' then '毛衣'
            when sp.third_cat_name='棉服' then '棉服'
            when sp.third_cat_name='皮毛一体' then '皮毛一体'
            else '其他'
        end as third_cat_name
    from yishou_data.dws_goods_id_action_statistics_incr_dt sp
    where sp.dt = '${one_day_ago}'
) sp
left join yishou_data.dim_supply_info dsi on sp.supply_id_new = dsi.supply_id
left join yishou_data.dim_goods_no_info_full_h dgn on dgn.goods_no = sp.goods_no  -- 商品款, 用法较多
left join (
    SELECT 
        supply_id
        ,CASE                 
            WHEN supply_gmv >= 10000000 THEN '11_[1000w,+)'
            WHEN supply_gmv >=  8000000 THEN '10_[800-1000w)'
            WHEN supply_gmv >=  5000000 THEN '9_[500-800w)'
            WHEN supply_gmv >=  3000000 THEN '8_[300-500w)'
            WHEN supply_gmv >=  1000000 THEN '7_[100-300w)'
            WHEN supply_gmv >=   500000 THEN '6_[50-100w)'
            WHEN supply_gmv >=   300000 THEN '5_[30-50w)'
            WHEN supply_gmv >=   100000 THEN '4_[10-30w)'
            WHEN supply_gmv >=    50000 THEN '3_[5-10w)'
            WHEN supply_gmv >=    10000 THEN '2_[1-5w)'
            ELSE '1_[0-1w)'
        END AS supply_gmv_level
    FROM (
        SELECT 
            supply_id
            ,IFNULL(SUM(CAST(real_buy_amount AS DOUBLE)),0) supply_gmv
        FROM yishou_data.dws_goods_id_action_statistics_incr_dt
        WHERE dt between to_char(dateadd(to_date1('${one_day_ago}','yyyymmdd'),-29,'dd'),'yyyymmdd') and '${one_day_ago}'
        GROUP BY supply_id
    )
) mon on mon.supply_id = sp.supply_id_new 
-- 近7天
left join (
    select 
        supply_id
        ,goods_no
        ,sum(real_buy_amount) as seven_day_gmv
        ,sum(add_cart_uv) as seven_add_cart_uv
        ,sum(real_buy_num) as seven_buy_num
    from yishou_data.dws_goods_id_action_statistics_incr_dt
    where dt between to_char(dateadd(to_date1('${one_day_ago}','yyyymmdd'),-6,'dd'),'yyyymmdd') and '${one_day_ago}'
    group by supply_id,goods_no
) seven_day
on sp.supply_id_new = seven_day.supply_id and sp.goods_no = seven_day.goods_no
-- 近5天
left join (
    select 
        supply_id
        ,goods_no
        ,sum(real_buy_user_num) as five_buy_user_num
        ,sum(real_buy_amount) / sum(goods_exposure_uv) as five_uv_value
        ,sum(real_buy_amount) as five_goods_gmv
        ,sum(add_cart_uv) as five_add_cart_uv
        ,sum(real_buy_user_num) as five_pay_uv
    from yishou_data.dws_goods_id_action_statistics_incr_dt
    where dt between to_char(dateadd(to_date1('${one_day_ago}','yyyymmdd'),-4,'dd'),'yyyymmdd') and '${one_day_ago}'
    group by supply_id,goods_no
) five_day
on sp.supply_id_new = five_day.supply_id and sp.goods_no = five_day.goods_no
-- 近15天
left join (
     select 
        supply_id
        ,goods_no
        ,sum(real_buy_num) as fifth_buy_num
    from yishou_data.dws_goods_id_action_statistics_incr_dt
    where dt between to_char(dateadd(to_date1('${one_day_ago}','yyyymmdd'),-14,'dd'),'yyyymmdd') and '${one_day_ago}'
    group by supply_id,goods_no
) fifth_day
on sp.supply_id_new = fifth_day.supply_id and sp.goods_no = fifth_day.goods_no


