-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/01/10 12:50:58 GMT+08:00
-- ******************************************************************** --

-- CREATE TABLE `data_annual_bill` (
--   `id` int unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
--   `user_id` int unsigned NOT NULL DEFAULT '0' COMMENT '用户id',
--   `years` smallint unsigned NOT NULL DEFAULT '9' COMMENT '第几周年，本次录入9',
--   `total` decimal(11,2) unsigned NOT NULL DEFAULT '0.00' COMMENT '拿货额',
--   `total_exceeded_percentage` decimal(11,3) unsigned NOT NULL DEFAULT '0.000' COMMENT '累计拿货额打败用户数百分比，例如0.998即超越99.8%',
--   `traffic_cost` int unsigned NOT NULL DEFAULT '0' COMMENT '交通费',
--   `hotel_cost` int unsigned NOT NULL DEFAULT '0' COMMENT '住宿费',
--   `price_diff` decimal(11,2) unsigned NOT NULL DEFAULT '0.00' COMMENT '拿货差价',
--   `max_total` decimal(11,2) unsigned NOT NULL DEFAULT '0.00' COMMENT '拿货额最高的一天的拿货额',
--   `max_total_day` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '拿货额最高的日期：年-月-日',
--   `bought_supply_count` int unsigned NOT NULL DEFAULT '0' COMMENT '拿过的档口数',
--   `new_supply_count` int unsigned NOT NULL DEFAULT '0' COMMENT '尝鲜的档口数',
--   `login_activities_time` tinyint unsigned NOT NULL DEFAULT '0' COMMENT '登录活跃时间段，{1:凌晨：[2-5),2:早上[5-9),3:上午[9-11),4:中午[11-13),5:下午[13-16),6:傍晚[16-18),7:晚上[18-22),8:深夜[22-次日2点)}',
--   `early_order_time` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '最接近凌晨5点下单日期：年-月-日 时:分:秒',
--   `avg_order_time` int unsigned NOT NULL DEFAULT '0' COMMENT '平均下单时长，n小时',
--   `avg_order_exceeded_percentage` decimal(11,3) unsigned NOT NULL DEFAULT '0.000' COMMENT '平均下单时长超越人数百分比，例如0.998即超越99.8%',
--   `fastest_order_time` int unsigned NOT NULL DEFAULT '0' COMMENT '下单最快的时长，加购到支付最短的一次的时间，n秒',
--   `order_count` int unsigned NOT NULL DEFAULT '0' COMMENT '拿货件数',
--   `user_label` tinyint unsigned NOT NULL DEFAULT '0' COMMENT '用户标签{1：夜行猫，2：疾行豹，3： 先行羊，4： 纳金蛇，5：百宝象}',
--   `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
--   `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
--   PRIMARY KEY (`id`),
--   UNIQUE KEY `uniq_uid_years` (`user_id`,`years`) USING BTREE,
--   KEY `idx_user_id` (`user_id`)
-- ) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='周年账单';

-- CREATE TABLE `data_annual_bill_market_ranking` (
--   `id` int unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
--   `user_id` int unsigned NOT NULL DEFAULT '0' COMMENT '用户id',
--   `market_id` int unsigned NOT NULL DEFAULT '0' COMMENT '一级市场id',
--   `market_total_percentage` decimal(10,2) unsigned NOT NULL DEFAULT '0.00' COMMENT '一级市场拿货占比，占比0.52即52%，取该用户拿货最多一级市场top5',
--   `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
--   `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
--   PRIMARY KEY (`id`),
--   KEY `idx_user_id` (`user_id`)
-- ) ENGINE=InnoDB COMMENT='年度账单-用户拿货市场排行数据';

-- CREATE TABLE `data_annual_bill_supply_ranking` (
--   `id` int unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
--   `user_id` int unsigned NOT NULL DEFAULT '0' COMMENT '用户id',
--   `supply_id` int unsigned NOT NULL DEFAULT '0' COMMENT '档口id',
--   `bought_count` int unsigned NOT NULL DEFAULT '0' COMMENT '档口拿货次数，取该用户拿货最多的档口top8',
--   `created_at` timestamp NOT NULL COMMENT '创建时间',
--   `updated_at` timestamp NOT NULL COMMENT '更新时间',
--   PRIMARY KEY (`id`),
--   KEY `idx_user_id` (`user_id`)
-- ) ENGINE=InnoDB COMMENT='年度账单-用户拿货档口排行数据';

-- CREATE TABLE `data_annual_bill_season_count` (
--   `id` int unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
--   `user_id` int unsigned NOT NULL DEFAULT '0' COMMENT '用户id',
--   `season` tinyint unsigned NOT NULL DEFAULT '0' COMMENT '季节{1：春(2-4月)，2：夏(5-7月)，3：秋(8-10月)，4冬(11-1月)}',
--   `order_count` int unsigned NOT NULL DEFAULT '0' COMMENT '拿货件数',
--   `order_total` decimal(11,2) unsigned NOT NULL DEFAULT '0.00' COMMENT '拿货额度',
--   `supply_id` int unsigned NOT NULL DEFAULT '0' COMMENT '偏好档口id',
--   `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
--   `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
--   PRIMARY KEY (`id`),
--   KEY `idx_user_id` (`user_id`)
-- ) ENGINE=InnoDB COMMENT='年度账单-用户季节数据统计';

---------------------------------------------前置表-------------------------------------------------------
-- -- 想要快点出数据，所以直接用底层表
-- drop table if exists yishou_data.temp_rec_id_info_20250110;
-- create table yishou_data.temp_rec_id_info_20250110 as 
-- select 
--     order_info.user_id
--     , order_info.supply_id
--     , order_info.goods_no
--     , order_info.buy_num
--     , order_info.shop_price
--     , order_info.rec_id
--     , order_info.order_id 
--     , mi.primary_market_id
--     , order_info.add_time
--     , order_info.pay_time
--     , order_info.sa_add_cart_time 
-- from yishou_data.dwd_sale_order_info_dt order_info
-- left join yishou_data.dim_market_info mi on order_info.market_id = mi.market_id
-- -- 24年全年
-- where dt between '20240101' and '20241231'
-- and to_char(order_info.add_time,'yyyymmdd') between '20240101' and '20241231'
-- -- 实际支付逻辑
-- and order_info.is_real_pay = 1
-- ;

------------------------------------------ 周年账单----------------------------------------------------------
-- insert overwrite table yishou_daily.temp_data_annual_bill_9_year
drop table if exists yishou_data.temp_data_annual_bill_9_year;
create table yishou_data.temp_data_annual_bill_9_year 
as
select 
    a.user_id
    , 9 as years
    , coalesce(round(a.total,2),0.00) as total
    , coalesce(round(a.total_exceeded_percentage,3),0.000) as total_exceeded_percentage
    , coalesce(a.traffic_cost,0) as traffic_cost
    , coalesce(a.hotel_cost,0) as hotel_cost
    , coalesce(round(a.price_diff,2),0.00) as price_diff
    , coalesce(round(b.max_total,2),0.00) as max_total
    , coalesce(b.max_total_day,'0000-00-00 00:00:00') as max_total_day
    , coalesce(c.bought_supply_count,0) as bought_supply_count
    , coalesce(c.new_supply_count,0) as new_supply_count
    , coalesce(d.login_activities_time,0) as login_activities_time
    , coalesce(e.early_order_time,'0000-00-00 00:00:00') as early_order_time
    , coalesce(f.avg_order_time,0) as avg_order_time
    , coalesce(round(f.avg_order_exceeded_percentage,3),0.000) as avg_order_exceeded_percentage
    , coalesce(g.fastest_order_time,0) as fastest_order_time
    , coalesce(a.order_count,0) as order_count
    , CASE 
        WHEN e.user_label_1 = greatest(e.user_label_1,g.user_label_2,c.user_label_3,a.total_exceeded_percentage,c.user_label_5)
        THEN 1
        WHEN g.user_label_2 = greatest(e.user_label_1,g.user_label_2,c.user_label_3,a.total_exceeded_percentage,c.user_label_5)
        THEN 2
        WHEN c.user_label_3 = greatest(e.user_label_1,g.user_label_2,c.user_label_3,a.total_exceeded_percentage,c.user_label_5)
        THEN 3
        WHEN a.total_exceeded_percentage = greatest(e.user_label_1,g.user_label_2,c.user_label_3,a.total_exceeded_percentage,c.user_label_5)
        THEN 4
        WHEN c.user_label_5 = greatest(e.user_label_1,g.user_label_2,c.user_label_3,a.total_exceeded_percentage,c.user_label_5)
        THEN 5 
        else 0
    end as user_label
from (
    select 
        user_id
        ,total
        ,traffic_cost
        ,hotel_cost
        ,price_diff
        ,rank_num
        -- 拿货额超过的用户数（有实际下单的用户） / 24年全年有下单的用户数（实际下单）
        -- 纳金蛇
        ,case 
            when (total_user - rank_num) / total_user >= 0.99 then 0.99
            else round((total_user - rank_num) / total_user,3)
        end as total_exceeded_percentage
        ,order_count
    from (
        select 
            oi.user_id
            ,sum(oi.buy_num*oi.shop_price) as total
            ,row_number() over(order by sum(oi.buy_num*oi.shop_price) desc) as rank_num
            -- 交通费  用户有下单的日期*500元
            ,count(distinct to_char(oi.add_time,'yyyymmdd'))*500 as traffic_cost 
            -- 住宿费 用户有下单的日期*300元
            ,count(distinct to_char(oi.add_time,'yyyymmdd'))*300 as hotel_cost 
            -- 拿货差价 用户拿货额*0.28
            ,sum(oi.buy_num*oi.shop_price)*0.28 as price_diff
            -- 拿货件数
            ,sum(oi.buy_num) as order_count
        from yishou_data.temp_rec_id_info_20250110 oi
        group by oi.user_id
    ) a
    left join (
        -- 累计总用户数
        select count(DISTINCT user_id) as total_user from yishou_data.temp_rec_id_info_20250110
    ) total_user on 1=1
)a
left join (
    -- 拿货额最大的那天及金额
    select 
        user_id,max_total,max_total_day
    from (
        select
            oi.user_id
            ,to_char(oi.add_time,'yyyy-mm-dd 00:00:00') as max_total_day
            ,SUM(oi.buy_num * oi.shop_price) as max_total
            ,ROW_NUMBER() OVER(PARTITION BY oi.user_id ORDER BY SUM(oi.buy_num * oi.shop_price) DESC) as rank_num
        from yishou_data.temp_rec_id_info_20250110 oi
        group by 1,2
    )
    where rank_num = 1 
)b on a.user_id = b.user_id
left join ( 
    -- 拿货多的档口
    select 
        user_id
        ,bought_supply_count
        ,new_supply_count
        -- 先行羊
        ,case 
            when (total_user - rank_num) / total_user >= 0.99 then 0.99
            else round((total_user - rank_num) / total_user,3)
        end as user_label_3
        -- 百宝象
        ,case 
            when (total_user - rank_num_2) / total_user >= 0.99 then 0.99
            else round((total_user - rank_num_2) / total_user,3)
        end as user_label_5
    from (
        select 
            user_id
            ,bought_supply_count
            ,new_supply_count
            ,ROW_NUMBER() OVER(ORDER BY new_supply_count DESC) as rank_num
            ,ROW_NUMBER() OVER(ORDER BY bought_supply_count DESC) as rank_num_2
        from (
            select 
                oi.user_id
                -- 取有拿过货的对应档口数量
                ,count(oi.supply_id) as bought_supply_count
                -- 取用户今年所有有拿过的档口，然后看下这些档口哪些是2024年新入驻的
                ,count(
                    case 
                        when to_char(from_unixtime(fsr.create_time),'yyyymmdd') between '20240101' and '20241231' then oi.supply_id 
                        else null 
                    end
                ) as new_supply_count
            from yishou_data.temp_rec_id_info_20250110 oi
            left join yishou_data.all_fmys_supply_readonly fsr on oi.supply_id = fsr.supply_id
            -- where oi.user_id in ('65','933759','14628085')
            group by oi.user_id
        )
    ) supply_user 
    left join (
        -- 累计总用户数
        select count(DISTINCT user_id) as total_user from yishou_data.temp_rec_id_info_20250110
    ) total_user on 1=1
) c on a.user_id = c.user_id
left join (
    -- 登录时间
    select 
        user_id
        ,max(login_activities_time) as login_activities_time
    from (
        select 
            user_id
            ,case 
                when hour(time) >=2 and hour(time) < 5 then 1 
                when hour(time) >=5 and hour(time) < 9 then 2 
                when hour(time) >=9 and hour(time) < 11 then 3 
                when hour(time) >=11 and hour(time) < 13 then 4 
                when hour(time) >=13 and hour(time) < 16 then 5 
                when hour(time) >=16 and hour(time) < 18 then 6 
                when hour(time) >=18 and hour(time) < 22 then 7
                when hour(time) >=22 and hour(time) < 24 then 8 
                when hour(time) >=0 and hour(time) < 2 then 8 
            end as login_activities_time
            ,ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY count(hour(time)) DESC) as rank_num
        from yishou_data.dwd_log_app_wx_login_dt
        where dt between '20240101' and '20241231'
        -- where dt = '20250109'
        and type = 'app'
        group by 1,2
        -- limit 1000
    )
    where rank_num = 1
    group by user_id
) d on a.user_id = d.user_id
left join (
    -- 下单时间最接近凌晨5点
    select 
        user_id
        ,early_order_time
        -- 夜行猫
        ,case 
            when (total_user - rank_num_2) / total_user >= 0.99 then 0.99
            else round((total_user - rank_num_2) / total_user,3)
        end as user_label_1
    from (
        select 
            user_id
            , max(early_order_time) as early_order_time
            , row_number() over(
                order by to_char(dateadd(max(early_order_time),-5,'hh'),'hh') desc
                ,to_char(dateadd(max(early_order_time),-5,'hh'),'mi') desc
                ,to_char(dateadd(max(early_order_time),-5,'hh'),'ss') desc
            ) as rank_num_2
        from (
            select 
                oi.user_id
                ,oi.add_time as early_order_time
                ,row_number() over(
                    partition by oi.user_id 
                    order by to_char(dateadd(oi.add_time,-5,'hh'),'hh') desc,to_char(dateadd(oi.add_time,-5,'hh'),'mi') desc,to_char(dateadd(oi.add_time,-5,'hh'),'ss') desc
                ) as rank_num
            from yishou_data.temp_rec_id_info_20250110 oi
            -- where user_id in ('65','933759','14628085')
        )
        where rank_num = 1
        group by user_id
    ) add_order_user
    left join (
        -- 全年最晚下单时间的所有用户(分子分母均为在22点到次日凌晨5点下单的用户)
        select count(DISTINCT user_id) as total_user from yishou_data.temp_rec_id_info_20250110 
        where (hour(add_time) >= 22 and hour(add_time) <=24) or (hour(add_time) >= 0 and hour(add_time) <=5)
    ) total_add_order_user on 1=1
) e on e.user_id = a.user_id
left join (
    select 
        user_login.user_id
        ,user_login.avg_order_time
        ,user_login.rank_num
        -- 平均时长超过的用户数 / 24年全年有下单的用户数（实际下单）
        ,case 
            when (total_user.total_user - user_login.rank_num) / total_user.total_user >= 0.99 then 0.99
            else round((total_user.total_user - user_login.rank_num) / total_user.total_user,3)
        end as avg_order_exceeded_percentage 
    from (
        -- 平均登录时长
        select 
            login.user_id
            ,round(avg(total_login_user * 24 / total_order_user),0) as avg_order_time
            ,row_number() over(PARTITION by login.user_id order by avg(total_login_user * 24 / total_order_user) desc) as rank_num
        from (
            select user_id,count(1) as total_login_user from yishou_data.dwd_log_app_wx_login_dt
            where dt between '20240101' and '20241231' and type = 'app'
            group by user_id
        ) login
        left join (
            select oi.user_id,count(oi.order_id) as total_order_user from yishou_data.temp_rec_id_info_20250110 oi group by oi.user_id
        ) order_user
        on login.user_id = order_user.user_id
        group by login.user_id
    ) user_login
    left join (
        -- 累计总用户数
        select count(DISTINCT user_id) as total_user from yishou_data.temp_rec_id_info_20250110
    ) total_user on 1=1
) f on f.user_id = a.user_id
left join (
    -- 加购到支付最快的时间（同一订单内）
    select 
        user_id
        ,fastest_order_time
        -- 急行豹
        ,case 
            when (total_user - rank_num_2) / total_user >= 0.99 then 0.99
            else round((total_user - rank_num_2) / total_user,3)
        end as user_label_2
    from (
        select 
           user_id
           ,min(fastest_order_time) as fastest_order_time
           ,ROW_NUMBER() OVER(ORDER BY min(fastest_order_time) asc) as rank_num_2
        from (
            select 
                oi.user_id
                ,oi.order_id
                ,unix_timestamp(oi.pay_time) - unix_timestamp(oi.sa_add_cart_time) as fastest_order_time
                ,ROW_NUMBER() OVER(
                    PARTITION BY oi.user_id,oi.order_id 
                    ORDER BY (unix_timestamp(oi.pay_time) - unix_timestamp(oi.sa_add_cart_time)) asc
                ) as rank_num
            from yishou_data.temp_rec_id_info_20250110 oi
            -- where oi.user_id in ('65','933759','14628085')
        )
        where rank_num = 1
        group by user_id
    ) fast_user
    left join (
        -- 累计总用户数
        select count(DISTINCT user_id) as total_user from yishou_data.temp_rec_id_info_20250110
    ) total_user on 1=1
) g on a.user_id = g.user_id
;

--------------------------------------- 年度账单-用户拿货市场排行数据 ----------------------------------
-- insert overwrite table temp_data_annual_bill_market_ranking_9_year
drop table if exists yishou_data.temp_data_annual_bill_market_ranking_9_year;
create table yishou_data.temp_data_annual_bill_market_ranking_9_year 
as
-- 用户拿货最多的top5市场及对应占比
select
    coalesce(a.user_id,0) as user_id
    ,coalesce(a.primary_market_id,0) as market_id
    ,coalesce(max(round(a.market_total / b.total,2)),0.00) as market_total_percentage
from (
    select 
        oi.user_id
        ,oi.primary_market_id
        ,ROW_NUMBER() OVER(PARTITION BY oi.user_id ORDER BY SUM(oi.buy_num * oi.shop_price) DESC,oi.primary_market_id asc) as rank_num
        ,SUM(oi.buy_num * oi.shop_price) as market_total
    from yishou_data.temp_rec_id_info_20250110 oi
    group by oi.user_id,oi.primary_market_id
) a
left join (
    select oi.user_id,SUM(oi.buy_num * oi.shop_price) as total 
    from yishou_data.temp_rec_id_info_20250110 oi
    group by oi.user_id
) b on a.user_id = b.user_id
where rank_num <= 5 
group by a.user_id,a.primary_market_id
;

-------------------------------------------年度账单-用户拿货档口排行数据-----------------------------------
-- insert OVERWRITE table temp_data_annual_bill_supply_ranking_9_year
drop table if exists yishou_data.temp_data_annual_bill_supply_ranking_9_year;
create table yishou_data.temp_data_annual_bill_supply_ranking_9_year 
as
select 
    coalesce(user_id,0) as user_id
    ,coalesce(supply_id,0) as supply_id
    ,coalesce(bought_count,0) as bought_count
from (
    select 
        oi.user_id
        ,oi.supply_id
        ,count(oi.supply_id) as bought_count
        -- 以档口次数排名，一个用户相同次数时按档口id排序
        ,ROW_NUMBER() OVER(PARTITION BY oi.user_id ORDER BY count(oi.supply_id) DESC,oi.supply_id asc) as rank_num
    from yishou_data.temp_rec_id_info_20250110 oi
    -- where oi.user_id = '65'
    group by oi.user_id,oi.supply_id 
)
where rank_num <= 8
;

------------------------------------------年度账单-用户季节数据统计-----------------------------------------
-- insert OVERWRITE table temp_data_annual_bill_season_count_9_year
drop table if exists yishou_data.temp_data_annual_bill_season_count_9_year;
create table yishou_data.temp_data_annual_bill_season_count_9_year 
as
select 
    coalesce(a.user_id,0) as user_id
    ,coalesce(a.season,0) as season
    ,coalesce(a.order_count,0) as order_count
    ,coalesce(a.order_total,0) as order_total
    ,coalesce(b.supply_id,0) as supply_id
from (
    select 
        oi.user_id
        ,case 
            when month(oi.add_time) >= 2 and month(oi.add_time) < 5 then 1
            when month(oi.add_time) >= 5 and month(oi.add_time) < 8 then 2
            when month(oi.add_time) >= 8 and month(oi.add_time) < 11 then 3
            when month(oi.add_time) >= 11 and month(oi.add_time) <= 12 then 4
            else 4
        end as season 
        ,coalesce(sum(oi.buy_num),0) as order_count
        ,coalesce(sum(oi.buy_num*oi.shop_price),2) as order_total
    from yishou_data.temp_rec_id_info_20250110 oi
    -- where oi.user_id = '72878'
    group by 1,2
) a 
left join (
    select 
        user_id
        ,season
        ,supply_id
    from (
        select 
            user_id
            ,season
            ,supply_id
            ,supply_user_count
            ,ROW_NUMBER() OVER(PARTITION BY user_id,season ORDER BY supply_user_count DESC) as rank_num
        from (
            select 
                oi.user_id
                ,case 
                    when month(oi.add_time) >= 2 and month(oi.add_time) < 5 then 1
                    when month(oi.add_time) >= 5 and month(oi.add_time) < 8 then 2
                    when month(oi.add_time) >= 8 and month(oi.add_time) < 11 then 3
                    when month(oi.add_time) >= 11 and month(oi.add_time) <= 12 then 4
                    else 4
                end as season
                ,oi.supply_id
                ,sum(oi.buy_num) as supply_user_count
            from yishou_data.temp_rec_id_info_20250110 oi
            -- where user_id = '72878'
            group by 1,2,3
        )
    )
    where rank_num = 1
) b on a.user_id = b.user_id and a.season = b.season
;

------------------------------平台维度，单独取，不走表-------------------------------------------------------
-- -- 2024新增档口数： new_supply 
-- select count(supply_id) as new_supply from yishou_data.all_fmys_supply where to_char(from_unixtime(create_time),'yyyy') = '2024';
-- -- 2024上架款数：new_goods 
-- select count(goods_no) as new_goods from yishou_data.dim_goods_no_info_full_h where to_char(first_up_time,'yyyy') = '2024';
-- -- 2024上架款数增加百分比：goods_growth_percentage
-- select (a.goods_2024 / b.goods_2023) - 1 as goods_growth_percentage 
-- from (
--     select count(goods_no) as goods_2024 from yishou_data.dim_goods_no_info_full_h
--     where to_char(first_up_time,'yyyy') = '2024'
-- ) a 
-- left join (
--     select count(goods_no) as goods_2023 from yishou_data.dim_goods_no_info_full_h
--     where to_char(first_up_time,'yyyy') = '2023'
-- ) b on 1=1
-- ;
-- 2024快了x天
-- select
--     abs(round(a.shipping_speed_2024 - b.shipping_speed_2023,6)) as shipping_speed_faster_day
-- from (
--     select
--         sum(unix_timestamp(shipping.first_create_time) - unix_timestamp(from_unixtime(fs.special_add_time))) / 60 / sum(shipping.order_total)  as shipping_speed_2024
--     from yishou_data.all_fmys_order fo
--     join yishou_data.all_fmys_order_infos foi on fo.order_id = foi.order_id
--     left join (
--         select 
--             order_id
--             ,count(if(create_time is not null,order_id,null)) as order_total
--             ,min(from_unixtime(create_time)) as first_create_time
--         from yishou_data.all_fmys_shipping_detail 
--         where to_char(from_unixtime(create_time),'yyyy') = '2024'
--         group by order_id 
--     ) shipping on shipping.order_id = fo.order_id
--     left join yishou_data.all_fmys_special fs on fs.special_id = foi.special_id
--     where to_char(from_unixtime(fo.add_time),'yyyy') = '2024'
-- ) a
-- left join (
        -- 取2023年每一笔有发货的订单，然后算这笔订单的 【首个发货时间 - 专场结束时间】/24，算出每一笔订单的发货时间（单位天），然后每笔订单发货时间求和 / 有发货的订单数 = 2023年的平均发货时长
--     select
--         sum((unix_timestamp(shipping.first_create_time) - unix_timestamp(from_unixtime(fs.special_add_time)))) / 60 / sum(shipping.order_total) as shipping_speed_2023
--     from yishou_data.all_fmys_order fo
--     join yishou_data.all_fmys_order_infos foi on fo.order_id = foi.order_id
--     left join (
--         select 
--             order_id
--             ,count(if(create_time is not null,order_id,null)) as order_total
--             ,min(from_unixtime(create_time)) as first_create_time
--         from yishou_data.all_fmys_shipping_detail 
--         where to_char(from_unixtime(create_time),'yyyy') = '2023'
--         group by order_id 
--     ) shipping on shipping.order_id = fo.order_id
--     left join yishou_data.all_fmys_special fs on fs.special_id = foi.special_id
--     where to_char(from_unixtime(fo.add_time),'yyyy') = '2023'
-- ) b on 1=1
-- ;
-----------------------------------------------------------------------------------------------------
