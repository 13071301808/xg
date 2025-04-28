-- 算法训练表
-- https://yb7ao262ru.feishu.cn/wiki/MdKGwxbLMi00brk8zYDcTjsGnfh
WITH exposure_table AS (
    SELECT 
        user_id
        , event_id
        , special_id
        , dt
        , receive_time
    FROM yishou_data.dwd_app_goods_exposure_view
    WHERE dt = '${gmtdate}'
    AND pid = 10
    AND coalesce(user_id, '') != ''
    AND coalesce(event_id, '') != ''
    AND coalesce(special_id, '') != ''
    AND coalesce(receive_time, '') != ''
    -- 验数
    -- AND user_id = '2248'
)
,click_table as (
    SELECT 
        user_id
        , event_id
        , special_id
        , dt
        , receive_time
    FROM yishou_data.dwd_app_goods_click_view
    WHERE dt = '${gmtdate}'
    AND pid = 10
    AND coalesce(user_id, '') != ''
    AND coalesce(event_id, '') != ''
    AND coalesce(special_id, '') != ''
    AND coalesce(receive_time, '') != ''
    -- 验数
    -- AND user_id = '2248'
)
,wide_table as (
    SELECT
        coalesce(click_table.user_id,  exposure_table.user_id) AS user_id
        , coalesce(click_table.event_id, exposure_table.event_id)  AS event_id
        , coalesce(click_table.special_id, exposure_table.special_id) AS special_id
        , coalesce(click_table.dt, exposure_table.dt) AS dt
        , if(click_table.receive_time IS NOT NULL, 1, 0) AS label
        , coalesce(click_table.receive_time, exposure_table.receive_time) AS receive_time
        , row_number() over (
            partition by coalesce(click_table.event_id, exposure_table.event_id), coalesce(click_table.special_id, exposure_table.special_id)
            order by if(click_table.receive_time IS NOT NULL, 1, 0) desc, coalesce(click_table.receive_time, exposure_table.receive_time)
        ) as rn
    FROM exposure_table
    FULL JOIN click_table
    ON exposure_table.user_id = click_table.user_id
    AND exposure_table.event_id = click_table.event_id
    AND exposure_table.special_id = click_table.special_id
    AND exposure_table.dt = click_table.dt
    -- 验数
    -- WHERE coalesce(click_table.user_id,  exposure_table.user_id) = '2248'
)
,filtered_wide_table as (
    SELECT
        user_id
        , event_id
        , special_id
        , dt
        , label
        , receive_time
    FROM wide_table
    WHERE rn = 1
)
,score_detail_table as (
    -- 明细特征
    SELECT 
        filtered_wide_table.user_id
        , filtered_wide_table.event_id
        , filtered_wide_table.receive_time
        , filtered_wide_table.special_id
        -- 档口
        -- , special_info.supply_id_1
        -- , special_info.supply_id_2
        -- , special_info.supply_id_3
        , round(special_info.supply_sale_ratio_1,2) as supply_sale_ratio_1
        , round(special_info.supply_sale_ratio_2,2) as supply_sale_ratio_2
        , round(special_info.supply_sale_ratio_3,2) as supply_sale_ratio_3
        , case when su_score1.supply_id is not null then round(su_score1.score,2) end as supply_180_score1
        , case when su_score2.supply_id is not null then round(su_score2.score,2) end as supply_180_score2
        , case when su_score3.supply_id is not null then round(su_score3.score,2) end as supply_180_score3
        , case when su_15_score1.supply_id is not null then round(su_15_score1.score,2) end as supply_15_score1
        , case when su_15_score2.supply_id is not null then round(su_15_score2.score,2) end as supply_15_score2
        , case when su_15_score3.supply_id is not null then round(su_15_score3.score,2) end as supply_15_score3
        -- 档口实时曝光点击
        , case 
            when supply_realtime_click1.supply_id is not null then round(supply_realtime_click1.user_supply_exposure_click_ratio,2) 
        end as supply_realtime_click_score1
        , case 
            when supply_realtime_click2.supply_id is not null then round(supply_realtime_click2.user_supply_exposure_click_ratio,2) 
        end as supply_realtime_click_score2
        , case 
            when supply_realtime_click3.supply_id is not null then round(supply_realtime_click3.user_supply_exposure_click_ratio,2) 
        end as supply_realtime_click_score3
        -- 档口实时曝光加购
        , case 
            when supply_realtime_addcart1.supply_id is not null then round(supply_realtime_addcart1.user_supply_exposure_add_cart_ratio,2) 
        end as supply_realtime_addcart_score1
        , case 
            when supply_realtime_addcart2.supply_id is not null then round(supply_realtime_addcart2.user_supply_exposure_add_cart_ratio,2) 
        end as supply_realtime_addcart_score2
        , case 
            when supply_realtime_addcart3.supply_id is not null then round(supply_realtime_addcart3.user_supply_exposure_add_cart_ratio,2) 
        end as supply_realtime_addcart_score3
        -- 档口实时曝光支付
        , case 
            when supply_realtime_pay1.supply_id is not null then round(supply_realtime_pay1.user_supply_exposure_pay_ratio,2) 
        end as supply_realtime_pay_score1
        , case 
            when supply_realtime_pay2.supply_id is not null then round(supply_realtime_pay2.user_supply_exposure_pay_ratio,2) 
        end as supply_realtime_pay_score2
        , case 
            when supply_realtime_pay3.supply_id is not null then round(supply_realtime_pay3.user_supply_exposure_pay_ratio,2) 
        end as supply_realtime_pay_score3
        -- 品类
        -- , special_info.cat_id_1
        -- , special_info.cat_id_2
        -- , special_info.cat_id_3
        , round(special_info.cat_sale_ratio_1,2) as cat_sale_ratio_1
        , round(special_info.cat_sale_ratio_2,2) as cat_sale_ratio_2
        , round(special_info.cat_sale_ratio_3,2) as cat_sale_ratio_3
        , case when cat_score1.third_cat_id is not null then round(cat_score1.score,2) end as cat_180_score1
        , case when cat_score2.third_cat_id is not null then round(cat_score2.score,2) end as cat_180_score2
        , case when cat_score3.third_cat_id is not null then round(cat_score3.score,2) end as cat_180_score3
        , case when cat_15_score1.third_cat_id is not null then round(cat_15_score1.score,2) end as cat_15_score1
        , case when cat_15_score2.third_cat_id is not null then round(cat_15_score2.score,2) end as cat_15_score2
        , case when cat_15_score3.third_cat_id is not null then round(cat_15_score3.score,2) end as cat_15_score3
        -- 品类实时曝光点击
        , case 
            when cat_realtime_click1.cat_id is not null then round(cat_realtime_click1.user_cat_exposure_click_ratio,2) 
        end as cat_realtime_click_score1
        , case 
            when cat_realtime_click2.cat_id is not null then round(cat_realtime_click2.user_cat_exposure_click_ratio,2) 
        end as cat_realtime_click_score2
        , case 
            when cat_realtime_click3.cat_id is not null then round(cat_realtime_click3.user_cat_exposure_click_ratio,2) 
        end as cat_realtime_click_score3
        -- 品类实时曝光加购
        , case 
            when cat_realtime_addcart1.cat_id is not null then round(cat_realtime_addcart1.user_cat_exposure_add_cart_ratio,2) 
        end as cat_realtime_addcart_score1
        , case 
            when cat_realtime_addcart2.cat_id is not null then round(cat_realtime_addcart2.user_cat_exposure_add_cart_ratio,2) 
        end as cat_realtime_addcart_score2
        , case 
            when cat_realtime_addcart3.cat_id is not null then round(cat_realtime_addcart3.user_cat_exposure_add_cart_ratio,2) 
        end as cat_realtime_addcart_score3
        -- 品类实时曝光支付
        , case 
            when cat_realtime_pay1.cat_id is not null then round(cat_realtime_pay1.user_cat_supply_exposure_pay_ratio,2) 
        end as cat_realtime_pay_score1
        , case 
            when cat_realtime_pay2.cat_id is not null then round(cat_realtime_pay2.user_cat_supply_exposure_pay_ratio,2) 
        end as cat_realtime_pay_score2
        , case 
            when cat_realtime_pay3.cat_id is not null then round(cat_realtime_pay3.user_cat_supply_exposure_pay_ratio,2) 
        end as cat_realtime_pay_score3
        -- 风格
        -- , special_info.style_id_1
        , round(special_info.style_sale_ratio_1,2) as style_sale_ratio_1
        , case when style_score1.style_id is not null then round(style_score1.score,2) end as style_180_score1
        , case when style_15_score1.style_id is not null then round(style_15_score1.score,2) end as style_15_score1
        -- 大市场
        -- , special_info.big_market_id_1
        -- , special_info.big_market_id_2
        , round(special_info.big_market_sale_ratio_1,2) as big_market_sale_ratio_1
        , round(special_info.big_market_sale_ratio_2,2) as big_market_sale_ratio_2
        , case when market_score1.big_market_id is not null then round(market_score1.score,2) end as market_180_score1
        , case when market_15_score1.big_market_id is not null then round(market_15_score1.score,2) end as market_15_score1
        , filtered_wide_table.label as label
        , filtered_wide_table.dt
    FROM filtered_wide_table
    LEFT JOIN (
        SELECT * FROM yishou_daily.dtl_special_goods_label_choose_ht
        WHERE dt = '${gmtdate}' 
        and substr(ht,1,8) = '${gmtdate}'
    ) AS special_info
    ON filtered_wide_table.special_id = special_info.special_id 
    AND filtered_wide_table.dt = special_info.dt and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht
    -- 档口180天长期特征
    LEFT JOIN (select * from yishou_data.dwt_user_supply_score_180day where dt = '${one_day_ago}') su_score1
    ON su_score1.user_id = filtered_wide_table.user_id AND su_score1.supply_id = special_info.supply_id_1 
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht
    LEFT JOIN (select * from yishou_data.dwt_user_supply_score_180day where dt = '${one_day_ago}') su_score2
    ON su_score2.user_id = filtered_wide_table.user_id AND su_score2.supply_id = special_info.supply_id_2
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht
    LEFT JOIN (select * from yishou_data.dwt_user_supply_score_180day where dt = '${one_day_ago}') su_score3
    ON su_score3.user_id = filtered_wide_table.user_id AND su_score3.supply_id = special_info.supply_id_3 
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht
    -- 档口15天短期特征
    LEFT JOIN (select * from yishou_data.dwt_user_supply_score_15day where dt = '${one_day_ago}') su_15_score1
    ON su_15_score1.user_id = filtered_wide_table.user_id AND su_15_score1.supply_id = special_info.supply_id_1 
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht
    LEFT JOIN (select * from yishou_data.dwt_user_supply_score_15day where dt = '${one_day_ago}') su_15_score2
    ON su_15_score2.user_id = filtered_wide_table.user_id AND su_15_score2.supply_id = special_info.supply_id_2
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht
    LEFT JOIN (select * from yishou_data.dwt_user_supply_score_15day where dt = '${one_day_ago}') su_15_score3
    ON su_15_score3.user_id = filtered_wide_table.user_id AND su_15_score3.supply_id = special_info.supply_id_3 
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht
    -- 品类180天长期特征
    LEFT JOIN (select * from yishou_data.dwt_user_third_cat_score_180day where dt = '${one_day_ago}') cat_score1
    ON cat_score1.user_id = filtered_wide_table.user_id AND cat_score1.third_cat_id = special_info.cat_id_1 
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht
    LEFT JOIN (select * from yishou_data.dwt_user_third_cat_score_180day where dt = '${one_day_ago}') cat_score2
    ON cat_score2.user_id = filtered_wide_table.user_id AND cat_score2.third_cat_id = special_info.cat_id_2
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht
    LEFT JOIN (select * from yishou_data.dwt_user_third_cat_score_180day where dt = '${one_day_ago}') cat_score3
    ON cat_score3.user_id = filtered_wide_table.user_id AND cat_score3.third_cat_id = special_info.cat_id_3 
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht
    -- 品类15天短期特征
    LEFT JOIN (select * from yishou_data.dwt_user_third_cat_score_15day where dt = '${one_day_ago}') cat_15_score1
    ON cat_15_score1.user_id = filtered_wide_table.user_id AND cat_15_score1.third_cat_id = special_info.cat_id_1 
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht
    LEFT JOIN (select * from yishou_data.dwt_user_third_cat_score_15day where dt = '${one_day_ago}') cat_15_score2
    ON cat_15_score2.user_id = filtered_wide_table.user_id AND cat_15_score2.third_cat_id = special_info.cat_id_2
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht
    LEFT JOIN (select * from yishou_data.dwt_user_third_cat_score_15day where dt = '${one_day_ago}') cat_15_score3
    ON cat_15_score3.user_id = filtered_wide_table.user_id AND cat_15_score3.third_cat_id = special_info.cat_id_3 
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht
    -- 风格180天长期特征
    LEFT JOIN (select * from yishou_data.dwt_user_style_score_180day where dt = '${one_day_ago}') style_score1
    ON style_score1.user_id = filtered_wide_table.user_id AND style_score1.style_id = special_info.style_id_1 
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht
    -- 风格15天短期特征
    LEFT JOIN (select * from yishou_data.dwt_user_style_score_15day where dt = '${one_day_ago}') style_15_score1
    ON style_15_score1.user_id = filtered_wide_table.user_id AND style_15_score1.style_id = special_info.style_id_1 
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht
    -- 大市场180天长期特征
    LEFT JOIN (select * from yishou_data.dwt_user_big_market_score_180day where dt = '${one_day_ago}') market_score1
    ON market_score1.user_id = filtered_wide_table.user_id AND market_score1.big_market_id = special_info.big_market_id_1
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht
    -- 大市场15天短期特征
    LEFT JOIN (select * from yishou_data.dwt_user_big_market_score_15day where dt = '${one_day_ago}') market_15_score1
    ON market_15_score1.user_id = filtered_wide_table.user_id AND market_15_score1.big_market_id = special_info.big_market_id_1
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht
    -- 实时偏好特征
    -- 实时档口曝光点击特征
    LEFT JOIN (select * from yishou_data.dws_user_goods_cutoff_current_time_statistics_full_dt where dt = '${one_day_ago}') supply_realtime_click1
    ON supply_realtime_click1.user_id = filtered_wide_table.user_id AND supply_realtime_click1.supply_id = special_info.supply_id_1
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht and filtered_wide_table.receive_time = supply_realtime_click1.receive_time
    LEFT JOIN (select * from yishou_data.dws_user_goods_cutoff_current_time_statistics_full_dt where dt = '${one_day_ago}') supply_realtime_click2
    ON supply_realtime_click2.user_id = filtered_wide_table.user_id AND supply_realtime_click2.supply_id = special_info.supply_id_2
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht and filtered_wide_table.receive_time = supply_realtime_click2.receive_time
    LEFT JOIN (select * from yishou_data.dws_user_goods_cutoff_current_time_statistics_full_dt where dt = '${one_day_ago}') supply_realtime_click3
    ON supply_realtime_click3.user_id = filtered_wide_table.user_id AND supply_realtime_click3.supply_id = special_info.supply_id_3
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht and filtered_wide_table.receive_time = supply_realtime_click3.receive_time
    -- 实时档口曝光加购特征
    LEFT JOIN (select * from yishou_data.dws_user_goods_cutoff_current_time_statistics_full_dt where dt = '${one_day_ago}') supply_realtime_addcart1
    ON supply_realtime_addcart1.user_id = filtered_wide_table.user_id AND supply_realtime_addcart1.supply_id = special_info.supply_id_1
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht and filtered_wide_table.receive_time = supply_realtime_addcart1.receive_time
    LEFT JOIN (select * from yishou_data.dws_user_goods_cutoff_current_time_statistics_full_dt where dt = '${one_day_ago}') supply_realtime_addcart2
    ON supply_realtime_addcart2.user_id = filtered_wide_table.user_id AND supply_realtime_addcart2.supply_id = special_info.supply_id_2
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht and filtered_wide_table.receive_time = supply_realtime_addcart2.receive_time
    LEFT JOIN (select * from yishou_data.dws_user_goods_cutoff_current_time_statistics_full_dt where dt = '${one_day_ago}') supply_realtime_addcart3
    ON supply_realtime_addcart3.user_id = filtered_wide_table.user_id AND supply_realtime_addcart3.supply_id = special_info.supply_id_3
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht and filtered_wide_table.receive_time = supply_realtime_addcart3.receive_time
    -- 实时档口曝光支付特征
    LEFT JOIN (select * from yishou_data.dws_user_goods_cutoff_current_time_statistics_full_dt where dt = '${one_day_ago}') supply_realtime_pay1
    ON supply_realtime_pay1.user_id = filtered_wide_table.user_id AND supply_realtime_pay1.supply_id = special_info.supply_id_1
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht and filtered_wide_table.receive_time = supply_realtime_pay1.receive_time
    LEFT JOIN (select * from yishou_data.dws_user_goods_cutoff_current_time_statistics_full_dt where dt = '${one_day_ago}') supply_realtime_pay2
    ON supply_realtime_pay2.user_id = filtered_wide_table.user_id AND supply_realtime_pay2.supply_id = special_info.supply_id_2
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht and filtered_wide_table.receive_time = supply_realtime_pay2.receive_time
    LEFT JOIN (select * from yishou_data.dws_user_goods_cutoff_current_time_statistics_full_dt where dt = '${one_day_ago}') supply_realtime_pay3
    ON supply_realtime_pay3.user_id = filtered_wide_table.user_id AND supply_realtime_pay3.supply_id = special_info.supply_id_3
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht and filtered_wide_table.receive_time = supply_realtime_pay3.receive_time
    -- 实时品类曝光点击特征
    LEFT JOIN (select * from yishou_data.dws_user_goods_cutoff_current_time_statistics_full_dt where dt = '${one_day_ago}') cat_realtime_click1
    ON cat_realtime_click1.user_id = filtered_wide_table.user_id AND cat_realtime_click1.cat_id = special_info.cat_id_1
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht and filtered_wide_table.receive_time = cat_realtime_click1.receive_time
    LEFT JOIN (select * from yishou_data.dws_user_goods_cutoff_current_time_statistics_full_dt where dt = '${one_day_ago}') cat_realtime_click2
    ON cat_realtime_click2.user_id = filtered_wide_table.user_id AND cat_realtime_click2.cat_id = special_info.cat_id_2
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht and filtered_wide_table.receive_time = cat_realtime_click2.receive_time
    LEFT JOIN (select * from yishou_data.dws_user_goods_cutoff_current_time_statistics_full_dt where dt = '${one_day_ago}') cat_realtime_click3
    ON cat_realtime_click3.user_id = filtered_wide_table.user_id AND cat_realtime_click3.cat_id = special_info.cat_id_3
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht and filtered_wide_table.receive_time = cat_realtime_click3.receive_time
    -- 实时品类曝光加购特征
    LEFT JOIN (select * from yishou_data.dws_user_goods_cutoff_current_time_statistics_full_dt where dt = '${one_day_ago}') cat_realtime_addcart1
    ON cat_realtime_addcart1.user_id = filtered_wide_table.user_id AND cat_realtime_addcart1.cat_id = special_info.cat_id_1
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht and filtered_wide_table.receive_time = cat_realtime_addcart1.receive_time
    LEFT JOIN (select * from yishou_data.dws_user_goods_cutoff_current_time_statistics_full_dt where dt = '${one_day_ago}') cat_realtime_addcart2
    ON cat_realtime_addcart2.user_id = filtered_wide_table.user_id AND cat_realtime_addcart2.cat_id = special_info.cat_id_2
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht and filtered_wide_table.receive_time = cat_realtime_addcart2.receive_time
    LEFT JOIN (select * from yishou_data.dws_user_goods_cutoff_current_time_statistics_full_dt where dt = '${one_day_ago}') cat_realtime_addcart3
    ON cat_realtime_addcart3.user_id = filtered_wide_table.user_id AND cat_realtime_addcart3.cat_id = special_info.cat_id_3
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht and filtered_wide_table.receive_time = cat_realtime_addcart3.receive_time
    -- 实时品类曝光支付特征
    LEFT JOIN (select * from yishou_data.dws_user_goods_cutoff_current_time_statistics_full_dt where dt = '${one_day_ago}') cat_realtime_pay1
    ON cat_realtime_pay1.user_id = filtered_wide_table.user_id AND cat_realtime_pay1.cat_id = special_info.cat_id_1
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht and filtered_wide_table.receive_time = cat_realtime_pay1.receive_time
    LEFT JOIN (select * from yishou_data.dws_user_goods_cutoff_current_time_statistics_full_dt where dt = '${one_day_ago}') cat_realtime_pay2
    ON cat_realtime_pay2.user_id = filtered_wide_table.user_id AND cat_realtime_pay2.cat_id = special_info.cat_id_2
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht and filtered_wide_table.receive_time = cat_realtime_pay2.receive_time
    LEFT JOIN (select * from yishou_data.dws_user_goods_cutoff_current_time_statistics_full_dt where dt = '${one_day_ago}') cat_realtime_pay3
    ON cat_realtime_pay3.user_id = filtered_wide_table.user_id AND cat_realtime_pay3.cat_id = special_info.cat_id_3
    and to_char(from_unixtime(filtered_wide_table.receive_time),'yyyymmddhh') = special_info.ht and filtered_wide_table.receive_time = cat_realtime_pay3.receive_time
    -- 验数
    -- WHERE filtered_wide_table.user_id = '2248'
)
insert OVERWRITE table yishou_daily.dtl_special_goods_label_algorithm_train_dt partition(dt)
select 
    user_id
    ,event_id
    ,receive_time
    ,special_id
    ,label
    ,round(user_special_supply_180days,2) as user_special_supply_180days
    ,round(user_special_cat_180days,2) as user_special_cat_180days
    ,round(user_special_style_180days,2) as user_special_style_180days
    ,round(user_special_market_180days,2) as user_special_market_180days
    ,round(user_special_supply_15days,2) as user_special_supply_15days
    ,round(user_special_cat_15days,2) as user_special_cat_15days
    ,round(user_special_style_15days,2) as user_special_style_15days
    ,round(user_special_market_15days,2) as user_special_market_15days
    ,round(user_special_supply_exposure_click_ratio_realtime,2) as user_special_supply_exposure_click_ratio_realtime
    ,round(user_special_supply_exposure_add_cart_ratio_realtime,2) as user_special_supply_exposure_add_cart_ratio_realtime
    ,round(user_special_supply_exposure_pay_ratio_realtime,2) as user_special_supply_exposure_pay_ratio_realtime
    ,round(user_special_cat_exposure_click_ratio_realtime,2) as user_special_cat_exposure_click_ratio_realtime
    ,round(user_special_cat_exposure_add_cart_ratio_realtime,2) as user_special_cat_exposure_add_cart_ratio_realtime
    ,round(user_special_cat_exposure_pay_ratio_realtime,2) as user_special_cat_exposure_pay_ratio_realtime
    ,dt
from (
    select 
        user_id
        ,event_id
        ,receive_time
        ,special_id
        ,label
        -- 长期离线特征
        ,round(coalesce(supply_sale_ratio_1 * supply_180_score1,0),2) 
          + round(coalesce(supply_sale_ratio_2 * supply_180_score2,0),2) 
          + round(coalesce(supply_sale_ratio_3 * supply_180_score3,0),2) as user_special_supply_180days
        ,round(coalesce(cat_sale_ratio_1 * cat_180_score1,0),2) 
          + round(coalesce(cat_sale_ratio_2 * cat_180_score2,0),2) 
          + round(coalesce(cat_sale_ratio_3 * cat_180_score3,0),2) as user_special_cat_180days
        ,round(coalesce(style_sale_ratio_1 * style_180_score1,0),2) as user_special_style_180days
        ,round(coalesce(big_market_sale_ratio_1 * market_180_score1,0),2) as user_special_market_180days
        -- 短期离线特征
        ,round(coalesce(supply_sale_ratio_1 * supply_15_score1,0),2) 
          + round(coalesce(supply_sale_ratio_2 * supply_15_score2,0),2) 
          + round(coalesce(supply_sale_ratio_3 * supply_15_score3,0),2) as user_special_supply_15days
        ,round(coalesce(cat_sale_ratio_1 * cat_15_score1,0),2) 
          + round(coalesce(cat_sale_ratio_2 * cat_15_score2,0),2) 
          + round(coalesce(cat_sale_ratio_3 * cat_15_score3,0),2) as user_special_cat_15days
        ,round(coalesce(style_sale_ratio_1 * style_15_score1,0),2) as user_special_style_15days
        ,round(coalesce(big_market_sale_ratio_1 * market_15_score1,0),2) as user_special_market_15days
        -- 实时特征
        ,round(coalesce(supply_sale_ratio_1 * supply_realtime_click_score1,0),2) 
          + round(coalesce(supply_sale_ratio_2 * supply_realtime_click_score2,0),2) 
          + round(coalesce(supply_sale_ratio_3 * supply_realtime_click_score3,0),2) as user_special_supply_exposure_click_ratio_realtime
        ,round(coalesce(supply_sale_ratio_1 * supply_realtime_addcart_score1,0),2) 
          + round(coalesce(supply_sale_ratio_2 * supply_realtime_addcart_score2,0),2) 
          + round(coalesce(supply_sale_ratio_3 * supply_realtime_addcart_score3,0),2) as user_special_supply_exposure_add_cart_ratio_realtime
        ,round(coalesce(supply_sale_ratio_1 * supply_realtime_pay_score1,0),2) 
          + round(coalesce(supply_sale_ratio_2 * supply_realtime_pay_score2,0),2) 
          + round(coalesce(supply_sale_ratio_3 * supply_realtime_pay_score3,0),2) as user_special_supply_exposure_pay_ratio_realtime
        ,round(coalesce(cat_sale_ratio_1 * cat_realtime_click_score1,0),2) 
          + round(coalesce(cat_sale_ratio_2 * cat_realtime_click_score2,0),2) 
          + round(coalesce(cat_sale_ratio_3 * cat_realtime_click_score3,0),2) as user_special_cat_exposure_click_ratio_realtime
        ,round(coalesce(cat_sale_ratio_1 * cat_realtime_addcart_score1,0),2) 
          + round(coalesce(cat_sale_ratio_2 * cat_realtime_addcart_score2,0),2) 
          + round(coalesce(cat_sale_ratio_3 * cat_realtime_addcart_score3,0),2) as user_special_cat_exposure_add_cart_ratio_realtime
        ,round(coalesce(cat_sale_ratio_1 * cat_realtime_pay_score1,0),2) 
          + round(coalesce(cat_sale_ratio_2 * cat_realtime_pay_score2,0),2) 
          + round(coalesce(cat_sale_ratio_3 * cat_realtime_pay_score3,0),2) as user_special_cat_exposure_pay_ratio_realtime
        ,dt
    from score_detail_table
)
DISTRIBUTE BY dt
;