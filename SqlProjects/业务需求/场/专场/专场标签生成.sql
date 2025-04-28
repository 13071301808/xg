-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/01/07 09:45:07 GMT+08:00
-- ******************************************************************** --
-- 每个专场id下标签
-- 商品统计维度：均为上架状态
-- 折扣率
-- 折扣率≤0.7的上架状态商品款数占比总款数50%,展示标签：特价场
-- 爆款场
-- 口径为：
-- 1> 货号id款数，在专场渠道下，T-1日专场渠道实际gmv>0且专场渠道实际销量>0
-- 2> T日，专场id下货号id累计款数>32款
-- 新品标签
-- 新款占比≥40%，展示标签：大上新
-- 品类优先级12：优先级1等级最高
-- 优先级1：品类1占比≥35%，只展示标签：品类1
-- 优先级2：品类1占比≥25%, 且品类2占比>15%，展示标签：品类1/品类2
-- 档口优先级12：优先级1等级最高
-- 优先级1：档口1占比≥35%，只展示标签：档口1
-- 优先级2：档口1占比≥25%，且品类2占比>20%，展示标签：档口1/档口2
-- 风格
-- 优先级1：除了“休闲简约”的风格，展示其他标签
-- 展示优先级：
-- 只取3个标签
-- 优先级: 折扣率>爆款场>新品>品类>档口>风格

with special_label_info as (
    -- 专场标签明细
    select 
        a.*
        ,case when baokuan.special_id is not null then 1 else 0 end as is_baokuan 
    from (select * from yishou_daily.dtl_special_goods_label_choose_dt where dt = '${gmtdate}') a
    left join (
        -- 爆款场
        select 
            today.special_id
            ,count(DISTINCT if(one_day_ago.商品货号 is not null,today.goods_no,null)) as special_goods_num
        from (
            select 
                special_date, special_id, goods_no
            from(
                select 
                    special_date
                    , goods_no
                    , special_id
                    ,ROW_NUMBER() over(PARTITION BY goods_id ORDER BY special_id desc) rank_num 
                from yishou_daily.finebi_realdata_special_onsale_goods
                where is_putaway = '上架'
            ) 
            where rank_num = 1
        ) today
        left join (
            select 
                DISTINCT 商品货号
            from (
                select 
                    商品货号
                    ,ROW_NUMBER() over(PARTITION BY 商品id ORDER BY 专场id desc) rank_num 
                from yishou_daily.finebi_temp_shishi_date
                where dt = '${one_day_ago}'
                and 专场状态栏 = '上架'
            )
            where rank_num = 1
        ) one_day_ago on today.goods_no = one_day_ago.商品货号
        group by today.special_id
        having special_goods_num > 32
    )baokuan on baokuan.special_id = a.special_id
)
-- select 
--     'special_label' as key
--     ,concat('{', special_id, ':', cast(label_lists as string), '}') as value
-- from (
--     SELECT
--         special_id
--         ,collect_list(label) as label_lists
--     FROM (
--         select
--             special_id,
--             label
--         from (
            select 
                a.special_id
                ,label_1_2.label_top_1
                ,label_1_2.label_top_2
                ,case 
                    when label_1_2.label_top_2 = '爆款场' then (
                        case 
                            when a.new_sale_kh_1 / a.sale_goods_num >= 0.4 then '大上新'
                            when a.cat_sale_ratio_1 >= 0.35 then a.cat_one 
                            when a.cat_sale_ratio_1 >= 0.25 and a.cat_sale_ratio_2 > 0.15 then concat(a.cat_one,'/',a.cat_two) 
                            when a.supply_sale_ratio_1 >= 0.35 then a.supply_name_1 
                            when a.supply_sale_ratio_1 >= 0.25 and a.supply_sale_ratio_2 > 0.2 then concat(a.supply_name_1,'/',a.supply_name_2)
                            when a.special_style_type_name_1 != '休闲简约' then a.special_style_type_name_1 
                        end
                    ) 
                    when label_1_2.label_top_2 = '大上新' then (
                        case 
                            when a.cat_sale_ratio_1 >= 0.35 then a.cat_one 
                            when a.cat_sale_ratio_1 >= 0.25 and a.cat_sale_ratio_2 > 0.15 then concat(a.cat_one,'/',a.cat_two) 
                            when a.supply_sale_ratio_1 >= 0.35 then a.supply_name_1 
                            when a.supply_sale_ratio_1 >= 0.25 and a.supply_sale_ratio_2 > 0.2 then concat(a.supply_name_1,'/',a.supply_name_2)
                            when a.special_style_type_name_1 != '休闲简约' then a.special_style_type_name_1 
                        end
                    ) 
                    when label_1_2.label_top_2 = a.cat_one then (
                        case 
                            when a.supply_sale_ratio_1 >= 0.35 then a.supply_name_1 
                            when a.supply_sale_ratio_1 >= 0.25 and a.supply_sale_ratio_2 > 0.2 then concat(a.supply_name_1,'/',a.supply_name_2)
                            when a.special_style_type_name_1 != '休闲简约' then a.special_style_type_name_1 
                        end
                    )
                    when label_1_2.label_top_2 = concat(a.cat_one,'/',a.cat_two) then (
                        case 
                            when a.supply_sale_ratio_1 >= 0.35 then a.supply_name_1 
                            when a.supply_sale_ratio_1 >= 0.25 and a.supply_sale_ratio_2 > 0.2 then concat(a.supply_name_1,'/',a.supply_name_2)
                            when a.special_style_type_name_1 != '休闲简约' then a.special_style_type_name_1 
                        end
                    )
                    when label_1_2.label_top_2 = a.supply_name_1 then (
                        case 
                            when a.special_style_type_name_1 != '休闲简约' then a.special_style_type_name_1 
                        end
                    )
                    when label_1_2.label_top_2 = concat(a.supply_name_1,'/',a.supply_name_2) then (
                        case 
                            when a.special_style_type_name_1 != '休闲简约' then a.special_style_type_name_1 
                        end
                    )
                    else null
                end as label_top_3
            from special_label_info a
            left join (
                select 
                    a.special_id
                    ,label_1.label as label_top_1
                    ,case 
                        when label_1.label = '特价场' then (
                            case 
                                when a.is_baokuan = 1 then '爆款场'
                                when a.new_sale_kh_1 / a.sale_goods_num >= 0.4 then '大上新'
                                when a.cat_sale_ratio_1 >= 0.35 then a.cat_one 
                                when a.cat_sale_ratio_1 >= 0.25 and a.cat_sale_ratio_2 > 0.15 then concat(a.cat_one,'/',a.cat_two) 
                                when a.supply_sale_ratio_1 >= 0.35 then a.supply_name_1 
                                when a.supply_sale_ratio_1 >= 0.25 and a.supply_sale_ratio_2 > 0.2 then concat(a.supply_name_1,'/',a.supply_name_2)
                                when a.special_style_type_name_1 != '休闲简约' then a.special_style_type_name_1 
                            end
                        ) 
                        when label_1.label = '爆款场' then (
                            case 
                                when a.new_sale_kh_1 / a.sale_goods_num >= 0.4 then '大上新'
                                when a.cat_sale_ratio_1 >= 0.35 then a.cat_one 
                                when a.cat_sale_ratio_1 >= 0.25 and a.cat_sale_ratio_2 > 0.15 then concat(a.cat_one,'/',a.cat_two) 
                                when a.supply_sale_ratio_1 >= 0.35 then a.supply_name_1 
                                when a.supply_sale_ratio_1 >= 0.25 and a.supply_sale_ratio_2 > 0.2 then concat(a.supply_name_1,'/',a.supply_name_2)
                                when a.special_style_type_name_1 != '休闲简约' then a.special_style_type_name_1 
                            end
                        ) 
                        when label_1.label = '大上新' then (
                            case 
                                when a.cat_sale_ratio_1 >= 0.35 then a.cat_one 
                                when a.cat_sale_ratio_1 >= 0.25 and a.cat_sale_ratio_2 > 0.15 then concat(a.cat_one,'/',a.cat_two) 
                                when a.supply_sale_ratio_1 >= 0.35 then a.supply_name_1 
                                when a.supply_sale_ratio_1 >= 0.25 and a.supply_sale_ratio_2 > 0.2 then concat(a.supply_name_1,'/',a.supply_name_2)
                                when a.special_style_type_name_1 != '休闲简约' then a.special_style_type_name_1 
                            end
                        )
                        when label_1.label = a.cat_one then (
                            case 
                                when a.supply_sale_ratio_1 >= 0.35 then a.supply_name_1 
                                when a.supply_sale_ratio_1 >= 0.25 and a.supply_sale_ratio_2 > 0.2 then concat(a.supply_name_1,'/',a.supply_name_2)
                                when a.special_style_type_name_1 != '休闲简约' then a.special_style_type_name_1 
                            end
                        )
                        when label_1.label = concat(a.cat_one,'/',a.cat_two) then (
                            case 
                                when a.supply_sale_ratio_1 >= 0.35 then a.supply_name_1 
                                when a.supply_sale_ratio_1 >= 0.25 and a.supply_sale_ratio_2 > 0.2 then concat(a.supply_name_1,'/',a.supply_name_2)
                                when a.special_style_type_name_1 != '休闲简约' then a.special_style_type_name_1 
                            end
                        )
                        when label_1.label = a.supply_name_1 then (
                            case 
                                when a.special_style_type_name_1 != '休闲简约' then a.special_style_type_name_1 
                            end
                        )
                        when label_1.label = concat(a.supply_name_1,'/',a.supply_name_2) then (
                            case 
                                when a.special_style_type_name_1 != '休闲简约' then a.special_style_type_name_1 
                            end
                        )
                        else null
                    end as label_top_2
                from special_label_info a
                left join (
                    select 
                        special_id
                        ,case 
                            when (discount_ratio_1 <= 0.7) and (sale_goods_num / total_goods_num = 0.5) then '特价场'
                            when is_baokuan = 1 then '爆款场'
                            when new_sale_kh_1 / sale_goods_num >= 0.4 then '大上新'
                            when cat_sale_ratio_1 >= 0.35 then cat_one
                            when cat_sale_ratio_1 >= 0.25 and cat_sale_ratio_2 > 0.15 then concat(cat_one,'/',cat_two) 
                            when supply_sale_ratio_1 >= 0.35 then supply_name_1 
                            when supply_sale_ratio_1 >= 0.25 and supply_sale_ratio_2 > 0.2 then concat(supply_name_1,'/',supply_name_2) 
                            when special_style_type_name_1 != '休闲简约' then special_style_type_name_1 
                        end as label
                    from special_label_info
                ) label_1 on label_1.special_id = a.special_id
            ) label_1_2 on label_1_2.special_id = a.special_id
--         )
--         LATERAL VIEW explode(array(label_top_1, label_top_2, label_top_3)) exploded_table as label
--     )
--     GROUP BY special_id
-- )