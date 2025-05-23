SELECT
    max(case when dt = SUBSTRING(replace(CURRENT_DATE() - interval 1 day,'-',''),1,6) then 有效召回数 end) 本月有效召回数
    ,max(case when dt = SUBSTRING(replace(CURRENT_DATE() - interval 1 day - interval 1 year,'-',''),1,6) then 有效召回数 end) 去年有效召回数
FROM (
    select
        substring(dtt,1,6) dt,count(distinct user_id) 有效召回数
    from (
        select
            dtt,user_id,sum(real_buy_amount) gmv
        from (
            select
                t1.*,t2.real_buy_amount,t2.dt tttt
            from (
                select
                    dt dtt,user_id
                from dw_yishou_data.daily_lost_users_dt_bi
                where user_type = '流失召回'
                and ((dt between 20240101 and CURRENT_DATE() - interval 1 day - interval 1 year )
                or (dt between 20250101 and CURRENT_DATE() - interval 1 day))
            ) t1
            left join dw_yishou_data.dws_ys_sale_log_sp_user_info_log_detail_dt t2 on t1.user_id = t2.user_id
            and t2.dt between t1.dtt and t1.dtt + interval 14 day
            and ((t2.dt between 20240101 and CURRENT_DATE() - interval 1 day - interval 1 year )
            or (t2.dt between 20250101 and CURRENT_DATE() - interval 1 day))
            and real_buy_amount > 0
        )t3 group by 1,2 having gmv >= 300
    ) t4
    group by 1
)