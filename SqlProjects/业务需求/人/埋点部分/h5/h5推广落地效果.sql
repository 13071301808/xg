select 
    *
FROM (
    select 
        t1.id                -- 推广id
        ,t1.created_at
        ,t1.page_id             -- 落地页id
        ,t1.promoter_name
        ,coalesce(t4.user_id,t2.user_id) as user_id
        ,t3.supply_id
        ,coalesce(t4.time,t2.time) as min_scan_time
        ,row_number() over(partition by coalesce(t4.user_id,t2.user_id) order by coalesce(t4.time,t2.time)) rk
    from yishou_data.all_fmys_promotion t1
    left join (
        select 
            user_id,promotion_id,from_unixtime(time / 1000) as time 
        from yishou_data.dcl_wx_stall_exposure_d 
        where dt>='20240810' and systemOs = '微信小程序'
    )t2 on t1.id=t2.promotion_id
    left join (
        -- 推广落地到h5页面
        select 
            get_json_object(data, '$.uid') as user_id
            ,from_unixtime(log_time) as time
            ,get_json_object(data, '$.promotion_id') as promotion_id
        from yishou_data.ods_h5_event_action_d
        where dt >='20240810' and event = 'h5exposure'
        and data is not null
        and get_json_object(data, '$.uid') is not null 
        and get_json_object(data, '$.promotion_id') is not null
    ) t4 on t4.promotion_id = t1.id
    inner join yishou_daily.supply_promote_list t3 on t1.id=t3.id
    -- where coalesce(t4.user_id,t2.user_id) in (17778411,17778358,17778198,17778179,17778182,17777333,17777295,17777279,17777123,17777082,17777070)
    -- and t1.id = '3189'
)t where rk=1 and user_id is not null