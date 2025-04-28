select 
    dt
    ,位置
    ,sum(点击uv) as 点击uv
    ,sum(曝光uv) as 曝光uv
from (
    -- 订单详情
    select 
        dt
        ,'订单详情页' as 位置
        ,count(DISTINCT user_id) as 点击uv
        ,0 as 曝光uv
    from yishou_data.dcl_event_my_order_click_d 
    where dt = '${one_day_ago}' and click_source in ('快速补货','我要补单')
    group by dt
    union ALL 
    select 
        dt
        ,'订单详情页' as 位置
        ,0 as 点击uv
        ,count(DISTINCT user_id) as 曝光uv
    from yishou_data.dcl_event_order_list_exposure_d  
    where dt = '${one_day_ago}' and pid=1
    group by dt
    union all
    -- 今日新款
    select 
        dt
        ,'今日新款页' as 位置 
        ,count(DISTINCT user_id) as 点击uv
        ,0 as 曝光uv
    from yishou_data.dcl_h5_first_sale_new 
    WHERE dt = '${one_day_ago}' and click_source != '曝光'
    group by dt
    union ALL 
    select 
        dt
        ,'今日新款页' as 位置 
        ,0 as 点击uv
        ,count(DISTINCT user_id) as 曝光uv
    from yishou_data.dcl_h5_first_sale_new
    WHERE dt = '${one_day_ago}' and click_source = '曝光'
    group by dt
    union all
    -- 个人中心
    select  
        dt
        ,'个人中心页' as 位置 
        ,count(DISTINCT user_id) as 点击uv
        ,0 as 曝光uv 
    from yishou_data.dcl_event_user_center_click_d
    where dt = '${one_day_ago}' and click_source='会员权益'
    group by dt
    union ALL 
    select  
        dt
        ,'个人中心页' as 位置 
        ,count(DISTINCT user_id) as 点击uv
        ,0 as 曝光uv 
    from yishou_data.dcl_event_user_center_click_d
    where dt = '${one_day_ago}' and click_source='我的服务' and banner_name='一手大学'
    group by dt
    union all 
    SELECT 
        dt,
        '个人中心页' AS 位置,
        0 AS 点击uv,
        COUNT(DISTINCT user_id) AS 曝光uv
    FROM yishou_data.dcl_event_user_center_exposure_d
    LATERAL VIEW OUTER EXPLODE(split(replace(replace(replace(my_services_arr, '[', ''),']', ''), '},{', '}@@@@@{'), '@@@@@')) t AS usercenter_banner
    WHERE dt = '${one_day_ago}' AND get_json_object(usercenter_banner, '$.banner_name') = '一手大学'
    GROUP BY dt
    union all
    -- 爆款排行榜
    select 
        dt
        ,'爆款排行榜页' as 位置 
        ,count(DISTINCT user_id) as 点击uv
        ,0 as 曝光uv
    from yishou_data.dcl_event_list_center_click_d
    where dt = '${one_day_ago}'
    group by dt
    union all
    select 
        dt
        ,'爆款排行榜页' as 位置 
        ,0 as 点击uv
        ,count(DISTINCT user_id) as 曝光uv
    from yishou_data.dcl_event_list_center_exposure_d
    where dt = '${one_day_ago}'
    group by dt
)
group by 1,2;



