-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/02/07 14:42:47 GMT+08:00
-- ******************************************************************** --
with total_user as (
    -- 总数
    select 
        avg(supply_num) as avg_supply_num
    from (    
        select 
            coalesce(a.dt,b.dt) as dt 
            ,count(DISTINCT coalesce(a.supply_id,b.supply_id)) as supply_num
        from (
            -- 爆版app
            select 
                dt
                ,GET_JSON_OBJECT(scdata, '$.supply_id') AS supply_id
            from yishou_data.ods_baoban_app_event_log_exposure_d
            where dt between '20241201' and '20241231'
            and event = 'homeexposure'
        ) a 
        full join (
            -- 爆版pc
            select 
                dt
                ,supply_id
            from yishou_data.dcl_baoban_h5_baoban_pc_exposure_dt
            where dt between '20241201' and '20241231'
        ) b on a.dt = b.dt and a.supply_id = b.supply_id
        group by 1
    ) 
)
,data_center as (
    -- 数据中心
    select 
        avg(supply_num) as avg_supply
        -- 日均商家数 / 日均有平台化的商家登录数
        ,round(avg(supply_num) / 574,4) as avg_user_ratio
    from (
        select 
            coalesce(a.dt,b.dt) as dt
            ,count(DISTINCT coalesce(a.supply_id,b.supply_id)) as supply_num
        from (
            -- 爆版app 
            select 
                dt
                ,GET_JSON_OBJECT(scdata, '$.properties.supply_id') as supply_id
            from yishou_data.ods_baoban_app_event_log_d 
            where dt between '20241201' and '20241231' and event = 'datacenter'
        ) a 
        full join (
            -- 爆版pc
            select 
                dt
                ,supply_id
            from yishou_data.dcl_baoban_h5_baoban_pc_exposure_dt
            where dt between '20241201' and '20241231'
            and page_name in ('经营概览','交易数据','商品数据','流量数据','履约数据')
        ) b on a.dt = b.dt and a.supply_id = b.supply_id
        group by 1
    )
)
,session as (
    -- 消息通知
    select 
        avg(a.supply_num) as avg_supply
        ,round(avg(a.supply_num) / max(total_user.avg_supply_num),4) as avg_user_ratio
    from (
        select 
            coalesce(a.dt,b.dt) as dt
            ,count(DISTINCT coalesce(a.supply_id,b.supply_id)) as supply_num
        from (
            -- 爆版app
            select 
                dt
                ,GET_JSON_OBJECT(scdata, '$.properties.supply_id') as supply_id
            from yishou_data.ods_baoban_app_event_log_d
            where dt between '20241201' and '20241231' and event = 'bottomtabclick'
            and GET_JSON_OBJECT(scdata, '$.properties.click_source') = '消息'
        ) a 
        full join (
            -- 爆版pc
            select 
                dt
                ,GET_JSON_OBJECT(data, '$.supply_id') as supply_id
            from yishou_data.ods_baoban_h5_event_action_d 
            where dt between '20241201' and '20241231' and event = 'pcmainfloatingframe'
            and GET_JSON_OBJECT(data, '$.click_source') = '消息'
        ) b on a.dt = b.dt and a.supply_id = b.supply_id
        group by 1
    ) a 
    left join total_user on 1 = 1
)
, goodsmanager as (
    -- 商品管理
    select 
        avg(a.supply_num) as avg_supply
        ,round(avg(a.supply_num) / max(total_user.avg_supply_num),4) as avg_user_ratio
    from (
        select 
            coalesce(a.dt,b.dt) as dt
            ,count(DISTINCT coalesce(a.supply_id,b.supply_id)) as supply_num
        from (
            -- 爆版app
            select 
                dt
                ,GET_JSON_OBJECT(scdata, '$.properties.supply_id') as supply_id
            from yishou_data.ods_baoban_app_event_log_d
            where dt between '20241201' and '20241231' and event = 'goodsmanager'
        ) a 
        full join (
            -- 爆版pc
            select 
                dt
                ,supply_id
            from yishou_data.dcl_baoban_h5_baoban_pc_exposure_dt
            where dt between '20241201' and '20241231'
            and page_name in ('商品列表','商品发布','商品素材','商品缺图预警')
        ) b on a.dt = b.dt and a.supply_id = b.supply_id
        group by 1
    ) a
    left join total_user on 1 = 1
)
, order_info as (
    -- 欠货明细
    select 
        avg(a.supply_num) as avg_supply
        ,round(avg(a.supply_num) / max(total_user.avg_supply_num),4) as avg_user_ratio
    from (
        select 
            coalesce(a.dt,b.dt) as dt
            ,count(DISTINCT coalesce(a.supply_id,b.supply_id)) as supply_num
        from (
            -- 爆版app
            select 
                dt
                ,GET_JSON_OBJECT(scdata, '$.properties.click_source') as click_source
                ,GET_JSON_OBJECT(scdata, '$.properties.supply_id') as supply_id
            from yishou_data.ods_baoban_app_event_log_d 
            where dt between '20241201' and '20241231' and event = 'owegoodsdetail'
            group by 1,2,3 
            having count(GET_JSON_OBJECT(scdata, '$.properties.click_source')) >= 2
        ) a 
        full join (
            -- 爆版pc
            select 
                dt
                ,get_json_object(data, '$.supply_id') as supply_id
                ,get_json_object(data, '$.click_source') as click_source
            from yishou_data.ods_baoban_h5_event_action_d
            where dt between '20241201' and '20241231'
            and event = 'pcowegoods'
            and get_json_object(data, '$.click_source') = 'tab'
            group by 1,2,3 
            having count(get_json_object(data, '$.click_source')) >= 2
        ) b on a.dt = b.dt and a.supply_id = b.supply_id
        group by 1
    ) a 
    left join total_user on 1 = 1
)
, pay_info as (
    -- 账单明细
    select 
        avg(a.supply_num) as avg_supply
        ,round(avg(a.supply_num) / max(total_user.avg_supply_num),4) as avg_user_ratio
    from (
        select 
            coalesce(a.dt,b.dt) as dt
            ,count(DISTINCT coalesce(a.supply_id,b.supply_id)) as supply_num
        from (
            -- 爆版app
            select 
                dt
                ,GET_JSON_OBJECT(scdata, '$.properties.supply_id') as supply_id
            from yishou_data.ods_baoban_app_event_log_d 
            where dt between '20241201' and '20241231' and event = 'ybill'
        ) a 
        full join (
            -- 爆版pc
            select 
                dt
                ,get_json_object(data, '$.supply_id') as supply_id
            from yishou_data.ods_baoban_h5_event_action_d
            where dt between '20241201' and '20241231'
            and event = 'pcfundlist'
        ) b on a.dt = b.dt and a.supply_id = b.supply_id
        group by 1
    ) a 
    left join total_user on 1 = 1
)
select 
    data_center.avg_user_ratio as 
    ,session.avg_user_ratio
    ,goodsmanager.avg_user_ratio
    ,order_info.avg_user_ratio
    ,pay_info.avg_user_ratio
from total_user 
left join data_center on 1=1
left join session on 1=1
left join goodsmanager on 1=1
left join order_info on 1=1
left join pay_info on 1=1
