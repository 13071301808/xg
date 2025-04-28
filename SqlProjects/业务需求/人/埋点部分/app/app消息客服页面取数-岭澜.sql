-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/01/06 17:36:22 GMT+08:00
-- ******************************************************************** --
-- https://yb7ao262ru.feishu.cn/wiki/EjsXwDA2HivPDukEzk4cqDRBnlf
select 
    dt as 日期
    ,click_point as 点击位置
    ,sum(user_uv) as 用户UV
    ,sum(user_pv) as 用户PV
from (
    -- 我的
    select 
        dt
        ,case 
            when click_source = '右上角客服' then '我的-客服'
            when click_source = '消息' then '我的-消息'
            when click_source = '售后进度' then '我的-售后进度'
        end as click_point
        ,count(DISTINCT user_id) as user_uv
        ,count(user_id) as user_pv
    from yishou_data.dcl_event_user_center_click_d
    where dt between '${start}' and '${end}' 
    group by 1,2
    union ALL 
    -- 所有入口的消息
    select 
        dt
        ,'所有入口-消息' as click_point
        ,count(DISTINCT user_id) as user_uv
        ,count(user_id) as user_pv
    from yishou_data.dcl_event_service_message_center_d
    where dt between '${start}' and '${end}'
    group by 1,2
    union all
    -- 消息
    select 
        dt
        ,case 
            when click_source = '拿货指南' then '消息-客户服务'
            when click_source = '自助售后' then '消息-自助售后'
            when click_source = '我的售后单' then '消息-我的售后单'
            when click_source = '一手账单' then '消息-一手账单'
        end as click_point
        ,count(DISTINCT user_id) as user_uv
        ,count(user_id) as user_pv
    from yishou_data.dcl_event_message_center_click_d
    where dt between '${start}' and '${end}'
    group by 1,2
    union all
    -- 自助客服
    select 
        dt
        ,case 
            when element = '申请售后' then '所有入口-申请售后'
            when element = '售后进度' then '所有入口-售后进度'
            when element = '修改收寄信息' then '所有入口-修改收寄信息'
            when element = '催促发货' then '所有入口-催促发货'
            when element = '合并发货' then '所有入口-合并发货'
            when element = '修改快递' then '所有入口-修改快递'
            when element = '不放发货单' then '所有入口-不放发货单'
            when element = '物流异常' then '所有入口-物流异常'
        end as click_point
        ,count(DISTINCT user_id) as user_uv
        ,count(user_id) as user_pv
    from yishou_data.dcl_h5_element_click_d
    where dt between '${start}' and '${end}' and activity = '自助客服'
    group by 1,2
    union ALL 
    -- 帮助中心
    select 
        dt
        ,'帮助中心问题详情' as click_point
        ,count(DISTINCT user_id) as user_uv
        ,count(user_id) as user_pv
    from yishou_data.dcl_h5_help_center_cell_detail_d
    where dt between '${start}' and '${end}' and click_source <> ''
    group by 1,2
    union ALL 
    -- 在线客服
    select 
        dt
        ,'在线客服' as click_point
        ,count(DISTINCT user_id) as user_uv
        ,count(user_id) as user_pv
    from yishou_data.dcl_event_qiyu_click_d
    where dt between '${start}' and '${end}' and click_source = '客服咨询'
    and source = '20'
    group by 1,2
    union all
    -- 售后月卡
    select 
        dt
        ,case 
            when source = 1 then '订单-申请售后' 
            when source = 2 then '售后进度-申请售后' 
        end as click_point
        ,count(DISTINCT user_id) as user_uv
        ,count(user_id) as user_pv
    from yishou_data.dcl_event_refund_service_type_click_d
    where dt between '${start}' and '${end}' and click_source = '曝光'
    and source in ('1','2')
    group by 1,2
)
where click_point is not null
group by 1,2
