-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/11/19 18:08:56 GMT+08:00
-- ******************************************************************** --
select 
    dt as 日期
    ,位置
    ,sum(点击pv) AS 点击pv
    ,sum(点击uv) AS 点击uv
from (
    select 
        dt
        ,'首页消息' as 位置
        ,count(user_id) as 点击pv
        ,count(DISTINCT user_id) as 点击uv
    from yishou_data.dcl_event_home_click_d
    where dt between '20241001' and '20241030'
    and click_source = '消息'
    group by dt
    union ALL 
    select 
        dt
        ,case
            when click_source = '拿货指南' then '客户服务'
            when click_source = '自助售后' then '自助售后'
            when click_source = '我的售后单' then '我的售后单'
            when click_source = '退款通知' then '退款通知'
            when click_source = '物流通知' then '物流通知'
            when click_source = '商品通知' then '商品通知'
            when click_source = '平台通知' then '平台通知'
            when click_source = '同行评价' then '评价消息提醒'
            when click_source = '权益通知' then '权益通知'
            when click_source = '邀约回答' then '问问进过货的店主'
        end as 位置
        ,count(user_id) as 点击pv
        ,count(DISTINCT user_id) as 点击uv
    from yishou_data.dcl_event_message_center_click_d
    where dt between '20241001' and '20241030'
    and click_source in ('拿货指南','自助售后','我的售后单','退款通知','物流通知','商品通知','平台通知','同行评价','权益通知','邀约回答')
    group by 1,2
    union ALL 
    select 
        dt
        ,'一手客服' as 位置
        ,count(user_id) as 点击pv
        ,count(DISTINCT user_id) as 点击uv
    from yishou_data.dcl_event_qiyu_click_d
    where dt between '20241001' and '20241030'
    and click_source = '客服咨询'
    group by dt
)
group by 1,2
