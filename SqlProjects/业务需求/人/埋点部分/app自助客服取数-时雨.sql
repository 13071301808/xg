-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/06/21 10:53:53 GMT+08:00
-- ******************************************************************** --
-- 底层h5事件
-- 等dcl没问题后用dcl的
-- select 
--     a.element as `页面`
--     ,a.uv as `页面uv`
--     ,round(a.uv/a.uv_num,2) as `页面占比`
-- from (
--     select 
--         json_parse(data, '$.name') as element
--         ,count(DISTINCT json_parse(data, '$.uid')) as uv
--         ,(
--             select 
--                 count(DISTINCT json_parse(data, '$.uid')) as uv_num
--             from ods_h5_event_action_d
--             where event= 'h5click' and dt between '20240616' and '20240620' and activity = '自助客服'
--         ) as uv_num
--     from ods_h5_event_action_d
--     where event= 'h5click' 
--     and dt between '20240616' and '20240620' 
--     and activity = '自助客服' 
--     and json_parse(data, '$.name') in ('催促发货','修改快递','不放发货单','物流异常')
--     group by json_parse(data, '$.name')
-- ) a
-- ;


-- 解析h5页面元素点击
select 
    a.element as `页面`
    ,a.uv as `页面uv`
    ,round(a.uv/a.uv_num,2) as `页面占比`
from (
    select 
        element
        ,count(DISTINCT user_id) as uv
        -- 开窗做统计uv总数
        ,sum(count(DISTINCT user_id)) over() as uv_num
    from yishou_data.dcl_h5_element_click_d
    where dt between '20240616' and '20240625'
    and activity = '自助客服' 
    -- and element in ('催促发货','修改快递','不放发货单','物流异常')
    group by element
) a
union all
-- 客户服务页面
select 
    '客户服务' as `页面`
    ,count(DISTINCT user_id) as `页面uv`
    ,null as `页面占比`
from yishou_data.dcl_h5_element_click_d 
where activity = '客户服务' and dt between '20240616' and '20240625'
;
-- union ALL 
-- -- 在线客服
-- select 
--     '在线客服' as `页面`
--     ,count(DISTINCT user_id) as `页面uv`
--     ,null as `页面占比`
-- from yishou_data.dcl_h5_element_click_d
-- where activity = '客户服务' and A在线客服 dt between '20240616' and '20240625' and source = 15

-- -- 
-- select 
--     source,count(1) 
-- from yishou_data.dcl_event_qiyu_click_d 
-- where dt = '20240624' 
-- group by source;


