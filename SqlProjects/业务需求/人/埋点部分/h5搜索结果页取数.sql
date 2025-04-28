-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/09/14 12:33:48 GMT+08:00
-- ******************************************************************** --
SELECT 
    dt
    ,sum(click_uv) as click_uv
    ,sum(click_pv) as click_pv
    ,sum(ysh5_exposure_uv) + sum(h5_exposure_uv) as exposure_uv
    ,sum(exposure_goods_num) as exposure_goods_num
from (
    select
        dt
        ,count(DISTINCT get_json_object(data,'$.uid')) as click_uv
        ,count(get_json_object(data,'$.uid')) as click_pv
        ,0 as ysh5_exposure_uv
        ,0 as h5_exposure_uv
        ,0 as exposure_goods_num
    from yishou_data.ods_h5_event_action_d
    where event = 'h5click'
    and regexp_extract(get_json_object(data,'$.source'),'production/([^/]+)/index.html',1) = 'screening-operate-search'
    and dt = '20240913'
    group by dt
    union all
    select
        dt
        ,0 as click_uv
        ,0 as click_pv
        ,count(DISTINCT user_id) as ysh5_exposure_uv
        ,0 as h5_exposure_uv
        ,count(goods_no) as exposure_goods_num
    from yishou_data.dcl_h5_ys_h5_goods_exposure
    where regexp_extract(url,'production/([^/]+)/index.html',1) = 'screening-operate-search'
    and dt = '20240913'
    group by dt
    union ALL 
    select 
        dt
        ,0 as click_uv
        ,0 as click_pv
        ,0 as ysh5_exposure_uv
        ,count(DISTINCT user_id) as h5_exposure_uv
        ,0 as exposure_goods_num
    from yishou_data.dcl_h5_element_exposure_d
    where regexp_extract(source,'production/([^/]+)/index.html',1) = 'screening-operate-search'
    and dt = '20240913'
    group by dt
)
group by dt  



