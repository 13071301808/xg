-- dcl_event_goods_detail_page_click_d

select 
    '档口回答' as click_source
    ,count(json_parse(action,'$.properties.userid')) as user_pv
from yishou_data.ods_app_event_log_d
where event = 'stallrequestpage' and dt = '20240708'
union all
select 
    '订阅档口' as click_source
    ,count(user_id) as user_pv
from yishou_data.dcl_event_user_center_click_d
where click_source = '我的档口' and dt = '20240708'
union all
select 
    '筛选' as click_source
    ,count(user_id) as user_pv
from yishou_data.dcl_event_search_result_click_d
where click_source = '筛选' and dt = '20240708'


