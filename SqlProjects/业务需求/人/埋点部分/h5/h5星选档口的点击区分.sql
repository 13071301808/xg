select 
    '该h5页面的下面的档口列表进的'
    ,count(DISTINCT a.user_id) as h5_click_uv
from (select * from yishou_data.dcl_ys_h5_stall_list_exposure where dt = '20250327') a 
left join yishou_data.dcl_event_check_stall_d b on a.event_id = b.source_event_id
where b.dt = '20250327' 
and b.source_event_id is not null and a.title = '星选档口' and b.stall_source = '120592'
union all
select 
    '该h5页面的品牌墙进的'
    ,count(DISTINCT a.user_id) as h5_click_uv
from (select * from yishou_data.dcl_h5_element_exposure_d where dt = '20250327') a 
left join yishou_data.dcl_event_check_stall_d b on a.event_id = b.source_event_id
where b.dt = '20250327' 
and b.source_event_id is not null 
and a.activity = '星选档口120592' and b.stall_source = '0'
;