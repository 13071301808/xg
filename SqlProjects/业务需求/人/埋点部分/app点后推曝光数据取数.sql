--
select 
    a.user_id
    ,get_json_object(goods_dict, '$.is_operat') as is_operat
    ,get_json_object(goods_dict, '$.click_to_recommend') as click_to_recommend
from (
    select
        coalesce(get_json_object(scdata, '$.user_id'),get_json_object(scdata, '$.userid')) as user_id 
        ,regexp_replace1(regexp_replace1(regexp_replace1(get_json_object(scdata, '$.click_goods_arr'), '^\\[',''),'\\]$',''),'},\\{','}|-|{') as click_goods 
        ,dt 
    from yishou_data.ods_app_event_log_exposure_d
    where dt = '20240910' and event ='goodsexposure' 
) a
lateral view outer explode (split(a.click_goods, '\\|-\\|'))t as goods_dict
where goods_dict is not null 
and goods_dict <> ''
and get_json_object(goods_dict, '$.click_to_recommend') is not null
and 
;

