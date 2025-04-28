select 
    dt as 日期
    ,supply_id as 商家id
    ,case 
        when click_source = '快速填充尺码-确定' then '埋点1'
        when click_source = '尺码模版-确定' then '埋点2'
        when click_source = '尺码数据-确定' then '埋点3'
    end as 位置
    ,count(1) as 点击pv
from yishou_data.dcl_baoban_app_ys_up_clothes_click_d
where dt between '20250221' and '20250317' 
and click_source in ('快速填充尺码-确定','尺码模版-确定','尺码数据-确定')
and user_id is not null
group by 1,2,3
;