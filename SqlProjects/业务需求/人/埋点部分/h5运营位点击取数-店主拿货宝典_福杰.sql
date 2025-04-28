-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/09/07 15:22:02 GMT+08:00
-- ******************************************************************** --
-- 76838、76843、76847、76850
select 
    case 
        when h5_banner_id = '76838' then '看更多市场介绍'
        when h5_banner_id = '76843' then '更多找赚钱档口攻略'
        when h5_banner_id = '76847' then '去看挑款攻略'
        when h5_banner_id = '76850' then '去了解'
    else '其他'
    end as return_picture
    ,count(user_id) as user_pv
    ,count(DISTINCT user_id) as user_uv  
from yishou_data.dcl_h5_ys_h5_banner_click_dt 
where 
dt between '20240806' and '20240906'
and activity = 'H5运营位点击' and title = '店主拿货宝典' and h5_banner_id in ('76838','76843','76847','76850')
group by h5_banner_id
;