select 
    dt as 日期
    ,case 
        when target_platform = 1 then '淘宝'
        when target_platform = 4 then '快手'
        when target_platform = 5 then '拼多多'
        when target_platform = 6 then '抖音'
        when target_platform = 7 then '微信小商店'
        when target_platform = 9 then '微信视频号'
        when target_platform = 10 then '小红书'
        when target_platform = 11 then '快团团'
    end as 平台
    ,case 
        when from = '1' then '铺货单' 
        when from = '2' then '商品详情页' 
    end as 来源
    ,name as 点击位置
    ,count(user_id) as 点击pv
    ,count(DISTINCT user_id) as 点击uv
from yishou_data.dcl_h5_distribution_goods_edit_page_dt
where dt between '20250101' and '20250515'
and name in ('修改标题','图片展示','图片裁剪')
group by 1,2,3,4