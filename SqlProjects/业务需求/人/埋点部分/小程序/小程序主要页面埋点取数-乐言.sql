-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/10/22 09:32:14 GMT+08:00
-- ******************************************************************** --
-- 小程序主要页面埋点取数-克虏伯
select 
    dt,位置,sum(click_uv) as click_uv,sum(exposure_uv) as exposure_uv
from (
    -- 首页弹窗
    -- yishou_data.dcl_wx_home_windows_exposure_d
    select 
        dt
        ,case 
            when get_json_object(data, '$.banner_id') in ('20080','37652') then '首页新人弹窗'
            else '首页活动弹窗'
        end as 位置
        ,count(DISTINCT get_json_object(data, '$.userid')) as click_uv
        ,0 as exposure_uv
    from yishou_data.ods_h5_event_action_d 
    where event = 'homewindows'
    and dt = '${one_day_ago}' and get_json_object(data, '$.banner_id') is not null
    group by 1,2
    union ALL 
    -- 首页
    -- yishou_data.dcl_wx_home_click_d
    select 
        dt
        ,case 
            when banner_id in ('20080','37652') then '首页新人弹窗'
            else '首页活动弹窗'
        end as 位置
        ,0 as click_uv
        ,count(DISTINCT user_id) as exposure_uv
    from yishou_data.dcl_wx_home_windows_exposure_d 
    where dt = '${one_day_ago}' and banner_id is not null
    group by 1,2
    union all
    select 
        dt
        ,case 
            when click_source = '首页市场上方banner' then '首页3坑运营位'
            when click_source = '首页市场bannner' then '首页市场导航'
            when click_source = '首页市场下方banner' then '首页市场下方banner' 
            when click_source = '营销频道' then '首页营销运营位'
        end as 位置
        ,count(DISTINCT user_id) as click_uv
        ,0 as exposure_uv
    from yishou_data.dcl_wx_home_click_d a
    -- left join (
    --     select 
    --         user_id,add_cart_id,good_price,good_number,dt 
    --     from yishou_data.dcl_wx_add_cart_click_d 
    --     where dt = '${one_day_ago}' and source in ('11','42') and add_cart_id is not null
    -- ) b on a.user_id = b.user_id and a.dt = b.dt
    -- left join (
    --     select user_id,sa_add_cart_id,buy_num,shop_price,dt from yishou_data.dwd_sale_order_info_dt where dt = '${one_day_ago}'
    -- ) c on a.user_id = c.user_id and concat(b.user_id,'_',SPLIT(b.add_cart_id, '_')[1] / 1000) = c.sa_add_cart_id
    where dt = '${one_day_ago}' 
    and click_source in ('首页市场上方banner','首页市场bannner','首页市场下方banner','营销频道')
    group by 1,2
    union all
    select 
        dt
        ,case 
            when home_market_top_banner is not null then '首页3坑运营位'
            when home_market_banner is not null then '首页市场导航'
            when home_market_below_banner is not null then '首页市场下方banner' 
            when marketing_channel is not null then '首页营销运营位'
        end as 位置
        ,0 as click_uv
        ,count(DISTINCT user_id) as exposure_uv
    from yishou_data.dcl_wx_home_exposure_d
    where dt = '${one_day_ago}'
    and (home_market_top_banner is not null or home_market_banner is not null or home_market_below_banner is not null or marketing_channel is not null)
    group by 1,2
    union all
    -- 搜索
    -- yishou_data.dcl_wx_search_click_d
    select 
        dt
        ,'商品搜索运营位' as 位置
        ,count(DISTINCT user_id) as click_uv
        ,0 as exposure_uv
    from yishou_data.dcl_wx_search_click_d
    where dt = '${one_day_ago}' and click_source = '搜索推荐运营位'
    group by dt
    union all
    select 
        dt
        ,'商品搜索运营位' as 位置
        ,0 as click_uv
        ,count(DISTINCT get_json_object(data, '$.userid')) as exposure_uv
    from yishou_data.ods_h5_event_action_d 
    where event = 'searchrecommendexposure'
    and dt = '${one_day_ago}'
    group by dt
    union all
    -- 个人中心
    select 
        dt
        ,'个人中心弹窗' as 位置
        ,count(DISTINCT get_json_object(data, '$.userid')) as click_uv
        ,0 as exposure_uv
    from yishou_data.ods_h5_event_action_d 
    where event = 'usercenterwindows'
    and dt = '${one_day_ago}'
    group by dt
    union ALL 
    select 
        dt
        ,'个人中心弹窗' as 位置
        ,0 as click_uv
        ,count(DISTINCT get_json_object(data, '$.userid')) as exposure_uv
    from yishou_data.ods_h5_event_action_d 
    where event = 'usercenterwindowsexposure'
    and dt = '${one_day_ago}'
    group by dt
)
group by 1,2
;
