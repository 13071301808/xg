-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/09/18 09:41:28 GMT+08:00
-- ******************************************************************** --
select 
    dt
    ,first_page_name
    ,second_page_name
    ,sum(exposure_pv) as exposure_pv
    ,sum(exposure_uv) as exposure_uv
    ,sum(click_uv) as click_uv
from (
    -- 曝光
    -- 首页市场
    select 
        dt
        ,'首页' as first_page_name
        ,'首页市场' as second_page_name
        ,count(user_id) as exposure_pv
        ,count(DISTINCT user_id) as exposure_uv
        ,0 as click_uv
    from yishou_data.dcl_event_home_exposure_d
    where dt = '20240917' and home_market is not null and home_market <> ''
    group by 1
    union ALL 
     -- 首页营销频道
    select 
        dt
        ,'首页' as first_page_name
        ,'首页营销频道' as second_page_name
        ,count(user_id) as exposure_pv
        ,count(DISTINCT user_id) as exposure_uv
        ,0 as click_uv
    from yishou_data.dcl_event_home_exposure_d
    where dt = '20240917' and marketing_channel is not null and marketing_channel <> ''
    group by 1
    union ALL 
    -- 首页分类
    select 
        dt
        ,'首页' as first_page_name
        ,'首页分类' as second_page_name
        ,count(user_id) as exposure_pv
        ,count(DISTINCT user_id) as exposure_uv
        ,0 as click_uv
    from yishou_data.dcl_event_home_exposure_d
    where dt = '20240917' and home_classification is not null and home_classification <> ''
    group by 1
    union ALL 
    -- 首页banner
    select 
        dt
        ,'首页' as first_page_name
        ,'首页banner' as second_page_name
        ,count(user_id) as exposure_pv
        ,count(DISTINCT user_id) as exposure_uv
        ,0 as click_uv
    from yishou_data.dcl_event_home_exposure_d
    where dt = '20240917' and real_home_top_banner is not null and real_home_top_banner <> ''
    group by 1
    union ALL 
    -- 首页浮层
    select 
        dt
        ,'首页' as first_page_name
        ,'首页浮层' as second_page_name
        ,count(user_id) as exposure_pv
        ,count(DISTINCT user_id) as exposure_uv
        ,0 as click_uv
    from yishou_data.dcl_event_home_exposure_d
    where dt = '20240917' and new_user_red_pack_window is not null and new_user_red_pack_window <> ''
    group by 1
    union ALL 
    -- 首页上三坑
    select 
        dt
        ,'首页' as first_page_name
        ,'首页上三坑' as second_page_name
        ,count(user_id) as exposure_pv
        ,count(DISTINCT user_id) as exposure_uv
        ,0 as click_uv
    from yishou_data.dcl_event_home_exposure_d
    where dt = '20240917' and new_activity_operating is not null and new_activity_operating <> ''
    group by 1
    union all
     -- 首页新人专享
    select 
        dt
        ,'首页' as first_page_name
        ,'首页新人专享' as second_page_name
        ,count(user_id) as exposure_pv
        ,count(DISTINCT user_id) as exposure_uv
        ,0 as click_uv
    from yishou_data.dcl_event_home_exposure_d
    where dt = '20240917' 
    and (new_user_special_area is not null or new_user_window is not null or new_user_limited_time_reward_banner is not null or new_user_red_pack_window is not null)
    group by 1
    union all
    -- 首页市场下方banner
    select 
        dt
        ,'首页' as first_page_name
        ,'首页市场下方banner' as second_page_name
        ,count(user_id) as exposure_pv
        ,count(DISTINCT user_id) as exposure_uv
        ,0 as click_uv
    from yishou_data.dcl_event_home_exposure_d
    where dt = '20240917' 
    and home_market_below_banner is not null
    group by 1
    union all
    -- 首页顶部图标
    select 
        dt
        ,'首页' as first_page_name
        ,'首页顶部图标' as second_page_name
        ,count(user_id) as exposure_pv
        ,count(DISTINCT user_id) as exposure_uv
        ,0 as click_uv
    from yishou_data.dcl_event_home_exposure_d
    where dt = '20240917' and home_top_icon is not null
    group by 1
    union all
    -- 首页活动banner
    select 
        dt
        ,'首页' as first_page_name
        ,'首页活动banner' as second_page_name
        ,count(user_id) as exposure_pv
        ,count(DISTINCT user_id) as exposure_uv
        ,0 as click_uv
    from yishou_data.dcl_event_home_exposure_d
    where dt = '20240917' and arc_activity_banner is not null
    group by 1
    union all
    -- 首页其他
    select 
        dt
        ,'首页' as first_page_name
        ,case 
            when home_market is null and arc_activity_banner is null and home_top_icon is null and home_market_below_banner is null 
            and (new_user_special_area is null or new_user_window is null or new_user_limited_time_reward_banner is null)
            and new_user_red_pack_window is null and new_activity_operating is null and real_home_top_banner is null 
            and home_classification is null and marketing_channel is null
            then '首页其他'
            else '首页其他'
        end as second_page_name
        ,count(user_id) as exposure_pv
        ,count(DISTINCT user_id) as exposure_uv
        ,0 as click_uv
    from yishou_data.dcl_event_home_exposure_d
    where dt = '20240917'
    group by 1,3
    union all
    -- 首页专场
    select 
        dt
        ,'首页' as first_page_name
        ,'首页专场' as second_page_name
        ,count(user_id) as exposure_pv
        ,count(DISTINCT user_id) as exposure_uv
        ,0 as click_uv
    from yishou_data.dcl_event_big_special_exposure_d 
    where dt = '20240917'
    group by 1
    union all
    -- 首页弹窗
    select  
        dt
        ,'首页' as first_page_name
        ,'首页弹窗' as second_page_name
        ,count(user_id) as exposure_pv
        ,count(DISTINCT user_id) as exposure_uv
        ,0 as click_uv
    from yishou_data.dcl_event_business_banner_show_d
    where dt = '20240917' and source in ('1','3','5','6')
    group by 1
    union all
    -- 首页分类页_新
    select 
        dt
        ,'首页' as first_page_name
        ,'首页分类页_新' as second_page_name
        ,count(user_id) as exposure_pv
        ,count(DISTINCT user_id) as exposure_uv
        ,0 as click_uv
    from yishou_data.dcl_event_home_classify_exposure 
    where dt = '20240917'
    group by 1
    union all
    -- 点击
    select
        dt
        ,'首页' as first_page_name
        ,case 
            when click_source = '搜索' then '首页搜索'
            -- when click_source in ('点击专场tab','专场筛选项','专场运营位','H5专场') then '首页专场'
            -- when click_source = '点击专场tab' then '首页专场'
            when click_source = '市场' then '首页市场'
            when click_source = '营销频道' then '首页营销频道'
            when click_source = '分类' then '首页分类'
            when click_source = '消息' then '首页消息'
            when click_source = '顶部banner' then '首页banner'
            when click_source in ('首页2023版会员弹窗','新人限时奖励弹窗','新人专享弹窗运营位(571)','首页倒计时弹窗') then '首页弹窗'
            when click_source = '首页-浮窗' then '首页浮层'
            when banner_name in ('1020新客标签组合实验组#左坑 好评档口榜','新客页面右坑动态货盘','1020新客标签组合实验组#中坑 最好卖的款') then '首页上三坑'
            when click_source in ('新人专享弹窗运营位(571)','首页倒计时弹窗','新人专享运营位(571)','新人专享模块','新人红包运营位(571)','新人红包浮窗运营位(571)') then '首页新人专享'
            when click_source = '首页市场下方banner' then '首页市场下方banner'
            when click_source = '首页顶部图标' then '首页顶部图标'
            when click_source = '弧形活动banner' then '首页活动banner'
            else '首页其他'
        end as second_page_name
        ,0 as exposure_pv
        ,0 as exposure_uv
        ,count(DISTINCT user_id) as click_uv
    from yishou_data.dcl_event_home_click_d
    where dt = '20240917' and user_id is not null and user_id <> ''
    group by 1,3
    union ALL 
    select 
        dt
        ,'首页' as first_page_name
        ,'首页专场' as second_page_name
        ,0 as exposure_pv
        ,0 as exposure_uv
        ,count(DISTINCT user_id) as click_uv
    from yishou_data.dcl_event_enter_special
    where dt = '20240917'
    group by 1
    union all
    select  
        dt
        ,'首页' as first_page_name
        ,'首页分类页_新' as second_page_name
        ,0 as exposure_pv
        ,0 as exposure_uv  
        ,count(DISTINCT user_id) as click_uv
    from yishou_data.dcl_event_home_classify_click_d
    where dt = '20240917'
    group by 1
)
group by 1,2,3
;
-- select click_source,count(1) from yishou_data.dcl_event_home_click_d where click_source = '弧形活动banner' and dt = '20240917' group by 1
-- select home_market,count(1) from yishou_data.dcl_event_home_exposure_d where dt = '20240917' group by 1;
