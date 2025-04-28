-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/12/16 11:40:43 GMT+08:00
-- ******************************************************************** 
-- 用户裂变活动漏斗
with temp_user_center as (
    -- 个人中心页曝光
    -- app
    select 
        dt
        , user_id
        , regexp_replace1(regexp_replace1(regexp_replace1(user_center_banner,'^\\[',''),'\\]$',''),'},\\{','}|-|{') as user_center_banner
        , regexp_replace1(regexp_replace1(regexp_replace1(my_activities_arr,'^\\[',''),'\\]$',''),'},\\{','}|-|{') as my_activities
    from yishou_data.dcl_event_user_center_exposure_d
    where dt = '${bizdate}' and is_number(user_id)
    and (user_center_banner is not null or my_activities_arr is not null)
    union all
    -- 小程序
    select 
        dt
        , user_id
        , regexp_replace1(regexp_replace1(regexp_replace1(user_center_banner,'^\\[',''),'\\]$',''),'},\\{','}|-|{') as user_center_banner
        , null as my_activities
    from yishou_data.dcl_wx_user_center_exposure_d
    where dt = '${bizdate}' and is_number(user_id)
),
temp_user_center_click as (
    -- 个人中心页点击
    -- app
    select 
        dt
        ,user_id
        ,banner_id
        ,case 
            when click_source = '我的活动' then '个人中心六坑位' 
            when click_source = '个人中心banner' then '个人中心banner'
        end as enter_name
        ,case 
            when banner_name in ('每日赚钱','邀请有奖') then '邀请有奖'
            when banner_name in ('助力拼团活动','助力活动','新人助力') then '助力活动'
        end as activity_name
    from yishou_data.dcl_event_user_center_click_d
    where dt = '${bizdate}' and is_number(user_id) and banner_id in ('17842','46064','48312','48410')
    and click_source in ('我的活动','个人中心banner')
    union all
    -- 小程序
    select 
        dt
        ,user_id
        ,banner_id
        ,case 
            when click_source = '我的活动' then '个人中心六坑位' 
            when click_source = '个人中心banner' then '个人中心banner'
        end as enter_name
        ,case 
            when banner_name in ('每日赚钱','邀请有奖') then '邀请有奖'
            when banner_name in ('助力拼团活动','助力活动','新人助力') then '助力活动'
        end as activity_name
    from yishou_data.dcl_wx_user_center_click_d
    where dt = '${bizdate}' and is_number(user_id) and banner_id in ('17842','46064','48312','48410')
    and click_source in ('我的活动','个人中心banner')
),
temp_user_center_windows as (
    -- 个人中心弹窗
    -- app
    select 
        dt
        ,user_id
        ,banner_id
        ,'个人中心弹窗' as enter_name
        ,case 
            when banner_id in ('23014','46065') then '邀请有奖' 
            when banner_id = '50336' then '助力活动' 
        end as activity_name
    from yishou_data.dcl_personal_center_window_d
    where dt = '${bizdate}' and is_number(user_id) and banner_id in ('23014','46065','50336')
    and clickSource = '曝光'
    union all
    -- 小程序
    select 
        dt
        ,user_id
        ,banner_id
        ,'个人中心弹窗' as enter_name
        ,case 
            when banner_id in ('23014','46065') then '邀请有奖' 
            when banner_id = '50336' then '助力活动' 
        end as activity_name
    from yishou_data.dcl_wx_user_center_windows_exposure_d
    where dt = '${bizdate}' and is_number(user_id) and banner_id in ('23014','46065','50336')
),
temp_user_center_windows_click as (
    -- 个人中心弹窗
    -- app
    select 
        dt
        ,user_id
        ,banner_id
        ,'个人中心弹窗' as enter_name
        ,case 
            when banner_id in ('23014','46065') then '邀请有奖' 
            when banner_id = '50336' then '助力活动' 
        end as activity_name
    from yishou_data.dcl_personal_center_window_d
    where dt = '${bizdate}' and is_number(user_id) and banner_id in ('23014','46065','50336')
    and clickSource != '曝光'
    union ALL 
    -- 小程序
    select 
        dt
        ,user_id
        ,banner_id
        ,'个人中心弹窗' as enter_name
        ,case 
            when banner_id in ('23014','46065') then '邀请有奖' 
            when banner_id = '50336' then '助力活动' 
        end as activity_name
    from yishou_data.dcl_wx_user_center_windows_d
    where dt = '${bizdate}' and is_number(user_id) and banner_id in ('23014','46065','50336')
),
temp_pay_windows as (
    -- 支付成功弹窗
    -- app
    select 
        dt
        ,user_id
        ,banner_id
        ,'支付成功弹窗' as enter_name                                                 
        ,'邀请有奖' as activity_name
    from yishou_data.dcl_event_business_banner_show_d
    where dt = '${bizdate}' and is_number(user_id) and banner_id in ('45062','46067') 
    and source = 2
),
-- temp_pay_windows_click as (
--     -- 支付成功弹窗点击
--     -- app
--     select 
--         dt
--         ,user_id
--         ,banner_id
--         ,'支付成功弹窗' as enter_name
--         ,'邀请有奖' as activity_name
--     from yishou_data.dcl_event_window_effect_d
--     where dt = '${bizdate}' and is_number(user_id) and banner_id in ('45062','46067') 
--     and source = 2
-- ),
temp_order_detail as (
    -- 订单详情公告位
    -- app
    select 
        dt
        ,user_id
        ,get_json_object(notice_banner_dict,'$.banner_id') as banner_id
        ,'订单详情公告位' as enter_name
        ,'邀请有奖' as activity_name
    from (
        select 
            dt
            ,user_id
            ,regexp_replace1(regexp_replace1(regexp_replace1(notice_banner,'^\\[',''),'\\]$',''),'},\\{','}|-|{') as notice_banner
        from yishou_data.dcl_event_order_detail_exposure_d
        where dt = '${bizdate}' and is_number(user_id)
        and notice_banner is not null
    )
    lateral view outer explode(split(notice_banner, '\\|-\\|'))t as notice_banner_dict
    where get_json_object(notice_banner_dict,'$.banner_id') in ('45805','46068')
),
temp_goods_detail as (
    -- 商品详情页
    -- app
    select 
        dt
        ,user_id
        ,get_json_object(operation_banner_dict,'$.banner_id') as banner_id
        ,'商品详情页' as enter_name
        ,'助力活动' as activity_name
    from (
        select 
            dt
            ,user_id
            ,regexp_replace1(regexp_replace1(regexp_replace1(operation_banner_arr,'^\\[',''),'\\]$',''),'},\\{','}|-|{') as operation_banner_arr
        from yishou_data.dcl_event_goods_detail_page_exposure_d
        where dt = '${bizdate}' and is_number(user_id)
    )
    lateral view outer explode(split(operation_banner_arr, '\\|-\\|'))t as operation_banner_dict
    where get_json_object(operation_banner_dict,'$.banner_id') = '48406'
    -- union all 
    -- -- 小程序
    -- select 
    --     dt
    --     ,user_id
    --     ,banner_id
    --     ,'商详页助力横条' as enter_name
    --     ,'助力活动' as activity_name
    -- from yishou_data.dcl_wx_good_detail_page_exposure_d
    -- where dt = '${bizdate}' and is_number(user_id) 
    -- and assistance_banner_arr is not null and banner_id is not null
),
temp_goods_detail_click as (
    -- 商品详情页点击
    -- app
    select 
        dt
        ,user_id
        ,banner_id
        ,'商品详情页' as enter_name
        ,'助力活动' as activity_name
    from yishou_data.dcl_event_goods_detail_page_click_d
    where dt = '${bizdate}' and is_number(user_id) and banner_id = '48406'
    and click_source = '运营位'
    union all
    -- 小程序
    select 
        dt
        ,user_id
        ,banner_id
        ,case 
            when click_source = '商详运营位' then '商品详情页'
            -- when click_source = '邀新助力价横条' then '商详页助力横条'
        end as enter_name
        ,'助力活动' as activity_name
    from yishou_data.dcl_wx_good_detail_page_click_d
    where dt = '${bizdate}' and is_number(user_id)
    -- and click_source in ('邀新助力价横条','商详运营位') 
    and click_source = '商详运营位' 
    -- and banner_id is not null
    and banner_id = '48406'
),
temp_home_page as (
    -- 首页
    select 
        dt
        ,user_id
        ,get_json_object(new_user_red_pack_window_dict,'$.banner_id') as banner_id
        ,'首页浮层' as enter_name
        ,'助力活动' as activity_name
    from (
        select 
            dt
            ,user_id
            ,regexp_replace1(regexp_replace1(regexp_replace1(new_user_red_pack_window,'^\\[',''),'\\]$',''),'},\\{','}|-|{') as new_user_red_pack_window
        from yishou_data.dcl_event_home_exposure_d
        where dt = '${bizdate}' and is_number(user_id) 
        and new_user_red_pack_window is not null
    )
    lateral view outer explode(split(new_user_red_pack_window, '\\|-\\|'))t as new_user_red_pack_window_dict
    where get_json_object(new_user_red_pack_window_dict,'$.banner_id') in ('51249','51540')
),
temp_home_page_click as (
    -- 首页点击
    -- app
    select 
        dt
        ,user_id
        ,banner_id
        ,'首页浮层' as enter_name
        ,'助力活动' as activity_name
    from yishou_data.dcl_event_home_click_d
    where dt = '${bizdate}' and is_number(user_id) and banner_id in ('51249','51540')
    and click_source = '首页-浮窗'
),
user_sale as (
    -- 订单类字段
    select 
        coalesce(to_char(dyuod.reg_time,'yyyymmdd'),to_char(dyuod.real_first_time,'yyyymmdd')) as dt
        ,dyuod.user_id
        ,count(dyuod.user_id) as reg_num
        ,count(if(dyuod.real_first_order_id is not null,dyuod.user_id,null)) as first_order_num
        ,count(if(dyuod.real_first_buy_amount>=100,dyuod.user_id,null)) as effective_first_order_num
        ,count(if(dyuod.real_first_14d_buy_amount>=300,dyuod.user_id,null)) as effective_new_prople_num
    from yishou_data.dim_ys_user_order_data dyuod
    left join (
        select user_id,primary_channel_name,second_channel_name from yishou_data.dim_user_superman_franky_snap_dt where dt = '${bizdate}'
    ) dusfs on dyuod.user_id = dusfs.user_id
    where dusfs.primary_channel_name = '用户裂变' and dusfs.second_channel_name = '店主拉店主'
    group by 1,2
)
select 
    dt
    ,banner_id
    ,enter_name as 进入运营位名称
    ,activity_name as 活动名称
    ,sum(banner_exposure_uv) as 运营位曝光UV
    ,sum(banner_click_uv) as 运营位点击UV
    ,sum(reg_num) as 注册量
    ,sum(first_order_num) as 首单量
    ,sum(effective_first_order_num) as 有效首单
    ,sum(effective_new_prople_num) as 有效新客
from (
    -- 个人中心六坑位
    select 
        a.dt
        ,get_json_object(my_activities_dict,'$.banner_id') as banner_id
        ,'个人中心六坑位' as enter_name
        ,case 
            when get_json_object(my_activities_dict,'$.banner_name') = '每日赚钱' then '邀请有奖'
            when get_json_object(my_activities_dict,'$.banner_name') = '新人助力' then '助力活动'
        end as activity_name
        ,count(DISTINCT a.user_id) as banner_exposure_uv
        ,0 as banner_click_uv
        ,sum(reg_num) as reg_num
        ,sum(first_order_num) as first_order_num
        ,sum(effective_first_order_num) as effective_first_order_num
        ,sum(effective_new_prople_num) as effective_new_prople_num
    from temp_user_center a
    left join user_sale b on a.dt = b.dt and a.user_id = b.user_id
    lateral view outer explode (split(a.my_activities, '\\|-\\|'))t as my_activities_dict
    where my_activities is not null and a.my_activities <> ''
    and get_json_object(my_activities_dict,'$.banner_id') in ('17842','46064','48312')
    group by 1,2,3,4
    union all 
    -- 个人中心banner
    select 
        a.dt
        ,get_json_object(user_center_banner_dict,'$.banner_id') as banner_id
        ,'个人中心banner' as enter_name
        ,'助力活动' as activity_name
        ,count(DISTINCT a.user_id) as banner_exposure_uv
        ,0 as banner_click_uv
        ,sum(reg_num) as reg_num
        ,sum(first_order_num) as first_order_num
        ,sum(effective_first_order_num) as effective_first_order_num
        ,sum(effective_new_prople_num) as effective_new_prople_num
    from temp_user_center a
    left join user_sale b on a.dt = b.dt and a.user_id = b.user_id
    lateral view outer explode (split(a.user_center_banner, '\\|-\\|'))t as user_center_banner_dict
    where user_center_banner is not null and user_center_banner <> ''
    -- and get_json_object(user_center_banner_dict,'$.banner_name') like '%助力拼团活动%'
    and get_json_object(user_center_banner_dict,'$.banner_id') = '48410'
    group by 1,2,3,4
    union all
    -- 个人中心点击
    select   
        a.dt
        ,banner_id
        ,a.enter_name
        ,a.activity_name
        ,0 as banner_exposure_uv
        ,count(DISTINCT a.user_id) as banner_click_uv
        ,sum(reg_num) as reg_num
        ,sum(first_order_num) as first_order_num
        ,sum(effective_first_order_num) as effective_first_order_num
        ,sum(effective_new_prople_num) as effective_new_prople_num
    from temp_user_center_click a
    left join user_sale b on a.dt = b.dt and a.user_id = b.user_id
    group by 1,2,3,4
    union all
    -- 个人中心弹窗
    select 
        a.dt
        ,a.banner_id
        ,a.enter_name
        ,a.activity_name
        ,count(DISTINCT a.user_id) as banner_exposure_uv
        ,0 as banner_click_uv
        ,sum(reg_num) as reg_num
        ,sum(first_order_num) as first_order_num
        ,sum(effective_first_order_num) as effective_first_order_num
        ,sum(effective_new_prople_num) as effective_new_prople_num
    from temp_user_center_windows a
    left join user_sale b on a.dt = b.dt and a.user_id = b.user_id
    group by 1,2,3,4
    union ALL 
    -- 个人中心弹窗点击
    select 
        a.dt
        ,a.banner_id
        ,a.enter_name
        ,a.activity_name
        ,0 as banner_exposure_uv
        ,count(DISTINCT a.user_id) as banner_click_uv
        ,sum(reg_num) as reg_num
        ,sum(first_order_num) as first_order_num
        ,sum(effective_first_order_num) as effective_first_order_num
        ,sum(effective_new_prople_num) as effective_new_prople_num
    from temp_user_center_windows_click a
    left join user_sale b on a.dt = b.dt and a.user_id = b.user_id
    group by 1,2,3,4
    union all
    -- 支付成功弹窗
    select 
        a.dt
        ,a.banner_id
        ,a.enter_name
        ,a.activity_name
        ,count(DISTINCT a.user_id) as banner_exposure_uv
        ,0 as banner_click_uv
        ,sum(reg_num) as reg_num
        ,sum(first_order_num) as first_order_num
        ,sum(effective_first_order_num) as effective_first_order_num
        ,sum(effective_new_prople_num) as effective_new_prople_num
    from temp_pay_windows a
    left join user_sale b on a.dt = b.dt and a.user_id = b.user_id
    group by 1,2,3,4
    union all
    -- 订单详情公告位
    select 
        a.dt
        ,a.banner_id
        ,a.enter_name
        ,a.activity_name
        ,count(DISTINCT a.user_id) as banner_exposure_uv
        ,0 as banner_click_uv
        ,sum(reg_num) as reg_num
        ,sum(first_order_num) as first_order_num
        ,sum(effective_first_order_num) as effective_first_order_num
        ,sum(effective_new_prople_num) as effective_new_prople_num
    from temp_order_detail a
    left join user_sale b on a.dt = b.dt and a.user_id = b.user_id
    group by 1,2,3,4
    union all
    -- 商品详情页
    select 
        a.dt
        ,a.banner_id
        ,a.enter_name
        ,a.activity_name
        ,count(DISTINCT a.user_id) as banner_exposure_uv
        ,0 as banner_click_uv
        ,sum(reg_num) as reg_num
        ,sum(first_order_num) as first_order_num
        ,sum(effective_first_order_num) as effective_first_order_num
        ,sum(effective_new_prople_num) as effective_new_prople_num
    from temp_goods_detail a
    left join user_sale b on a.dt = b.dt and a.user_id = b.user_id
    group by 1,2,3,4
    union all
    -- 商品详情页点击
    select 
        a.dt
        ,a.banner_id
        ,a.enter_name
        ,a.activity_name
        ,0 as banner_exposure_uv
        ,count(DISTINCT a.user_id) as banner_click_uv
        ,sum(reg_num) as reg_num
        ,sum(first_order_num) as first_order_num
        ,sum(effective_first_order_num) as effective_first_order_num
        ,sum(effective_new_prople_num) as effective_new_prople_num
    from temp_goods_detail_click a
    left join user_sale b on a.dt = b.dt and a.user_id = b.user_id
    group by 1,2,3,4
    union ALL 
    -- 首页浮层
    select 
        a.dt
        ,a.banner_id
        ,a.enter_name
        ,a.activity_name
        ,count(DISTINCT a.user_id) as banner_exposure_uv
        ,0 as banner_click_uv
        ,sum(reg_num) as reg_num
        ,sum(first_order_num) as first_order_num
        ,sum(effective_first_order_num) as effective_first_order_num
        ,sum(effective_new_prople_num) as effective_new_prople_num
    from temp_home_page a
    left join user_sale b on a.dt = b.dt and a.user_id = b.user_id
    group by 1,2,3,4
    union ALL 
    -- 首页浮层点击
    select 
        a.dt
        ,a.banner_id
        ,a.enter_name
        ,a.activity_name
        ,0 as banner_exposure_uv
        ,count(DISTINCT a.user_id) as banner_click_uv
        ,sum(reg_num) as reg_num
        ,sum(first_order_num) as first_order_num
        ,sum(effective_first_order_num) as effective_first_order_num
        ,sum(effective_new_prople_num) as effective_new_prople_num
    from temp_home_page_click a
    left join user_sale b on a.dt = b.dt and a.user_id = b.user_id
    group by 1,2,3,4
)
group by 1,2,3,4
