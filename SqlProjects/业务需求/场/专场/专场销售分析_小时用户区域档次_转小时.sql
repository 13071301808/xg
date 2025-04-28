-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/07/13 10:57:59 GMT+08:00
-- ******************************************************************** --
-- 目的：转为小时
DROP table IF EXISTS yishou_daily.temp_special_info_user_info_20240713;
CREATE TABLE IF NOT EXISTS yishou_daily.temp_special_info_user_info_20240713
AS
SELECT
    coalesce(t1.user_id,t2.user_id) as user_id
    ,case when goods_preference_level in (1,2) then '高档'--高档
        when goods_preference_level = 3 then '中档'--中档
        when goods_preference_level in (4,5) then '低档'--低档
        else '中档' 
    end as goods_preference_level
    ,case when substr(t2.province_name,1,2) in ('黑龙','吉林','辽宁','天津','北京','河北','山西','内蒙','山东','新疆','宁夏','甘肃','青海','陕西','西藏') then '北部'
        when substr(t2.province_name,1,2) in ('香港','澳门','台湾','海南','福建','广东','广西','云南') then '南部'
        when substr(t2.province_name,1,2) in ('上海','浙江','江苏','江西','安徽','湖北','湖南','河南','四川','重庆','贵州') then '中部'
        else '未知' 
    end as province_region_level
FROM yishou_daily.dtl_new_user_feature t1
full join yishou_data.all_fmys_user_region_h t2 
on t1.user_id = t2.user_id
where t1.dt = to_char(dateadd(to_date1('${bizdate}','yyyymmdd'),-1,'dd'),'yyyymmdd');


DROP TABLE IF EXISTS yishou_daily.temp_special_info_user_data_20240713;
CREATE TABLE IF NOT EXISTS yishou_daily.temp_special_info_user_data_20240713
-- 创建各个埋点表的小时子表
with 
special_exposure_table as (
    -- 专场列表曝光数据(对应dcl_event_special_exposure_d)
    select 
        json_parse(action,'$.time') as time
        ,json_parse(action,'$.distinct_id') as distinct_id
        ,json_parse(action,'$.properties.origin_name') as origin_name
        ,json_parse(action,'$.properties.os') as os
        ,json_parse(action,'$.properties.os_version') as os_version
        ,json_parse(action,'$.properties.page') as page
        ,json_parse(action,'$.properties.ptime') as ptime
        ,json_parse(action,'$.properties.reportTime') as report_time
        ,json_parse(action,'$.properties.special_ids') as special_ids
        ,json_parse(action,'$.properties.specialCount') as special_count
        ,json_parse(action,'$.properties.udid') as udid
        ,json_parse(action,'$.properties.userid') as user_id
        ,dt
    from yishou_data.ods_app_event_log_d
    where event = 'specialexposure' 
    AND dt >= to_char(dateadd(to_date1(${cyctime} ,'yyyymmddhhmiss'), -8, 'hh'),'yyyymmdd')
    AND to_char(dateadd (from_unixtime (get_json_object(action, '$.time') / 1000), -7, 'hh'), 'yyyymmdd') = to_char(dateadd(to_date1(${cyctime} ,'yyyymmddhhmiss'), -8, 'hh'),'yyyymmdd')
),
big_special_exposure_table as (
    -- 大专场列表曝光表（对应dcl_event_big_special_exposure_d）
    select 
          time
        , case when is_number(user_id) then user_id else 0 end as user_id  
        , coalesce(get_json_object(special_dict, '$.special_id'), get_json_object(special_dict, '$.special_id')) as special_id 
        , get_json_object(special_dict, '$.home_index') as home_index
        , get_json_object(special_dict, '$.index') as exposure_index
        , get_json_object(special_dict, '$.tab_name') as tab_name
        , os 
        , cast(report_time as string) as report_time
        , os_version
        , plate
        , event_id
        , get_json_object(special_dict, '$.index') as index
        , get_json_object(special_dict, '$.special_name') as special_name
        , get_json_object(special_dict, '$.is_operat') as is_operat
        , dt 
    from yishou_data.temp_special_big_expsoure_20240722
    lateral view  explode (split(special, '\\|-\\|'))t as special_dict
    where special is not null 
    and special <> ''
    and user_id is not null
),
special_banner_table as (
    -- 大专场banner曝光表
    select 
          time
        , case when is_number(user_id) then user_id else 0 end as user_id  
        , coalesce(get_json_object(banner_dict, '$.banner_id'), get_json_object(banner_dict, '$.banner_id')) as banner_id 
        , get_json_object(banner_dict, '$.home_index') as home_index
        , get_json_object(banner_dict, '$.index') as exposure_index
        , get_json_object(banner_dict, '$.tab_name') as tab_name
        , os 
        , cast(report_time as string) as report_time
        , os_version
        , plate 
        , get_json_object(banner_dict, '$.parent_banner_id') as parent_banner_id
        , get_json_object(banner_dict, '$.goods_id') as goods_id
        , get_json_object(banner_dict, '$.stall_id') as stall_id
        , get_json_object(banner_dict, '$.room_id') as room_id
        , get_json_object(banner_dict, '$.rank_name') as rank_name
        , event_id
        , get_json_object(banner_dict, '$.index') as index 
        , dt 
    from yishou_data.temp_special_big_expsoure_20240722
    lateral view  explode (split(banner, '\\|-\\|'))t as banner_dict
    where banner is not null 
    and banner <> ''
    and user_id is not null
),
enter_special_table as (
    -- 进入专场表
    select 
        from_unixtime(time / 1000) as time
        , datetrunc(dateadd(from_unixtime(time / 1000),-7,'hh'), 'dd') as special_date
        , distinct_id
        -- , system
        , isaorb
        , special_name
        , tab_id
        , case when special_id = '' or not is_number(special_id) then 0 else special_id end as special_id
        , tab_name
        , is_first_day
        , case when user_id = '' or not is_number(user_id) then 0 else user_id end as user_id 
        , app_version
        , device_id
        , case when trim(special_source) = '' or not is_number(special_source) then 0 else special_source end as special_source 
        , case when trim(special_pos) = '' or not is_number(special_pos) then 0 else special_pos end as special_pos
        -- , network_type
        -- , manufacturer
        -- , carrier
        , udid
        , case when trim(ptime) = '' or not is_number(ptime) then 0 else ptime end as ptime
        , source_event_id
    	, event_id 
    	, if(is_operat <> '',is_operat,null) is_operat
        , dt
    from yishou_data.temp_enter_special_20240722
),
all_special_exposure_table as (
    -- 专场大曝光汇总（对应dwd_log_big_special_exposure_dt）
    -- 专场列表曝光
    select 
        from_unixtime(time / 1000) as time,
        datetrunc(dateadd(from_unixtime(time / 1000),-7,'hh'), 'dd') as special_date,
        distinct_id,
        origin_name,
        os,
        os_version
        , case when page = '' or not is_number(page) then 0 else page end as page
        , case when ptime = '' or not is_number(ptime) then 0 else ptime end as ptime
        , '0' as report_time--from_unixtime(report_time) as report_time
        , special_id
        , case when special_count = '' or not is_number(special_count) then 0 else special_count end as special_count
        , udid
        , case when user_id = '' or not is_number(user_id) then 0 else user_id end as user_id
        , null as home_index
        , null as banner_id 
        , null as event_id
        , null as index
        , null as special_name
        , null as is_operat
        , dt
    from special_exposure_table 
    lateral view  explode (split(special_ids,',') )t as special_id 
    union all
    -- 专场大曝光
    select 
        from_unixtime(time) as time,
        datetrunc(dateadd(from_unixtime(time),-7,'hh'), 'dd') as special_date,
        null as distinct_id,
        tab_name,
        os,
        os_version,
        '0' as page,
        '0' as ptime,
        '0' as report_time,
        --from_unixtime(report_time) as report_time,
        cast(special_id as string ),
        '0' as special_count,
        null as udid,
        case when user_id = '' or not is_number(user_id) then 0 else user_id end as user_id,
        home_index,
        null as banner_id,
        event_id,
        index,
        special_name,
        if(is_operat <> '',is_operat,null),
        dt
    from big_special_exposure_table 
    union all
    -- 专场运营位曝光
    select 
        from_unixtime(time) as time,
        datetrunc(dateadd(from_unixtime(time),-7,'hh'), 'dd') as special_date,
        null as distinct_id,
        tab_name,
        os,
        os_version,
        '0' as page,
        '0' as ptime,
        '0' as report_time,
        null as sepcial_id,
        '0' as special_count,
        null as udid,
        case when user_id = '' or not is_number(user_id) then 0 else user_id end as user_id,
        home_index,
        cast(banner_id as string),
        event_id as event_id,
        index as index,
        null as special_name,
        null as is_operat,
        dt
    from special_banner_table
),
addcart_table as (
    -- 加购表
    select 
        from_unixtime(time / 1000) as time
        , datetrunc(dateadd(from_unixtime(time / 1000),-7,'hh'), 'dd') as special_date
        , distinct_id
        , user_id
        , case when goods_id <> '' and length(goods_id)> 0 and is_number(goods_id) then goods_id else '0' end as goods_id
        , os
        , case when source = '' or not is_number(source) then 0 else source end as source
        , case when source = '0' then '默认'
                when source = '1' then '快速补货'
                when source = '2' then '我的收藏'
                when source = '3' then '订单'
                when source = '4' then '首页->专场->详情'
                when source = '5' then '首页->详情'
                when source = '6' then '我的足迹'
                when source = '7' then '店主推荐(购物车底部推荐商品)'
                when source = '8' then '分类'
                when source = '9' then '购物车'
                when source = '10' then '特定专场'
                when source = '11' then '档口详情'
                when source = '12' then '好卖反馈'
                when source = '13' then '订单搜索'
                when source = '14' then '商品退款'
                when source = '15' then '搜索列表'
                when source = '16' then '退款详情页面'
                when source = '18' then '个人中心商品推荐'
                when source = '19' then '市场着陆页'
                when source = '20' then '详情推荐商品(商品详情底部推荐商品)'
                when source = '21' then '以图搜图'
                when source = '22' then '找相似'
                when source = '23' then '一手币商城'
                when source = '24' then '个性化推荐H5'
                when source = '25' then '品牌推荐H5'
                when source = '26' then '一手币组合'
                when source = '27' then '商品详情档口爆款 '
                when source = '28' then '直播'
                when source = '29' then '一手币筛选结果列表'
                when source = '30' then '搭配购'
                when source = '31' then '今日上新'
                when source = '32' then '今日特卖'
                when source = '33' then '订阅上架找相似'
                when source = '34' then '今日特卖闪拼商品'
                when source = '35' then '相似档口'
                when source = '36' then '首页上新'
                when source = '37' then '买手主页'
                when source = '38' then '聚合页'
                when source = '39' then '直播-正在讲解或搭配'
                when source = '40' then '市场档口排榜单'
                when source = '41' then '商品榜单'
                when source = '42' then '市场主页商品'
                when source = '43' then '点播'
                when source = '44' then '专属好货'
                when source = '45' then '直播预告'
                when source = '46' then '降价推荐'
                when source = '47' then '小视频播放页'
                when source = '48' then '品牌馆'
                when source = '49' then '购物车-编辑'
                when source = '50' then '今日特卖-秒杀商品'
                when source = '51' then '秒杀聚合页'
                when source = '52' then '关注-商品'
                when source = '53' then '关注-足迹'
                when source = '54' then '首页容器化'
                when source = '55' then '策略中心'
                when source = '56' then '我的档口商品列表'
                when source = '57' then '会员提前看款列表页（我的专属）'
                when source = '58' then '商品详情档口大爆款'
                when source = '59' then '商品详情档口最新款'
                when source = '60' then '首页挽回弹窗(经商详)'
                when source = '61' then '首页挽回弹窗(不经商详)'
                when source = '62' then '首页多商品弹窗'
                when source = '63' then '达人搭配feed流'
                when source = '64' then '达人搭配feed流关注列表'
                when source = '65' then '一衣多搭搭配购'
                when source = '66' then '无忧退商品'
                when source = '67' then '档口评价页'
                when source = '68' then '评价消息提醒页面'
                when source = '70' then '订阅-商品聚合页'
                when source = '71' then '订阅-商品聚合页直接加购'
                when source = '72' then '首页订阅页-档口动态'
                when source = '73' then '达人主页推荐界面'
                when source = '74' then '搭配盘（不经过商详）'
                when source = '75' then '首页分类页'
                when source = '76' then '足迹直接加购(不经过商详)'
                when source = '77' then '足迹结果页进入商详加购'
                when source = '78' then '足迹结果页直接加购'
                when source = '79' then '档口主页搜索结果页'
                when source = '80' then '发货进度跟踪'
                when source = '82' then '首页订阅页-商品集'
                when source = '83' then '个人中心订阅档口-商品集'
                when source = '84' then '个人中心订阅档口-档口动态'
                when source = '501' then '专场半屏购'
                when source = '1472'	then '新人专享价-商品列表'
                when source = '1476'	then '首发新款-大牌首发'
                when source = '1477'	then '首发新款-新品快抢'
                when source = '1478'	then '首发新款-更多新品'
                when source = '1479'	then '预流失用户落地页'
                when source = '1487'	then '秋上新页面-怎么搭最好卖整套入更划算'
                when source = '1488'	then '秋上新页面-秋季人气新品商品点击'
                when source = '1489'	then '秋上新页面-新品24h发专区'
                when source = '1490'	then '秋上新页面-无理由退货专区'
                when source = '1491'	then '私人组货专场'
                when source = '1492'	then '种草秀'
                when source = '1493'	then '新人专享价-爆款推荐'
                when source = '1494'	then '热销频道'
                when source = '1495'	then '新版首发新款'
                when source = '1496'	then '新热销频道'
                when source = '1498'	then '新版新人专享价'
                when source = '1499'	then '首发新款简单版'
                when source = '1500'	then '商品榜单'
                when source = '1501'	then '快速补货'
                when source = '1502'	then '档口补货聚合页'
                when source = '1503'	then '全站商品'
                else source end
            as source_label
        , goods_category_name
        , keyword
        , case when special_id = '' or not is_number(special_id) then 0 else special_id end as special_id
        , category_name
        , goods_name
        , app_version
        , is_first_day
        , model
        , device_id
        , network_type
        , carrier
        , goods_number
        , wifi
        , case when add_goods_amount = '' or not is_number(add_goods_amount) then 0 else add_goods_amount end as add_goods_amount
        , specialname
        , case when goods_price = '' or not is_number(goods_price) then 0 else goods_price end as goods_price
        , stall_name
        , os_version
        , category_id
        , stall_id
        , ip
        , is_login_id
        , country
        , province
        , city
        , category_source
        , special_source
        , stall_source
        , today_special_offer_name
        , is_buy_now
        , activity_id
        , is_recommend_search
        , add_cart_id
        , origin_name
        , category_banner_name
        , stall_banner_id
        , search_from_source
        , live_id
        , is_default
        , strategy_id
        , is_operat
        ,source_search_event_id
        ,source_event_id 
        ,goods_index 
        ,event_id
        ,landing_event_id
        ,pgm_id
        ,stallcomment_source
        ,pgm_code
        ,two_level_source
        ,campaign_event_id
        ,activity_name 
        ,content_id 
        ,author_id 
        ,goods_seat_id 
        ,tab_name 
        ,special_index
        ,is_pro
        ,campaign_h5_banner_id
        ,ladder_price 
        ,ladder_threshold_number
        ,sku_json
        ,tick_sort_filter_item_names
        ,tick_filter_item_names
        ,tick_shortcut_filter_item_names
        ,tick_fine_filter_item_names
        , dt
    from yishou_data.temp_add_cart_20240722
    where length(user_id) > 0 and is_number(user_id)
    and length(goods_number) > 0 and is_number(goods_number)
),
goodsdetailpage_table as (
    -- 查看商详表
    select 
        from_unixtime(time / 1000) as time
        , datetrunc(dateadd(from_unixtime(time / 1000),-7,'hh'), 'dd') as special_date
        , distinct_id
        , os
        , ab_test
        , marker_type
        , source
        , case when goods_id = '' or not is_number(goods_id) then 0 else goods_id end as goods_id
        , goods_name
        , cat_name
        , keyword
        , case when special_id = '' or not is_number(special_id) then 0 else special_id end as special_id
        , special_name
        , case when category_source_id = '' or not is_number(category_source_id) then 0 else category_source_id end as category_source_id
        , category_name
        , goods_price
        , goods_tag_url
        , case when stall_id = '' or not is_number(stall_id) then 0 else stall_id end as stall_id
        , stall_name
        , city
        , province
        , country
        , is_first_day
        , case when user_id = '' or not is_number(user_id) then 0 else user_id end as user_id
        , app_version
        , device_id
        , special_source
        , stall_source
        , case when stall_banner_id = '' or not is_number(stall_banner_id) then 0 else stall_banner_id end as stall_banner_id
        , alone_screen_type
        , ptime
        , case when room_id = '' or not is_number(room_id) then 0 else room_id end as room_id
        , network_type
        , udid
        , manufacturer
        , carrier
        , os_version
        , category_source
        , goods_category_name
        , today_special_offer_name
        , activity_id
        , remind_type
        , is_recommend_search
        , search_from_source
        , origin_name
        , is_default
        , strategy_id
        , is_operat
        , goods_index
        ,event_id
        ,landing_event_id
        ,pgm_id
        ,stallcomment_source
        ,pgm_code
        ,two_level_source
        ,campaign_event_id
        ,status 
        ,welfare_type 
        ,exclusive_type 
        ,activity_name 
        ,content_id 
        ,author_id 
        ,goods_seat_id 
        ,tab_name 
        ,is_pro
        ,category_banner_name 
        ,source_event_id
        ,campaign_h5_banner_id 
        ,footprint_source_page 
        ,source_search_event_id 
        ,image_search_source 
        ,message_type
        ,ladder_price 
        ,ladder_threshold_number
        ,tick_sort_filter_item_names
        ,tick_filter_item_names
        ,tick_shortcut_filter_item_names
        ,tick_fine_filter_item_names
        , dt 
    from yishou_data.temp_goodsdetailpage_20240722
    where length(goods_id) > 0 and is_number(goods_id)
),
pre_goods_exposure_table as (
    -- dcl_event_goods_exposure_d
    SELECT 
        DISTINCT 
        user_id,
        goods_id,
        special_id,
        os,
        goods_no,
        pid,
        ptime,
        special_time,
        source,
        report_time,
        event_id,
        search_event_id,
        goods_count,
        click_goods_id_count,
        keyword,
        app_version,
        log_type,
        is_rec,
        abtest,
        INDEX,
        strategy_id,
        is_operat,
        is_default,
        special_index,
        page_name,
        dt
    FROM 
        yishou_data.temp_dwd_event_goods_exposure_incr_h
    WHERE 
        dt >= '${today}'
),
goods_exposure_table as (
    -- dwd_log_goods_exposure_dt
    select 
        case when user_id = '' or not is_number(user_id) then 0 else user_id end as user_id   -- 用户id
        , case when goods_id = '' or not is_number(goods_id) then 0 else goods_id end as goods_id     -- 商品id
        , case when special_id = '' or not is_number(special_id) then 0 else special_id end as special_id   -- 专场id  
        , os       -- 操作系统
        , goods_no     -- 商品货号
        , pid      -- 来源：10-专场,11-个人中心,12-分类详情,13-市场,14-搜索结果,15-按图搜款,16、支付结果,17、档口
        , ptime    -- 页面停留时间
        , datetrunc(to_date1(special_time,'yyyymmdd'),'dd') as goods_special_date   -- 商品的专场时间
        , datetrunc(dateadd(from_unixtime(report_time/1000), -7, 'hh'), 'dd') as special_date    -- 专场日期
        , source      -- 专场列表来源 ，使用场景 1-专场点入 3-活动点入 4-推送点入 5-通知入口断货闪亮返场进入 6-市场着陆页补货专场入口
        , from_unixtime(report_time/1000) as report_time         -- 上报时间
        , is_rec
        , abtest
        , index
        , strategy_id
        , is_operat
        , is_default
        , app_version
        , special_index 
        , page_name 
        , keyword
        , event_id
        , search_event_id
        , goods_count
        , click_goods_id_count
        , dt      -- 分区
     from pre_goods_exposure_table
     where dt >= to_char(dateadd(to_date1(${cyctime} ,'yyyymmddhhmiss'),-8,'hh'),'yyyymmdd')
     AND to_char(dateadd(from_unixtime(report_time/1000),-7,'hh'),'yyyymmdd') = to_char(dateadd(to_date1(${cyctime},'yyyymmddhhmiss'),-8,'hh'),'yyyymmdd')
)
-- 
select 
    coalesce(expo.special_id,cl.special_id,ac.special_id,gd.special_id,po.special_id,gex.special_id) as special_id
    ,coalesce(expo.special_date,cl.special_date,ac.special_date,gd.special_date,po.special_date,gex.special_date) as special_date
    ,coalesce(expo.date_hour,cl.date_hour,ac.date_hour,gd.date_hour,po.date_hour,gex.date_hour) date_hour
    ,coalesce(siui.goods_preference_level,'未知') goods_preference_level
    ,coalesce(siui.province_region_level,'中部') province_region_level
    ,sum(home_special_exposure_uv) home_special_exposure_uv
    ,sum(home_special_exposure_pv) home_special_exposure_pv
    ,sum(home_special_click_uv) home_special_click_uv
    ,sum(home_special_click_pv) home_special_click_pv
    ,sum(special_channel_exposure_goods_uv) special_channel_exposure_goods_uv
    ,sum(special_channel_exposure_goods_pv) special_channel_exposure_goods_pv
    ,sum(special_channel_exposure_user_num) special_channel_exposure_user_num
    ,sum(special_channel_goods_detail_goods_uv) special_channel_goods_detail_goods_uv
    ,sum(special_channel_goods_detail_goods_pv) special_channel_goods_detail_goods_pv
    ,sum(special_channel_goods_detail_user_num) special_channel_goods_detail_user_num
    ,sum(special_channel_add_cart_goods_uv) special_channel_add_cart_goods_uv
    ,sum(special_channel_add_cart_goods_pv) special_channel_add_cart_goods_pv
    ,sum(special_channel_add_cart_user_num) special_channel_add_cart_user_num
    ,sum(special_channel_add_cart_num) special_channel_add_cart_num
    ,sum(special_channel_buy_amount) special_channel_buy_amount
    ,sum(special_channel_buy_num) special_channel_buy_num
    ,sum(special_channel_pay_goods_uv) special_channel_pay_goods_uv
    ,sum(special_channel_pay_goods_pv) special_channel_pay_goods_pv
    ,sum(special_channel_pay_user_num) special_channel_pay_user_num
    ,sum(buy_amount) buy_amount
    ,sum(buy_num) buy_num
    ,sum(pay_goods_uv) pay_goods_uv
    ,sum(pay_goods_pv) pay_goods_pv
    ,sum(pay_user_num) pay_user_num
from (
    select 
        special_id
        , user_id
        , special_date
        , to_char(time,'yyyy-mm-dd hh') as date_hour
        , 1 as home_special_exposure_uv
        , count(1) as home_special_exposure_pv
    from all_special_exposure_table
    -- where dt between ${bizdate} and SUBSTR(${cyctime},1,8)
    -- and to_char(special_date,'yyyymmdd') = ${bizdate} 
    -- 修改地方(转小时，dt取当天,用埋点表的上报时间提小时出来作比对)
    where dt = '${today}'
    and substr(from_unixtime(unix_timestamp(time, 'yyyy-MM-dd HH:mm:ss'), 'yyyyMMddHHmmss'),1,10) = SUBSTR('${cyctime}',1,10)
    and is_number(special_id)
    and special_id <> ''
    group by special_id
        , special_date
        , to_char(time,'yyyy-mm-dd hh')
        , user_id
) expo 
full join (
    select special_id
        , user_id
        , special_date
        , to_char(time,'yyyy-mm-dd hh') as date_hour
        , 1 as home_special_click_uv
        , count(1) as home_special_click_pv
    from enter_special_table
    -- where dt between ${bizdate} and SUBSTR(${cyctime},1,8)
    -- and to_char(special_date,'yyyymmdd') = ${bizdate} 
    -- 修改地方(转小时，dt取当天,用埋点表的上报时间提小时出来作比对)
    where dt = '${today}'
    and substr(from_unixtime(unix_timestamp(time, 'yyyy-MM-dd HH:mm:ss'), 'yyyyMMddHHmmss'),1,10) = SUBSTR('${cyctime}',1,10)
    and is_number(special_id)
    and special_id <> ''
    group by special_id
        , special_date
        , to_char(time,'yyyy-mm-dd hh') 
        , user_id
) cl 
on expo.special_id = cl.special_id
and expo.user_id = cl.user_id
and expo.special_date = cl.special_date
and expo.date_hour = cl.date_hour
full join (
    select
        a.special_id
        , a.special_date
        , to_char(a.time,'yyyy-mm-dd hh') as date_hour
        , user_id
        ,count(distinct if(source='4',a.goods_id,null))  special_channel_add_cart_goods_uv --专场渠道商品加购UV
        ,sum(if(source='4',1,0)) special_channel_add_cart_goods_pv --专场渠道商品加购pv
        ,sum(if(source='4',goods_number,0)) special_channel_add_cart_num  -- 专场渠道加购件数
        ,max(if(source='4',1,0)) special_channel_add_cart_user_num  -- 专场渠道加购人数 
    from addcart_table a
    -- where dt between ${bizdate} and SUBSTR(${cyctime},1,8)
    -- and to_char(special_date,'yyyymmdd') = ${bizdate} 
    -- 修改地方(转小时，dt取当天,用埋点表的上报时间提小时出来作比对)
    where dt = '${today}'
    and substr(from_unixtime(unix_timestamp(time, 'yyyy-MM-dd HH:mm:ss'), 'yyyyMMddHHmmss'),1,10) = SUBSTR('${cyctime}',1,10)
    and source='4'
    group by 1,2,3,4
)ac
on  coalesce(expo.special_id,cl.special_id) = ac.special_id
and coalesce(expo.user_id,cl.user_id) = ac.user_id
and coalesce(expo.special_date,cl.special_date) = ac.special_date
and coalesce(expo.date_hour,cl.date_hour) = ac.date_hour
full join (
    select
        a.special_id
        , a.special_date
        , to_char(a.time,'yyyy-mm-dd hh') as date_hour
        , user_id
        ,count(distinct if(source='4',a.goods_id,null))  special_channel_goods_detail_goods_uv
        ,max(if(source='4',1,0)) special_channel_goods_detail_user_num 
        ,sum(if(source='4',1,0))  special_channel_goods_detail_goods_pv 
    from goodsdetailpage_table a
    -- where dt between ${bizdate} and SUBSTR(${cyctime},1,8)
    -- and to_char(special_date,'yyyymmdd') = ${bizdate} 
    -- 修改地方(转小时，dt取当天,用埋点表的上报时间提小时出来作比对)
    where dt = '${today}'
    and substr(from_unixtime(unix_timestamp(time, 'yyyy-MM-dd HH:mm:ss'), 'yyyyMMddHHmmss'),1,10) = SUBSTR('${cyctime}',1,10)
    and source='4'
    group by 1,2,3,4
)gd
on  coalesce(expo.special_id,cl.special_id,ac.special_id) = gd.special_id 
and coalesce(expo.user_id,cl.user_id,ac.user_id) = gd.user_id 
and coalesce(expo.special_date,cl.special_date,ac.special_date) = gd.special_date 
and coalesce(expo.date_hour,cl.date_hour,ac.date_hour) = gd.date_hour 
full join (
    -- 转小时
    select 
        fs.special_id
        ,fo.user_id
        ,from_unixtime(fo.pay_time, 'yyyy-MM-dd') as special_date
        ,to_char(from_unixtime(fo.pay_time, 'yyyy-MM-dd HH:mm:ss'),'yyyy-mm-dd hh') as date_hour
        -- 专场渠道直接GMV
        , sum(if(get_json_object(foi.sa, '$.source') = '4',foi.buy_num*foi.shop_price,0)) as special_channel_buy_amount  
        -- 专场渠道销售件数
        , sum(if(get_json_object(foi.sa, '$.source') = '4',foi.buy_num,0)) special_channel_buy_num 
        , count(distinct if(get_json_object(foi.sa, '$.source') = '4',foi.goods_id,null)) special_channel_pay_goods_uv
        , count(distinct if(get_json_object(foi.sa, '$.source') = '4',concat(foi.goods_id,fo.order_id),0)) special_channel_pay_goods_pv
        , max(if(get_json_object(foi.sa, '$.source')='4',1,0)) special_channel_pay_user_num
        , sum(foi.buy_num*foi.shop_price) as buy_amount  
        , sum(foi.buy_num) buy_num 
        , count(distinct foi.goods_id) pay_goods_uv 
        , count(distinct concat(foi.goods_id,fo.order_id)) pay_goods_pv
        -- 全款支付人数
        , 1 as pay_user_num 
    from yishou_data.all_fmys_order_h fo
    join yishou_data.all_fmys_order_infos_h foi on fo.order_id = foi.order_id
    left join yishou_data.all_fmys_special_h fs on foi.special_id = fs.special_id
    left join yishou_data.all_fmys_order_cancel_record_h cr on fo.order_id = cr.order_id
    and cr.cancel_type = 1
    where substr(from_unixtime(fo.pay_time, 'yyyyMMddHHmmss'),1,10) = SUBSTR('${cyctime}',1,10) 
    and case when fo.pay_status = 1 and cr.order_id is null then 1 else 0 end = 1
    group by 1,2,3,4
)po 
on  coalesce(expo.special_id,cl.special_id,ac.special_id,gd.special_id) = po.special_id
and coalesce(expo.user_id,cl.user_id,ac.user_id,gd.user_id) = po.user_id
and coalesce(expo.special_date,cl.special_date,ac.special_date,gd.special_date) = po.special_date
and coalesce(expo.date_hour,cl.date_hour,ac.date_hour,gd.date_hour) = po.date_hour
full join (
    select 
        a.special_id
        , a.special_date
        , to_char(a.report_time,'yyyy-mm-dd hh') as date_hour
        , user_id
        ,count(distinct a.user_id,a.goods_id) as special_channel_exposure_goods_uv
        ,sum(1) as special_channel_exposure_goods_pv
        ,1 as special_channel_exposure_user_num
    from goods_exposure_table a
    where  a.dt='${today}' 
        -- and a.dt<=SUBSTR('${cyctime}',1,8)
        and substr(from_unixtime(unix_timestamp(a.report_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyyMMddHHmmss'),1,10) = SUBSTR('${cyctime}',1,10)
        -- and to_char(special_date,'yyyymmdd')='${bizdate}' 
        and a.pid = 10 
        and a.user_id > 0 
        and a.special_id > 0 
    group by 1,2,3,4
)gex
    on  coalesce(expo.special_id,cl.special_id,ac.special_id,gd.special_id,po.special_id) = gex.special_id
    and coalesce(expo.user_id,cl.user_id,ac.user_id,gd.user_id,po.user_id) = gex.user_id
    and coalesce(expo.special_date,cl.special_date,ac.special_date,gd.special_date,po.special_date) = gex.special_date
    and coalesce(expo.date_hour,cl.date_hour,ac.date_hour,gd.date_hour,po.date_hour) = gex.date_hour
left join temp_special_info_user_info_20240713 siui
    on siui.user_id = coalesce(expo.user_id,cl.user_id,ac.user_id,gd.user_id,po.user_id,gex.user_id)
group by 1,2,3,4,5;

INSERT OVERWRITE TABLE cross_origin.finebi_dws_special_info_hour_h PARTITION(ht)
select
    siud.date_hour
    ,ds1.goods_preference_level
    ,ds1.province_region
    ,ds.special_id  -- 专场id
    ,ds.special_period --   期数
    , ds.special_thumb
    , ds.special_img
    , ds.special_start_time -- 专场开始时间
    , ds.special_name  
    , ds.special_market  -- 专场表的市场名称
    , ds.special_show  -- 是否隐形专场
    , ds.is_live_special -- 是否直播专场
    , ds.live_start_time -- 专场直播间开始时间
    , ds.special_style_type_name  -- 专场风格类型
    , ds.special_type_group -- 专场风格类型归类
    , if(ds.special_start_time<=to_date1('2021-12-20','yyyy-mm-dd'),ds.special_sort,ds.safe_sort) first_sort -- 未知初始排序
    , ds.horse_final_sort  -- 赛马最终排序
    , fpg.pg_name  -- 买手组名称
    , ds.picker_group_code  -- 买手代号
    , pii.market_name  -- 买手所在市场
    , pii.admin_name -- 买手姓名
    , ad.admin_names -- 买辅姓名
    ,case when ds1.province_region = '南部' and ds1.goods_preference_level = '高档' then  ds.ini_south_high 
        when ds1.province_region = '南部' and ds1.goods_preference_level = '中档' then  ds.ini_south_mid 
        when ds1.province_region = '南部' and ds1.goods_preference_level = '低档' then  ds.ini_south_low 
        when ds1.province_region = '中部' and ds1.goods_preference_level = '高档' then  ds.ini_middle_high 
        when ds1.province_region = '中部' and ds1.goods_preference_level = '中档' then  ds.ini_middle_mid 
        when ds1.province_region = '中部' and ds1.goods_preference_level = '低档' then  ds.ini_middle_low 
        when ds1.province_region = '北部' and ds1.goods_preference_level = '高档' then  ds.ini_north_high 
        when ds1.province_region = '北部' and ds1.goods_preference_level = '中档' then  ds.ini_north_mid 
        when ds1.province_region = '北部' and ds1.goods_preference_level = '低档' then  ds.ini_north_low 
        when ds1.province_region = '未知' and ds1.goods_preference_level = '高档' then  ds.ini_safe_high 
        when ds1.province_region = '未知' and ds1.goods_preference_level = '中档' then  ds.ini_safe_mid 
        when ds1.province_region = '未知' and ds1.goods_preference_level = '低档' then  ds.ini_safe_low  
        end ini_sort --初始排序
    ,case when ds1.province_region = '南部' and ds1.goods_preference_level = '高档' then  ds.fixed_south_high  
        when ds1.province_region = '南部' and ds1.goods_preference_level = '中档' then  ds.fixed_south_mid  
        when ds1.province_region = '南部' and ds1.goods_preference_level = '低档' then  ds.fixed_south_low  
        when ds1.province_region = '中部' and ds1.goods_preference_level = '高档' then  ds.fixed_middle_high  
        when ds1.province_region = '中部' and ds1.goods_preference_level = '中档' then  ds.fixed_middle_mid  
        when ds1.province_region = '中部' and ds1.goods_preference_level = '低档' then  ds.fixed_middle_low  
        when ds1.province_region = '北部' and ds1.goods_preference_level = '高档' then  ds.fixed_north_high  
        when ds1.province_region = '北部' and ds1.goods_preference_level = '中档' then  ds.fixed_north_mid  
        when ds1.province_region = '北部' and ds1.goods_preference_level = '低档' then  ds.fixed_north_low  
        when ds1.province_region = '未知' and ds1.goods_preference_level = '高档' then  ds.fixed_safe_high  
        when ds1.province_region = '未知' and ds1.goods_preference_level = '中档' then  ds.fixed_safe_mid  
        when ds1.province_region = '未知' and ds1.goods_preference_level = '低档' then  ds.fixed_safe_low  
        end fixed_sort -- 赛马排序
    ,case when ds1.province_region = '南部' and ds1.goods_preference_level = '高档' then  ds.final_south_high 
        when ds1.province_region = '南部' and ds1.goods_preference_level = '中档' then  ds.final_south_mid 
        when ds1.province_region = '南部' and ds1.goods_preference_level = '低档' then  ds.final_south_low 
        when ds1.province_region = '中部' and ds1.goods_preference_level = '高档' then  ds.final_middle_high 
        when ds1.province_region = '中部' and ds1.goods_preference_level = '中档' then  ds.final_middle_mid 
        when ds1.province_region = '中部' and ds1.goods_preference_level = '低档' then  ds.final_middle_low 
        when ds1.province_region = '北部' and ds1.goods_preference_level = '高档' then  ds.final_north_high 
        when ds1.province_region = '北部' and ds1.goods_preference_level = '中档' then  ds.final_north_mid 
        when ds1.province_region = '北部' and ds1.goods_preference_level = '低档' then  ds.final_north_low 
        when ds1.province_region = '未知' and ds1.goods_preference_level = '高档' then  ds.final_safe_high 
        when ds1.province_region = '未知' and ds1.goods_preference_level = '中档' then  ds.final_safe_mid 
        when ds1.province_region = '未知' and ds1.goods_preference_level = '低档' then  ds.final_safe_low 
        end final_sort --赛马最终排序
    ,case when ds1.province_region = '南部' and ds1.goods_preference_level = '高档' then  ds.ordered_south_high
        when ds1.province_region = '南部' and ds1.goods_preference_level = '中档' then  ds.ordered_south_mid  
        when ds1.province_region = '南部' and ds1.goods_preference_level = '低档' then  ds.ordered_south_low  
        when ds1.province_region = '中部' and ds1.goods_preference_level = '高档' then  ds.ordered_middle_high  
        when ds1.province_region = '中部' and ds1.goods_preference_level = '中档' then  ds.ordered_middle_mid  
        when ds1.province_region = '中部' and ds1.goods_preference_level = '低档' then  ds.ordered_middle_low  
        when ds1.province_region = '北部' and ds1.goods_preference_level = '高档' then  ds.ordered_north_high  
        when ds1.province_region = '北部' and ds1.goods_preference_level = '中档' then  ds.ordered_north_mid  
        when ds1.province_region = '北部' and ds1.goods_preference_level = '低档' then  ds.ordered_north_low  
        when ds1.province_region = '未知' and ds1.goods_preference_level = '高档' then  ds.ordered_safe_high  
        when ds1.province_region = '未知' and ds1.goods_preference_level = '中档' then  ds.ordered_safe_mid  
        when ds1.province_region = '未知' and ds1.goods_preference_level = '低档' then  ds.ordered_safe_low
        end  ordered_sort--专场无坑位排序
    ,siud.home_special_exposure_uv
    ,siud.home_special_exposure_pv
    ,siud.home_special_click_uv
    ,siud.home_special_click_pv
    ,siud.special_channel_exposure_goods_uv
    ,siud.special_channel_exposure_goods_pv
    ,siud.special_channel_exposure_user_num
    ,siud.special_channel_goods_detail_goods_uv
    ,siud.special_channel_goods_detail_goods_pv
    ,siud.special_channel_goods_detail_user_num
    ,siud.special_channel_add_cart_goods_uv
    ,siud.special_channel_add_cart_goods_pv
    ,siud.special_channel_add_cart_user_num
    ,siud.special_channel_add_cart_num
    ,siud.special_channel_buy_amount
    ,siud.special_channel_buy_num
    ,siud.special_channel_pay_goods_uv
    ,siud.special_channel_pay_goods_pv
    ,siud.special_channel_pay_user_num
    ,siud.buy_amount
    ,siud.buy_num
    ,siud.pay_goods_uv
    ,siud.pay_goods_pv
    ,siud.pay_user_num
    -- , to_char(coalesce(ds.special_start_time,siud.special_date),'yyyymmdd')  dt
    -- 修改为小时分区
    ,to_char(to_date1(${cyctime} ,'yyyymmddhhmiss'),'yyyymmddhh') AS ht
from yishou_data.dim_special_info ds
left join yishou_data.all_fmys_picker_group_h fpg on ds.picker_group = fpg.pg_id
left join (
    -- 买手信息表的小时
    select 
        pgm.admin_id
        ,pgm.pg_id
        ,pgm.code
        ,fa.admin_name
        ,pg.market_name
    from yishou_data.all_fmys_picker_group_member_h pgm 
    left join yishou_data.all_fmys_admin_h fa on pgm.admin_id = fa.admin_id
    left join yishou_data.all_fmys_picker_group_h pg on pgm.pg_id = pg.pg_id
) pii on ds.picker_group_code = pii.code
left join (     
    select
        fs_2.special_id,
        concat_ws('，', collect_list(pw.admin_name)) AS admin_names
    from
        (select
            special_id,
            picker_code
        from yishou_data.all_fmys_special_h lateral view explode(split(picker_assist, ',')) t AS picker_code
        ) fs_2
    left join (
        -- 买手信息表的小时
        select * from yishou_data.all_fmys_picker_group_member_h pgm 
        left join yishou_data.all_fmys_admin_h fa on pgm.admin_id = fa.admin_id
    ) pw on fs_2.picker_code = pw.code
    group by
        fs_2.special_id
) ad on ad.special_id = ds.special_id
left join (
    select
        fs_1.special_id,
        concat_ws('，', collect_list(su.supply_name)) AS supply_names,
        concat_ws('，', collect_list(fs_1.supply_id)) AS supply_id
    from
        (
        select
            special_id,
            supply_id
        FROM yishou_data.all_fmys_special_h lateral view explode(split(supply_ids, ',')) t AS supply_id
        ) fs_1
        left join yishou_data.all_fmys_supply_h su on fs_1.supply_id = su.supply_id
    group by
        fs_1.special_id
) sd on sd.special_id = ad.special_id
--打散
LEFT JOIN 
(
select special_id,province_region,goods_preference_level
from yishou_data.all_fmys_special_h 
lateral view  explode (split('北部,南部,中部,未知',',')) t1 as province_region
lateral view  explode (split('高档,中档,低档',',')) t2 as goods_preference_level
) ds1
on ds.special_id = CAST(ds1.special_id AS BIGINT)
LEFT join temp_special_info_user_data_20240713 siud
    on  ds1.special_id = siud.special_id
    and ds1.province_region = siud.province_region_level
    and ds1.goods_preference_level = siud.goods_preference_level
    and to_char(ds.special_start_time,'yyyymmdd') = to_char(siud.special_date,'yyyymmdd')
where to_char(siud.special_date,'yyyymmdd') = '${today}'



