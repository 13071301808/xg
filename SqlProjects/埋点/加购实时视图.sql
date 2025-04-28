-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/03/27 17:33:56 GMT+08:00
-- ******************************************************************** --
show create table yishou_data.dwd_app_add_cart_dt_view;

CREATE VIEW `yishou_data`.`dwd_app_add_cart_dt_view`(
time, distinct_id, user_id, goods_id, os, source, goods_category_name, keyword, special_id, category_name, goods_name, app_version, is_first_day, model, device_id, network_type, carrier, goods_number, wifi, add_goods_amount, specialname, goods_price, stall_name, os_version, category_id, stall_id, ip, is_login_id, country, province, city, category_source, special_source, stall_source, today_special_offer_name, is_buy_now, activity_id, is_recommend_search, add_cart_id, origin_name, category_banner_name, stall_banner_id, search_from_source, live_id, is_default, strategy_id, is_operat, source_search_event_id, source_event_id, goods_index, event_id, landing_event_id, pgm_id, stallcomment_source, pgm_code, second_level_source, two_level_source, campaign_event_id, activity_name, content_id, author_id, goods_seat_id, tab_name, special_index, is_pro, campaign_h5_banner_id, ladder_price, ladder_threshold_number, sku_json, dt
) AS 
select 
time ,distinct_id ,user_id ,goods_id ,os ,source ,goods_category_name ,keyword ,special_id ,category_name ,goods_name ,app_version ,is_first_day ,model ,device_id ,network_type ,carrier ,goods_number ,wifi ,add_goods_amount ,specialname ,goods_price ,stall_name ,os_version ,category_id ,stall_id ,ip ,is_login_id ,country ,province ,city ,category_source ,special_source ,stall_source ,today_special_offer_name ,is_buy_now ,activity_id ,is_recommend_search ,add_cart_id ,origin_name ,category_banner_name ,stall_banner_id ,search_from_source ,live_id ,is_default ,strategy_id ,is_operat ,source_search_event_id ,source_event_id ,goods_index ,event_id ,landing_event_id ,pgm_id ,stallcomment_source ,pgm_code ,second_level_source ,two_level_source ,campaign_event_id ,activity_name ,content_id ,author_id ,goods_seat_id ,tab_name ,special_index ,is_pro ,campaign_h5_banner_id ,ladder_price ,ladder_threshold_number ,sku_json ,dt 
from yishou_data.dcl_event_add_cart_d 
where dt between to_char(dateadd(to_date1('20250326','yyyymmdd'),-13,'mm'),'yyyymm01') and '20250326' 
union all 
select 
get_json_object(action,'$.time') as time 
, get_json_object(action,'$.distinct_id') as distinct_id 
, get_json_object(action,'$.properties.userid') as user_id 
, get_json_object(action,'$.properties.goodID') as goods_id 
, get_json_object(action,'$.properties.os') as os 
, trim(get_json_object(action,'$.properties.source')) as source -- 来源方式 
, get_json_object(action,'$.properties.goodCategoryName') as goods_category_name -- 商品分类名称 
, get_json_object(action,'$.properties.keyword') as keyword -- 来源搜素关键字 
, if(is_number(get_json_object(action, '$.properties.special_id')),get_json_object(action, '$.properties.special_id'),null) as special_id -- 专场id 
, get_json_object(action,'$.properties.categoryName') as category_name -- 来源分类名称 
, get_json_object(action,'$.properties.goodName') as goods_name -- 商品名称 
, get_json_object(action,'$.properties.app_version') as app_version -- app 版本 
, get_json_object(action,'$.properties.is_first_day') as is_first_day -- 是否首日访问 
, get_json_object(action,'$.properties.model') as model -- 设备型号 
, get_json_object(action,'$.properties.device_id') as device_id -- 设备号 
, get_json_object(action,'$.properties.network_type') as network_type -- 网络类型 
, get_json_object(action,'$.properties.carrier') as carrier -- 运营商 
, if(is_number(get_json_object(action, '$.properties.goodNumber')),get_json_object(action, '$.properties.goodNumber'),'0') as goods_number -- 加购数量 
, get_json_object(action,'$.properties.wifi') as wifi -- 是否WiFi 
, if(is_number(get_json_object(action, '$.properties.AppGoodsPriceTotal')),get_json_object(action, '$.properties.AppGoodsPriceTotal'),'0') as add_goods_amount 
, get_json_object(action,'$.properties.specialName') as specialName -- 来源专场名 
, if(is_number(get_json_object(action, '$.properties.AppGoodPrice')),get_json_object(action, '$.properties.AppGoodPrice'),'0') as goods_price -- 商品单价 
, get_json_object(action,'$.properties.stallName') as stall_name -- 来源档口名称 
, get_json_object(action,'$.properties.os_version') as os_version -- 操作系统版本 
, get_json_object(action,'$.properties.categoryID') as category_id -- 来源分类ID 
, get_json_object(action,'$.properties.stallID') as stall_id -- 来源档口id 
, get_json_object(action,'$.properties.ip') as ip -- ip 
, get_json_object(action,'$.properties.is_login_id') as is_login_id -- 是否登录 ID 
, get_json_object(action,'$.properties.country') as country -- 国别 
, get_json_object(action,'$.properties.province') as province -- 省份 
, get_json_object(action,'$.properties.city') as city -- 城市 
, get_json_object(action,'$.properties.categorySource') as category_source 
, get_json_object(action,'$.properties.specialSource') as special_source -- 专场来源 
, if(get_json_object(action, '$.properties.stallSource') == '' ,null,get_json_object(action, '$.properties.stallSource')) AS stall_source --档口来源 
, get_json_object(action, '$.properties.todaySpecialOfferName') AS today_special_offer_name --今日特卖名称 
, get_json_object(action, '$.properties.isBuyNow') AS is_buy_now --是否立即购买 
, get_json_object(action, '$.properties.activityId') AS activity_id --活动id 
, get_json_object(action, '$.properties.is_recommend_search') AS is_recommend_search -- 
, get_json_object(action, '$.properties.add_cart_id') AS add_cart_id -- 
, get_json_object(action, '$.properties.originName') AS origin_name -- 
, get_json_object(action, '$.properties.categoryBannerName') AS category_banner_name 
, if(get_json_object(action, '$.properties.stallBannerId') == '',null,get_json_object(action, '$.properties.stallBannerId')) AS stall_banner_id 
, get_json_object(action, '$.properties.searchFromSource') AS search_from_source 
, if(get_json_object(action, '$.properties.liveID') == '' or not is_number(get_json_object(action, '$.properties.liveID')), null, get_json_object(action, '$.properties.liveID')) as live_id 
, get_json_object(action, '$.properties.is_default') AS is_default 
, get_json_object(action, '$.properties.strategy_id') AS strategy_id 
, get_json_object(action, '$.properties.is_operat') AS is_operat 
, get_json_object(action, '$.properties.source_search_event_id') AS source_search_event_id 
, get_json_object(action, '$.properties.source_event_id') AS source_event_id 
, get_json_object(action, '$.properties.good_index') AS goods_index 
, get_json_object(action, '$.properties.event_id') AS event_id 
, get_json_object(action, '$.properties.landing_event_id') AS landing_event_id 
, get_json_object(action, '$.properties.pgm_id') AS pgm_id 
, get_json_object(action, '$.properties.stallcommentSource') AS stallcomment_source 
, get_json_object(action, '$.properties.pgm_code') AS pgm_code 
, get_json_object(action, '$.properties.second_level_source') AS second_level_source 
, get_json_object(action, '$.properties.twoLevelSource') AS two_level_source 
, get_json_object(action, '$.properties.campaign_event_id') AS campaign_event_id 
, get_json_object(action, '$.properties.activityName') AS activity_name 
, get_json_object(action, '$.properties.content_id') AS content_id 
, get_json_object(action, '$.properties.author_id') AS author_id 
, get_json_object(action, '$.properties.goods_seat_id') AS goods_seat_id 
, get_json_object(action, '$.properties.tab_name') AS tab_name 
, get_json_object(action, '$.properties.special_index') AS special_index 
, get_json_object(action, '$.properties.isPro') AS is_pro 
, get_json_object(action, '$.properties.campaign_h5_banner_id') AS campaign_h5_banner_id 
, get_json_object(action, '$.properties.ladder_price') AS ladder_price 
, get_json_object(action, '$.properties.ladder_threshold_number') AS ladder_threshold_number 
, get_json_object(action, '$.properties.sku_json') AS sku_json 
, dt 
from yishou_data.ods_app_event_log_d 
where event = 'addcart' and dt = '20250327' 
and length(get_json_object(action, '$.properties.AppGoodsPriceTotal')) > 0 
and is_number(get_json_object(action, '$.properties.AppGoodsPriceTotal')) 
and get_json_object(action, '$.properties.AppGoodsPriceTotal') >= 0 
and length(get_json_object(action,'$.properties.userid')) > 0 
and is_number(get_json_object(action,'$.properties.userid')) 
and length(trim(get_json_object(action,'$.properties.source'))) > 0 
and is_number(trim(get_json_object(action,'$.properties.source')))


