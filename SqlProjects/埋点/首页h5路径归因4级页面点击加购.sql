-- CREATE EXTERNAL TABLE yishou_daily.temp_route_all_event_clickaddcart_detail_h5_dt (
--     special_date string comment '',
--     page_name string comment '',
--     event_id string comment '',
--     click_event_time string comment '',
--     add_cart_id string comment '',
--     add_cart_event_time string comment '',
--     first_page_name string comment '',
--     second_page_name string comment '',
--     third_page_name string comment '',
--     four_page_name string comment '',
--     is_join_add_cart string comment '',
--     add_cart_num string comment '',
--     add_cart_amount string comment '',
--     user_id string comment '',
--     goods_id string comment ''
-- )
-- comment '首页h5路径归因-档口现货临时点击加购'
-- partitioned by (dt string)
-- STORED AS ORC LOCATION 'obs://yishou-bigdata/yishou_daily.db/temp_route_all_event_clickaddcart_detail_h5_dt'
-- ;


with  all_event_id as (
    select 
        page_name,
        special_date,
        event_id,
        if (length(split(event_id,'_')[1]) = 13 , from_unixtime(split(event_id,'_')[1]/1000), from_unixtime(split(event_id,'_')[1])) click_event_time,
        add_cart_id,
        if (length(split(add_cart_id,'_')[1]) = 13 , from_unixtime(split(add_cart_id,'_')[1]/1000), from_unixtime(split(add_cart_id,'_')[1])) add_cart_event_time,
        user_id,
        goods_id,
        case 
            when first_page_name = '直播间列表' or (os = 'Android' and first_page_name in ('直播间','点播间','VideoActivity','VideoActivity(Live)','直播预告')) 
            then '直播间列表'
            when first_page_name = 'H5' and first_page_url like '%shopkeeper-community%' then '有料'
            else first_page_name
        end as first_page_name,
-----------------------------------------------------------2级页面-------------------------------------------------------------------------
        case 
            when first_page_name = '首页' and (
                substring(first_home_index,1,2) in ('BD','BG') 
                or 
                (substring(first_home_index,1,2) = 'AE' and first_home_index_desc in ('AE_首发新款','AE_一手直播','AE_今日特卖','AE_新品首发','AE_爆款24h发'))
            ) then '首页营销频道'
            when first_page_name = '首页' and substring(first_home_index,1,2) in ('AB','AH')                                               then '首页搜索'
            when first_page_name = '首页' and substring(first_home_index,1,2) in ("AG","AO","AN")                                          then '首页专场' 
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "AI"                                                      then '首页市场'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "AJ"                                                      then '首页分类'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "AD"                                                      then '首页活动banner'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "AC"                                                      then '首页banner'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "AS"                                                      then '首页上三坑'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "BE"                                                      then '首页下五坑' 
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "AA"                                                      then '首页弹窗' 
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "AQ"                                                      then '首页消息'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "AR"                                                      then '首页浮层'
            when first_page_name = '首页' and substring(first_home_index,1,2)  in ("AW",'AU','AM','AX','AY')                               then '首页新人专享'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "BE"                                                      then '首页赛马频道'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "BH"                                                      then '首页顶部banner'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "BL"                                                      then '首页市场下方banner'
            when first_page_name = '首页'                                                                                                  then '首页其他' 
            when first_page_name = '首页分类页'                                                                                            then '首页分类TAB'
            when first_page_name = '榜单中心' then '首页分类TAB'   
            when first_page_name = '分类页' and substring(first_home_index,1,4) in ('AA_1','AA_2')                                         then '分类页搜索'
            when first_page_name = '分类页' and substring(first_home_index,1,4) = 'AH_1'                                                   then '分类页找档口'
            when first_page_name = '分类页' and substring(first_home_index,1,2) = 'AG' then '分类页专题'
            when first_page_name = '分类页' and substring(first_home_index,1,2) = 'AC' and first_home_index_desc like '%热门分类推荐%'     then '分类页热门分类推荐'
            when first_page_name = '分类页' and substring(first_home_index,1,2) = 'AC' and first_home_index_desc like '%热门市场%'         then '分类页热门市场'
            when first_page_name = '分类页' and first_home_index_desc like '%风格%'                                                        then '分类页风格'
            when first_page_name = '分类页'                                                                                                then '分类页商品分类'    
            when first_page_name = '个人中心' and first_home_index = 'AA'                                                                  then '个人中心收藏夹'
            when first_page_name = '个人中心' and first_home_index = 'AB'                                                                  then '个人中心足迹'
            when first_page_name = '个人中心' and first_home_index in ('AF_1','AF_2','AF_3','AF_4','AF_5','AF_6')                          then '个人中心我的订单'
            when first_page_name = '个人中心' and first_home_index = 'AM'                                                                  then '个人中心我的档口'
            when first_page_name = '个人中心' and first_home_index in ('AD','AR','AS')                                                     then '个人中心我的资产'
            when first_page_name = '个人中心' and first_home_index in ('AU_1','AU_2','AU_3')                                               then '个人中心我的活动'
            when first_page_name = '个人中心' and first_home_index in ('AI_1','AI_2','AI_3','AI_4','AI_5')                                 then '个人中心我的服务'
            when first_page_name = '个人中心'                                                                                              then '个人中心其他'
            when first_page_name = '进货车'  and first_home_index = 'AD'                                                                   then '进货车编辑'
            when first_page_name = '进货车'  and substring(first_home_index,1,2) = 'AB'                                                    then '进货车专场'
            when first_page_name = '进货车'  and substring(first_home_index,1,2) = 'AC'                                                    then '进货车档口'
            when first_page_name = '进货车'  and substring(first_home_index,1,2) = 'AF'                                                    then '进货车推荐商品'
            when first_page_name = '进货车'                                                                                                then '进货车其他' 
            when first_page_name = '订阅' and first_home_index = 'AA'                                                                      then '订阅查看全部'
            when first_page_name = '订阅' and substring(first_home_index,1,2) = 'AB'                                                       then '订阅常逛档口'
            when first_page_name = '订阅' and substring(first_home_index,1,2) = 'AC'                                                       then '订阅档口列表'
            when first_page_name = '订阅' and substring(first_home_index,1,2) = 'AD'                                                       then '订阅商品列表'
            when first_page_name = '订阅' and substring(first_home_index,1,2) = 'AE'                                                       then '订阅推荐档口'
            when first_page_name = '订阅' and substring(first_home_index,1,2) = 'AF'                                                       then '订阅档口动态的运营位'
            when first_page_name = '订阅' and substring(first_home_index,1,2) = 'AG'                                                       then '订阅全部档口'
            when first_page_name = '订阅' and substring(first_home_index,1,2) = 'AH'                                                       then '订阅全部档口的推荐档口'
            when first_page_name = '订阅' and substring(first_home_index,1,2) = 'AI'                                                       then '订阅商品集'
            when first_page_name = '订阅' and substring(first_home_index,1,2) = 'AJ'                                                       then '订阅档口动态的穿插推荐档口'
            when first_page_name = '订阅' and substring(first_home_index,1,2) = 'AK'                                                       then '订阅档口动态的查看全部'            
            when first_page_name = '订阅'                                                                                                  then '订阅其他' 
            when first_page_name = '直播间列表' and second_page_name in ('直播间','VideoActivity','VideoActivity(Live)') then '直播间列表直播'
            when first_page_name = '直播间列表' and second_page_name = '点播间' then '直播间列表点播'
            when first_page_name = '直播间列表' and second_page_name = '直播预告' then '直播间列表直播预告'
            when os = 'Android' and first_page_name in ('直播间','VideoActivity','VideoActivity(Live)') then '直播间列表直播'
            when os = 'Android' and first_page_name = '点播间' then '直播间列表点播'
            when os = 'Android' and first_page_name = '直播预告' then '直播间列表直播预告'
            when first_page_name = '直播间列表' or (os = 'Android' and first_page_name in ('直播间','点播间','VideoActivity','VideoActivity(Live)','直播预告'))
            then '直播间列表其他'
            when first_page_name = 'H5' and first_page_url like '%shopkeeper-community%' and second_page_name = '商品详情' then '有料商品详情'
            when first_page_name = 'H5' and first_page_url like '%shopkeeper-community%' and second_page_name = 'H5' then '有料继续游览'
            when first_page_name = 'H5' and first_page_url like '%shopkeeper-community%' and second_page_name = '直播间列表' then '有料直播间列表'
            when first_page_name = 'H5' and first_page_url like '%shopkeeper-community%' then '有料其他'
            when page_index = 1 then '没有二级页面'
            else '其他' 
        end second_page_name,
-----------------------------------------------------------3级页面-------------------------------------------------------------------------             
        case 
            when first_page_name = '首页' and (
                substring(first_home_index,1,2) in ('BD','BG') 
                or (substring(first_home_index,1,2) = 'AE' and first_home_index_desc in ('AE_首发新款','AE_一手直播','AE_今日特卖','AE_新品首发','AE_爆款24h发'))
            ) 
            then concat('首页营销频道',split(first_home_index_desc,'_')[1])
            when first_page_name = '首页' and substring(first_home_index,1,2) = 'BE' then concat('首页赛马频道',split(first_home_index_desc,'_')[1])
            when first_page_name = '个人中心' and first_home_index = 'AF_2'  then '个人中心我的订单待发货'
            when first_page_name = '个人中心' and first_home_index = 'AF_3'  then '个人中心我的订单待收货'
            when first_page_name = '个人中心' and first_home_index = 'AF_5'  then '个人中心售后进度'
            when first_page_name = '个人中心' and first_home_index = 'AF_6'  then '个人中心我的订单查看全部'
            when first_page_name = '首页分类页'  then concat('首页分类TAB',first_activity_name)
            when first_page_name = '榜单中心' then '首页分类TAB榜单中心'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "AI" 
            then concat(
                '首页市场',split(first_home_index_desc,'_')[1]
                ,'_'
                ,case when substring(second_home_index,1,2) = 'AA' and second_page_name = '市场着陆详情页面'  then '档口上新'
                    when substring(second_home_index,1,2) = 'AB' and second_page_name = '市场着陆详情页面'  then '全部上新'
                    when substring(second_home_index,1,2) = 'AC' and second_page_name = '市场着陆详情页面'  then '特价优选档口上新'
                    when substring(second_home_index,1,2) = 'AD' and second_page_name = '市场着陆详情页面'  then '搜索'
                    when substring(second_home_index,1,2) = 'AE' and second_page_name = '市场着陆详情页面'  then '市场介绍'
                    when substring(second_home_index,1,2) = 'AE' and second_page_name = '市场着陆详情页面'  then '顶部运营位'
                    else '其他' end
                ,coalesce(second_banner_id,'')
            )
            when first_home_index_desc in ('AB_以图搜图','AA_以图搜图') or second_home_index_desc = 'AA_以图搜图' then  '以图搜图'
            when second_key_word is not null then '搜索词'
            when first_home_index_desc in ('AB_搜索栏','AA_搜索栏') then '搜索推荐' 
            when first_page_name = '首页' and substring(first_home_index,1,3) in ("AG_","AO_","AN_") then concat("专场TAB_", split(first_home_index_desc,'_')[1])
            when first_page_name = '订阅' and substring(first_home_index,1,2) = 'AF'    then replace(first_home_index_desc,'AF','订阅档口动态的运营位') --订阅运营位
            when first_page_name = '分类页' 
            then if(SUBSTR(first_home_index_desc,4,length(first_home_index_desc)-3)<>'',SUBSTR(first_home_index_desc,4,length(first_home_index_desc)-3),'其他')
            else '其他'
        end third_page_name,
-----------------------------------------------------------4级页面-------------------------------------------------------------------------    
        case 
            when second_page_name = 'H5' and third_page_name = 'H5' and third_h5_page_id = '115974' 
            and third_page_url like '%screening-operate-search%'
            then '档口现货搜索结果'
            when second_page_name = 'H5' and second_h5_page_id = '115974' and second_home_index = 'AA_2' 
            and second_page_url like '%screening-operate%'
            then concat('档口现货轮播图_',second_banner_id)
            when second_page_name = 'H5' and third_page_name = 'H5' and third_parent_h5_page_id = '115974' and second_home_index = 'AA_3' 
            and third_page_url like '%quick-replenishment%'
            then '档口现货快速补货'
            when second_page_name = 'H5' and third_page_name = 'H5' and third_parent_h5_page_id = '115974' and second_home_index = 'AA_3' 
            and third_page_url like '%screening-supply%'
            then '档口现货大牌联合入仓'
            -- 临时用，后面id还得换
            when second_page_name = 'H5' and third_page_name = 'H5' and third_parent_h5_page_id = '115974' and third_h5_page_id = '115844' and second_home_index = 'AA_4' 
            and second_page_url like '%screening-operate%'
            then '档口现货市场特价'
            -- 临时用，后面id还得换
            when second_page_name = 'H5' and third_page_name = 'H5' and third_parent_h5_page_id = '115974' and third_h5_page_id = '115960' and second_home_index = 'AA_4' 
            and second_page_url like '%screening-operate%'
            then '档口现货市场新款'
            when second_page_name = 'H5' and third_page_name = 'H5' and third_parent_h5_page_id = '115974' and second_home_index = 'AA_4' 
            and third_page_url like '%goods-rank%'
            then '档口现货返单爆款'
            -- 临时用，后面id还得换
            when second_page_name = 'H5' and third_page_name = 'H5' and third_parent_h5_page_id = '115974' and third_h5_page_id = '115961' and second_home_index = 'AA_5' 
            and second_page_url like '%screening-operate%'
            then '档口现货品类_连衣裙套装'
            -- 临时用，后面id还得换
            when second_page_name = 'H5' and third_page_name = 'H5' and third_parent_h5_page_id = '115974' and third_h5_page_id = '120430' and second_home_index = 'AA_5' 
            and second_page_url like '%screening-operate%'
            then '档口现货品类_T恤'
            -- 临时用，后面id还得换
            when second_page_name = 'H5' and third_page_name = 'H5' and third_parent_h5_page_id = '115974' and third_h5_page_id = '116053' and second_home_index = 'AA_5' 
            and second_page_url like '%screening-operate%'
            then '档口现货品类_小衫'
            -- 临时用，后面id还得换
            when second_page_name = 'H5' and third_page_name = 'H5' and third_parent_h5_page_id = '115974' and third_h5_page_id = '116054' and second_home_index = 'AA_5' 
            and second_page_url like '%screening-operate%'
            then '档口现货品类_衬衫'
            -- 临时用，后面id还得换
            when second_page_name = 'H5' and third_page_name = 'H5' and third_parent_h5_page_id = '115974' and third_h5_page_id = '115970' and second_home_index = 'AA_5' 
            and second_page_url like '%screening-operate%'
            then '档口现货品类_牛仔裤'
            -- 临时用，后面id还得换
            when second_page_name = 'H5' and third_page_name = 'H5' and third_parent_h5_page_id = '115974' and third_h5_page_id = '115967' and second_home_index = 'AA_5' 
            and second_page_url like '%screening-operate%'
            then '档口现货品类_休闲裤'
            -- 临时用，后面id还得换
            when second_page_name = 'H5' and third_page_name = 'H5' and third_parent_h5_page_id = '115974' and third_h5_page_id = '115964' and second_home_index = 'AA_5' 
            and second_page_url like '%screening-operate%'
            then '档口现货品类_半身裙'
            -- 临时用，后面id还得换
            when second_page_name = 'H5' and third_page_name = 'H5' and third_parent_h5_page_id = '115974' and third_h5_page_id = '115843' and second_home_index = 'AA_5' 
            and second_page_url like '%screening-operate%'
            then '档口现货品类_鞋包配'
            else '其他'
        end as four_page_name,
        case when add_cart_id = '' or add_cart_id is null then concat(round(rand()*10,5),'_空值避免数据倾斜') else add_cart_id end add_cart_id_rand
    from (
        select 
            page_name,
            special_date,
            event_id,
            add_cart_id,
            user_id,
            goods_id,
            first_page_name,
            first_home_index,
            first_home_index_desc,
            page_index,
            first_activity_name,
            os,
            get_json_object(first_route_detail,'$.url') first_page_url,
            regexp_replace(regexp_replace(get_json_object(second_route_detail, '$.url'),'\\\\u003d','='),'\\\\u0026','&') second_page_url,
            get_json_object(get_json_object(second_route_detail,'$.operateDict'),'$.key_word') second_key_word,
            get_json_object(get_json_object(second_route_detail,'$.operateDict'),'$.home_index_desc') second_home_index_desc,
            get_json_object(get_json_object(second_route_detail,'$.operateDict'),'$.home_index') second_home_index,
            coalesce(
                get_json_object(get_json_object(second_route_detail,'$.operateDict'),'$.banner_id'),
                get_json_object(get_json_object(second_route_detail,'$.operateDict'),'$.h5_banner_id')
            ) second_banner_id,
            split(
                split(
                    regexp_replace(regexp_replace(get_json_object(second_route_detail, '$.url'),'\\\\u003d','='),'\\\\u0026','&'),'[&\|?]id='
                )[1],'&'
            )[0] as second_h5_page_id,
            get_json_object(second_route_detail,'$.page_name') second_page_name,
            regexp_replace(regexp_replace(get_json_object(third_route_detail, '$.url'),'\\\\u003d','='),'\\\\u0026','&') third_page_url,
            coalesce(
                get_json_object(get_json_object(third_route_detail,'$.operateDict'),'$.key_word'),
                split(split(regexp_replace(regexp_replace(get_json_object(third_route_detail, '$.url'),'\\\\u003d','='),'\\\\u0026','&'), '[&\|?]keyword=') [1],'&')[0]
            ) third_key_word,
            split(
                split(
                    regexp_replace(regexp_replace(get_json_object(third_route_detail, '$.url'),'\\\\u003d','='),'\\\\u0026','&'),'[&\|?]id='
                )[1],'&'
            )[0] as third_h5_page_id,
            split(
                split(
                    regexp_replace(regexp_replace(get_json_object(third_route_detail, '$.url'),'\\\\u003d','='),'\\\\u0026','&'),'[&\|?]parent_h5_id='
                )[1],'&'
            )[0] as third_parent_h5_page_id,
            get_json_object(get_json_object(third_route_detail,'$.operateDict'),'$.key_word') third_key_word ,
            get_json_object(get_json_object(third_route_detail,'$.operateDict'),'$.home_index_desc') third_home_index_desc ,
            get_json_object(get_json_object(third_route_detail,'$.operateDict'),'$.home_index') third_home_index ,
            get_json_object(get_json_object(third_route_detail,'$.operateDict'),'$.banner_id') third_banner_id ,
            get_json_object(third_route_detail,'$.page_name') third_page_name
        from yishou_data.dwd_log_app_route_dt 
        where dt between '${one_day_ago}' and '${gmtdate}'
        and to_char(special_date,'yyyymmdd') = '${one_day_ago}'
        and first_page_name = '首页' and get_json_object(second_route_detail,'$.page_name') = 'H5'
        and page_name in ('商品详情','加购')
        and length(event_id) <= 30 
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29
    )
)  
insert overwrite table yishou_daily.temp_route_all_event_clickaddcart_detail_h5_dt PARTITION(dt)
select  
    t3.special_date,
    t3.page_name,
    t3.event_id,
    t3.click_event_time,
    t3.add_cart_id,
    t3.add_cart_event_time,
    t3.first_page_name,
    t3.second_page_name,
    t3.third_page_name,
    t3.four_page_name,
    case when t4.add_cart_id is not null then 1 else 0 end is_join_add_cart,
    t4.add_cart_num,
    t4.add_cart_amount,
    t3.user_id,
    t3.goods_id,
    to_char(special_date,'yyyymmdd') dt
from all_event_id t3
left join (
    select 
        add_cart_id
        ,goods_number add_cart_num
        ,add_goods_amount  add_cart_amount
        ,row_number()over(partition by add_cart_id order by time) nu
    from yishou_data.dwd_log_add_cart_dt
    where dt between '${one_day_ago}' and '${gmtdate}'
    and to_char(special_date,'yyyymmdd') = '${one_day_ago}'
    and goods_number > 0
    and add_cart_id <> ''
)t4 on t3.add_cart_id_rand = t4.add_cart_id and t4.nu = 1
where t3.four_page_name <> '其他' and t3.four_page_name is not null
DISTRIBUTE BY dt
;