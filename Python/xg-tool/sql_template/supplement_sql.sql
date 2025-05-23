-- DLI sql
-- ******************************************************************** --
-- author: huozhiqiang
-- create time: 2022/10/07 13:47:11 GMT+08:00
-- ******************************************************************** --
with all_event_id as
(
  select event_id
        ,os
        ,special_date
        ,page_index
        ,first_page_name
        ,first_home_index
        ,first_home_index_desc
        ,first_activity_name
        ,first_page_url
        ,second_key_word
        ,second_home_index_desc
        ,second_home_index
        ,second_banner_id
        ,second_page_name
  from(
  select
      event_id
      ,os
      ,special_date
      ,page_index
      ,first_page_name
      ,first_home_index
      ,first_home_index_desc
      ,first_activity_name
      ,get_json_object(first_route_detail, '$.url') first_page_url
      ,get_json_object(get_json_object(second_route_detail,'$.operateDict'),'$.key_word') second_key_word
      ,get_json_object(get_json_object(second_route_detail,'$.operateDict'),'$.home_index_desc') second_home_index_desc
      ,get_json_object(get_json_object(second_route_detail,'$.operateDict'),'$.home_index') second_home_index
      ,get_json_object(get_json_object(second_route_detail,'$.operateDict'),'$.banner_id') second_banner_id
      ,get_json_object(second_route_detail,'$.page_name') second_page_name
      ,time
      ,row_number() over(partition by event_id order by time) nu
    from yishou_data.dwd_log_app_route_dt
    where dt between TO_CHAR(DATEADD(to_date1('${bdp.system.bizdate}','yyyymmdd'),-5,"dd"),"yyyymmdd") and TO_CHAR(DATEADD(to_date1('${bdp.system.bizdate}','yyyymmdd'),1,"dd"),"yyyymmdd")
    and to_char(special_date,'yyyymmdd') >= TO_CHAR(DATEADD(to_date1('${bdp.system.bizdate}','yyyymmdd'),-4,"dd"),"yyyymmdd")
    and first_page_name is not null
    and length(event_id) <= 30
    ) t1
    where nu = 1
)
INSERT OVERWRITE table yishou_daily.route_all_event_exposure_detail_v2 partition(dt)
select
    special_date,event_id,event_time,goods_id,user_id,first_home_index,first_home_index_desc,
    case
        when first_page_name in ('首页','进货车','个人中心','分类页','订阅','直播间列表','有料') then first_page_name
        when first_page_name in ('首页分类页','榜单中心') then '首页'
        when first_page_name = '市场着陆详情页面' then '首页'
        when first_page_name = '直播' then '直播'
    else '其他' end first_page_name,
    case
        when first_page_name = '市场着陆详情页面' then '首页市场'
        when first_page_name in ('首页分类页','榜单中心') then '首页分类页_新'
    else  second_page_name end second_page_name,
    三级页面 as third_page_name,
    搜索词 as key_word,
    关键词分类 as key_word_type,
    to_char(special_date,'yyyymmdd') dt
from
(
    select
        event_id
        ,special_date
        ,if (length(split(event_id,'_')[1]) = 13 , from_unixtime(split(event_id,'_')[1]/1000), from_unixtime(split(event_id,'_')[1])) event_time
        ,case
            when first_page_name = '直播间列表' or (os='Android' and first_page_name in ('直播间','点播间','VideoActivity','VideoActivity(Live)','直播预告')) then '直播间列表'
            when first_page_name = 'H5' and first_page_url like '%shopkeeper-community%' then '有料'
            else first_page_name
        end first_page_name
        ,first_home_index
        ,first_home_index_desc
-----------------------------------------------------------2级页面--------------------------------------------------------------------------------------------------------
        ,case
            -- 首页
            when first_page_name = '首页' and (substring(first_home_index,1,2) in ('BD','BG')
                                                or (substring(first_home_index,1,2) = 'AE' and first_home_index_desc in ('AE_首发新款','AE_一手直播','AE_今日特卖','AE_新品首发','AE_爆款24h发')))                                                                                           then '首页营销频道'
            when first_page_name = '首页' and substring(first_home_index,1,2) in ('AB','AH')                                              then '首页搜索'
            when first_page_name = '首页' and substring(first_home_index,1,2) in ("AG","AO","AN")                                         then '首页专场'
            -- BO按路径实际为顶部tab，这里特殊处理算为首页底部商品
            when first_page_name = '首页' and substring(first_home_index,1,2) in ("BO","CA") and pid = '52'                             then '首页专场商品流推荐'
            when first_page_name = '首页' and substring(first_home_index,1,2) in ("BO","CA") and pid = '54'                             then '首页专场嵌入商品推荐'
            when first_page_name = '首页' and substring(first_home_index,1,2) = "BN"                                                    then '首页专场商品流推荐'
            when first_page_name = '首页' and substring(first_home_index,1,2) = "BM"                                                    then '首页专场嵌入商品推荐'
            when first_page_name = '首页' and substring(first_home_index,1,2) in ("BO","CA")                                              then '首页底部商品'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "AI"                                                     then '首页市场'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "AJ"                                                     then '首页分类'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "AD"                                                     then '首页活动banner'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "AC"                                                     then '首页banner'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "AS"                                                     then '首页上三坑'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "BE"                                                     then '首页下五坑'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "AA"                                                     then '首页弹窗'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "AQ"                                                     then '首页消息'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "AR"                                                     then '首页浮层'
            when first_page_name = '首页' and substring(first_home_index,1,2)  in ("AW",'AU','AM','AX','AY')                              then '首页新人专享'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "BE"                                                     then '首页赛马频道'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "BH"                                                     then '首页顶部banner'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "BL"                                                     then '首页市场下方banner'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "CA"                                                     then '首页底部tab'
            when first_page_name in ('首页分类页','榜单中心')                                                                             then '首页分类TAB'
            when first_page_name = '首页'                                                                                                 then '首页其他'
            -- 分类页
            when first_page_name = '分类页' and substring(first_home_index,1,4) in ('AA_1','AA_2')                                        then '分类页搜索'
            when first_page_name = '分类页' and substring(first_home_index,1,4) = 'AH_1'                                                  then '分类页找档口'
            when first_page_name = '分类页' and substring(first_home_index,1,2) = 'AG' then '分类页专题'
            when first_page_name = '分类页' and substring(first_home_index,1,2) = 'AC' and first_home_index_desc like '%热门分类推荐%'    then '分类页热门分类推荐'
            when first_page_name = '分类页' and substring(first_home_index,1,2) = 'AC' and first_home_index_desc like '%热门市场%'        then '分类页热门市场'
            when first_page_name = '分类页' and first_home_index_desc like '%风格%'                                                       then '分类页风格'
            when first_page_name = '分类页' and substring(first_home_index,1,2) = 'BA'                                                    then '分类页底部tab'
            when first_page_name = '分类页'                                                                                               then '分类页商品分类'
            -- 我的页面
            when first_page_name = '个人中心' and first_home_index = 'AA'                                                                 then '个人中心收藏夹'
            when first_page_name = '个人中心' and first_home_index = 'AB'                                                                 then '个人中心足迹'
            when first_page_name = '个人中心' and first_home_index in ('AF_1','AF_2','AF_3','AF_4','AF_5','AF_6')                         then '个人中心我的订单'
            when first_page_name = '个人中心' and first_home_index = 'AM'                                                                 then '个人中心我的档口'
            when first_page_name = '个人中心' and first_home_index in ('AD','AR','AS')                                                    then '个人中心我的资产'
            when first_page_name = '个人中心' and first_home_index in ('AU_1','AU_2','AU_3')                                              then '个人中心我的活动'
            when first_page_name = '个人中心' and first_home_index in ('AI_1','AI_2','AI_3','AI_4','AI_5')                                then '个人中心我的服务'
            when first_page_name = '个人中心' and substring(first_home_index,1,2) = 'CA'                                                  then '个人中心底部商品'
            when first_page_name = '个人中心'                                                                                             then '个人中心其他'
            -- 进货车
            when first_page_name = '进货车'  and first_home_index = 'AD'                                                                  then '进货车编辑'
            when first_page_name = '进货车'  and substring(first_home_index,1,2) = 'AB'                                                   then '进货车专场'
            when first_page_name = '进货车'  and substring(first_home_index,1,2) = 'AC'                                                   then '进货车档口'
            when first_page_name = '进货车'  and substring(first_home_index,1,2) in ('AF','BA')                                           then '进货车推荐商品'
            when first_page_name = '进货车'                                                                                               then '进货车其他'
            -- 订阅页
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
            -- 直播页
            when first_page_name = '直播间列表' and second_page_name in ('直播间','VideoActivity','VideoActivity(Live)') then '直播间列表直播'
            when first_page_name = '直播间列表' and second_page_name = '点播间' then '直播间列表点播'
            when first_page_name = '直播间列表' and second_page_name = '直播预告' then '直播间列表直播预告'
            when os = 'Android' and first_page_name in ('直播间','VideoActivity','VideoActivity(Live)') then '直播间列表直播'
            when os = 'Android' and first_page_name = '点播间' then '直播间列表点播'
            when os = 'Android' and first_page_name = '直播预告' then '直播间列表直播预告'
            when first_page_name = '直播间列表' and substring(first_home_index,1,2) = 'BA' then '直播间列表底部tab'
            when first_page_name = '直播间列表' or (os = 'Android' and first_page_name in ('直播间','点播间','VideoActivity','VideoActivity(Live)','直播预告'))  then '直播间列表其他'
            -- H5
            when first_page_name = 'H5' and first_page_url like '%shopkeeper-community%' and second_page_name = '商品详情' then '有料商品详情'
            when first_page_name = 'H5' and first_page_url like '%shopkeeper-community%' and second_page_name = 'H5' then '有料继续游览'
            when first_page_name = 'H5' and first_page_url like '%shopkeeper-community%' and second_page_name = '直播间列表' then '有料直播间列表'
            when first_page_name = 'H5' and substring(first_home_index,1,2) = 'BA' then '有料底部tab'
            when first_page_name = 'H5' and first_page_url like '%shopkeeper-community%' then '有料其他'
            when page_index = 1 then '没有二级页面'
            else '其他'
        end second_page_name
-----------------------------------------------------------3级页面-------------------------------------------------------------------------
        ,case
            when first_page_name = '首页' and (substring(first_home_index,1,2) in ('BD','BG') or (substring(first_home_index,1,2) = 'AE' and first_home_index_desc in ('AE_首发新款','AE_一手直播','AE_今日特卖','AE_新品首发','AE_爆款24h发'))) then concat('首页营销频道',split(first_home_index_desc,'_')[1])
            when first_page_name = '首页' and substring(first_home_index,1,2) = 'BE' then concat('首页赛马频道',split(first_home_index_desc,'_')[1])
            when first_page_name = '首页' and substring(first_home_index,1,3) in ("AG_","AO_","AN_") then concat("专场TAB_", split(first_home_index_desc,'_')[1])
            when first_page_name = '首页' and substring(first_home_index,1,2) = "BN" then concat("专场TAB_", substring_index(first_home_index_desc, '_', -1))
            when first_page_name = '首页' and substring(first_home_index,1,2) = "BM" then '专场嵌入商品推荐'
            -- BO按路径实际为顶部tab，这里特殊处理算为首页底部商品
            when first_page_name = '首页' and substring(first_home_index,1,2) = "BO" then '首页底部商品'
            when first_page_name = '首页' and substring(first_home_index,1,2)  = "AI" then concat('首页市场',split(first_home_index_desc,'_')[1]
            ,'_'
            ,case
                when substring(second_home_index,1,2) = 'AA' and second_page_name = '市场着陆详情页面'  then '档口上新'
                when substring(second_home_index,1,2) = 'AB' and second_page_name = '市场着陆详情页面'  then '全部上新'
                when substring(second_home_index,1,2) = 'AC' and second_page_name = '市场着陆详情页面'  then '特价优选档口上新'
                when substring(second_home_index,1,2) = 'AD' and second_page_name = '市场着陆详情页面'  then '搜索'
                when substring(second_home_index,1,2) = 'AE' and second_page_name = '市场着陆详情页面'  then '市场介绍'
                when substring(second_home_index,1,2) = 'AE' and second_page_name = '市场着陆详情页面'  then '顶部运营位'
                else '其他'
            end,coalesce(second_banner_id,''))
            when first_page_name = '个人中心' and first_home_index = 'AF_2' then '个人中心我的订单待发货'
            when first_page_name = '个人中心' and first_home_index = 'AF_3' then '个人中心我的订单待收货'
            when first_page_name = '个人中心' and first_home_index = 'AF_5' then '个人中心售后进度'
            when first_page_name = '个人中心' and first_home_index = 'AF_6' then '个人中心我的订单查看全部'
            when first_page_name = '首页分类页' then concat('首页分类TAB',first_activity_name)
            when first_page_name = '榜单中心' then '首页分类TAB榜单中心'
            when first_home_index_desc in ('AB_以图搜图','AA_以图搜图') or second_home_index_desc = 'AA_以图搜图' then '以图搜图'
            when second_key_word is not null then  '搜索词'
            when first_home_index_desc in ('AB_搜索栏','AA_搜索栏') then '搜索推荐'
            when first_page_name = '订阅' and substring(first_home_index,1,2) = 'AF' then replace(first_home_index_desc,'AF','订阅档口动态的运营位') --订阅运营位
            when first_page_name = '分类页' then if(SUBSTR(first_home_index_desc,4,length(first_home_index_desc)-3)<>'',SUBSTR(first_home_index_desc,4,length(first_home_index_desc)-3),'其他')
           else '其他'
        end as 三级页面
        ,second_key_word as 搜索词
        ,t2.关键词分类
        ,t3.user_id
        ,t3.goods_id
    from (
        select
            t1.event_id
            ,if((t1.activity_name <> '' and pid = '37'),t1.special_date,t2.special_date) special_date
            ,if((t1.activity_name <> '' and pid = '37'),1,t2.page_index) page_index
            ,if((t1.activity_name <> '' and pid = '37'),'首页分类页',t2.first_page_name) first_page_name
            ,t2.first_home_index
            ,t2.first_home_index_desc
            ,if((t1.activity_name <> '' and pid = '37'),t1.activity_name,t2.first_activity_name) first_activity_name
            ,t2.second_key_word
            ,t2.second_home_index_desc
            ,t2.second_home_index
            ,t2.second_banner_id
            ,t2.second_page_name
            ,if((t1.activity_name <> '' and pid = '37'),t1.os,t2.os) os
            ,t2.first_page_url
            ,nvl(t2.second_key_word,concat(round(rand()*10,5),'_空值避免数据倾斜')) second_key_word_round
            ,t1.user_id
            ,t1.goods_id
            ,t1.pid
        from (
            select event_id,goods_id,user_id,activity_name,pid,os,datetrunc(from_unixtime(report_time/1000-25200),'dd') special_date
            from yishou_data.dcl_event_big_goods_exposure_d
            where dt >= TO_CHAR(DATEADD(to_date1('${bdp.system.bizdate}','yyyymmdd'),-5,"dd"),"yyyymmdd")
            union all
            select event_id,goods_id,user_id,'' activity_name,'' pid,'' os,datetrunc(from_unixtime(log_time-25200),'dd') special_date
            from yishou_data.dcl_h5_ys_h5_goods_exposure
            where dt >= TO_CHAR(DATEADD(to_date1('${bdp.system.bizdate}','yyyymmdd'),-5,"dd"),"yyyymmdd")
        ) t1
        left join all_event_id t2 on t1.event_id = t2.event_id
        where (t1.event_id = t2.event_id or (t1.activity_name <> '' and pid = '37')) --activity_name是首页分类页特有字段,pid是首页分类页
    ) t3
    left join yishou_daily.route_key_word_mapping_last_10days t2 on t3.second_key_word_round = t2.关键词
    where to_char(special_date,'yyyymmdd') >= TO_CHAR(DATEADD(to_date1('${bdp.system.bizdate}','yyyymmdd'),-4,"dd"),"yyyymmdd")
)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
having dt = '${bdp.system.bizdate}'
;