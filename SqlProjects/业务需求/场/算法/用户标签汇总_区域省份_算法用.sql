-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2024/08/20 17:40:46 GMT+08:00
-- ******************************************************************** --
-- CREATE EXTERNAL TABLE yishou_data.dws_user_label_delivery_algorithm_dt (
--     user_id BIGINT COMMENT '用户id',
--     delivery_ip_region string comment '片区（综合）',
--     delivery_ip_area string comment '区域（综合）'
-- )
-- comment '用户标签-区域(算法使用)'
-- partitioned by (dt string)
-- STORED AS ORC LOCATION 'obs://yishou-bigdata/yishou_data.db/dws_user_label_delivery_algorithm_dt'
-- ;

with temp_user_login_geo_trans as (
    select 
        a.user_id 
        , a.ip 
        , case when  a.country = '香港' then '中国'
            when a.country = '台湾' then '中国'
            else a.country
            end as country
        , case when a.province = '北京市' then '北京'
            when a.province = '新疆' then '新疆维吾尔族自治区'
            when a.province = '云南' then '云南省'
            when a.province = '天津市' then '天津'
            when a.province = '陕西' then '陕西省'
            when a.province = '山西' then '山西省'
            when a.province = '辽宁' then '辽宁省'
            when a.province = '江西' then '江西省'
            when a.province = '吉林' then '吉林省'
            when a.province = '湖南' then '湖南省'
            when a.province = '河南' then '河南省'
            when a.province = '海南' then '海南省'
            when a.province = '贵州' then '贵州省'
            when a.province = '广东' then '广东省'
            when a.province = '甘肃' then '甘肃省'
            when a.province = '安徽' then '安徽省'
            when a.country = '香港' then '香港'
            when a.country = '台湾' then '台湾'
            else a.province 
        end as province
        , a.city
        , b.city_name
        , b.province_name
        , b.country_name
    from yishou_data.dtl_user_geo_info a
    left join(
        select regexp_replace(city_name, '自治州|市','') as city_name_trans
            , province_name
            , country_name
            , city_name
        from yishou_data.dim_city_area
        group by regexp_replace(city_name, '自治州|市','') 
            , province_name
            , country_name 
            , city_name
    ) b
    on regexp_replace(a.city, '自治州|市','') = b.city_name_trans
)
, temp_user_city_trans as (
    select a.user_id 
        , case 
            when b.country_name is not null then b.country_name
            when b.country is not null and b.country != '' then b.country
            when d.country is not null then d.country
            end as country 
        , case  
            when b.province_name is not null then b.province_name
            when b.province is not null and b.province != '' then b.province
            when d.province is not null then d.province
            end as province 
        , case 
            
            when b.city_name is not null then b.city_name
            when b.city is not null and b.city != '' then b.city
            when d.city is not null then d.city
            end as city 
        , a.pt as dt 
    from yishou_data.all_fmys_users a
    left join temp_user_login_geo_trans b
    on a.user_id = b.user_id 
    left join yishou_data.dwd_user_reg_ip_analy_dt d
    on d.dt = '${bizdate}'
    and a.user_id = d.user_id 
    where a.pt = '${bizdate}'
)
insert OVERWRITE table yishou_data.dws_user_label_delivery_algorithm_dt PARTITION(dt)
select 
    user_id
    ,case when city <> '' then area_order_province else area_ip_province end delivery_ip_region
    ,case when city <> '' then area else ip_area end delivery_ip_area
    ,dt
from (
    select 
        t1.user_id 
        ,t3.city
        ,t3.area
        ,case 
            when t1.province in (
                '黑龙江省','吉林省','辽宁省','天津','北京','河北省','山西省','内蒙古自治区',
                '山东省','新疆维吾尔自治区','宁夏回族自治区','甘肃省','青海省','陕西省','西藏自治区'
            ) then '北部'
            when t1.province in ('香港','澳门','台湾','海南省','福建省','广东省','广西壮族自治区','云南省') then '南部'
            when t1.province in ('上海','浙江省','江苏省','江西省','安徽省','湖北省','湖南省','河南省','四川省','重庆','贵州省') then '中部'
        end as area_ip_province
        ,t3.area_order_province
        ,t2.area as ip_area
        ,t1.dt
    from temp_user_city_trans t1
    left join (
        select substr(province_name,1,2) province_name,max(area) area
        from yishou_data.dim_city_area
        where province_name <> ''
        group by 1
    )t2
    on substr(t1.province,1,2) = t2.province_name
    left join (
        select 
            user_id
            ,city
            ,area
            ,case 
                when province in ('黑龙江省','吉林省','辽宁省','天津','北京','河北省','山西省','内蒙古自治区','山东省','新疆维吾尔自治区','宁夏回族自治区','甘肃省','青海省','陕西省','西藏自治区') then '北部'
                when province in ('香港','澳门','台湾','海南省','福建省','广东省','广西壮族自治区','云南省') then '南部'
                when province in ('上海','浙江省','江苏省','江西省','安徽省','湖北省','湖南省','河南省','四川省','重庆','贵州省') then '中部'
            end as area_order_province
        from yishou_data.dw_user_address_wt
    ) t3 on t1.user_id = t3.user_id
    where t3.city is not null
)
DISTRIBUTE BY dt
;



