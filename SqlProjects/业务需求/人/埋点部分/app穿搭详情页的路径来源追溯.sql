-- 穿搭详情页的路径来源追溯
select 
    case 
        when (home_index_desc like '%AC%' and home_index_desc like '%专属穿搭推荐%') then 1 -- 搜索
        when home_index_desc = 'BE_专属穿搭' then 2 -- 首页
        else 3
    end as po
    ,count(1) 
from yishou_data.dwd_log_app_route_dt 
where dt = '20250317'
group by 1
;
 
select  
    case 
        when route = '首页_H5_H5' and home_index_desc = 'BE_专属穿搭' then '穿搭详情-首页入'
        when route like '%搜索结果_H5_H5%' and (home_index_desc like '%AC%' and home_index_desc like '%专属穿搭推荐%') 
        then '穿搭详情-搜索运营位入'
        else '其他'
    end as po
    ,count(1) 
from yishou_data.dwd_log_app_route_dt 
where dt = '20250317' 
group by 1
;
