select 
    dt
    ,click_source
    ,banner_name
    ,sum(click_uv) as click_uv
    ,sum(exposure_uv) as exposure_uv
from (
    select 
        dt
        ,click_source
        ,banner_name
        ,count(DISTINCT user_id) as click_uv 
        ,0 as exposure_uv
    from yishou_data.dcl_wx_home_click_d 
    where dt between '20240704' and '20240918'
    and click_source = '首页市场bannner'
    and banner_name in ('十三行','沙河','意法','南油','新意法')
    group by 1,2,3
    union ALL 
    SELECT 
        dt,
        '首页市场bannner' AS click_source,
        get_json_object(home_market, '$.banner_name') AS banner_name,
        0 as click_uv,
        COUNT(DISTINCT user_id) AS exposure_uv
    FROM yishou_data.dcl_wx_home_exposure_d 
    LATERAL VIEW OUTER EXPLODE(split(replace(replace(replace(home_market_banner, '[', ''),']', ''), '},{', '}@@@@@{'), '@@@@@')) t AS home_market
    WHERE dt between '20240704' and '20240918'
    AND home_market_banner IS NOT NULL 
    AND home_market_banner <> ''
    AND get_json_object(home_market, '$.banner_name') IN ('十三行', '沙河', '意法', '南油', '新意法')
    GROUP BY 1,3
)
group by 1,2,3