SELECT 
    user_id
    , user_profile
    , dt
    , cat_id
    , regexp_extract(one_big_market_data, '"([^"]+)"', 1) AS big_market_id
    , CAST(regexp_extract(one_big_market_data, '":([^,}]+)', 1) AS DOUBLE) AS score
FROM (
    SELECT 
        *
        , regexp_extract(big_market_data, '"([^"]+)":\\{', 1) cat_id
        , big_market_data
    FROM yishou_daily.dwt_user_preference_score_for_bpr_15day_sp_dt
    LATERAL VIEW 
        explode(split(REPLACE(REGEXP_REPLACE(get_json_object(user_profile, '$.big_market_id'), '^\\{|\\}$', ''),'},"', '}@@@"'), '@@@')) lt AS big_market_data
    WHERE dt = 20241209 
    AND user_id = 2737141
)
LATERAL VIEW explode(split(regexp_extract(big_market_data, '\\{(.*)\\}', 1), ',')) lt2 as one_big_market_data
