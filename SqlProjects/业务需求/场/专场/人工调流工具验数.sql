select 
    goods_no
    ,strategy_id
    ,strategy_addflow_type
    ,strategy_user_tails
    ,experiment_exposure_uv
    ,stage_next_stage_exposure_uv
    ,experiment_gmv
    ,stage_next_stage_gmv
    ,experiment_add_cart_num
    ,stage_next_stage_add_cart_count
    ,experiment_gmv / experiment_exposure_uv as ex_uv_value
    ,experiment_add_cart_uv / experiment_click_uv as addcart_click_ratio
    ,stage_next_stage_add_cart_rate
    ,experiment_click_uv / experiment_exposure_uv as stage_exposure_rate
    ,stage_next_stage_exposure_rate
    ,stage_next_stage_uv_value
    ,experiment_exposure_uv  - (contrast_exposure_uv / (10 - size(split(strategy_user_tails, ',')))) as uv_diff
    ,target_exposure_uv_diff_value
    ,is_black
    ,partition_field
from yishou_daily.ads_artificial_flow_control_result_partition_table
where goods_no = '1328409077' and partition_field >= 20250412155500
;

