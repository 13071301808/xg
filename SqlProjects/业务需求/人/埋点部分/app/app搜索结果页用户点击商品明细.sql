select 
    DISTINCT a.user_id as 用户id
    ,a.goods_no as 点击货号
    ,b.supply_id as 供应商id 
    ,b.supply_name as 供应商名称
from yishou_data.dcl_event_goods_detail_page_d a
left join yishou_data.dim_goods_no_info_full_h b on a.goods_no = b.goods_no
where a.dt between '20241001' and '20241028'
and a.source = '15'
and a.goods_no is not null
;