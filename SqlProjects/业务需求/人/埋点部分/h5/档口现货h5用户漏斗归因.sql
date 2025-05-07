select 
    special_date
    ,case 
        when four_page_name = '档口现货搜索结果' then '现货频道-搜索栏'
        when four_page_name like '档口现货轮播图%' then '现货频道-轮播banner'
        when four_page_name in ('档口现货快速补货','档口现货大牌联合入仓') then '现货频道-轮播下两坑运营位'
        when four_page_name in ('档口现货市场特价','档口现货市场新款','档口现货返单爆款') then '现货频道-三坑运营位'
        when four_page_name like '档口现货品类%' then '现货频道-品类八坑运营位'
        when four_page_name = '档口现货品类下方运营位' then '档口现货品类下方运营位'
    end as model_name
    ,case 
        when four_page_name = '档口现货快速补货' then '现货频道-轮播下两坑运营位-左坑'
        when four_page_name = '档口现货大牌联合入仓' then '现货频道-轮播下两坑运营位-右坑'
        when four_page_name = '档口现货市场特价' then '现货频道-三坑运营位-左坑'
        when four_page_name = '档口现货市场新款' then '现货频道-三坑运营位-中坑'
        when four_page_name = '档口现货返单爆款' then '现货频道-三坑运营位-右坑'
        when four_page_name = '档口现货品类_连衣裙套装' then '现货频道-品类运营位-上1坑'
        when four_page_name = '档口现货品类_T恤' then '现货频道-品类运营位-上2坑'
        when four_page_name = '档口现货品类_小衫' then '现货频道-品类运营位-上3坑'
        when four_page_name = '档口现货品类_衬衫' then '现货频道-品类运营位-上4坑'
        when four_page_name = '档口现货品类_牛仔裤' then '现货频道-品类运营位-下1坑'
        when four_page_name = '档口现货品类_休闲裤' then '现货频道-品类运营位-下2坑'
        when four_page_name = '档口现货品类_半身裙' then '现货频道-品类运营位-下3坑'
        when four_page_name = '档口现货品类_鞋包配' then '现货频道-品类运营位-下4坑'
    end as second_model_name
    ,four_page_name as page_name
    ,coalesce(count(DISTINCT case when goods_exposure_uv > 0 then user_id else null end),0) as exposure_uv
    ,coalesce(count(DISTINCT case when goods_click_uv > 0 then user_id else null end),0) as click_uv
    ,coalesce(count(DISTINCT case when goods_add_uv > 0 then user_id else null end),0) as add_cart_uv
    ,coalesce(count(DISTINCT case when goods_pay_uv > 0 then user_id else null end),0) as pay_uv
    ,coalesce(sum(goods_exposure_uv),0) as goods_exposure_uv
    ,coalesce(sum(goods_click_uv),0) as goods_click_uv
    ,coalesce(sum(goods_add_uv),0) AS goods_add_uv
    ,coalesce(round(sum(real_gmv),2),0) as real_gmv
    ,coalesce(sum(real_buy_num),0) as real_buy_num
from yishou_daily.temp_route_all_event_flow_detail_h5_dt
where dt between '${start}' and '${end}' and four_page_name is not null
group by 1,2,3,4
union all 
select 
    special_date
    , model_name
    , case 
        when page_name = '新款' then '现货频道-商品流-tab1'
        when page_name = '爆款' then '现货频道-商品流-tab2'
        when page_name = '特价' then '现货频道-商品流-tab3'
        when page_name = '猜你喜欢' then '现货频道-商品流-tab4'
        when page_name = '❤限量补贴' then '现货频道-商品流-tab5'
        when page_name = '十三行' then '现货频道-商品流-tab6'
        when page_name = '沙河' then '现货频道-商品流-tab7'
        when page_name = '杭濮' then '现货频道-商品流-tab8'
        when page_name = '金马' then '现货频道-商品流-tab9'
        when page_name = '南油' then '现货频道-商品流-tab10'
        when page_name = '大批拿货' then '现货频道-商品流-tab11'
        when page_name = '直播款' then '现货频道-商品流-tab12'
        when page_name = '连衣裙/套装' then '现货频道-商品流-tab13'
        when page_name = '上衣' then '现货频道-商品流-tab14'
        when page_name = '下装' then '现货频道-商品流-tab15'
        when page_name = '外套' then '现货频道-商品流-tab16'
        when page_name = '鞋包配' then '现货频道-商品流-tab17'
        when page_name = '男装/童装' then '现货频道-商品流-tab18'
    end as second_model_name
    , page_name
    , coalesce(sum(exposure_uv),0) as exposure_uv
    , coalesce(sum(click_uv),0) as click_uv
    , coalesce(sum(add_cart_uv),0) as add_cart_uv
    , coalesce(sum(pay_uv),0) as pay_uv
    , coalesce(sum(goods_exposure_uv),0) as goods_exposure_uv
    , coalesce(sum(goods_click_uv),0) as goods_click_uv
    , coalesce(sum(goods_add_uv),0) as goods_add_uv
    , coalesce(round(sum(real_gmv),2),0) as real_gmv
    , coalesce(sum(real_buy_num),0) as real_buy_num
from yishou_daily.temp_route_supply_goods_flow_detail_h5_dt
where dt between '${start}' and '${end}' 
and page_name is not null
group by 1,2,3,4
;