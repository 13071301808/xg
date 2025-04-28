with s as  ( 
    ---取用户搜索事件
    select
      to_char(s.time,'yyyymmdd') natural_date
      ,s.user_id
      ,s.search_event_id
      ,sum(s.recall_num) as goods_recall_num
    from yishou_data.dwd_log_search_dt s   --存在一个搜索事件ID有多个关键词。
    where s.user_id is not null and s.user_id <> ''
    and s.dt  between  20241001  and 20241215  and to_char(s.time,'yyyymmdd') >  20241001  
    and length(s.search_event_id) > 2
    group by 1,2,3
),
ck as(
  select 
    to_char(ck.time,'yyyymmdd') natural_date
    ,ck.user_id
    ,ck.source_search_event_id as search_event_id
    ,count(DISTINCT ck.goods_id) as goods_click_uv
    ,0 goods_addcart_uv
    ,0 goods_pay_uv
    ,0.0 as gmv
  from yishou_data.dwd_log_goods_detail_page_dt ck
  left join yishou_data.dim_goods_id_info_full_h gi on gi.goods_id=ck.goods_id
  where ck.user_id is not null and ck.user_id <> '' and length(ck.source_search_event_id)>2
  and ck.dt between  20241001  and 20241215  and to_char(ck.time,'yyyymmdd') >= 20241001  
  group by 1,2,3
),
adp as(
    select 
        ad.natural_date
        ,ad.user_id
        ,ad.search_event_id
        ,0 goods_click_uv
        ,sum(ad.goods_addcart_uv)goods_addcart_uv
        ,sum(si.goods_pay_uv) goods_pay_uv
        ,sum(si.gmv)  gmv
    from(
        select 
            to_char(ad.time,'yyyymmdd') natural_date
            ,ad.user_id
            ,ad.source_search_event_id search_event_id
            ,ad.add_cart_id
            ,count(DISTINCT ad.goods_id) goods_addcart_uv
        from yishou_data.dwd_log_add_cart_dt ad
        left join yishou_data.dim_goods_id_info_full_h gi on gi.goods_id=ad.goods_id
        where ad.user_id is not null and ad.user_id <> ''
        and length(ad.source_search_event_id)>2
        and ad.dt between  20241001  and 20241215   and to_char(ad.time,'yyyymmdd') >= 20241001 
        group by 1,2,3,4
    ) ad
    left join (
        select 
          sa_add_cart_id add_cart_id
          ,count(distinct si.goods_id) goods_pay_uv
          ,sum(si.shop_price*si.buy_num)  gmv
        from yishou_data.dwd_sale_order_info_dt si 
        where si.pay_status=1 and si.user_id not in (2,10,17,387836)
        and si.dt between  20241001  and 20241215 
        group by 1
    )si on si.add_cart_id=ad.add_cart_id
    group by 1,2,3
),
t1 as (
    select
        coalesce(r.natural_date,s.natural_date)natural_date
        ,coalesce(r.user_id,s.user_id)user_id
        ,coalesce(r.search_event_id,s.search_event_id)search_event_id
        ,coalesce(s.goods_recall_num,r.goods_recall_num) as goods_recall_num
        ,coalesce(r.goods_click_uv,0) goods_click_uv
        ,coalesce(r.goods_addcart_uv,0) goods_addcart_uv
        ,coalesce(r.goods_pay_uv,0) goods_pay_uv
        ,coalesce(r.gmv,0) gmv
        -- ,to_char(coalesce(r.natural_date,s.natural_date),'yyyymmdd')  dt
    from(
        select 
            t.natural_date
            ,t.user_id
            ,t.search_event_id
            ,0 as goods_recall_num
            ,sum(t.goods_click_uv)goods_click_uv
            ,sum(t.goods_addcart_uv) goods_addcart_uv
            ,sum(t.goods_pay_uv) goods_pay_uv
            ,sum(t.gmv) gmv
        from(
            select * from ck
            union all
            select * from adp
        ) t
        group by 1,2,3
    )r    
    left join s on r.natural_date=s.natural_date and r.search_event_id=s.search_event_id
    where s.search_event_id is null 
)
select 
    substr(a.natural_date,1,6) mon,
    b.user_group,
    case 
        when coalesce(goods_recall_num,0)  = 0 then '无结果'
        when coalesce(goods_recall_num,0)  <  50 and coalesce(goods_recall_num,0) > 0 then '少结果'
        when coalesce(goods_recall_num,0)  < 1116 and coalesce(goods_click_uv,0) > 1.8 then '不精准'
        else '其他' 
    end 类别,
    count(distinct search_event_id) 搜索次数,
    sum(goods_recall_num) 搜索商品结果数,
    sum(goods_recall_num)/count(distinct search_event_id) 平均搜索商品数,
    sum(goods_click_uv)  搜索点击次数,
    sum(goods_click_uv)/count(distinct search_event_id)  搜索点击率,
    sum(goods_addcart_uv) 加购商品数,
    sum(goods_pay_uv) 支付商品总数,
    sum(gmv)   实际支付GMV
from t1 a 
left join yishou_daily.dim_user_superman_franky_snap_mt b 
on substr(a.natural_date,1,6) = b.mt and a.user_id = b.user_id  and b.mt >= 202410 
where coalesce(a.user_id,0) > 0 
group by 1,2,3
