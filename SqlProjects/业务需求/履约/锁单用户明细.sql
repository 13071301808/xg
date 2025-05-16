select 
    suodan.special_date as 专场日期
    ,suodan.user_id  as 用户ID
    ,suodan.goods_no as 商品货号
    ,suodan.co_val as 颜色
    ,suodan.si_val as 尺码
    ,case 
        when qiyu.user_id is not null and qiyu.special_date is not null then '是' 
        else '否' 
    end as 锁单当天是否有点击过客服入口
    ,max(suodan.shop_price) as 商品单价
    ,sum(suodan.cumulative_buy_num) as 已开单件数
    ,sum(suodan.cancel_num) as 取消件数
from (
    SELECT 
        t1.lock_time as special_date
        ,t1.goods_no
        ,t2.user_id
        ,t2.co_val
        ,t2.si_val
        ,max(t2.shop_price) shop_price
        ,sum(t2.cumulative_buy_num) cumulative_buy_num
        ,sum(t2.cancel_num) cancel_num
    FROM (
        -- 锁单数据
        SELECT 
            to_char(from_unixtime(special_date),'yyyymmdd') lock_time
            ,goods_no
            ,sum(lock_num) lock_num
        from yishou_data.ods_fmys_lock_goods_num_to_plan_view 
        where to_char(from_unixtime(special_date),'yyyymmdd') between '${start}' and '${end}'
        group by 1,2
    ) t1 
    left join (
        SELECT 
            to_char(dateadd(fo.add_time,-7,'hh'),'yyyymmdd') special_date
            ,fo.goods_no
            ,fo.user_id
            ,fo.co_val
            ,fo.si_val
            ,max(fo.shop_price) shop_price
            ,sum(fo.buy_num) buy_num
            ,sum(case when cr.order_id is not null then fo.buy_num else null end) cancel_num
            -- 滚动累计（按商品+颜色+尺码分组，按日期排序）
            ,sum(sum(fo.buy_num)) over (
                partition by fo.goods_no, fo.co_val, fo.si_val 
                order by to_char(dateadd(fo.add_time,-7,'hh'),'yyyymmdd')
                rows between unbounded preceding and current row
            ) as cumulative_buy_num
        from yishou_data.dwd_sale_order_info_dt fo
        left join yishou_data.all_fmys_order_cancel_record_h cr on fo.order_id = cr.order_id and cr.cancel_type = 1
        left join yishou_data.ods_fmys_order_cancel_blacklist_view focb on fo.user_id = focb.user_id
        where dt between '${start}' and '${end}'
        and to_char(dateadd(fo.add_time,-7,'hh'),'yyyymmdd') between '${start}' and '${end}'
        -- 精确排除5点至7点（含5点不含7点）
        and (hour(fo.add_time) < 5 or hour(fo.add_time) >= 7) 
        and focb.user_id is null
        group by 1,2,3,4,5
    ) t2 on t1.goods_no = t2.goods_no and t1.lock_time = t2.special_date
    where t1.lock_num > t2.cumulative_buy_num  -- 修改比较条件为累计量
    group by 1,2,3,4,5
) suodan
left join (
    -- 客服入口数据
    select 
        to_char(from_unixtime((time / 1000)-25200),'yyyymmdd') special_date
        ,user_id
    from yishou_data.dcl_event_qiyu_click_d
    where dt between '${start}' and '${end}'
    and to_char(from_unixtime((time / 1000)-25200),'yyyymmdd') between '${start}' and '${end}'
    group by 1,2
) qiyu 
on suodan.special_date = qiyu.special_date and suodan.user_id = qiyu.user_id
group by 1,2,3,4,5,6