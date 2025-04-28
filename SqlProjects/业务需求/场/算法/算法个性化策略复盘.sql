-- DLI sql 
-- ******************************************************************** --
-- author: huangxiaoyan
-- create time: 2024/06/12 18:24:12 GMT+08:00
-- ******************************************************************** --


-- drop table if exists yishou_daily.temp_icebear_abtest_all_index;
-- create table if not exists yishou_daily.temp_icebear_abtest_all_index
-- (
--       user_md5_tail string
--     , source string
--     , goods_index string
--     , goods_choice_id string
--     , recall_id string
--     , pre_rank_id string
--     , rank_id string
--     , rerank_id string
--     , 频道曝光dau string
--     , 曝光人款 string
--     , 商品曝光次数 string
--     , 频道点击dau string
--     , 点击人款 string
--     , 商品点击次数 string
--     , 频道加购dau string
--     , 加购人款 string
--     , 商品加购次数 string
--     , 频道支付dau string
--     , 支付人款 string
--     , 商品支付次数 string
--     , 实际gmv double
--     , 实际销售件数 string

-- )COMMENT 'abtest_频道_index' 
-- STORED AS orc
-- LOCATION 'obs://yishou-data/yishou_daily-dws.db/temp_icebear_abtest_all_index'--路径要改
-- TBLPROPERTIES (
--     'hive.serialization.extend.nesting.levels' = 'true',
--     'transient_lastDdlTime' = '1619629718',
--     'orc.compress' = 'SNAPPY'
-- )
-- PARTITIONED BY (dt STRING COMMENT '分区') 
-- ;

-- show partitions yishou_daily.temp_icebear_abtest_all_index;
-- select * from yishou_daily.temp_icebear_abtest_all_index where dt between '${start}' and '${end}';




drop table if exists temp_icebear_20240615_exp;
create table if not exists temp_icebear_20240615_exp as 
select
    d,
    a.pid_source,
    a.user_id,
    a.goods_id,
    a.goods_no, 
    index goods_index, --坑位下标
    goods_choice_id,
    recall_id, 
    pre_rank_id, 
    rank_id, 
    rerank_id, 
    event_id
    
from 

    (select *
        , to_char(special_date,'yyyymmdd') d
        , CASE 
            when pid=12 then "分类"
            when pid=14 then "搜索列表"
            when pid=17 then "档口详情"
            when pid=28 then "今日特卖"
        else '其他'
        END pid_source
    from yishou_data.dwd_log_goods_exposure_dt   
    where dt BETWEEN ${start} and to_char(dateadd(to_date1(${end}, 'yyyymmdd'), 1, 'dd'), 'yyyymmdd')
    and to_char(special_date,'yyyymmdd') BETWEEN ${start} and ${end}
    -- and pid in (28, 17, 30, 31, 34, 35, 22, 12, 14) 
    and user_id > 0
    ) a
    
left join 

    (select *,
        source as source1
    -- from yishou_data.all_goods_choice_id_list_test
    from yishou_data.all_goods_choice_id_list_test
    where dt between '${start}' and '${end}'
    -- having source in (15, 8)
    ) b

on a.goods_id = b.goods_id and a.user_id = b.user_id and a.d = b.dt 
and a.index = b.goods_index and a.pid_source = b.source1

left join 

    (select *,
        case 
            when source = 8 then "分类"
            when source = 11 then "档口详情"
            when source = 15 then "搜索列表"
            when source = 32 then "今日特卖"
        else '其他'
        end source1
    from yishou_data.all_recall_id_list_test
    where dt between '${start}' and '${end}'
    ) c  -- 召回
    
on a.goods_id = c.goods_id and a.user_id = c.user_id and a.d = c.dt 
and a.index = c.goods_index and a.pid_source = c.source1

left join 

    (select *,
        case 
            when source = 8 then "分类"
            when source = 11 then "档口详情"
            when source = 15 then "搜索列表"
            when source = 32 then "今日特卖"
        else '其他'
        end source1
    from yishou_data.all_pre_rank_id_test
    where dt between '${start}' and '${end}'
    ) d  -- 粗排

on a.goods_id = d.goods_id and a.user_id = d.user_id and a.d = d.dt 
and a.index = d.goods_index and a.pid_source = d.source1

left join 

    (select *,
        case 
            when source = 8 then "分类"
            when source = 11 then "档口详情"
            when source = 15 then "搜索列表"
            when source = 32 then "今日特卖"
        else '其他'
        end source1
    from yishou_data.all_rank_id_list_test
    where dt between '${start}' and '${end}'
    ) f  -- 精排

on a.goods_id = f.goods_id and a.user_id = f.user_id and a.d = f.dt 
and a.index = f.goods_index and a.pid_source = f.source1

left join 

    (select *,
        case 
            when source = 8 then "分类"
            when source = 11 then "档口详情"
            when source = 15 then "搜索列表"
            when source = 32 then "今日特卖"
        else '其他'
        end source1
    from yishou_data.all_rerank_id_list_test
    where dt between '${start}' and '${end}'
    ) g  -- 重排
    
on a.goods_id = g.goods_id and a.user_id = g.user_id and a.d = g.dt 
and a.index = g.goods_index and a.pid_source = g.source1
;


select d, user_md5_tail, pid_source, goods_choice_id, count(distinct t1.user_id, goods_id) exp_uv 
from temp_icebear_20240615_exp t1
left join yishou_data.dim_user_md5_tail_info t2
on t1.user_id = t2.user_id and t2.head_number = 2 
group by 1,2,3,4
;

select d, user_md5_tail, pid_source, recall_id, count(distinct t1.user_id, goods_id) exp_uv 
from temp_icebear_20240615_exp t1
left join yishou_data.dim_user_md5_tail_info t2
on t1.user_id = t2.user_id and t2.head_number = 2 
group by 1,2,3  
;

select user_md5_tail, pid_source, pre_rank_id, count(distinct t1.user_id, goods_id) exp_uv 
from temp_icebear_20240615_exp t1
left join yishou_data.dim_user_md5_tail_info t2
on t1.user_id = t2.user_id and t2.head_number = 2 
group by 1,2,3  
;

select user_md5_tail, pid_source, rank_id, count(distinct t1.user_id, goods_id) exp_uv 
from temp_icebear_20240615_exp t1
left join yishou_data.dim_user_md5_tail_info t2
on t1.user_id = t2.user_id and t2.head_number = 2 
group by 1,2,3  
;

--=====================================================
drop table if exists temp_icebear_20240615_ck;
create table if not exists temp_icebear_20240615_ck as 
select 
    d,
    source,
    a.user_id,
    a.goods_id,
    b.goods_no,
    goods_index, --坑位下标
    goods_choice_id, 
    recall_id, 
    pre_rank_id, 
    rank_id, 
    rerank_id, 
    event_id
from 
    (select 
        d,
        case 
            when a.source = 8 then "分类"
            when a.source = 11 then "档口详情"
            when a.source = 15 then "搜索列表"
            when a.source = 32 then "今日特卖"
        else '其他'
        end source,
        a.user_id,
        a.goods_id,
        a.goods_index, --坑位下标
        goods_choice_id, 
        recall_id, 
        pre_rank_id, 
        rank_id, 
        rerank_id, 
        event_id
    from 
        (select *, to_char(special_date,'yyyymmdd') d 
        from yishou_data.dwd_log_goods_detail_page_dt
        where dt BETWEEN ${start} and to_char(dateadd(to_date1(${end}, 'yyyymmdd'), 1, 'dd'), 'yyyymmdd')
        and to_char(special_date,'yyyymmdd') BETWEEN ${start} and ${end}
        and source in (8, 11, 56, 15, 32)
        ) a
    left join 
        (select *
        from yishou_data.all_goods_choice_id_list_test
        where dt between '${start}' and '${end}'
        ) b 
    on a.goods_id = b.goods_id and a.user_id = b.user_id and a.d = b.dt 
    and a.goods_index = b.goods_index and a.source = b.source
    left join 
        (select * 
        from yishou_data.all_recall_id_list_test
        where dt between '${start}' and '${end}'
        ) c  -- 召回
    on a.goods_id = c.goods_id and a.user_id = c.user_id and a.d = c.dt 
    and a.goods_index = c.goods_index and a.source = c.source
    left join 
        (select * 
        from yishou_data.all_pre_rank_id_test
        where dt between '${start}' and '${end}'
        ) d  -- 粗排
    on a.goods_id = d.goods_id and a.user_id = d.user_id and a.d = d.dt 
    and a.goods_index = d.goods_index and a.source = d.source
    left join 
        (select * 
        from yishou_data.all_rank_id_list_test
        where dt between '${start}' and '${end}'
        ) f  -- 精排
    on a.goods_id = f.goods_id and a.user_id = f.user_id and a.d = f.dt 
    and a.goods_index = f.goods_index and a.source = f.source
    left join 
        (select *
        from yishou_data.all_rerank_id_list_test
        where dt between '${start}' and '${end}'
        ) g  -- 重排
    on a.goods_id = g.goods_id and a.user_id = g.user_id and a.d = g.dt 
    and a.goods_index = g.goods_index and a.source = g.source
    ) a 
join yishou_data.dim_goods_id_info b
on a.goods_id = b.goods_id
;


drop table if exists temp_icebear_20240615_ac;
create table if not exists temp_icebear_20240615_ac as 
select 
    d,
    a.source,
    a.user_id,
    a.goods_id,
    b.goods_no,
    a.goods_index, --坑位下标
    goods_choice_id, 
    recall_id, 
    pre_rank_id, 
    rank_id, 
    rerank_id, 
    event_id
from 
    (select 
        d,
        case 
            when a.source = 8 then "分类"
            when a.source = 11 then "档口详情"
            when a.source = 15 then "搜索列表"
            when a.source = 32 then "今日特卖"
        else '其他'
        end source,
        a.user_id,
        a.goods_id,
        a.goods_index, --坑位下标
        goods_choice_id, 
        recall_id, 
        pre_rank_id, 
        rank_id, 
        rerank_id, 
        event_id
    from 
        (
        select *, to_char(special_date,'yyyymmdd') d
        from yishou_data.dwd_log_add_cart_dt
        where dt BETWEEN ${start} and to_char(dateadd(to_date1(${end}, 'yyyymmdd'), 1, 'dd'), 'yyyymmdd')
        and to_char(special_date,'yyyymmdd') BETWEEN ${start} and ${end}
        and source in (8, 11, 56, 15, 32)
        ) a
    left join 
        (select *
        from yishou_data.all_goods_choice_id_list_test
        where dt between '${start}' and '${end}'
        ) b 
    on a.goods_id = b.goods_id and a.user_id = b.user_id and a.d = b.dt 
    and a.goods_index = b.goods_index and a.source = b.source
    left join 
        (select * 
        from yishou_data.all_recall_id_list_test
        where dt between '${start}' and '${end}'
        ) c  -- 召回
    on a.goods_id = c.goods_id and a.user_id = c.user_id and a.d = c.dt 
    and a.goods_index = c.goods_index and a.source = c.source
    left join 
        (select * 
        from yishou_data.all_pre_rank_id_test
        where dt between '${start}' and '${end}'
        ) d  -- 粗排
    on a.goods_id = d.goods_id and a.user_id = d.user_id and a.d = d.dt 
    and a.goods_index = d.goods_index and a.source = d.source
    left join 
        (select * 
        from yishou_data.all_rank_id_list_test
        where dt between '${start}' and '${end}'
        ) f  -- 精排
    on a.goods_id = f.goods_id and a.user_id = f.user_id and a.d = f.dt 
    and a.goods_index = f.goods_index and a.source = f.source
    left join 
        (select *
        from yishou_data.all_rerank_id_list_test
        where dt between '${start}' and '${end}'
        ) g  -- 重排
    on a.goods_id = g.goods_id and a.user_id = g.user_id and a.d = g.dt 
    and a.goods_index = g.goods_index and a.source = g.source
    ) a 
join yishou_data.dim_goods_id_info b
on a.goods_id = b.goods_id
;


-- select sum(实际gmv), d from temp_icebear_20240615_so group by d;
drop table if exists temp_icebear_20240615_so;
create table if not exists temp_icebear_20240615_so as 
select 
    o.user_md5_tail,
    d,
    case 
        when source = 8 then "分类"
        when source = 11 then "档口详情"
        when source = 15 then "搜索列表"
        when source = 32 then "今日特卖"
    else '其他'
    end source,
    goods_index, --'坑位'
    goods_choice_id, 
    recall_id, 
    pre_rank_id, 
    rank_id, 
    rerank_id, 
    count(distinct a.user_id) 频道支付dau,
    count(distinct a.user_id, a.goods_no) 支付人款,
    count(distinct a.user_id, a.goods_no, a.event_id) 商品支付次数,
    sum(real_gmv) 实际gmv,
    sum(buy_num) 实际销售件数
from 

    (select 
        a.d,
        o.source, --渠道
        a.user_id,
        a.goods_no,
        o.goods_index, --坑位下标
        goods_choice_id, 
        recall_id, 
        pre_rank_id, 
        rank_id, 
        rerank_id, 
        o.event_id,
        sum(buy_num) buy_num, 
        sum(real_gmv) real_gmv
    from 
        (
        select user_id
            , goods_no
            , goods_id
            , sa_add_cart_id
            , to_char(special_start_time,'yyyymmdd') d
            , sum(buy_num) buy_num
            , sum(buy_num*shop_price) real_gmv
        from yishou_data.dwd_sale_order_info_dt
        where dt BETWEEN ${start} and to_char(dateadd(to_date1(${end}, 'yyyymmdd'), 1, 'dd'), 'yyyymmdd')
        and to_char(special_start_time,'yyyymmdd') BETWEEN ${start} and ${end}
        and pay_status = 1 
        and is_real_pay = 1
        group by 1,2,3,4,5
        ) a
    left join 
        (
        SELECT
            row_number() over (partition by add_cart_id order by goods_index asc) index_rk,
            goods_index, 
            event_id, 
            add_cart_id,
            source
        from yishou_data.dwd_log_add_cart_dt 
        where dt >= 20230101
        having index_rk = 1
        ) o
    on a.sa_add_cart_id = o.add_cart_id 
    
    left join 
        (select *
        from yishou_data.all_goods_choice_id_list_test
        where dt between '${start}' and '${end}'
        ) b 
    on a.goods_id = b.goods_id and a.user_id = b.user_id and a.d = b.dt 
    and o.goods_index = b.goods_index and o.source = b.source
    
    left join 
        (select *
        from yishou_data.all_recall_id_list_test
        where dt between '${start}' and '${end}'
        ) c  -- 召回
    on a.goods_id = c.goods_id and a.user_id = c.user_id and a.d = c.dt 
    and o.goods_index = c.goods_index and o.source = c.source
    
    left join 
        (select *
        from yishou_data.all_pre_rank_id_test
        where dt between '${start}' and '${end}'
        ) d  -- 粗排
    on a.goods_id = d.goods_id and a.user_id = d.user_id and a.d = d.dt 
    and o.goods_index = d.goods_index and o.source = d.source
    
    left join 
        (select *
        from yishou_data.all_rank_id_list_test
        where dt between '${start}' and '${end}'
        ) f  -- 精排
    on a.goods_id = f.goods_id and a.user_id = f.user_id and a.d = f.dt 
    and o.goods_index = f.goods_index and o.source = f.source
    
    left join 
        (select *
        from yishou_data.all_rerank_id_list_test
        where dt between '${start}' and '${end}'
        ) g  -- 重排
    on a.goods_id = g.goods_id and a.user_id = g.user_id and a.d = g.dt 
    and o.goods_index = g.goods_index and o.source = g.source
    
    group by 1,2,3,4,5,6,7,8,9,10,11
    ) a 
    
left join yishou_data.dim_user_md5_tail_info o
on a.user_id = o.user_id and o.head_number = 2 
GROUP BY 1,2,3,4,5,6,7,8,9
;



insert overwrite table yishou_daily.temp_icebear_abtest_all_index partition (dt)
select 
      coalesce(t1.user_md5_tail, t2.user_md5_tail, t3.user_md5_tail, t4.user_md5_tail) as user_md5_tail
    , coalesce(t1.source, t2.source, t3.source, t4.source) source 
    , coalesce(t1.goods_index, t2.goods_index, t3.goods_index, t4.goods_index) goods_index
    , coalesce(t1.goods_choice_id, t2.goods_choice_id, t3.goods_choice_id, t4.goods_choice_id) goods_choice_id
    , coalesce(t1.recall_id, t2.recall_id, t3.recall_id, t4.recall_id) recall_id
    , coalesce(t1.pre_rank_id, t2.pre_rank_id, t3.pre_rank_id, t4.pre_rank_id) pre_rank_id
    , coalesce(t1.rank_id, t2.rank_id, t3.rank_id, t4.rank_id) rank_id
    , coalesce(t1.rerank_id, t2.rerank_id, t3.rerank_id, t4.rerank_id) rerank_id
    , 频道曝光dau
    , 曝光人款
    , 商品曝光次数
    
    , 频道点击dau
    , 点击人款
    , 商品点击次数
    
    , 频道加购dau
    , 加购人款
    , 商品加购次数
    
    , 频道支付dau
    , 支付人款
    , 商品支付次数

    , 实际gmv
    , 实际销售件数
    , coalesce(t1.d, t2.d, t3.d, t4.d) as d
from temp_icebear_20240615_exp t1  -- 曝光
full outer join temp_icebear_20240615_ck t2 -- 点击
on t1.user_md5_tail = t2.user_md5_tail and t1.source = t2.source 
and t1.goods_choice_id = t2.goods_choice_id and t1.goods_index = t2.goods_index
and t1.recall_id = t2.recall_id and t1.pre_rank_id = t2.pre_rank_id
and t1.rank_id = t2.rank_id and t1.rerank_id = t2.rerank_id
and t1.d = t2.d

full outer join temp_icebear_20240615_ac t3 -- 加购
on t1.user_md5_tail = t3.user_md5_tail and t1.source = t3.source 
and t1.goods_choice_id = t3.goods_choice_id and t1.goods_index = t3.goods_index
and t1.recall_id = t3.recall_id and t1.pre_rank_id = t3.pre_rank_id
and t1.rank_id = t3.rank_id and t1.rerank_id = t3.rerank_id
and t1.d = t3.d

full outer join temp_icebear_20240615_so t4  -- 支付
on t1.user_md5_tail = t4.user_md5_tail and t1.source = t4.source 
and t1.goods_choice_id = t4.goods_choice_id and t1.goods_index = t4.goods_index
and t1.recall_id = t4.recall_id and t1.pre_rank_id = t4.pre_rank_id
and t1.rank_id = t4.rank_id and t1.rerank_id = t4.rerank_id
and t1.d = t4.d
;



