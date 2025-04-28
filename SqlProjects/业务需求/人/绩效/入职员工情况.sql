-- 入职员工情况
with
this_year_join as (
    -- 今年入职的员工情况
    select
        a.job_level as 职位等级
        ,a.dep_name as 部门名称
        ,count(a.user_id) as 今年入职
    from (
        select 
            user_id
            ,job_level
            ,dep_name
        from yishou_data.all_fmys_feishu_user_test 
        where 
            -- 将输入的20230101格式的数据转成2023-01-01 00:00:00格式的数据
            join_time >= to_timestamp('${this_year}','yyyy') 
            and 
            -- 去除无等级的客服机器人等非员工
            job_level <> ''
    ) as a
    group by a.job_level,a.dep_name
),
last_year_join as (
    -- 去年入职的员工情况
    select
        b.job_level as 职位等级
        ,b.dep_name as 部门名称
        ,count(b.user_id) as 去年入职
    from (
        select 
            user_id
            ,job_level
            ,dep_name
        from yishou_data.all_fmys_feishu_user_test 
        where 
            -- 将输入的20220101格式的数据转成2022-01-01 00:00:00格式的数据
            join_time BETWEEN to_timestamp('${last_year}','yyyy') and to_timestamp('${this_year}','yyyy')
            and 
            -- 去除无等级的客服机器人等非员工
            job_level <> ''
    ) as b
    group by b.job_level,b.dep_name
),
this_month_join as (
    -- 这个月入职的员工情况
    select
        c.job_level as 职位等级
        ,c.dep_name as 部门名称
        ,count(c.user_id) as 当月入职
    from (
        select 
            user_id
            ,job_level
            ,dep_name
        from yishou_data.all_fmys_feishu_user_test 
        where 
            -- 将输入的202201格式的数据转成2022-01-01 00:00:00格式的数据
            join_time >= to_timestamp('${this_month}','yyyyMM') 
            and 
            -- 去除无等级的客服机器人等非员工
            job_level <> ''
    ) as c
    group by c.job_level,c.dep_name
),
last_month_join as (
    -- 上个月入职的员工情况
    select
        d.job_level as 职位等级
        ,d.dep_name as 部门名称
        ,count(d.user_id) as 上月入职
    from (
        select 
            user_id
            ,job_level
            ,dep_name
        from yishou_data.all_fmys_feishu_user_test 
        where 
            -- 将输入的202201格式的数据转成2022-01-01 00:00:00格式的数据
            join_time BETWEEN to_timestamp('${last_month}','yyyyMM') and to_timestamp('${this_month}','yyyyMM') 
            and 
            -- 去除无等级的客服机器人等非员工
            job_level <> ''
    ) as d
    group by d.job_level,d.dep_name
),
result_table as (
    -- 结果汇总
    select
        t.职位等级
        ,t.部门名称
        ,t.今年入职
        -- 用coalesce函数将null返回0方便计算指标
        ,coalesce(l.去年入职,0) as 去年入职
        ,coalesce(t1.当月入职,0) as 当月入职
        ,coalesce(l1.上月入职,0) as 上月入职
        ,round(coalesce((t1.当月入职 / l1.上月入职) - 1,0),2) as 入职数量月环比
        ,round(coalesce(t1.当月入职 / l1.上月入职,0),2) as 入职数量月同比
        -- 环比 = (今月-去月)/去月 * 100%，用round函数做数据约分
        ,round(coalesce((t.今年入职 / l.去年入职) - 1,0),2) as 入职数量年环比
        -- 同比 = (今年-去年)/去年 * 100%，用round函数做数据约分
        ,round(coalesce(t.今年入职 / l.去年入职,0),2) as 入职数量年同比
    from this_year_join t
    left join last_year_join l on t.职位等级=l.职位等级 and t.部门名称=l.部门名称
    left join last_month_join l1 on t.职位等级=l1.职位等级 and t.部门名称=l1.部门名称
    left join this_month_join t1 on t.职位等级=t1.职位等级 and t.部门名称=t1.部门名称
)
-- 最后查询结果
select
    *
from result_table;



