with search_cnt_dt as (
    select sp_dt::date 专场日,
     keyword
    ,keyword_cat
    ,search_cnt
    ,search_dau
    ,gmv as 搜索页gmv
    from finebi.finebi_search_channel_detail_result 
    where sp_dt::date>=current_date()-800
    and (keyword is not null or keyword <> '')
    and (keyword_cat is not null or keyword_cat <> '')
)
select left(t1.专场日,7) 年月,
 t1.*
,t2.search_cnt  搜索次数年同期
,t2.search_dau  搜索人数年同期
,t2.搜索页gmv   搜索页gmv年同期
from search_cnt_dt t1 
left join search_cnt_dt t2 on t1.keyword=t2.keyword 
and   t1.专场日=t2.专场日 + integer'365'
where t1.专场日 between '${开始日期}' and '${结束日期}' 
and   t1.search_cnt>=10 
