
-- 背景：算法开发推荐接口已上线，现需要通过解析埋点中商品明细页下该字段的数据来最后以表的形式输出，以便分析师对这个数据进行分析和归因。

-- 商品选择id表goods_choice_id
insert OVERWRITE table yishou_data.all_goods_choice_id_list_test
select 
    DISTINCT user_id,
    goods_id,
    regexp_replace(split_data, '\\[|\\]', '') AS goods_choice_id,
    pid as source,
    index as goods_index,
    dt
from yishou_data.dcl_event_goods_exposure_d 
LATERAL VIEW explode(split(get_json_object(is_operat, '$.goods_choice_id_list'), ',')) exploded_table AS split_data
WHERE dt = ${one_day_ago} AND is_operat LIKE '{%' and pid is not null
;

-- pre_rank_id表
insert OVERWRITE table yishou_data.all_pre_rank_id_test
select
    DISTINCT user_id,
    goods_id,
    get_json_object(is_operat, '$.pre_rank_id') AS pre_rank_id,
    pid as source,
    index as goods_index,
    dt
from yishou_data.dcl_event_goods_exposure_d 
WHERE dt = ${one_day_ago} AND is_operat LIKE '{%' and pid is not null
;

-- rank_id_list表
insert OVERWRITE table yishou_data.all_rank_id_list_test
select
    DISTINCT user_id,
    goods_id,
    regexp_replace(split_data1, '\\[|\\]', '') AS rank_id,
    pid as source,
    index as goods_index,
    dt
from yishou_data.dcl_event_goods_exposure_d 
LATERAL VIEW explode(split(get_json_object(is_operat, '$.rank_id_list'), ',')) exploded_table1 AS split_data1
WHERE dt = ${one_day_ago} AND is_operat LIKE '{%' and pid is not null
;

-- 返回商品id表recall_id_list
insert OVERWRITE table yishou_data.all_recall_id_list_test
select
    DISTINCT user_id,
    goods_id,
    regexp_replace(split_data2, '\\[|\\]', '') AS recall_id,
    pid as source,
    index as goods_index,
    dt
from yishou_data.dcl_event_goods_exposure_d 
LATERAL VIEW explode(split(get_json_object(is_operat, '$.recall_id_list'), ',')) exploded_table2 AS split_data2
WHERE dt = ${one_day_ago} AND is_operat LIKE '{%' and pid is not null
;

-- rerank_id_list表
insert OVERWRITE table yishou_data.all_rerank_id_list_test
select
    DISTINCT user_id,
    goods_id,
    regexp_replace(split_data3, '\\[|\\]', '') AS rerank_id,
    pid as source,
    index as goods_index,
    dt
from yishou_data.dcl_event_goods_exposure_d 
LATERAL VIEW explode(split(get_json_object(is_operat, '$.rerank_id_list'), ',')) exploded_table2 AS split_data3
WHERE dt = ${one_day_ago} AND is_operat LIKE '{%' and pid is not null
;