-- DLI sql 
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/01/06 17:34:24 GMT+08:00
-- ******************************************************************** --
-- https://yb7ao262ru.feishu.cn/wiki/Bnb6wrR1ziPTGfkXmnVcS9Eandd
select 
    dt
    ,enter_point
    ,sum(user_pv) as user_pv
    ,round(avg(user_pv_ratio),4) as user_pv_ratio
    ,sum(user_uv) as user_uv
    ,round(avg(user_uv_ratio),4) as user_uv_ratio
from (
    -- 样板管理-编辑物料-点击添加供应商
    select 
        dt
        ,'样板管理-编辑物料-点击添加供应商' as enter_point
        ,count(user_id) as user_pv
        ,0 as user_pv_ratio
        ,count(DISTINCT user_id) as user_uv
        ,0 as user_uv_ratio
    from yishou_data.dcl_yizhe_edit_sample_click_d
    where dt between '${start}' and '${end}'
    and click_source = '添加供应商'
    group by 1,2
    union ALL 
    -- 样板管理-编辑物料-点击添加供应商-点击去选择供应商
    select 
        a.dt
        ,'样板管理-编辑物料-点击添加供应商-点击去选择供应商' as enter_point
        ,count(a.user_id) as user_pv
        ,count(a.user_id) / count(b.user_id) as user_pv_ratio
        ,count(DISTINCT a.user_id) as user_uv
        ,count(DISTINCT a.user_id) / count(DISTINCT b.user_id) as user_uv_ratio
    from yishou_data.dcl_yizhe_supply_windows_click_d a
    left join (
        select 
            *
        from yishou_data.dcl_yizhe_edit_sample_click_d
        where dt between '${start}' and '${end}'
        and click_source = '添加供应商'
    ) b on a.dt = b.dt
    where a.dt between '${start}' and '${end}'
    and a.click_source = '去选择-供应商'
    and a.soruce = '1'
    group by 1,2
    union ALL 
    -- 采购列表-编辑物料-点击添加供应商
    select 
        dt
        ,'采购列表-编辑物料-点击添加供应商' as enter_point
        ,count(user_id) as user_pv
        ,0 as user_pv_ratio
        ,count(DISTINCT user_id) as user_uv
        ,0 as user_uv_ratio
    from yishou_data.dcl_yizhe_edit_purchase_order_click_d
    where dt between '${start}' and '${end}'
    and click_source = '添加供应商'
    group by 1,2
    union all
    -- 采购列表-编辑物料-点击添加供应商-点击去选择供应商
    select 
        a.dt
        ,'采购列表-编辑物料-点击添加供应商-点击去选择供应商' as enter_point
        ,count(a.user_id) as user_pv
        ,count(a.user_id) / count(b.user_id) as user_pv_ratio
        ,count(DISTINCT a.user_id) as user_uv
        ,count(DISTINCT a.user_id) / count(DISTINCT b.user_id) as user_uv_ratio
    from yishou_data.dcl_yizhe_supply_windows_click_d a
    left join (
        select 
            *
        from yishou_data.dcl_yizhe_edit_purchase_order_click_d
        where dt between '${start}' and '${end}'
        and click_source = '添加供应商'
    ) b on a.dt = b.dt
    where a.dt between '${start}' and '${end}'
    and a.click_source = '去选择-供应商'
    and a.soruce = '2'
    group by 1,2
    union ALL 
    -- 供应商管理-点击添加供应商
    select 
        dt
        ,'供应商管理-点击添加供应商' as enter_point
        ,count(user_id) as user_pv
        ,0 as user_pv_ratio
        ,count(DISTINCT user_id) as user_uv
        ,0 as user_uv_ratio
    from yishou_data.dcl_yizhe_supply_mangement_click_d
    where dt between '${start}' and '${end}'
    and click_source = '添加供应商'
    group by 1,2
    union ALL 
    -- 供应商管理-点击添加供应商-点击去选择供应商
    select 
        a.dt
        ,'供应商管理-点击添加供应商-点击去选择供应商' as enter_point
        ,count(a.user_id) as user_pv
        ,count(a.user_id) / count(b.user_id) as user_pv_ratio
        ,count(DISTINCT a.user_id) as user_uv
        ,count(DISTINCT a.user_id) / count(DISTINCT b.user_id) as user_uv_ratio
    from yishou_data.dcl_yizhe_supply_windows_click_d a
    left join (
        select 
            *
        from yishou_data.dcl_yizhe_supply_mangement_click_d
        where dt between '${start}' and '${end}'
        and click_source = '添加供应商'
    ) b on a.dt = b.dt
    where a.dt between '${start}' and '${end}'
    and a.click_source = '去选择-供应商'
    and a.soruce = '3'
    group by 1,2
    union all
    -- 点击找面/辅料tab
    select 
        dt
        ,'点击找面/辅料tab' as enter_point
        ,count(user_id) as user_pv
        ,0 as user_pv_ratio
        ,count(DISTINCT user_id) as user_uv
        ,0 as user_uv_ratio
    from yishou_data.dcl_yizhe_noodle_supermarket_click_d
    where dt between '${start}' and '${end}'
    and top_tab_name = '找面辅料'
    group by 1,2
    union all
    -- 点击找面/辅料tab-找面/辅料查询
    select 
        a.dt
        ,'点击找面/辅料tab-找面/辅料查询' as enter_point
        ,count(a.user_id) as user_pv
        ,count(a.user_id) / count(b.user_id) as user_pv_ratio
        ,count(DISTINCT a.user_id) as user_uv
        ,count(DISTINCT a.user_id) / count(DISTINCT b.user_id) as user_uv_ratio
    from yishou_data.dcl_yizhe_noodle_supermarket_click_d a
    left join (
        select 
            *
        from yishou_data.dcl_yizhe_noodle_supermarket_click_d
        where dt between '${start}' and '${end}'
        and top_tab_name = '找面辅料'
    ) b on a.dt = b.dt
    where a.dt between '${start}' and '${end}'
    and a.click_source = '找面/辅料筛选'
    and a.top_tab_name = '找面辅料'
    group by 1,2
    union ALL 
    -- 点击找面/辅料tab-克重要求查询
    select 
        a.dt
        ,'点击找面/辅料tab-克重要求查询' as enter_point
        ,count(a.user_id) as user_pv
        ,count(a.user_id) / count(b.user_id) as user_pv_ratio
        ,count(DISTINCT a.user_id) as user_uv
        ,count(DISTINCT a.user_id) / count(DISTINCT b.user_id) as user_uv_ratio
    from yishou_data.dcl_yizhe_noodle_supermarket_click_d a
    left join (
        select 
            *
        from yishou_data.dcl_yizhe_noodle_supermarket_click_d
        where dt between '${start}' and '${end}'
        and top_tab_name = '找面辅料'
    ) b on a.dt = b.dt
    where a.dt between '${start}' and '${end}'
    and a.click_source = '克重要求筛选'
    and a.top_tab_name = '找面辅料'
    group by 1,2
    union ALL 
    -- 点击找面/辅料tab-点击（列表中的）打电话按钮
    select 
        a.dt
        ,'点击找面/辅料tab-点击（列表中的）打电话按钮' as enter_point
        ,count(a.user_id) as user_pv
        ,count(a.user_id) / count(b.user_id) as user_pv_ratio
        ,count(DISTINCT a.user_id) as user_uv
        ,count(DISTINCT a.user_id) / count(DISTINCT b.user_id) as user_uv_ratio
    from yishou_data.dcl_yizhe_noodle_supermarket_click_d a
    left join (
        select 
            *
        from yishou_data.dcl_yizhe_noodle_supermarket_click_d
        where dt between '${start}' and '${end}'
        and top_tab_name = '找面辅料'
    ) b on a.dt = b.dt
    where a.dt between '${start}' and '${end}'
    and a.click_source = '打电话'
    and a.top_tab_name = '找面辅料'
    group by 1,2
    union ALL 
    -- 点击找面/辅料tab-点击面辅料卡片
    select 
        a.dt
        ,'点击找面/辅料tab-点击面辅料卡片' as enter_point
        ,count(a.user_id) as user_pv
        ,count(a.user_id) / count(b.user_id) as user_pv_ratio
        ,count(DISTINCT a.user_id) as user_uv
        ,count(DISTINCT a.user_id) / count(DISTINCT b.user_id) as user_uv_ratio
    from yishou_data.dcl_yizhe_noodle_supermarket_click_d a
    left join (
        select 
            *
        from yishou_data.dcl_yizhe_noodle_supermarket_click_d
        where dt between '${start}' and '${end}'
        and top_tab_name = '找面辅料'
    ) b on a.dt = b.dt
    where a.dt between '${start}' and '${end}'
    and a.click_source = '面辅料卡片'
    and a.top_tab_name = '找面辅料'
    group by 1,2
    union ALL 
    -- 点击面辅料卡片-点击（详情中的）打电话按钮
    select 
        a.dt
        ,'点击面辅料卡片-点击（详情中的）打电话按钮' as enter_point
        ,count(a.user_id) as user_pv
        ,count(a.user_id) / count(b.user_id) as user_pv_ratio
        ,count(DISTINCT a.user_id) as user_uv
        ,count(DISTINCT a.user_id) / count(DISTINCT b.user_id) as user_uv_ratio
    from yishou_data.dcl_yizhe_noodle_detail_click_d a
    left join (
        select 
            *
        from yishou_data.dcl_yizhe_noodle_supermarket_click_d
        where dt between '${start}' and '${end}'
        and click_source = '面辅料卡片'
    ) b on a.dt = b.dt
    where a.dt between '${start}' and '${end}'
    and a.click_source = '打电话'
    group by 1,2
    union all
    -- 点击找供应商tab
    select 
        dt
        ,'点击找供应商tab' as enter_point
        ,count(user_id) as user_pv
        ,0 as user_pv_ratio
        ,count(DISTINCT user_id) as user_uv
        ,0 as user_uv_ratio
    from yishou_data.dcl_yizhe_noodle_supermarket_click_d
    where dt between '${start}' and '${end}'
    and top_tab_name = '找供应商'
    group by 1,2
    union ALL 
    -- 点击找供应商tab-找供应商查询
    select 
        a.dt
        ,'点击找供应商tab-找供应商查询' as enter_point
        ,count(a.user_id) as user_pv
        ,count(a.user_id) / count(b.user_id) as user_pv_ratio
        ,count(DISTINCT a.user_id) as user_uv
        ,count(DISTINCT a.user_id) / count(DISTINCT b.user_id) as user_uv_ratio
    from yishou_data.dcl_yizhe_noodle_supermarket_click_d a
    left join (
        select 
            *
        from yishou_data.dcl_yizhe_noodle_supermarket_click_d
        where dt between '${start}' and '${end}'
        and top_tab_name = '找供应商'
    ) b on a.dt = b.dt
    where a.dt between '${start}' and '${end}'
    and a.click_source = '找供应商查询'
    and a.top_tab_name = '找供应商'
    group by 1,2
    union ALL 
    -- 点击找供应商tab-点击（列表中的）关联按钮
    select 
        a.dt
        ,'点击找供应商tab-点击（列表中的）关联按钮' as enter_point
        ,count(a.user_id) as user_pv
        ,count(a.user_id) / count(b.user_id) as user_pv_ratio
        ,count(DISTINCT a.user_id) as user_uv
        ,count(DISTINCT a.user_id) / count(DISTINCT b.user_id) as user_uv_ratio
    from yishou_data.dcl_yizhe_noodle_supermarket_click_d a
    left join (
        select 
            *
        from yishou_data.dcl_yizhe_noodle_supermarket_click_d
        where dt between '${start}' and '${end}'
        and top_tab_name = '找供应商'
    ) b on a.dt = b.dt
    where a.dt between '${start}' and '${end}'
    and a.click_source = '关联'
    and a.top_tab_name = '找供应商'
    group by 1,2
    union all 
    -- 点击找供应商tab-点击供应商卡片
    select 
        a.dt
        ,'点击找供应商tab-点击供应商卡片' as enter_point
        ,count(a.user_id) as user_pv
        ,count(a.user_id) / count(b.user_id) as user_pv_ratio
        ,count(DISTINCT a.user_id) as user_uv
        ,count(DISTINCT a.user_id) / count(DISTINCT b.user_id) as user_uv_ratio
    from yishou_data.dcl_yizhe_noodle_supermarket_click_d a
    left join (
        select 
            *
        from yishou_data.dcl_yizhe_noodle_supermarket_click_d
        where dt between '${start}' and '${end}'
        and top_tab_name = '找供应商'
    ) b on a.dt = b.dt
    where a.dt between '${start}' and '${end}'
    and a.click_source = '供应商卡片'
    and a.top_tab_name = '找供应商'
    group by 1,2
    union all 
    -- 点击供应商卡片-点击（详情中的）关联按钮
    select 
        a.dt
        ,'点击面辅料卡片-点击（详情中的）打电话按钮' as enter_point
        ,count(a.user_id) as user_pv
        ,count(a.user_id) / count(b.user_id) as user_pv_ratio
        ,count(DISTINCT a.user_id) as user_uv
        ,count(DISTINCT a.user_id) / count(DISTINCT b.user_id) as user_uv_ratio
    from yishou_data.dcl_yizhe_supply_detail_click_d a
    left join (
        select 
            *
        from yishou_data.dcl_yizhe_noodle_supermarket_click_d
        where dt between '${start}' and '${end}'
        and click_source = '供应商卡片'
    ) b on a.dt = b.dt
    where a.dt between '${start}' and '${end}'
    and a.click_source = '我要关联'
    group by 1,2
)
group by 1,2
