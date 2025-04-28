-- INSERT OVERWRITE TABLE yishou_daily.finebi_delivery_ck_work_rk_dbid_date_dt PARTITION(dt)
SELECT 
-------------------------------------------------出库模块
    ck.dbid 出库打标id
    ,ck.sor_create_time 出库时间
    ,case when ck.sor_type = 1 then '发货出库'
        when ck.sor_type = 10 then '售后寄出' 
        else ck.sor_type end 出库类型
    ,ck.origin 出库仓库地
    ,case when ck.origin = 1 then '广州佛山仓'
        when ck.origin = 2 then '杭州萧山仓'
        when ck.origin = 3 then '广州增城仓'
        when ck.origin = 4 then '杭州红山仓' 
        when ck.origin = 5 then '河南郑州仓'
        else ck.origin end 出库仓库地_中文
    ,ck.sir_stock_source 原入库类型
    ,case when ck.sir_stock_source = 1 then '采购欠货'
        when ck.sir_stock_source = 2 then '退换货欠货'
        when ck.sir_stock_source = 3 then '售后欠货'
        when ck.sir_stock_source = 4 then '售后拆包'  
        when ck.sir_stock_source = 5 then '调拨'
        when ck.sir_stock_source = 7 then '核销'
        when ck.sir_stock_source = 8 then '其他入库'
        when ck.sir_stock_source = 9 then '生产盘盈'
        when ck.sir_stock_source = 10 then '售后盘盈'
        when ck.sir_stock_source = 11 then '旧前置仓'
        when ck.sir_stock_source = 12 then '售后拆包错发核销'
        when ck.sir_stock_source = 13 then '退换货其它入库'
        when ck.sir_stock_source = 14 then '新前置仓'
        when ck.sir_stock_source = 15 then '达人售后'
        when ck.sir_stock_source = 16 then '售后拆包(退供)'
        when ck.sir_stock_source = 17 then '售后欠货(退供)'
        when ck.sir_stock_source = 18 then '买版计划'
        when ck.sir_stock_source = 19 then '前置仓异常入库'
        else ck.sir_stock_source
        end 原入库类型_中文
    ,b.create_time as rk_time ---原入库时间
    ,b.woi_price as rk_woi_price ---原入库采购价
    -------------------------------------------------基本属性模块
    ,ck.sku 
    ,ck.goods_no 
    ,gn.goods_kh 
    ,ck.co_id  -- 颜色id
    ,ck.co_val  -- 颜色
    ,ck.si_id  -- 尺码id
    ,ck.si_val  -- 尺码
    ,gn.origin  -- 商品发货地
    ,gn.supply_id  -- 供应商id
    ,si.supply_name  -- 供应商名称
    ,mi.big_market  -- 大市场
    ,mi.primary_market_name  -- 一级市场
    ,mi.second_market_name  -- 二级市场
    ,ci.primary_cat_name  -- 一级品类
    ,ci.second_cat_name  -- 二级品类
    ,ci.third_cat_name  -- 三级品类
    ------------------------------------------------订单模块
    ,ck.rec_id  -- 订单详情id
    ,ck.order_id  -- 订单id
    ,oi.order_sn  -- 订单号
    ,oi.user_id  -- 用户id 
    ,oi.add_time --订单下单时间
    ,datetrunc(oi.special_start_time,'dd')  special_date
    ,from_unixtime(t4.sign_time) 订单签收时间
    ,oi.shop_price 订单销售价
    -----------------------------------------------质检改标模块
    -- ,case when t3.type = 1 then '常规' when t3.type = 2 then '质检' when t3.type = 3 then '改标' else t3.type end 发货出库波次类型
    ,case when ck.wp_type = 1 then '常规' when ck.wp_type = 2 then '质检' when ck.wp_type = 3 then '改标' else ck.wp_type end 发货出库波次类型
    -- ,from_unixtime(t3.check_time) 波次校验完成时间
    ,ck.wp_check_time  波次校验完成时间
    -- ,case when qc.dbid is not null then '是' else '否' end 是否质检
    ,case when ck.is_pc_check =1 then '是' else '否' end    是否质检
    -- ,qc.pc_check_admin_name 全部质检人
    ,ck.pc_check_admin_name      全部质检人
    -- ,qc.pc_check_time 全部质检时间
    ,ck.pc_check_time   全部质检时间
    ,zj.min_qc_time --首次质检时间
    ,zj.max_qc_time --最后一次质检时间
    -- ,case when coalesce(wpe.quality_level,if(qc.dbid> 0,wpe1.quality_level,null)) = 0 then '无'
    --     when coalesce(wpe.quality_level,if(qc.dbid> 0,wpe1.quality_level,null)) = 1 then '初级质检'
    --     when coalesce(wpe.quality_level,if(qc.dbid> 0,wpe1.quality_level,null)) = 2 then '高级质检'
    --     when coalesce(wpe.quality_level,if(qc.dbid> 0,wpe1.quality_level,null)) = 3 then '精心质检'
    --     else to_char(coalesce(wpe.quality_level,if(qc.dbid> 0,wpe1.quality_level,null))) end  质检类型
    ,ck.quality_level_name  质检类型
    ,zj.quality_rule_zw  -- 质检规则
    ,zj.secondary_quality_rule_zw  -- 默认质检二级规则
    ,zj.quality_type_id  -- 质检类型id
    ,zj.quality_type_name  -- 质检类型名称
    ,zj.quality_type_desc  -- 质检类型说明    
    
    ,qc2.min_admin_name --首次质检人员
    ,qc2.max_admin_name --最后一次质检人员
    -- ,case when coalesce(wpe.quality_level,if(qc.dbid> 0,wpe1.quality_level,null)) = 2 then '是' end 是否高级质检
    -- ,case when coalesce(wpe.quality_level,if(ck.is_pc_check==1,wpe1.quality_level,null)) = 2 then '是' end 是否高级质检
    ,case when ck.quality_level_name='高级质检' then '是' end 是否高级质检
    ,case when zj.is_cp = 1 then '是' when is_cp = 2 then '否' end 是否次品
    ,zj.cp_one_defective 质检次品一级分类
    ,zj.cp_two_defective 质检次品二级分类
    
    ,ck.is_gb 是否改标商品
    ,ck.gb_create_time 改标时间
    ,ck.gb_user_id 改标人员id
    ,gb1.name gb_name --改标人员名称  
    -------------------------------------------------工单模块
    ,w.id work_id 
    ,w.add_time work_add_time --工单创建时间
    ,w.handle_type --工单处理方式
    ,case when w.handle_type = 1 then '换货'
        when w.handle_type = 3 then '退款'
        when w.handle_type = 4 then '补偿'
        when w.handle_type = 5 then '退货' 
        else w.handle_type end handle_type_zw --工单处理方式_中文
    ,w.handle_status --售后处理状态
    ,case when w.handle_status = 0 then '等待处理'
        when w.handle_status = 1 then '等候寄回'
        when w.handle_status = 2 then '换货-等候寄出'
        when w.handle_status = 3 then '补发-等候寄出'
        when w.handle_status = 4 then '退款-等候审核'
        when w.handle_status = 5 then '退款-等候付款'
        when w.handle_status = 6 then '退款-退款驳回'
        when w.handle_status = 7 then '已完成'
        when w.handle_status = 8 then '已结束'
        when w.handle_status = 9 then '拒签-等候寄回'
        else w.handle_status end handle_status_zw 
    ,w1.parent_problem_name  工单一级问题类型名称
    ,w1.problem_name  工单二级问题类型名称
    ,case when w1.is_turn_refund = 1 then '是' else '否' end is_turn_refund --是否换转退工单 
    ,w1.back_shipping_time  ---用户寄回商品时间
    ,case when w1.service_order_type_v1 in (0,1) then '普通工单'
        when w1.service_order_type_v1 = 2 then '退货月卡'
        when w1.service_order_type_v1 = 3 then '100%无理由'
        when w1.service_order_type_v1 = 4 then '大牌无忧'
        else w1.service_order_type_v1 end service_order_type_zw --工单类型
    -----------------------------------------------售后入库模块
    ,rk.dbid 售后入库打标id
    ,rk.create_time 售后入库时间 
    ,case when rk.stock_source = 4 then '售后拆包'  
        when rk.stock_source = 16 then '售后拆包(退供)'
        else rk.stock_source  end 售后入库类型
    ,case when rk.blpid <> 0 and rk.blp_type_name <> '售后退供' then '是' 
        when rk.dbid is not null then '否' end 是否不良品
    ,rk.blpid 售后入库不良品id
    ,rk.blp_type_name 售后入库不良品名称
    ,case when rk.dbid is null then ''
        when rk.blpid = 0 or rk.blp_type_name='售后退供' then '正常品'
        when rk.blpid <> 0 and rk.blp_type_name in ('次品','次品（可选)') then '次品类型'
        else '错漏发类型' end 不良品类型判断
    ---------------------------------------------签收申请计算模块
    ,case when t4.sign_time is null and w.add_time is not null then 7 
        when t4.sign_time is null then null 
        else datediff1(w.add_time,from_unixtime(t4.sign_time),'dd') end 签收申请天数
    
    ,case when (w1.parent_problem_name = '实物不符' or w1.parent_problem_name like '质量问题%') then '是' when w.add_time is not null then '否' end 是否申请次品工单
    ,case when t4.sign_time is null and w.add_time is not null then '是'
        when datediff1(w.add_time,from_unixtime(t4.sign_time),'dd') <= 7 then '是'
        else '否' end 是否签收后7天内申请
    ,case when t4.sign_time is null and w.add_time is not null then '是'
        when datediff1(w.add_time,from_unixtime(t4.sign_time),'dd') <= 15 then '是'
        else '否' end 是否签收后15天内申请
    
    ,case when t4.sign_time is null and w.add_time is not null and (w1.parent_problem_name = '实物不符' or w1.parent_problem_name like '质量问题%') then '是'
        when datediff1(w.add_time,from_unixtime(t4.sign_time),'dd') <= 7 and (w1.parent_problem_name = '实物不符' or w1.parent_problem_name like '质量问题%') then '是'
        else '否' end 是否签收后7天内申请次品
    ,case when t4.sign_time is null and w.add_time is not null and (w1.parent_problem_name = '实物不符' or w1.parent_problem_name like '质量问题%') then '是'
        when datediff1(w.add_time,from_unixtime(t4.sign_time),'dd') <= 15 and (w1.parent_problem_name = '实物不符' or w1.parent_problem_name like '质量问题%') then '是'
        else '否' end 是否签收后15天内申请次品
    ,dr.create_admin         as     最大的层级对应的打标人id
    ,dr.admin_name        as  最大的层级对应的打标人名称
    ,ad.admin_name as 修复人员
    ,rd.created_at as 修复时间
    ,cd.fix_type as 修复类型
    ,concat(rk.dbid, ':', fcd.blp_note) as 拆包备注
    ,to_char(ck.sor_create_time,'yyyymmdd') dt
FROM (
    select 
        *
        ,case when rec_id =0 then -rand() else rec_id end rec_rand 
        ,case when shipping_num is null then -rand() else shipping_num end shipping_num_rand
        ,case when gb_user_id is null then -rand() else gb_user_id end gb_user_id_rand
    from yishou_data.dwd_storage_stock_out_record
) ck   
-- left join (
--     SELECT
--     first_dbid,max(dbid) max_dbid
--     from yishou_data.all_fmys_dbid 
--     group by 1 
-- )a on ck.dbid = a.max_dbid 
-- left join yishou_data.dwd_storage_stock_in_record b 
-- on a.first_dbid = b.first_dbid ---这样会出现一对多的情况，一定要保证first_dbid唯一
left join yishou_data.dwm_first_db_id_instock_tb_mt b on ck.dbid = b.so_dbid 
left join (
    select * 
    from yishou_data.dwd_sale_order_info_dt 
    where dt >= to_char(dateadd(TO_DATE1(${bdp.system.bizdate},'yyyymmdd'),-12,'mm'),'yyyymmdd') and dt<='${bdp.system.bizdate}'
        and pay_status = 1 
) oi on ck.rec_rand = oi.rec_id 

-- 获取发货对应的用户签收时间
-- left join yishou_data.all_fmys_wave_picking_dbid t1 on ck.dbid = t1.dbid and t1.dbid > 0 
-- left join yishou_data.all_fmys_wave_picking_info t2 on t1.wave_picking_info_id = t2.wave_picking_info_id 
-- left join yishou_data.all_fmys_wave_picking t3 on t2.wave_picking_id = t3.wave_picking_id 
left join(
    SELECT 
    shipping_num
    ,min(sign_time) sign_time
    from yishou_data.all_fmys_shipping_detail 
    where sign_time >= unix_timestamp(dateadd(TO_DATE1(${bdp.system.bizdate},'yyyymmdd'),-13,'mm'))
    group by 1
)t4 on ck.shipping_num_rand = t4.shipping_num 
--------质检数据处理
left join(
    SELECT
    qc_dbid as dbid 
    ,min(qc_time) min_qc_time
    ,max(qc_time) max_qc_time
    ,sum(distinct is_cp) is_cp 
    ,concat_ws('_',collect_set(quality_rule_zw)) quality_rule_zw
    ,concat_ws('_',collect_set(secondary_quality_rule_zw)) secondary_quality_rule_zw 
    ,concat_ws('_',collect_set(quality_type_id)) quality_type_id  --质检类型id
    ,concat_ws('_',collect_set(quality_type_name)) quality_type_name  --质检类型名称
    ,concat_ws('_',collect_set(quality_type_desc)) quality_type_desc  --质检类型说明
    ,count(distinct is_cp) 是否次品
    ,concat_ws('_',collect_set(cp_one_defective)) cp_one_defective --次品一级分类
    ,concat_ws('_',collect_set(cp_two_defective)) cp_two_defective --次品二级分类
    from(
        SELECT
        *
        ,case when quality_rule = 1 then '默认质检规则配置'
            when quality_rule = 2 then '质检绿色通道配置'
            when quality_rule = 3 then '高级质检商品配置'
            when quality_rule = 4 then '二次重配商品配置'
            when quality_rule = 5 then '高价值商品配置'
            when quality_rule = 6 then '高排单/区域订单商品配置'  
            when quality_rule = 7 then '新客订单质检配置'
            when quality_rule = 8 then '高净值用户质检配置'
            else quality_rule end quality_rule_zw
        ,case when secondary_quality_rule = 1 then '首单金额150以上的订单'
            when secondary_quality_rule = 2 then '累计下单15件一下且次品率大于大于等于10%'
            when secondary_quality_rule = 3 then '累计下单15件以上且次品率大于等于12%'
            when secondary_quality_rule = 4 then '地推前5单'
            when secondary_quality_rule = 5 then '前3天有两个质量问题类型工单'
            when secondary_quality_rule = 6 then '绿色通道'
            when secondary_quality_rule = 7 then '非地推首5单'
            else secondary_quality_rule end secondary_quality_rule_zw
        from yishou_daily.supply_chain_fmys_quality_checking_dt 
        where dt >= to_char(dateadd(TO_DATE1(${bdp.system.bizdate},'yyyymmdd'),-12,'mm'),'yyyymmdd')
    )t group by 1 
)zj on ck.dbid = zj.dbid
-- left join( --质检人--是否质检判断
--     select /*+mapjoin(t2)*/
--         dbid
--         ,concat_ws('_', collect_set(t2.admin_name)) pc_check_admin_name
--         ,concat_ws('_', collect_set(from_unixtime(t1.create_time))) pc_check_time  
--     from yishou_data.all_fmys_quality_checking t1  --质检列表
--         left join yishou_data.all_fmys_admin t2 on t1.pc_check_admin = t2.admin_id
--     group by dbid
-- )qc on ck.dbid=qc.dbid
left join( --质检人--是否质检判断
    SELECT 
        dbid
        ,max(case when min_num = 1 then admin_name end) min_admin_name
        ,max(case when max_num = 1 then admin_name end) max_admin_name
    from(
        select
            t1.dbid
            ,t1.create_time
            ,t2.admin_name
            ,row_number() over(PARTITION BY t1.dbid order by t1.create_time desc) max_num
            ,row_number() over(PARTITION BY t1.dbid order by t1.create_time asc) min_num
            ,t2.admin_name
        from yishou_data.all_fmys_quality_checking t1  --质检列表
            join yishou_data.all_fmys_admin t2 on t1.pc_check_admin = t2.admin_id
        where t1.pc_check_admin > 0
    )t
    group by dbid
)qc2 on ck.dbid=qc2.dbid
-- 售后寄出对应的改标波次质检判断如下
-- left join (
--     select shb.dbid 
--         ,max(wpe1.quality_level) quality_level
--     from (select * from yishou_data.ods_fmys_as_wave_picking_dbid_dt where dt = ${one_day_ago}) shb  --通过出库打标id获取波次id
--         join yishou_data.all_fmys_wave_picking_ext wpe1 on shb.as_wave_picking_id = wpe1.wave_picking_id
--     where shb.dbid > 0
--     group by dbid
-- )wpe1 on ck.dbid=wpe1.dbid

--初高精心质检类型
-- left join yishou_data.all_fmys_wave_picking_ext wpe on t1.wave_picking_id = wpe.wave_picking_id
---获取改标，改标dbid表，关联出库记录表
-- left join yishou_data.all_fmys_gb_input_record gb on ck.dbid = gb.dbid 
left join (select * from yishou_data.obs_fmys_gb_user_list_dt where dt = ${bdp.system.bizdate}) gb1 on ck.gb_user_id_rand = gb1.id
----工单模块
left join (select * from yishou_data.dws_scm_service_order_stock_out_detail_dt where sto_dbid > 0 and dt = ${bdp.system.bizdate} ) w on ck.dbid = w.sto_dbid
left join yishou_data.dwd_as_service_order_detail w1 on w.id = w1.id  -- 有数据倾斜，当w.id和ck标关联不出来数据时
----售后入库模块
----售后入库模块
--left join (
--    select rk.dbid
--        ,wd.old_dbid
--        ,rk.stock_source
--        ,rk.create_time
--        ,rk.blpid
--        ,rk.blp_type_name
--    from yishou_data.all_fmys_work_dbid wd
--        left join yishou_data.dwd_storage_stock_in_record rk on wd.dbid = rk.dbid 
--    where wd.old_dbid>0 ) rk on ck.dbid = rk.old_dbid 
--20240703改口径
left join yishou_data.dwd_storage_stock_in_record rk on w.sale_af_in_stock_dbid = rk.dbid 
-- left join yishou_data.dwd_storage_stock_in_record rk on wrk.dbid = rk.dbid 
left join yishou_data.dim_goods_no_info gn on ck.goods_no = gn.goods_no 
left join yishou_data.dim_market_info mi on gn.market_id=mi.market_id
left join yishou_data.dim_cat_info ci on gn.cat_id=ci.cat_id
left join yishou_data.dim_supply_info si on gn.supply_id=si.supply_id
left join (SELECT   dbid,
                    min(first_dbid) as first_dbid
                from yishou_data.ods_fmys_dbid_view
                group by 1) a on ck.dbid = a.dbid
LEFT JOIN (
    SELECT 
        *
    from (select *, row_number() OVER ( PARTITION by first_dbid ORDER BY db_layer desc ) as rn
          from yishou_data.dwd_scm_dbid_record
          where dt = '${bdp.system.bizdate}' DISTRIBUTE BY first_dbid)
    WHERE rn = 1
) dr
on a.first_dbid = dr.first_dbid
-- 关联修复表
left join (
    select 
        dbid
        ,id
        ,repair_admin
        ,created_at 
    from yishou_data.ods_fmys_goods_repair_record_dt 
    where dt = '${bdp.system.bizdate}'
) rd
on ck.dbid =rd.dbid
-- 关联管理人员
left join yishou_data.all_fmys_admin ad
on rd.repair_admin = ad.admin_id
-- 修复类型
left join (
    select 
        grr_id
        , concat_ws(',',collect_list(distinct name)) as fix_type 
    from yishou_data.ods_fmys_goods_repair_class_dt 
    where dt = '${bdp.system.bizdate}'
    group by grr_id
) cd
on rd.id = cd.grr_id   
-- 拆包备注
left join yishou_data.all_fmys_blp_related fcd on rk.blpid = fcd.blpid
where ck.sor_status=9
    and ck.sor_type in (1,10) 
    and to_char(ck.sor_create_time,'yyyymmdd') >= to_char(dateadd(TO_DATE1(${bdp.system.bizdate},'yyyymmdd'),-6,'mm'),'yyyymmdd')
    -- and to_char(ck.sor_create_time,'yyyymmdd') >= 20210101
    and to_char(ck.sor_create_time,'yyyymmdd') <= ${bdp.system.bizdate}
-- DISTRIBUTE BY dt 
;


