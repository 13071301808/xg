-- DLI sql 
-- ******************************************************************** --
-- author: chenyiming
-- create time: 2024/03/28 14:49:04 GMT+08:00
-- ******************************************************************** --
-- create table yishou_data.evil_device_association
-- (
--     id                  int unsigned auto_increment comment '自增id' primary key,
--     device              varchar(255) not null default '' comment '设备号',
--     user_ids            text not null comment '用户id（英文逗号分隔）',
--     total               int not null default 0 comment '账号总数量',
--     is_all_audit        tinyint not null default 0 comment '是否全部审核{1：是、0：否}',
--     is_delete           tinyint not null default 0 comment '是否删除{1：是、0：否}',
--     is_auto           tinyint not null default 0 comment '是否自动限购{1：是、0：否}',
--     created_at          timestamp not null default CURRENT_TIMESTAMP comment '新增时间',
--     updated_at          timestamp not null default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP comment '修改时间',
--     unique key idx_device (device)
-- ) ENGINE = InnoDB comment '恶意设备关联表';


-- 计算逻辑说明：【3.22】

-- 第一步：取近一年有使用公司IP登录的设备id
-- 第二步：取近一年有登录过的用户id以及最后一次登录对应的设备id
-- 第三步：剔除掉第一步的设备id对应的记录
-- 第四步：按照设备id将用户id聚合在一起
--全量脚本
-- create table dtl_evil_device_association_liby_whitelist
-- as
-- select device_id
--     ,max(time) max_time
-- from yishou_data.dwd_log_app_wx_login_dt
-- where dt >= 20230301
-- and ip in ('219.135.147.242','183.3.129.117','183.6.45.4','210.21.23.108')
-- and device_id <> ''
-- group by 1;

--为了剔除立白中心的IP的设备，为了剔除公司内部的设备
INSERT OVERWRITE TABLE dtl_evil_device_association_liby_whitelist
select device_id
    ,max(max_time) max_time
from(
select device_id
    ,max_time
from dtl_evil_device_association_liby_whitelist
where to_char(max_time,'yyyymmdd') >= to_char(dateadd(to_date1('${bizdate}','yyyymmdd'),-365,'dd'),'yyyymmdd')
union all
select device_id
    ,max(time) max_time
from yishou_data.dwd_log_app_wx_login_dt
where dt between '${bizdate}' and '${gmtdate}'
and ip in ('219.135.147.242','183.3.129.117','183.6.45.4','210.21.23.108')
group by 1
)group by 1;

--全量脚本
-- drop table if exists  dtl_evil_device_association_user_last_device;
-- create table if not exists dtl_evil_device_association_user_last_device
-- as
-- select
--         t1.user_id
--         ,t1.device_id
--         ,t1.time
--         ,row_number()over(partition by t1.user_id order by t1.time desc) time_rank
-- from yishou_data.dwd_log_app_wx_login_dt t1
-- left join dtl_evil_device_association_liby_whitelist t2
--     on t1.device_id = t2.device_id 
-- where dt between  to_char(dateadd(to_date1('${bizdate}','yyyymmdd'),-12,'mm'),'yyyymmdd') and '${bizdate}'
-- and to_char(time,'yyyymmdd') <= '${gmtdate}'
-- and user_id > 0
-- and t2.device_id is null
-- and t1.device_id <> ''
-- and t1.device_id  not in 
-- (
-- '876E86B4-8A74-449C-A929-4CD11E0116F9', --跳跳糖iOS
-- 'a9ba5a92df5ec277', --跳跳糖安卓备用
-- 'C38A2FE5-DEFB-4019-800A-56E913A59CEF', --秀则iOS
-- '74909857-BBB8-417D-B251-3A996BB49B75', --秀则iOS备用
-- '4FF6E381-1BA5-4091-A247-5E019DCB69BD', --北海ios
-- '08291e67c2bd0906' --景天安卓
-- )
-- HAVING time_rank = 1
-- ;


INSERT OVERWRITE TABLE dtl_evil_device_association_user_last_device
select user_id
    ,device_id
    ,time
    ,row_number()over(partition by user_id order by time desc) time_rank
from(
select t1.user_id
    ,t1.device_id
    ,t1.time
from dtl_evil_device_association_user_last_device t1
left join dtl_evil_device_association_liby_whitelist t2
    on t1.device_id = t2.device_id 
where t2.device_id is null
and  to_char(time,'yyyymmdd') between  to_char(dateadd(to_date1('${bizdate}','yyyymmdd'),-12,'mm'),'yyyymmdd') and '${bizdate}'
union all
select
        t1.user_id
        ,t1.device_id
        ,t1.time
from yishou_data.dwd_log_app_wx_login_dt t1
left join dtl_evil_device_association_liby_whitelist t2
    on t1.device_id = t2.device_id 
where dt between  '${bizdate}' and '${gmtdate}'
and to_char(time,'yyyymmdd') <= '${gmtdate}'
and user_id > 0
and t2.device_id is null
and t1.device_id <> ''
and t1.device_id  not in 
(
'876E86B4-8A74-449C-A929-4CD11E0116F9', --跳跳糖iOS
'a9ba5a92df5ec277', --跳跳糖安卓备用
'C38A2FE5-DEFB-4019-800A-56E913A59CEF', --秀则iOS
'74909857-BBB8-417D-B251-3A996BB49B75', --秀则iOS备用
'4FF6E381-1BA5-4091-A247-5E019DCB69BD', --北海ios
'08291e67c2bd0906' --景天安卓
)
)
HAVING time_rank = 1;

-- 开启自动限购的临时表
DROP TABLE IF EXISTS temp.evil_device_association;
CREATE TABLE temp.evil_device_association AS
SELECT
	*,
	row_number() over (PARTITION BY device order by dt DESC) rn
FROM yishou_data.ods_evil_device_association_dt
WHERE dt >= to_char(dateadd(to_date1('${bizdate}','yyyymmdd'),-1,'mm'),'yyyymmdd') AND is_auto = 1
HAVING rn = 1
;

INSERT OVERWRITE TABLE yishou_daily.dtl_evil_device_association_dt PARTITION(dt)
SELECT    
    coalesce(a.device, b.device) as device,
    coalesce(a.user_ids, b.user_ids) as user_ids,
    coalesce(a.total, b.total) as total,
    a.total_sign_7_apply_cp_rate,
    a.total_stock_cacel_rate,
    a.total_order_cacel_rate,
    a.total_all_deliver_gmv,
    a.total_all_return_rate,
    a.total_all_sign_num,
    a.total_all_defective_rate,
    a.total_recent_year_sign_num,
    a.total_recent_year_defective_rate,
    a.evil_rate,
    coalesce(a.dt, '${bizdate}') as dt
FROM (
    select 
        t1.device_id device
        ,concat(',',concat_ws(',', sort_array(collect_list(t1.user_id))),',') user_ids
        ,count(t1.user_id) total
        ,ifnull(round(sum(fs7ad.sign_7_apply_cp_num) / sum(fs7ad.sign_num),2),0) as total_sign_7_apply_cp_rate -- 总计-签收7天次品率
        ,ifnull(round(sum(cr.xh_cancel_num) / sum(cr.xh_buy_num),2),0) as total_stock_cacel_rate -- 总计-现货取消率
        ,ifnull(round(sum(cr.cancel_num) / sum(cr.buy_num),2),0) as total_order_cacel_rate  -- 总计-订单取消率
        ,sum(c.all_deliver_gmv) as total_all_deliver_gmv  -- 总计-累计发货GMV
        ,ifnull(round(sum(c.refund_monny) / sum(c.all_deliver_gmv),2),0) as total_all_return_rate  -- 总计-累计退货率
        ,sum(fs7ad_5_year.all_sign_num) as total_all_sign_num  -- 总计-近5年签收件数
        ,ifnull(round(sum(fs7ad_5_year.sign_7_apply_cp_num)/sum(fs7ad_5_year.all_sign_num),2),0) as total_all_defective_rate -- 总计-近5年次品率
        ,sum(fs7ad_1_year.recent_year_sign_num) as total_recent_year_sign_num -- 总计-近1年签收件数
        ,ifnull(round(sum(fs7ad_1_year.sign_7_apply_cp_num)/sum(fs7ad_1_year.recent_year_sign_num),2),0) as total_recent_year_defective_rate -- 总计-近1年次品率
        ,coalesce(count(distinct case when tt1.user_id is not null or tt2.user_id is not null or tt3.user_id is not null or tt4.user_id is not null then t1.user_id else null end),0)/coalesce(count(t1.user_id),0) as evil_rate
        ,'${bizdate}' dt
    from dtl_evil_device_association_user_last_device t1
    join (
        select 
            t2.device_id
            ,count(t2.user_id) user_num
        from dtl_evil_device_association_user_last_device t2
        join yishou_data.all_fmys_cart_blacklist t3 on t2.user_id = t3.user_id and t3.type = 1 
        group by 1
        HAVING user_num >= 2 
    )t4 on t1.device_id = t4.device_id
    left join (
        SELECT 
            fo.user_id
            ,sum(foi.all_deliver_gmv) as all_deliver_gmv -- 累计发货GMV
            ,sum(re_mon.refund_monny) as refund_monny
        from yishou_data.all_fmys_order fo 
        left join (
            select 
                order_id
                , sum(if(allot_status>0,buy_num * shop_price - refund_num * shop_price,0)) as all_deliver_gmv
            from yishou_data.all_fmys_order_infos 
            group by order_id
        ) foi 
        on fo.order_id = foi.order_id
        left join (
            SELECT 
                order_id
                ,sum(refund_monny) as refund_monny
            FROM yishou_data.all_fmys_service_order 
            WHERE handle_status = 7 AND handle_type = 5
            group by order_id
        ) re_mon
        on fo.order_id = re_mon.order_id
        WHERE user_id NOT IN (2,17,10,387836)
        AND pay_status=1
        and TO_CHAR(DATEADD(from_unixtime(add_time),-7,'hh'),'yyyymmdd') <= '${bizdate}'
        group by user_id
    )c
    on t1.user_id = c.user_id
    left join (
        select user_id
            ,sum(sign_7_apply_cp_num) sign_7_apply_cp_num
            ,sum(sign_num) sign_num
        from yishou_daily.finebi_sign_7days_application_defective
        where dt between to_char(dateadd(to_date1('${bizdate}','yyyymmdd'),-13,'dd'),'yyyymmdd') and to_char(dateadd(to_date1('${bizdate}','yyyymmdd'),-7,'dd'),'yyyymmdd')
        group by 1
        having sign_7_apply_cp_num > 0 and sign_num > 0
    )fs7ad
    on fs7ad.user_id = t1.user_id
    -- 取5年
    left join (
        select 
            user_id
            ,sum(sign_7_apply_cp_num) sign_7_apply_cp_num
            ,sum(sign_num) all_sign_num
        from yishou_daily.finebi_sign_7days_application_defective
        where dt BETWEEN to_char(dateadd(dateadd(to_date1('${bizdate}','yyyymmdd'),-7,'dd'),-5,'yyyy'),'yyyymmdd') AND to_char(dateadd(to_date1('${bizdate}','yyyymmdd'),-7,'dd'),'yyyymmdd')
        group by user_id
        having sign_7_apply_cp_num > 0 and all_sign_num > 0
    ) fs7ad_5_year
    on fs7ad_5_year.user_id = t1.user_id
    -- 取1年
    left join (
        select 
            user_id
            ,sum(sign_7_apply_cp_num) sign_7_apply_cp_num
            ,sum(sign_num) recent_year_sign_num
        from yishou_daily.finebi_sign_7days_application_defective
        where dt BETWEEN to_char(dateadd(dateadd(to_date1('${bizdate}','yyyymmdd'),-7,'dd'),-1,'yyyy'),'yyyymmdd') AND to_char(dateadd(to_date1('${bizdate}','yyyymmdd'),-7,'dd'),'yyyymmdd')
        group by user_id
        having sign_7_apply_cp_num > 0 and recent_year_sign_num > 0
    ) fs7ad_1_year
    on fs7ad_1_year.user_id = t1.user_id
    LEFT JOIN (
        select 
            t1.user_id
            ,sum(if(t1.is_real_pay = 0 ,t1.buy_num,0)) cancel_num
            ,sum(t1.buy_num) buy_num --order_cacel_rate --近一个月件数取消率=>订单取消率
            ,sum(if(is_real_pay = 0 AND xh.goods_id is not null ,t1.buy_num ,0)) xh_cancel_num
            ,sum(if(xh.goods_id is not null ,t1.buy_num ,0)) xh_buy_num    --stock_cacel_rate --近一个月现货形式上架取消率=>现货取消率
        FROM yishou_data.dwd_sale_order_info_dt t1
        left join yishou_data.dim_goods_id_info xh
            on t1.goods_id = xh.goods_id and xh.is_in_stock = 1
        WHERE t1.dt >= to_char(dateadd(to_date1('${bizdate}','yyyymmdd'),-30,'dd'),'yyyymmdd') 
            and to_char(t1.special_start_time,'yyyymmdd') >= to_char(dateadd(to_date1('${bizdate}','yyyymmdd'),-30,'dd'),'yyyymmdd')
            and buy_num > 0
        GROUP BY 1
    ) cr
    on t1.user_id = cr.user_id
    -- 恶意用户标签的
    left join (select user_id from yishou_daily.dtl_user_evil_label_dt where dt='${bizdate}' and evil_users_label != 0) tt1 on tt1.user_id=t1.user_id -- 恶意次品
    left join (select user_id from yishou_daily.ads_higt_defective_user where dt='${bizdate}' and higt_defective_user not in (0,6)) tt2 on t1.user_id=tt2.user_id -- 高次
    left join (select user_id from yishou_daily.dtl_user_suspected_malice_defective where dt='${bizdate}' and suspected_malice_defective != 0) tt3 
    on t1.user_id=tt3.user_id -- 人为次品
    left join (select user_id from yishou_daily.dtl_user_malice_mistake_send where dt='${bizdate}' and malice_mistake_send != 0) tt4 on t1.user_id=tt4.user_id -- 恶意错发
    left join (select device from yishou_data.ods_evil_device_association_dt where dt='${bizdate}' and is_auto=1) x on t1.device_id=x.device
    group by 1
    having total >= 3 and evil_rate >= 0.20
) a
-- 开启自动限购
FULL JOIN temp.evil_device_association b ON a.device = b.device
DISTRIBUTE BY dt
;


drop table if EXISTS dtl_evil_device_association_dt_update;
create table if not EXISTS  dtl_evil_device_association_dt_update
as
SELECT
	t1.device
    , t1.user_ids
    , t1.total
    , coalesce(t2.is_all_audit, 0) is_all_audit
    , coalesce(t2.is_auto, 0) is_auto
    , coalesce(t1.total_sign_7_apply_cp_rate, 0) as total_sign_7_apply_cp_rate -- 总计-签收7天次品率
    , coalesce(t1.total_stock_cacel_rate, 0) as total_stock_cacel_rate -- 总计-现货取消率
    , coalesce(t1.total_order_cacel_rate, 0) as total_order_cacel_rate  -- 总计-订单取消率
    , coalesce(t1.total_all_deliver_gmv, 0) as total_all_deliver_gmv  -- 总计-累计发货GMV
    , coalesce(t1.total_all_return_rate, 0) as total_all_return_rate -- 总计-累计退货率
    , coalesce(t1.total_all_sign_num,0) as total_all_sign_num  -- 总计-近5年签收件数
    , coalesce(t1.total_all_defective_rate,0) as total_all_defective_rate -- 总计-近5年次品率
    , coalesce(t1.total_recent_year_sign_num,0) as total_recent_year_sign_num -- 总计-近1年签收件数
    , coalesce(t1.total_recent_year_defective_rate,0) as total_recent_year_defective_rate -- 总计-近1年次品率
FROM yishou_daily.dtl_evil_device_association_dt t1
LEFT JOIN yishou_data.ods_evil_device_association_dt t2
on t1.device = t2.device and t2.dt = '${bizdate}'
WHERE t1.dt = '${bizdate}'
;
