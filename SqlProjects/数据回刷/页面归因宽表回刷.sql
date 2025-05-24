-- DLI sql
-- ******************************************************************** --
-- author: chenzhigao
-- create time: 2025/05/22 09:46:16 GMT+08:00
-- ******************************************************************** --
-- drop table yishou_data.dwm_page_name_user_goods_incr_dt_0327;
-- create table yishou_data.dwm_page_name_user_goods_incr_dt_0327
-- as
with app_exposure_page_temp1 as (
 SELECT
        dlg.user_id
        , dlg.goods_id
        , dlg.goods_no
        , dlg.pid AS source_code
        , '1' AS mapping_type_code
        , 'pid' AS mapping_field_name
        , to_char (dlg.special_date, 'yyyymmdd') AS special_date
        , COUNT(1) exposure_pv
    FROM yishou_data.dwd_log_goods_exposure_dt dlg
    WHERE dlg.dt BETWEEN '${start}' AND to_char(dateadd(to_date1('${end}','yyyymmdd'),1,'dd'),'yyyymmdd')
    AND  to_char(dlg.special_date,'yyyymmdd') between '${start}' and '${end}'
    GROUP BY dlg.user_id,dlg.goods_id,dlg.goods_no,dlg.pid,to_char(dlg.special_date,'yyyymmdd')
)
,h5_exposure_page_temp1 as (
    select
          dlh.user_id
        , dlh.goods_id
        , dgi.goods_no
        , title AS source_code
        , '2' AS mapping_type_code
        , 'title'AS mapping_field_name
        , to_char(dlh.special_date,'yyyymmdd') special_date
        , count(1) h5_exposure_pv
    from yishou_data.dwd_log_h5_goods_list_exposure_dt dlh
    left join yishou_data.all_fmys_goods dgi on dlh.goods_id=dgi.goods_id
    where dlh.dt BETWEEN '${start}' AND to_char(dateadd(to_date1('${end}','yyyymmdd'),1,'dd'),'yyyymmdd')
    AND  to_char(dlh.special_date,'yyyymmdd') BETWEEN '${start}' AND '${end}'
    group by dlh.user_id,dlh.goods_id,dgi.goods_no,dlh.title,to_char(dlh.special_date,'yyyymmdd')
)
,click_page_temp1 as (
    SELECT
        dlg.user_id
        , dlg.goods_id
        , dgi.goods_no
        , dlg.source AS source_code
        , '3' AS mapping_type_code
        , 'source' AS mapping_field_name
        , SUM(dps.stay_time) AS stay_time
        , sum(dlg.click_pv) AS click_pv
        , coalesce(dlg.special_date,dps.dt) AS special_date
    FROM (
        SELECT
            dlgd.user_id
            , dlgd.goods_id
            , dlgd.event_id
            , dlgd.source
            , count(1) AS click_pv
            , to_char(dlgd.special_date, 'yyyymmdd') AS special_date
        FROM yishou_data.dwd_log_goods_detail_page_dt dlgd
        WHERE dlgd.dt BETWEEN '${start}' AND to_char(dateadd(to_date1('${end}','yyyymmdd'),1,'dd'),'yyyymmdd')
        AND to_char(dlgd.special_date, 'yyyymmdd') BETWEEN '${start}' AND '${end}'
        GROUP BY  dlgd.user_id,dlgd.goods_id,dlgd.event_id,dlgd.source,to_char(dlgd.special_date,'yyyymmdd')
    ) dlg
    FULL JOIN (
        SELECT
            user_id,
            goods_id,
            event_id,
            SUM(stay_time) AS stay_time,
            dt
        FROM  yishou_data.dwd_page_stay_time_exposure_sp_dt
        WHERE dt BETWEEN '${start}' AND '${end}' AND stay_time !=0
        GROUP BY user_id,goods_id,event_id,dt
    ) dps ON dlg.user_id = dps.user_id AND  dlg.event_id = dps.event_id AND  dlg.special_date = dps.dt
    left join yishou_data.all_fmys_goods dgi ON coalesce(dlg.goods_id,dps.goods_id) = dgi.goods_id
    GROUP BY  dlg.user_id,dlg.goods_id,dgi.goods_no,dlg.source,coalesce(dlg.special_date,dps.dt)
)
,addcart_page_temp1 as (
    select
        dla.user_id
        ,dla.goods_id
        ,dgi.goods_no
        ,dla.source  AS source_code
        , '4' AS mapping_type_code
        , 'source' AS mapping_field_name
        ,to_char(dla.special_date, 'yyyymmdd') special_date
        ,count(1) addcart_pv
        ,sum(dla.goods_number) addcart_num
        ,sum(add_goods_amount) addcart_amount
    from yishou_data.dwd_log_add_cart_dt dla
    left join yishou_data.all_fmys_goods dgi ON dla.goods_id = dgi.goods_id
    where dla.dt BETWEEN '${start}' AND to_char(dateadd(to_date1('${end}','yyyymmdd'),1,'dd'),'yyyymmdd')
    AND to_char(dla.special_date, 'yyyymmdd') BETWEEN '${start}' AND '${end}'
    AND coalesce(dla.user_id,0) != 0
    group by dla.user_id,dla.goods_id,dgi.goods_no,dla.source,to_char(dla.special_date,'yyyymmdd')
)
, pay_page_temp1 as (
    select
        dso.user_id
        , dso.goods_id
        , dso.goods_no
        ,case
            when sa_os <> 'wx' then dso.sa_source
            when sa_os = 'wx' then 0
            else dso.sa_source
        end as source_code
        , '5' AS mapping_type_code
        , 'sa_source' AS mapping_field_name
        , to_char(from_unixtime(add_time - 7 * 3600),'yyyymmdd') special_date
        , count(1) pay_pv
        , sum(case when dso.is_real_pay = 1 then 1 ELSE 0 end) real_pay_pv
        , sum(dso.buy_num) pay_num
        , sum(case when dso.is_real_pay = 1 then dso.buy_num end) real_pay_num
        , sum(dso.shop_price * dso.buy_num) gmv
        , sum(case when dso.is_real_pay = 1 then dso.shop_price * dso.buy_num end) real_gmv
    from (
        select
            foi.shop_price
            ,fo.user_id
            ,foi.goods_id
            ,foi.goods_no
            ,get_json_object(foi.sa, '$.source')  sa_source
            ,get_json_object(foi.sa, '$.os')  sa_os
            ,foi.buy_num
            ,from_unixtime(fs.special_start_time)  special_start_time
            ,case when fo.pay_status = 1 and cr.order_id is null then 1 else 0 end is_real_pay
            ,fo.pay_status
            ,to_char(from_unixtime(fo.add_time), 'yyyymmdd') as dt
            ,add_time
        from yishou_data.all_fmys_order fo
        join yishou_data.all_fmys_order_infos foi on fo.order_id = foi.order_id
        left join yishou_data.all_fmys_order_cancel_record cr on fo.order_id = cr.order_id
        left join yishou_data.all_fmys_special fs on foi.special_id = fs.special_id
        where to_char(from_unixtime(add_time - 7 * 3600),'yyyymmdd')  BETWEEN '${start}' AND '${end}'
    ) dso
    where dso.dt BETWEEN '${start}' AND to_char(dateadd(to_date1('${end}','yyyymmdd'),1,'dd'),'yyyymmdd')
    and pay_status = 1
    group by
        dso.user_id
        , dso.goods_id
        , dso.goods_no
        , case
            when sa_os <> 'wx' then dso.sa_source
            when sa_os = 'wx' then 0
            else dso.sa_source
        end
        , to_char(from_unixtime(add_time - 7 * 3600),'yyyymmdd')
)
insert overwrite table yishou_data.dwm_page_name_user_goods_incr_dt PARTITION(dt)
SELECT
    /*+ BROADCAST(h5_dcp) */
    main_table.user_id
    , main_table.goods_id
    , main_table.goods_no
    , coalesce(h5_dcp.activity, main_table.source_code) AS source_code
    , main_table.mapping_type_code
    , main_table.mapping_field_name
    , coalesce(h5_dcp.activity, dcp.channel,'其他') AS channel
    , main_table.app_exposure_pv
    , main_table.app_stay_time
    , main_table.h5_exposure_pv
    , main_table.click_pv
    , main_table.addcart_pv
    , main_table.addcart_num
    , main_table.addcart_amount
    , main_table.pay_pv
    , main_table.real_pay_pv
    , main_table.pay_num
    , main_table.real_pay_num
    , main_table.gmv
    , main_table.real_gmv
    , main_table.dt
FROM (
    SELECT
        coalesce(agg_table.user_id,0) AS user_id
        , coalesce(agg_table.goods_id,0) AS goods_id
        , coalesce(agg_table.goods_no,0) AS goods_no
        , agg_table.source_code AS source_code
        , agg_table.mapping_type_code AS mapping_type_code
        , agg_table.mapping_field_name AS mapping_field_name
        , sum(agg_table.exposure_pv) AS app_exposure_pv
        , sum(agg_table.stay_time) AS app_stay_time
        , sum(agg_table.h5_exposure_pv) AS h5_exposure_pv
        , sum(agg_table.click_pv) AS click_pv
        , sum(agg_table.addcart_pv) AS addcart_pv
        , sum(agg_table.addcart_num) AS addcart_num
        , sum(agg_table.addcart_amount) AS addcart_amount
        , sum(agg_table.pay_pv) AS pay_pv
        , sum(agg_table.real_pay_pv) AS real_pay_pv
        , sum(agg_table.pay_num) AS pay_num
        , sum(agg_table.real_pay_num) AS real_pay_num
        , sum(agg_table.gmv) AS gmv
        , sum(agg_table.real_gmv) AS real_gmv
        , agg_table.special_date AS dt
    FROM (
        --  app曝光
        SELECT
            dlg.user_id
            , dlg.goods_id
            , dlg.goods_no
            , dlg.source_code
            , dlg.mapping_type_code
            , dlg.mapping_field_name
            , dlg.special_date
            , dlg.exposure_pv
            , NULL AS stay_time
            , NULL  AS h5_exposure_pv
            , NULL  AS click_pv
            , NULL  AS addcart_pv
            , NULL  AS addcart_num
            , NULL  AS addcart_amount
            , NULL  AS pay_pv
            , NULL  AS real_pay_pv
            , NULL  AS pay_num
            , NULL  AS real_pay_num
            , NULL  AS gmv
            , NULL  AS real_gmv
        FROM app_exposure_page_temp1 dlg
        --H5曝光
        UNION ALL
        SELECT
            dlh.user_id
            , dlh.goods_id
            , dlh.goods_no
            , dlh.source_code
            , dlh.mapping_type_code
            , dlh.mapping_field_name
            , dlh.special_date
            , NULL  AS exposure_pv
            , NULL  AS stay_time
            , dlh.h5_exposure_pv
            , NULL  AS click_pv
            , NULL  AS addcart_pv
            , NULL  AS addcart_num
            , NULL  AS addcart_amount
            , NULL  AS pay_pv
            , NULL  AS real_pay_pv
            , NULL  AS pay_num
            , NULL  AS real_pay_num
            , NULL  AS gmv
            , NULL  AS real_gmv
        FROM h5_exposure_page_temp1 dlh
        --点击
        UNION ALL
        SELECT
            dlg.user_id
            , dlg.goods_id
            , dlg.goods_no
            , dlg.source_code
            , dlg.mapping_type_code
            , dlg.mapping_field_name
            , dlg.special_date
            , NULL  AS exposure_pv
            , dlg.stay_time
            , NULL  AS h5_exposure_pv
            , dlg.click_pv
            , NULL  AS addcart_pv
            , NULL  AS addcart_num
            , NULL  AS addcart_amount
            , NULL  AS pay_pv
            , NULL  AS real_pay_pv
            , NULL  AS pay_num
            , NULL  AS real_pay_num
            , NULL  AS gmv
            , NULL  AS real_gmv
        FROM click_page_temp1 dlg
        --加购
        UNION ALL
        SELECT
            dla.user_id
            , dla.goods_id
            , dla.goods_no
            , dla.source_code
            , dla.mapping_type_code
            , dla.mapping_field_name
            , dla.special_date
            , NULL  AS exposure_pv
            , NULL  AS stay_time
            , NULL  AS h5_exposure_pv
            , NULL  AS click_pv
            , dla.addcart_pv
            , dla.addcart_num
            , dla.addcart_amount
            , NULL  AS pay_pv
            , NULL  AS real_pay_pv
            , NULL  AS pay_num
            , NULL  AS real_pay_num
            , NULL  AS gmv
            , NULL  AS real_gmv
        FROM addcart_page_temp1 dla
        --支付
        UNION ALL
        SELECT
            dso.user_id
            , dso.goods_id
            , dso.goods_no
            , dso.source_code
            , dso.mapping_type_code
            , dso.mapping_field_name
            , dso.special_date
            , NULL  AS exposure_pv
            , NULL  AS stay_time
            , NULL  AS h5_exposure_pv
            , NULL  AS click_pv
            , NULL  AS addcart_pv
            , NULL  AS addcart_num
            , NULL  AS addcart_amount
            , dso.pay_pv
            , dso.real_pay_pv
            , dso.pay_num
            , dso.real_pay_num
            , dso.gmv
            , dso.real_gmv
        FROM pay_page_temp1 dso
    )agg_table
    GROUP BY   agg_table.user_id
                , agg_table.goods_id
                , agg_table.goods_no
                , agg_table.source_code
                , agg_table.mapping_type_code
                , agg_table.mapping_field_name
                , agg_table.special_date
) main_table
LEFT JOIN yishou_data.dim_channel_pidsource dcp
ON dcp.mapping_type_code = main_table.mapping_type_code AND main_table.source_code = dcp.source_code
LEFT JOIN yishou_data.dim_h5_channel_pidsource h5_dcp
ON main_table.source_code = h5_dcp.h5_id
AND to_date1(main_table.dt, 'yyyymmdd') >= h5_dcp.first_time
AND to_date1(main_table.dt, 'yyyymmdd') < h5_dcp.last_time