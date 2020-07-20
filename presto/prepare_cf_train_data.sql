# For app
delete from z_seanchuang.i2i_offline_train_raw where dt='${dt}';

insert into z_seanchuang.i2i_offline_train_raw
with data as ( 
    select 
        ad_id,
        replace(regexp_replace(content_id, '^([0-9]+):([0-9a-zA-Z\-_]+):([0-9]+)$', '$2:$3'), ' ') as content_id,
        behavior_types,
        ts,
        '${dt}' as dt 
    from (
        select 
            split(j(element_at(atp.data, 'partnerparameters')[1], '$.content_id'), ',') as content_ids, 
            element_at(atp.data, 'adid')[1] as ad_id,
            element_at(atp.data, 'event')[1] as behavior_types,
            cast(element_at(atp.data, 'createdat')[1] as bigint) as ts
        from hive_ad.default.ad_pixel atp
        where atp.dt > date_format(date_add('day', -10, date('${dt}')),'%Y-%m-%d')
            and atp.dt <= '${dt}'
            and element_at(atp.data, 'storeid')[1] in ('jp.co.rakuten.android', '419267350')
            and j(element_at(atp.data, 'partnerparameters')[1], '$.content_id') is not null
            and atp.log_type in ('adjust:callback:ViewContent',
                                'adjust:callback:AddToCart',
                                'adjust:callback:revenue')
    )
    cross join UNNEST(content_ids) as t(content_id)
)
select * from data     
where ad_id not in ('',
                   '00000000-0000-0000-0000-000000000000',
                   'b809a3a1-c846-41db-b0d4-8910a3fb21c0',
                   'DEFACE00-0000-0000-0000-000000000000')
;


# For web
delete from z_seanchuang.i2i_offline_train_raw where dt='${dt}';

insert into z_seanchuang.i2i_offline_train_raw
with data as ( 
    select 
        ad_id,
        replace(regexp_replace(content_id, '^([0-9]+):([0-9a-zA-Z\-_]+):([0-9]+)$', '$2:$3'), ' ') as content_id,
        behavior_types,
        ts,
        '${dt}' as dt 
    from (
        select 
            split(element_at(atp.data, 'content_ids')[1], ',') as content_ids, 
            element_at(atp.data, 'exid')[1] as ad_id,
            element_at(atp.data, 'e')[1] as behavior_types,
            ts
        from hive_ad.default.ad_pixel atp
        where atp.dt > date_format(date_add('day', -10, date('${dt}')),'%Y-%m-%d')
            and atp.dt <= '${dt}'
            and atp.log_type = 'pixel:event'
            and element_at(atp.data, 'url')[1] = 'https://grp15.ias.rakuten.co.jp/gw.js?v=2'
            and element_at(atp.data, 'content_ids')[1] is not null
    )
    cross join UNNEST(content_ids) as t(content_id)
)
select * from data     
where ad_id not in ('',
                   '00000000-0000-0000-0000-000000000000',
                   'b809a3a1-c846-41db-b0d4-8910a3fb21c0',
                   'DEFACE00-0000-0000-0000-000000000000')
;


-- select 
--     *
-- from hive_ad.default.ad_pixel atp
-- where atp.dt > date_format(date_add('day', -10, date('${dt}')),'%Y-%m-%d')
--     and atp.dt <= '${dt}'
--     and atp.log_type = 'pixel:event'
--     and element_at(atp.data, 'url')[1] = 'https://grp15.ias.rakuten.co.jp/gw.js?v=2'
--     and element_at(atp.data, 'content_ids')[1] is not null
