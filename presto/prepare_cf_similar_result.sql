delete from z_seanchuang.i2i_offline_item_topk_items where dt='${dt}';

insert into z_seanchuang.i2i_offline_item_topk_items
with user_weight as (
    select 
        ad_id, 
        1.0 / sqrt(count(*)) as user_weight
    from z_seanchuang.i2i_offline_train_raw
    where dt='${dt}'
    group by 1
),
user_item as (
    select 
        ad_id, 
        content_id
    from z_seanchuang.i2i_offline_train_raw 
    where dt='${dt}'
),
cooccurrence_table as (
    select 
        ad_id,
        user_weight,
        a1.content_id as item1,
        a2.content_id as item2
    from user_item a1
    join user_item a2 using(ad_id)
    left join user_weight using(ad_id)
),
item_cooccurrence as (
    select 
        item1 as item_a,
        item2 as item_b,
        sum(user_weight) as weight,
        count(*) as cnt
    from cooccurrence_table
    group by 1, 2
),
item_self_count as (
    select 
        item_a as item,
        cnt
    from item_cooccurrence
    where item_a = item_b
        and cnt > 3
),
item_item_similarity as (
    select 
        item_a,
        item_b,
        c.weight,
        s1.cnt as a_cnt,
        s2.cnt as b_cnt,
        c.weight / (s1.cnt * pow(s2.cnt, 0.1)) as score
    from item_cooccurrence c
    inner join item_self_count s1 on c.item_a = s1.item
    inner join item_self_count s2 on c.item_b = s2.item
)
select 
    item_a as item,
    slice(array_agg(concat(item_b, '=', cast(score AS VARCHAR)) order by score desc), 1, 20) as similar_item,
    '${dt}' as dt
from item_item_similarity
group by 1
;
