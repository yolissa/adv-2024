

--\c adv2024
-- create extension file_fdw;
-- create server adv2024 foreign data wrapper file_fdw;

-- drop foreign table if exists day2;
-- create foreign table day2 (ipt text)
--   server adv2024 options(filename '/tmp/day2.txt', null '');

-- part 1

with raw as (
    select rnum, cnt
    , count(*) filter(where diff > 0 and diff <= 3) over (partition by rnum) as cnt_pos
    , count(*) filter(where diff < 0 and diff >= -3) over (partition by rnum) as cnt_neg 
    from (
        select lvl
        , (lag(lvl) over(partition by rnum))::bigint - lvl::bigint as diff
        , rnum
        , count(*) over (partition by rnum) as cnt 
        from (
            select 
            regexp_split_to_table(ipt, '\s+') as lvl
            , row_number() over() as rnum
            from day2 
        )
    )
)
, agg as (
    select *
    from raw
    group by rnum, cnt, cnt_pos, cnt_neg order by rnum asc
)
select count(*) from agg where (cnt_pos = cnt - 1) or (cnt_neg = cnt - 1);


-- part 2

with main as (
    with raw as (
        select * from (
            select 
            ipt 
            , row_number() over() as rnum
            , regexp_count(ipt, '\s+') + 1 as cntbyset
            from day2 
        ) 
    )
    select string_agg(lvl, ' ' order by rnum asc) as ipt, it, rnum
    from (
        select * , row_number() over (partition by rnum, it)
            from (
            select regexp_split_to_table(ipt, '\s+') as lvl, it, rnum
            from raw
            , generate_series(0, cntbyset) as it
            group by rnum, ipt, it
            order by rnum, it asc
        )
    ) where it <> row_number
    group by rnum, it
    order by rnum, it asc
)
, raw as (
    select rnum, itnum, cnt
    , count(*) filter(where diff > 0 and diff <= 3) over (partition by rnum, itnum) as cnt_pos
    , count(*) filter(where diff < 0 and diff >= -3) over (partition by rnum, itnum) as cnt_neg 
    from (
        select lvl
        , (lag(lvl) over(partition by rnum, itnum))::bigint - lvl::bigint as diff
        , rnum
        , itnum
        , count(*) over (partition by rnum, itnum) as cnt 
        from (
            select 
            regexp_split_to_table(ipt, '\s+') as lvl
            , rnum
            , row_number() over(partition by rnum) as itnum
            from main 
        )
    )
)
, agg as (
    select *
    from raw
    group by rnum, itnum, cnt, cnt_pos, cnt_neg order by rnum asc, itnum asc
)
select count(*) from (
    select distinct rnum from (
        select 
        *
        ,  (cnt_pos = cnt - 1) or (cnt_neg = cnt - 1) as success
        from agg 
    ) where success = true
);