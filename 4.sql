

--\c adv2024
-- create extension file_fdw;
-- create server adv2024 foreign data wrapper file_fdw;

drop foreign table if exists day4;
create foreign table day4 (ipt text)
  server adv2024 options(filename '/tmp/day4.txt', null '');

-- part 1

create table cells as (
    select
        x,
        row_number() over (partition by x) as y,
        cell
    from (
        select 
            row_number() over () as x,
            regexp_split_to_table(ipt, '') as cell
            from day4
    )
);

with
_x as (
    select * from cells where cell = 'X'
)
, _m as (
    select prev.x as prevx, prev.y as prevy, m.* 
    from cells m
    inner join _x prev on  abs(prev.x - m.x) <= 1 and  abs(prev.y - m.y) <= 1
    where m.cell = 'M'
    order by prev.x, prev.y
)
, _a as (
    select prev.x as prevx, prev.y as prevy, a.* 
    from cells a
    inner join _m prev on  (prev.x - a.x) = (prev.prevx - prev.x) and  (prev.y - a.y) = (prev.prevy - prev.y)
    where a.cell = 'A'
    order by prev.x, prev.y
)
, _s as (
    select prev.x as prevx, prev.y as prevy, s.* 
    from cells s
    inner join _a prev on  (prev.x - s.x) = (prev.prevx - prev.x) and  (prev.y - s.y) = (prev.prevy - prev.y)
    where s.cell = 'S'
    order by prev.x, prev.y
)
select count(*) from _s;

-- part 2

with
_a as (
    select * from cells where cell = 'A'
)
, _m as (
    select prev.x as ax, prev.y as ay, m.*
    from cells m
    inner join _a prev on  abs(prev.x - m.x) = 1 and  abs(prev.y - m.y) = 1
    where m.cell = 'M'
    order by prev.x, prev.y
)
, _s as (
    select prev.x as ax, prev.y as ay, m.*
    from cells m
    inner join _a prev on  abs(prev.x - m.x) = 1 and  abs(prev.y - m.y) = 1
    where m.cell = 'S'
    order by prev.x, prev.y
)
, _segments as (
    select * from _s
    union
    select * from _m
)
, _cross as (
    select ax, ay, string_agg(cell, '' order by x,y) as pattern from (
        select *
        , count(*) filter(where cell = 'M') over (partition by ax, ay) as cnt_m
        , count(*) filter(where cell = 'S') over (partition by ax, ay) as cnt_s
        from _segments
    )
    where cnt_m = cnt_s and cnt_m = 2
    group by ax, ay
)
select count(*) from _cross  where pattern not in ('SMMS', 'MSSM')

