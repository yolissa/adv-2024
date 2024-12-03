create database if not exists adv2024;

--\c adv2024
create extension file_fdw;
create server adv2024 foreign data wrapper file_fdw;

drop foreign table if exists day1;
create foreign table day1 (ipt text)
  server adv2024 options(filename '/tmp/day1.txt', null '');


-- part 1
with
as_array as (
    select regexp_split_to_array(ipt, '\s+') arr , ipt from day1
)
, _left as (
    select num, row_number() over () as rnum from (
        select arr[1] as num from as_array order by 1 asc
    )
)
, _right as (
    select num, row_number() over () as rnum from (
        select arr[2] as num from as_array order by 1 asc
    )
)
select sum(abs(_left.num::bigint - _right.num::bigint)) 
from _left inner join _right on _left.rnum = _right.rnum;

-- part 2

with
as_array as (
    select regexp_split_to_array(ipt, '\s+') arr , ipt from day1
)
, _left as (
    select num, row_number() over () as rnum from (
        select arr[1] as num from as_array order by 1 asc
    )
)
, _right as (
    select num, row_number() over () as rnum from (
        select arr[2] as num from as_array order by 1 asc
    )
)
select sum(l::bigint * count) from (
    select l, rnum, count(*) from (
        select _left.num as l, _left.rnum,  _right.num as r 
        from _left inner join _right on _left.num = _right.num
    ) group by rnum, l
);