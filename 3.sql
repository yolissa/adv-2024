

--\c adv2024
-- create extension file_fdw;
-- create server adv2024 foreign data wrapper file_fdw;

drop foreign table if exists day3;
create foreign table day3 (ipt text)
  server adv2024 options(filename '/tmp/day3.txt', null '');

-- part 1
select sum(mul[1]::bigint * mul[2]::bigint) from (
    select regexp_matches(ipt, 'mul\((\d+),(\d+)\)', 'g') as mul from day3
);

-- part 2

with items as (
    select trim(both ',' from mul) as mul, row_number() over () as rnum from (
        select unnest(regexp_matches(ipt, '(don''t\(\))|(do\(\))|(mul\(\d+,\d+\))', 'g')) as mul from day3
    )
    where mul is not null
)
, dos as (select * from items where mul = 'do()' union select 'do()' as mul, 0 as rnum)
, dont as (select * from items where mul = 'don''t()')
, instructions as (
    select *, true as enabled from dos
    union select *, false as enabled from dont
    order by rnum asc
)
, enables as (
    select items.mul, items.rnum, enabled
    from items,
    lateral (
        select enabled
        from instructions i
        where i.rnum <= items.rnum
        order by i.rnum desc limit 1
    ) inst
)
, multiplications as (
    select * from enables where enabled = true and mul not in ('do()', 'don''t')
)
select sum(arr[1]::bigint * arr[2]::bigint) from (
    select regexp_matches(mul, 'mul\((\d+),(\d+)\)', 'g') as arr from multiplications
);
