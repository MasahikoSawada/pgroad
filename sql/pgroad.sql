create extension pgroad;

-- prepare a heap table.
create table tab1 (id int primary key, data text) using heap;
insert into tab1 select i, md5(i::text) from generate_series(1, 10000) i;

-- create a road table from tab1 (heap).
create table tab2 using road as select * from tab1;
create index on tab2 (id);

-- convert the heap table into a road table.
alter table tab1 set access method road;

-- create a road table from another road table.
create table tab3 using road as select * from tab1;
create index on tab3 (id, data);

select relname, amname from pg_class c join pg_am a on c.relam = a.oid where relname in ('tab1', 'tab2', 'tab3') order by 1;

-- SeqScan test. All queries must produce the same results.
set enable_seqscan to on;
set enable_indexscan to off;
explain select min(id), max(id), count(id) from tab1;
select min(id), max(id), count(id) from tab1;
select min(id), max(id), count(id) from tab2;
select min(id), max(id), count(id) from tab3;

-- IndexScan test. Again, all queries must produce the same
-- results.
set enable_seqscan to off;
set enable_indexscan to on;
explain select id from tab1 where id < 5 or id > 9995 order by 1;
select id from tab1 where id < 5 or id > 9995 order by 1;
select id from tab2 where id < 5 or id > 9995 order by 1;
select id from tab3 where id < 5 or id > 9995 order by 1;

