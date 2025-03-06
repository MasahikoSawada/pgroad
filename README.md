# pgroad - ROAD (Read-Only Archived Data) Table Access Method

A table access method for Read-Only Archived Data (ROAD) for PostgreSQL that has the following characteristics:

- Read-only
- Data compression
- Portable (attaching and detaching without dumping/loading)

**NOTE: pgroad extension is under development and NOT productoin ready. Please do NOT use pgroad in your production system.**

## Overview

The pgroad extension provides the ROAD table access method that is a Table AM designed to compactly store inactive tables in a read-only format through data compression, converting them into read-only tables. Forthermore, the detach & attach freature makes it possible to temporarily detach a ROAD table from PostgreSQL's control without data dump and restore, and later re-attach it under the control of the same or a different PostgreSQL instance with the `road` extension installed.

## Creating a ROAD table

The functionality of ROAD table is highly restricted. For example, data manipulation through INSERT (or COPY FROM), UPDATE, DELETE etc. is not possible on ROAD tables. Also, creation of ROAD tables is limted to the following two methods:

### Method 1: Converting an exsiting table into ROAD

By using `ALTER TABLE ... SET ACCESS METHOD` command, introduced in PostgreSQL 15 or later, it is possible to convert an existing non-ROAD table into a ROAD table. This allows inactive tables to be stored as read-only.

```sql
=# CREATE TABLE tbl (id int, name text) USING heap;
CREATE TABLE
=# INSERT INTO tbl SELECT id, md5(id::text) FROM generate_series(1, 1_000_000) id; -- load data to tbl
INSERT 0 1000000
=# \dt+ tbl
                                  List of relations
 Schema | Name | Type  |  Owner   | Persistence | Access method | Size  | Description
--------+------+-------+----------+-------------+---------------+-------+-------------
 public | tbl  | table | masahiko | permanent   | heap          | 65 MB |
(1 row)
=# ALTER TABLE tbl SET ACCESS METHOD road;
ALTER TABLE
=# \dt+ tbl
                                  List of relations
 Schema | Name | Type  |  Owner   | Persistence | Access method | Size  | Description
--------+------+-------+----------+-------------+---------------+-------+-------------
 public | tbl  | table | masahiko | permanent   | road          | 41 MB |
(1 row)
```

### Method 2: Creating a ROAD table from a query result

By using `CREATE TABLE ... AS` command, it is possible to create a ROAD table from a query.

```sql
=# CREATE TABLE road_test USING road AS SELECT c.oid, n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid;
=# \dt+ road_test
                                     List of relations
 Schema |   Name    | Type  |  Owner   | Persistence | Access method | Size  | Description
--------+-----------+-------+----------+-------------+---------------+-------+-------------
 public | road_test | table | masahiko | permanent   | road          | 24 kB |
(1 row)
```

## Data compression

When creating a ROAD table, tuples are inserted into a memory page called ChunkPage. When a Chunk Page becomes full, the Chunk Page is compressed, and the compressed data is inserted into the ROAD table. Below is a comparison of the size of a HEAP table vs. a ROAD table for the pgbench data:

```
$ pgbench -i -s 100
dropping old tables...
creating tables...
generating data (client-side)...
vacuuming...
creating primary keys...
done in 10.43 s (drop tables 0.00 s, create tables 0.01 s, client-side generate 7.41 s, vacuum 0.16 s, primary keys 2.86 s).
$ psql
=# \dt+
                                         List of relations
 Schema |       Name       | Type  |  Owner   | Persistence | Access method |  Size   | Description
--------+------------------+-------+----------+-------------+---------------+---------+-------------
 public | pgbench_accounts | table | masahiko | permanent   | heap          | 1281 MB |
 public | pgbench_branches | table | masahiko | permanent   | heap          | 40 kB   |
 public | pgbench_history  | table | masahiko | permanent   | heap          | 0 bytes |
 public | pgbench_tellers  | table | masahiko | permanent   | heap          | 80 kB   |
(4 rows)
```

After running `pgbench -i` with scale-factor = 100, `pgbench_accounts` table is approximately 1.2GB in size. Let's convert it into a ROAD table:

```sql
=# ALTER TABLE pgbench_accounts SET ACCESS METHOD road;
ALTER TABLE
=# \dt+
                                         List of relations
 Schema |       Name       | Type  |  Owner   | Persistence | Access method |  Size   | Description
--------+------------------+-------+----------+-------------+---------------+---------+-------------
 public | __skiplist_16394 | table | masahiko | permanent   | heap          | 1768 kB |
 public | pgbench_accounts | table | masahiko | permanent   | road          | 86 MB   |
 public | pgbench_branches | table | masahiko | permanent   | heap          | 40 kB   |
 public | pgbench_history  | table | masahiko | permanent   | heap          | 0 bytes |
 public | pgbench_tellers  | table | masahiko | permanent   | heap          | 80 kB   |
(5 rows)
```

The `pgbench_accounts` table is compressed to 86MB from 1.2GB.

## Supported Features

- Seq scans
- Index scans
- Index build (btree and hash)
- Crash recovery
- TOAST
- Data compression by pglz and lz4
- Detach and attach table (WIP)

## Installation

### Building from source code

Clone the repository from github and build the source code with `USE_PGXS=1`:

```
$ git clone https://github.com/MasahikoSawada/pgroad.git
$ cd pgroad
$ make USE_PGXS=1
$ sudo make USE_PGXS=1
```

### Install pgroad extension to your PostgreSQL instance

ROAD extension requries to be load at server startup, so you need to set it to `shared_preload_libraries` GUC parameter and run `CREATE EXTENSION`:

```
$ vi $PGDATA/postgresql.conf
shared_preload_libraries = 'pgroad'
$ pg_ctl start
$ psql
=# CREATE EXTENSION pgroad;
CREATE EXTENSION
=# \dx pgroad
                   List of installed extensions
  Name  | Version | Schema |             Description
--------+---------+--------+--------------------------------------
 pgroad | 1.0     | public | Table AM for Read-Only Archived Data
(1 row)
```

## GUC parameters

### pgroad.compresion = {pglz|lz4}

This variable sets the compression method for chunk pages. The supported compression methods are `pglz` and `lz4` (if PostgreSQL was compiled with `--with-lz4`). The default value is `pglz`.

# TODO

- Detach and attach support
- More regression tests
- Use more compact tuple format for chunk page
- More scan support (e.g. bitmap scan, tid scan)
