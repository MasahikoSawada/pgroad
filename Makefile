# pgroad Makefile

MODULE_big = pgroad
OBJS = \
	road_am.o \
	skiplist_table.o

PGFILEDESC = "pgroad -- Table AM for Read-Only Archived Data"

EXTENSION = pgroad
DATA = pgroad--1.0.sql

REGRESS = pgroad

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
