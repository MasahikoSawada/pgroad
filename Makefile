# road_am Makefile

MODULE_big = road_am
OBJS = \
	road_am.o

PGFILEDESC = "road_am -- Table AM for Read-Only Archived Data"

EXTENSION = road_am
DATA = road_am--1.0.sql

REGRESS = road_am

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
